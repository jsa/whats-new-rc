# -*- coding: utf-8 -*-
from datetime import datetime
import decimal
import json
import logging
import time
from urllib import urlencode

from google.appengine.api import search, urlfetch
from google.appengine.ext import deferred, ndb

from .models import Item, Price
from .util import cacheize, ok_resp


ITEMS_INDEX = 'items-20161212'

# 2016-12-14T10:27:40.492650
_iso_format = "%Y-%m-%dT%H:%M:%S.%f"


def to_unix(dt):
    return int(time.mktime(dt.timetuple()))


def from_unix(seconds):
    return datetime.utcfromtimestamp(seconds)


@cacheize(60 * 60)
def us_exchange_rate(currency):
    if currency == 'USD':
        return decimal.Decimal(1)

    query = {
        'base': currency,
        'symbols': "USD",
    }
    rs = urlfetch.fetch("http://api.fixer.io/latest?%s"
                        % urlencode(query))
    if rs.status_code == 422:
        raise ValueError("Unknown currency %r" % (currency,))
    ok_resp(rs)
    logging.debug("Response: %s" % rs.content)
    # sample response: {"base":"EUR","date":"2016-12-12","rates":{"USD":1.0596}}
    # (stupid float, but fortunately just informative...)
    rs = json.loads(rs.content)
    assert rs['base'] == currency
    return decimal.Decimal(rs['rates']['USD'])


def to_us_cents(price):
    us_cents = us_exchange_rate(price.currency) * price.cents
    return int(us_cents.quantize(1, decimal.ROUND_HALF_UP))


def format_history_price(price):
    return "%s:%s%s" % (price.timestamp.isoformat(),
                        price.currency,
                        decimal.Decimal(price.cents) / 100)


def parse_history_price(price):
    timestamp, price = price.rsplit(":", 1)
    cur, amt = price[:3], price[3:]
    return (datetime.strptime(timestamp, _iso_format),
            cur,
            decimal.Decimal(amt))


def index_items(item_keys):
    # de-duplicate
    item_keys = sorted(set(item_keys))

    def cat_path(cat_key):
        path, cat = [], cat_key.get()
        while cat:
            path.insert(0, cat.key)
            if cat.parent_cat and cat.parent_cat not in path:
                cat = cat.parent_cat.get()
            else:
                break
        return path

    def item_data(item):
        fields = [search.AtomField('store', item.key.parent().id()),
                  search.AtomField('sku', item.key.id()),
                  search.TextField('title', item.title),
                  # "Unsupported field type TOKENIZED_PREFIX"
                  # search.TokenizedPrefixField('title_prefix', item.title),
                  search.AtomField('url', item.url),
                  search.AtomField('image', item.image),
                  # DateField supports only date accuracy (ie. not second)
                  search.NumberField('added', to_unix(item.added)),
                  search.NumberField('checked', to_unix(item.checked))]

        tags, facets = [], []

        if item.category:
            id_path = ["%d" % ck.id() for ck in cat_path(item.category)]
            if id_path:
                fields.append(search.TextField('categories', " ".join(id_path)))
                # NumberFacet is 30 bit
                facets += [search.AtomFacet('category', cat_id)
                           for cat_id in id_path]

        prices = Price.query(ancestor=item.key) \
                      .order(-Price.timestamp) \
                      .fetch()
        if prices:
            fields.append(search.NumberField('us_cents', to_us_cents(prices[0])))
            prices = map(format_history_price, prices)
            fields.append(search.TextField('price_history', " ".join(prices)))

        # NOT queries are expensive, thus providing information in both forms
        tags.append('removed' if item.removed else 'active')

        fields += [search.AtomField('tags', "tag:%s" % t) for t in tags]

        return fields, facets

    adds, dels = [], []
    for item_key, item in zip(item_keys, ndb.get_multi(item_keys)):
        doc_id = "%s:%s" % (item_key.parent().id(),
                            item_key.id().replace(" ", "-"))
        if not item or item.removed:
            dels.append(doc_id)
        else:
            fields, facets = item_data(item)
            adds.append(search.Document(
                doc_id=doc_id,
                fields=fields,
                facets=facets,
                language='en',
                # global sort by latest-ness
                rank=to_unix(item.added)))

    index = search.Index(ITEMS_INDEX)
    if adds:
        logging.debug("Indexing %d documents:" % len(adds))
        for n, doc in enumerate(adds, start=1):
            logging.debug("%d: %s" % (n, doc))
        index.put(adds)
    if dels:
        logging.debug("Deleting %d documents: %s" % (len(dels), dels))
        index.delete(dels)


def reindex_items(cursor=None):
    start = time.time()
    while True:
        keys, cursor, more = \
            Item.query().fetch_page(page_size=50,
                                    keys_only=True,
                                    start_cursor=cursor)
        if not keys:
            break
        index_items(keys)
        if not (cursor and more):
            break
        if time.time() - start > 30:
            deferred.defer(reindex_items,
                           cursor=cursor,
                           _queue='indexing')
            return
    logging.info("All items reindexed")
