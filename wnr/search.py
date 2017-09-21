from datetime import datetime
import decimal
import json
import logging
import time
from urllib import urlencode

from google.appengine.api import search, urlfetch
from google.appengine.ext import deferred, ndb
from google.appengine.ext.ndb import query

from .models import Category, Item, Price
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
    from .views import get_categories

    # de-duplicate
    item_keys = sorted(set(item_keys))

    categories = get_categories()

    def cat_path(item_key, cat_key):
        path = []
        try:
            store, title, parent_id = categories[cat_key.id()]
        except KeyError:
            raise KeyError("Category not found, %r: %r %r"
                           % (item_key, cat_key, path))
        while cat_key:
            path.insert(0, cat_key)
            cat_key = None
            if parent_id:
                parent = ndb.Key(Category, parent_id)
                if parent not in path:
                    cat_key = parent
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

        if item.custom:
            custom = set()
            for val in item.custom.itervalues():
                if isinstance(val, basestring):
                    custom.add(val)
                elif isinstance(val, (int, long)):
                    custom.add(str(val))
            if custom:
                fields.append(search.TextField('custom', " ".join(sorted(custom))))

        facets = []
        if item.category:
            id_path = ["%d" % ck.id()
                       for ck in cat_path(item.key, item.category)]
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

        tags = []
        if item.removed:
            fields.append(search.NumberField('removed', to_unix(item.removed)))
            tags.append('removed')
        else:
            # NOT queries are expensive, thus providing information in both forms
            tags.append('active')

        fields += [search.AtomField('tags', "#%s" % t) for t in tags]

        return fields, facets

    adds, dels = [], []
    for item_key, item in zip(item_keys, ndb.get_multi(item_keys)):
        iid = item_key.string_id()
        if not iid:
            # ignore, not indexed
            continue
        doc_id = "%s:%s" % (item_key.parent().id(),
                            iid.replace(" ", "-"))
        if item:
            fields, facets = item_data(item)
            adds.append(search.Document(
                doc_id=doc_id,
                fields=fields,
                facets=facets,
                language='en',
                # global sort by latest-ness
                rank=to_unix(item.added)))
        else:
            dels.append(doc_id)

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


def delete_items(item_keys):
    assert all(k.integer_id() for k in item_keys)

    @ndb.transactional
    def del_item(ikey):
        if not ikey.get():
            # already deleted
            return
        prices = Price.query(ancestor=ikey) \
                      .fetch(query._MAX_LIMIT, keys_only=True)
        ndb.delete_multi([ikey] + prices)
        logging.debug("Deleted %r" % ikey)

    for ikey in item_keys:
        del_item(ikey)
