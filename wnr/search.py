import decimal
import json
import logging
from urllib import urlencode

from google.appengine.api import search, urlfetch
from google.appengine.ext import ndb

from .models import Price
from .util import cacheize, ok_resp


ITEMS_INDEX = 'items-20161212'


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


def format_price(price):
    return "%s:%s%s" % (price.timestamp.isoformat(),
                        price.currency,
                        decimal.Decimal(price.cents) / 100)


def index_items(item_keys):
    # de-duplicate
    item_keys = sorted(set(item_keys))

    def cat_path(cat_key):
        cat = cat_key.get()
        if cat:
            if cat.parent_cat:
                return cat_path(cat.parent_cat) + [cat.key]
            else:
                return [cat.key]
        else:
            return []

    def item_fields(item):
        fields = [search.AtomField('store', item.key.parent().id()),
                  search.AtomField('sku', item.key.id()),
                  search.TextField('title', item.title),
                  search.AtomField('url', item.url),
                  search.AtomField('image', item.image),
                  search.DateField('added', item.added),
                  search.DateField('checked', item.checked)]

        if item.category:
            id_path = ["%d" % ck.id() for ck in cat_path(item.category)]
            if id_path:
                fields.append(search.TextField('categories', " ".join(id_path)))

        prices = Price.query(ancestor=item.key) \
                      .order(-Price.timestamp) \
                      .fetch()
        if prices:
            fields.append(search.NumberField('us_cents', to_us_cents(prices[0])))
            prices = map(format_price, prices)
            fields.append(search.TextField('price_history', " ".join(prices)))

        return fields

    adds, dels = [], []
    for item_key, item in zip(item_keys, ndb.get_multi(item_keys)):
        doc_id = "%s:%s" % (item_key.parent().id(),
                            item_key.id().replace(" ", "-"))
        if not item or item.removed:
            dels.append(doc_id)
        else:
            adds.append(search.Document(
                doc_id=doc_id,
                fields=item_fields(item),
                language='en',
                # no global ordering
                rank=(2**31) / 2))

    index = search.Index(ITEMS_INDEX)
    if adds:
        logging.debug("Indexing %d documents:" % len(adds))
        for n, doc in enumerate(adds, start=1):
            logging.debug("%d: %s" % (n, doc))
        index.put(adds)
    if dels:
        logging.debug("Deleting %d documents: %s" % (len(dels), dels))
        index.delete(dels)
