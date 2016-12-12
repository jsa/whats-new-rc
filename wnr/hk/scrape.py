from datetime import datetime
from decimal import Decimal
import logging
import re

from google.appengine.api import search, urlfetch
from google.appengine.ext import deferred, ndb

from HTMLParser import HTMLParser
import webapp2

from ..models import Category, Item, PAGE_TYPE, Price, ScrapeQueue, Store
from ..util import get, ok_resp


_module = 'hk'

href = re.compile(r'href="(.+?)"')
itemprop = re.compile(r'itemprop="(.+?)" content="(.+?)"')
ogprop = re.compile(r'property="og:(.+?)" content="(.+?)"')

ITEMS_INDEX = 'items-20161212'


def trigger(rq):
    deferred.defer(queue_categories, _queue='scrape')
    return webapp2.Response()


def queue_categories():
    rs = ok_resp(urlfetch.fetch("https://hobbyking.com/en_us",
                                deadline=60))
    nav = rs.content.split('id="nav"', 1)[1] \
                    .split("</nav>", 1)[0]
    urls = href.findall(nav)
    assert len(urls) > 100, "Found only %d category URLs" % len(urls)
    logging.debug("Found %d categories" % len(urls))
    rpcs = [(url, ndb.Key(ScrapeQueue, url).get_async())
            for url in urls]
    queue = [ScrapeQueue(id=url, module=_module, type=PAGE_TYPE.CATEGORY)
             for url, rpc in rpcs
             if not rpc.get_result()]

    if queue:
        logging.debug("Queuing %d categories" % len(queue))
        ndb.put_multi(queue)

    deferred.defer(process_queue, _queue='scrape', _countdown=5)


def process_queue():
    url = ScrapeQueue.query(ScrapeQueue.module == _module) \
                     .order(ScrapeQueue.queued) \
                     .get()
    if not url:
        return

    logging.info("Scraping %r" % url.key)
    rs = urlfetch.fetch(url.key.id(), follow_redirects=False, deadline=60)
    if rs.status_code != 200:
        logging.warn("%d response for %s" % (rs.status_code, url.key.id()))
    if url.type == PAGE_TYPE.CATEGORY:
        if rs.status_code == 200:
            scrape_category(rs.content)
    else:
        assert url.type == PAGE_TYPE.ITEM
        if rs.status_code == 200:
            scrape_item(rs.content)
        elif rs.status_code == 404:
            items = Item.query(Item.url == url.key.id()).fetch()
            now, mod = datetime.utcnow(), []
            for item in items:
                if not item.removed:
                    item.removed = now
                    mod.append(item)
            if mod:
                ndb.put_multi(mod)

    url.key.delete()
    deferred.defer(process_queue, _queue='scrape', _countdown=5)


def scrape_category(html):
    items = html.split('id="list-item-')[1:]
    urls = [href.search(item).group(1) for item in items]
    logging.info("Found %d items" % len(urls))
    rpcs = [(url, ndb.Key(ScrapeQueue, url).get_async())
            for url in urls]
    queue = [ScrapeQueue(id=url, module=_module, type=PAGE_TYPE.ITEM)
             for url, rpc in rpcs
             if not rpc.get_result()]
    if queue:
        logging.debug("Queuing %d items" % len(queue))
        ndb.put_multi(queue)

    npage = re.search(r'href="([^"]+)" title="Next"', html)
    if npage:
        npage = npage.group(1)
        if not ndb.Key(ScrapeQueue, npage).get():
            k = ScrapeQueue(id=npage, module=_module, type=PAGE_TYPE.CATEGORY).put()
            logging.debug("Queued next page %r" % k)


def save_cats(data):
    store = ndb.Key(Store, _module)
    ckeys = []
    for url, title in data:
        cat = Category.query(Category.title == title,
                             ancestor=store) \
                      .get()
        if cat:
            cat.populate(title=title, url=url)
        else:
            cat = Category(parent=store, title=title, url=url)
        if ckeys:
            cat.parent_cat = ckeys[-1]
        cat.put()
        ckeys.append(cat.key)
    return ckeys


def scrape_item(html):
    h = HTMLParser()

    props = dict(itemprop.findall(html))
    og = dict(ogprop.findall(html))

    logging.debug("itemprop: %r\nog:%r" % (props, og))

    sku = props.get('sku')
    assert sku, "Couldn't find SKU"

    cur = props.get('priceCurrency')
    if cur:
        cents = int(Decimal(props['price']) * 100)
    else:
        g_params = re.search(r'google_tag_params = *{(.*?)}', html, re.DOTALL)
        assert g_params
        usd = re.search(r"value: '(.+?)'", g_params.group(1)).group(1)
        cur = 'USD'
        cents = int(Decimal(usd) * 100)

    assert cur and cents > 0

    image, title, typ, url = map(og.get, ('image', 'title', 'type', 'url'))
    assert typ == "product", "Unexpected type %r" % typ
    assert all((image, title, url))

    cat_html = html.split('class="breadcrumbsPos"', 1)[1] \
                   .rsplit('class="breadcrumbsPos"', 1)[0]
    cat_el = re.compile(r'<a href="(.+?)".*?><.+?>(.+?)</')
    cats = [(url, h.unescape(name))
            for url, name in cat_el.findall(cat_html)]
    assert cats
    logging.debug("Parsed categories:\n%s"
                  % "\n".join("%s (%s)" % (name, url)
                              for url, name in cats))
    cat_keys = save_cats(cats)

    fields = {'url': url,
              'title': title,
              'image': image,
              'category': cat_keys[-1],
              'removed': None}

    key = ndb.Key(Store, _module, Item, sku)
    item = key.get()
    if item:
        item.populate(**fields)
        puts = [item]
        price = Price.query(ancestor=item.key) \
                     .order(-Price.timestamp) \
                     .get()
        if (price.currency, price.cents) != (cur, cents):
            puts.append(Price(parent=item.key, cents=cents, currency=cur))
        ndb.put_multi(puts)
        logging.debug("Updated %r" % [e.key for e in puts])
    else:
        item = Item(key=key, **fields)
        price = Price(parent=item.key, cents=cents, currency=cur)
        ndb.put_multi([item, price])
        logging.debug("Added item %s / %r" % (item.key.id(), price.key))

    deferred.defer(index_items,
                   [item.key],
                   _queue='indexing',
                   _countdown=2)


def index_items(item_keys):
    # de-duplicate
    item_keys = sorted(set(item_keys))

    def cat_path(cat_key):
        cat = cat_key.get()
        if cat and cat.parent_cat:
            return cat_path(cat.parent_cat) + [cat.key]
        elif cat:
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

        return fields

    adds, dels = [], []
    for item_key, item in zip(item_keys, ndb.get_multi(item_keys)):
        doc_id = "%s:%s" % (item_key.parent().id(), item_key.id())
        if not item or item.removed:
            dels.append(doc_id)
        else:
            adds.append(search.Document(
                doc_id=doc_id,
                fields=item_fields(item),
                language='en',
                # no global ordering
                rank=(2**31) / 2))

    logging.info("Indexing %d and removing %d documents"
                 % (len(adds), len(dels)))

    index = search.Index(ITEMS_INDEX)
    if adds:
        index.put(adds)
    if dels:
        index.delete(dels)


def proxy(rq):
    rs = urlfetch.fetch(rq.GET['url'], deadline=60)
    return webapp2.Response(rs.content)


routes = [
    get("/proxy.html", proxy),
    get("/trigger/", trigger),
]
