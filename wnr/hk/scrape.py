from decimal import Decimal
import logging
import re

from google.appengine.api import urlfetch
from google.appengine.ext import deferred, ndb

from HTMLParser import HTMLParser
import webapp2

from ..models import Category, Item, PAGE_TYPE, Price, ScrapeQueue, Store
from ..search import index_items
from ..util import cacheize, get, INVALIDATE_CACHE, ok_resp


_store = 'hk'

href = re.compile(r'href="(.+?)"')
itemprop = re.compile(r'itemprop="(.+?)" content="(.+?)"')
ogprop = re.compile(r'property="og:(.+?)" content="(.+?)"')


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
    queue = [ScrapeQueue(id=url, store=_store, type=PAGE_TYPE.CATEGORY)
             for url, rpc in rpcs
             if not rpc.get_result()]

    if queue:
        logging.debug("Queuing %d categories" % len(queue))
        ndb.put_multi(queue)

    deferred.defer(process_queue, _queue='scrape', _countdown=5)


def process_queue():
    url = ScrapeQueue.query(ScrapeQueue.store == _store) \
                     .order(ScrapeQueue.queued) \
                     .get()
    if not url:
        return

    logging.info("Scraping %r" % url.key)
    rs = ok_resp(urlfetch.fetch(url.key.id(), follow_redirects=False, deadline=60))
    if url.type == PAGE_TYPE.CATEGORY:
        scrape_category(rs.content)
    else:
        assert url.type == PAGE_TYPE.ITEM
        scrape_item(rs.content)

    url.key.delete()
    deferred.defer(process_queue, _queue='scrape', _countdown=5)


def scrape_category(html):
    items = html.split('id="list-item-')[1:]
    urls = [href.search(item).group(1) for item in items]
    logging.info("Found %d items" % len(urls))
    rpcs = [(url, ndb.Key(ScrapeQueue, url).get_async())
            for url in urls]
    queue = [ScrapeQueue(id=url, store=_store, type=PAGE_TYPE.ITEM)
             for url, rpc in rpcs
             if not rpc.get_result()]
    if queue:
        logging.debug("Queuing %d items" % len(queue))
        ndb.put_multi(queue)

    npage = re.search(r'href="([^"]+)" title="Next"', html)
    if npage:
        npage = npage.group(1)
        if not ndb.Key(ScrapeQueue, npage).get():
            k = ScrapeQueue(id=npage, store=_store, type=PAGE_TYPE.CATEGORY).put()
            logging.debug("Queued next page %r" % k)


@cacheize(10 * 60)
def category_by_title(title):
    return Category.query(Category.title == title,
                          ancestor=ndb.Key(Store, _store)) \
                   .get()


def save_cats(data):
    store = ndb.Key(Store, _store)
    ckeys = []
    for url, title in data:
        cat = category_by_title(title)
        if not cat:
            cat = Category(parent=store, title=title, url=url)
            cat.put()
            category_by_title(INVALIDATE_CACHE)
        else:
            if ckeys:
                parent = ckeys[-1]
            else:
                # don't remove earlier parent
                parent = cat.parent_cat
            if (cat.title, cat.url, cat.parent_cat) != (title, url, parent):
                cat.populate(title=title, url=url, parent_cat=parent)
                cat.put()
                category_by_title(INVALIDATE_CACHE)
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

    key = ndb.Key(Store, _store, Item, sku)
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


def proxy(rq):
    rs = urlfetch.fetch(rq.GET['url'], deadline=60)
    return webapp2.Response(rs.content)


routes = [
    get(r"/proxy\.html", proxy),
    get(r"/scrape", trigger),
]
