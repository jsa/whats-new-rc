from decimal import Decimal
import logging
import re

from google.appengine.api import urlfetch
from google.appengine.ext import deferred, ndb

import webapp2

from ..models import Item, PAGE_TYPE, Price, ScrapeQueue, Store
from ..util import get, ok_resp


_module = 'hk'

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
    rs = ok_resp(urlfetch.fetch(url.key.id(), deadline=60))

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


def scrape_item(html):
    props = dict(itemprop.findall(html))
    og = dict(ogprop.findall(html))

    logging.debug("itemprop: %r\nog:%r" % (props, og))

    sku = props.get('sku')
    assert sku, "Couldn't find SKU"

    # not available
    #cur = props['priceCurrency']
    #cents = int(Decimal(props['price']) * 100)

    g_params = re.search(r'google_tag_params = *{(.*?)}', html, re.DOTALL)
    assert g_params
    usd = re.search(r"value: '(.+?)'", g_params.group(1)).group(1)
    cur = 'USD'
    cents = int(Decimal(usd) * 100)
    assert cur and cents > 0

    image, title, typ, url = map(og.get, ('image', 'title', 'type', 'url'))
    assert typ == "product", "Unexpected type %r" % typ
    assert all((image, title, url))

    key = ndb.Key(Store, _module, Item, sku)
    item = key.get()
    if item:
        item.populate(url=url, title=title, image=image)
        puts = [item]
        price = Price.query(ancestor=item.key) \
                     .order('-timestamp') \
                     .get()
        if (price.currency, price.cents) != (cur, cents):
            puts.append(Price(parent=item.key, cents=cents, currency=cur))
        ndb.put_multi(puts)
        logging.debug("Updated %r" % [e.key for e in puts])
    else:
        item = Item(key=key, url=url, title=title, image=image)
        price = Price(parent=item.key, cents=cents, currency=cur)
        ndb.put_multi([item, price])
        logging.debug("Added item %s / %r" % (item.key.id(), price.key))


def proxy(rq):
    rs = urlfetch.fetch(rq.GET['url'], deadline=60)
    return webapp2.Response(rs.content)


routes = [
    get("/proxy.html", proxy),
    get("/trigger/", trigger),
]
