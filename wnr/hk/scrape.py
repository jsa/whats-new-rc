import logging
import re

from google.appengine.api import urlfetch
from google.appengine.ext import deferred, ndb

import webapp2

from ..models import PAGE_TYPE, ScrapeQueue
from ..util import get, ok_resp


_module = 'hk'

href = re.compile(r'href="([^"]+)"')


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
    raise NotImplementedError


routes = [
    get("/trigger/", trigger),
]
