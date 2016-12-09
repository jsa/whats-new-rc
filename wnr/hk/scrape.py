from collections import namedtuple
import logging
import re

from google.appengine.api import urlfetch
from google.appengine.ext import deferred, ndb

import webapp2

from ..util import get, ok_resp


PAGE_TYPE = namedtuple('HKPageType',
    ('CATEGORY', 'ITEM')) \
    ("category", "item")


class HKUrl(ndb.Model):
    queued = ndb.DateTimeProperty(auto_now_add=True)
    type = ndb.StringProperty(required=True, choices=PAGE_TYPE)


def trigger(rq):
    deferred.defer(queue_categories, _queue="scrape")
    return webapp2.Response()


def queue_categories():
    rs = urlfetch.fetch("https://hobbyking.com/en_us",
                        deadline=60)
    ok_resp(rs)
    nav = rs.content.split('id="nav"', 1)[1] \
                    .split("</nav>", 1)[0]
    urls = re.findall(r'href="([^"]+)"', nav)
    assert len(urls) > 100, "Found only %d URLs" % len(urls)
    logging.debug("Found %d URLs" % len(urls))
    rpcs = [(url, ndb.Key(HKUrl, url).get_async())
            for url in urls]
    queue = [HKUrl(id=url, type=PAGE_TYPE.CATEGORY)
             for url, rpc in rpcs
             if not rpc.get_result()]
    if queue:
        logging.debug("Queuing %d URLs" % len(queue))
        ndb.put_multi(queue)


routes = [
    get("/trigger/", trigger),
]
