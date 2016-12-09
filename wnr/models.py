from collections import namedtuple

from google.appengine.ext import ndb


PAGE_TYPE = namedtuple('HKPageType',
    ('CATEGORY', 'ITEM')) \
    ("category", "item")


class ScrapeQueue(ndb.Model):
    queued = ndb.DateTimeProperty(auto_now_add=True)
    module = ndb.StringProperty(required=True)
    type = ndb.StringProperty(required=True, choices=PAGE_TYPE)
