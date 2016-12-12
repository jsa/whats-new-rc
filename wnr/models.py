from collections import namedtuple

from google.appengine.ext import ndb


PAGE_TYPE = namedtuple('HKPageType',
    ('CATEGORY', 'ITEM')) \
    ("category", "item")


class ScrapeQueue(ndb.Model):
    module = ndb.StringProperty(required=True)
    queued = ndb.DateTimeProperty(auto_now_add=True)
    type = ndb.StringProperty(required=True, choices=PAGE_TYPE)


class Store(ndb.Model):
    """Just for key hierarchy."""
    pass


class Category(ndb.Model):
    added = ndb.DateTimeProperty(auto_now_add=True)
    title = ndb.StringProperty(required=True)
    url = ndb.StringProperty(required=True)
    parent_cat = ndb.KeyProperty()


class Item(ndb.Model):
    added = ndb.DateTimeProperty(auto_now_add=True)
    checked = ndb.DateTimeProperty(auto_now=True)
    url = ndb.StringProperty(required=True)
    title = ndb.StringProperty(required=True)
    image = ndb.StringProperty(required=True)
    category = ndb.KeyProperty(kind=Category)
    removed = ndb.DateTimeProperty()


class Price(ndb.Model):
    timestamp = ndb.DateTimeProperty(auto_now_add=True)
    cents = ndb.IntegerProperty(required=True)
    currency = ndb.StringProperty(required=True)
