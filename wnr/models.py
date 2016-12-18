from collections import namedtuple

from google.appengine.api.datastore_errors import BadValueError
from google.appengine.ext import ndb

from .util import nub


PAGE_TYPE = namedtuple('QueuePageType',
    ('CATEGORY', 'ITEM')) \
    ("category", "item")


class Store(ndb.Model):
    """Just for key hierarchy."""
    pass


class ScrapeQueue(ndb.Model):
    modified = ndb.DateTimeProperty(auto_now=True)
    category_queue = ndb.TextProperty(repeated=True)
    item_queue = ndb.TextProperty(repeated=True)

    @classmethod
    @ndb.transactional
    def queue(cls, store, categories=None, items=None):
        if not (categories or items):
            return
        key = ndb.Key(Store, store, cls, 1)
        queue = key.get()
        if queue:
            if categories:
                queue.category_queue = nub(queue.category_queue + categories)
            if items:
                queue.item_queue = nub(queue.item_queue + items)
        else:
            queue = cls(key=key,
                        category_queue=categories or [],
                        item_queue=items or [])
        queue.put()

    @classmethod
    @ndb.transactional
    def peek(cls, store):
        queue = ndb.Key(Store, store, cls, 1).get()
        if not queue:
            return None, None
        # As items are commonly in multiple categories, prefer processing
        # categories first. But, this may lead to excessively long item
        # queue, for which reason priorize the item queue if the item queue
        # is sufficiently long.
        if queue.category_queue and len(queue.item_queue) < 1000:
            return queue.category_queue[0], PAGE_TYPE.CATEGORY
        if queue.item_queue:
            return queue.item_queue[0], PAGE_TYPE.ITEM
        return None, None

    @classmethod
    @ndb.transactional
    def pop(cls, store, url):
        queue = ndb.Key(Store, store, cls, 1).get()
        if not queue:
            return
        def ne(_url):
            return _url != url
        mod = False
        if url in queue.category_queue:
            queue.category_queue = filter(ne, queue.category_queue)
            mod = True
        if url in queue.item_queue:
            queue.item_queue = filter(ne, queue.item_queue)
            mod = True
        if mod:
            if queue.category_queue or queue.item_queue:
                queue.put()
            else:
                queue.key.delete()


class Category(ndb.Model):
    store = ndb.StringProperty(required=True)
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
    custom = ndb.JsonProperty()
    removed = ndb.DateTimeProperty()


def check_currency(prop, cur):
    if not isinstance(cur, basestring) \
       and len(cur) == 3:
        raise BadValueError("Invalid currency: %r" % (cur,))


class Price(ndb.Model):
    timestamp = ndb.DateTimeProperty(auto_now_add=True)
    cents = ndb.IntegerProperty(required=True)
    currency = ndb.StringProperty(required=True, validator=check_currency)
