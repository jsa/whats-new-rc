from collections import namedtuple
from Cookie import SimpleCookie
import logging
from random import randint

from google.appengine.api.datastore_errors import BadValueError
from google.appengine.ext import ndb

from pyblooming.bitmap import Bitmap
from pyblooming.bloom import BloomFilter


"""
# test snippet for bloomfilter

from pyblooming.bloom import BloomFilter
from pyblooming.bitmap import Bitmap

b = BloomFilter.for_capacity(100000, .01)
for x in range(20000):
  x = b.add(str(x))

for x in range(20000):
  if str(x) not in b:
    print x

for x in range(20000, 40000):
  if str(x) in b:
    print x

bloom_data = b.bitmap.mmap
print len(bloom_data)

bytes, ideal_k = BloomFilter.params_for_capacity(100000, .01)
bitmap = Bitmap(bytes)
bitmap.mmap = bloom_data
b = BloomFilter(bitmap, ideal_k)

for x in range(20000):
  if str(x) not in b:
    print x

for x in range(20000, 40000):
  if str(x) in b:
    print x
"""


PAGE_TYPE = namedtuple('QueuePageType',
    ('CATEGORY', 'ITEM')) \
    ("category", "item")


class Store(ndb.Model):
    """Just for key hierarchy."""
    pass


def filter_urls(urls):
    def check_url(url):
        if url == "#":
            return False
        if not any(url.startswith(proto)
                   for proto in ("http://", "https://")):
            raise ValueError("Invalid URL: '%s'" % url)
        return True
    return filter(check_url, urls)


class ScrapeQueue(ndb.Model):
    modified = ndb.DateTimeProperty(auto_now=True)
    category_queue = ndb.TextProperty(repeated=True)
    item_queue = ndb.TextProperty(repeated=True)
    # separating item and category blooms as there have been
    # some item URL mixups
    bloom_categories = ndb.BlobProperty(compressed=True)
    bloom_items = ndb.BlobProperty(compressed=True)
    bloom_salt = ndb.IntegerProperty(required=True)
    cookies = ndb.JsonProperty()

    @classmethod
    def initialize(cls, store_id, skip_indexed=True):
        """Returns True if a new crawl was initialized, and False if
        an earlier crawl was already in progress.
        """
        key = ndb.Key(cls, store_id)
        salt = randint(1, 100000)
        if skip_indexed:
            # using a throwaway entity
            bf_items = cls(key=key, bloom_salt=salt).scan_indexed()
        else:
            bf_items = None

        @ndb.transactional
        def tx():
            if key.get():
                return False
            queue = cls(key=key, bloom_salt=salt)
            if bf_items:
                queue.bloom_items = bf_items.bitmap.mmap
            queue.put()
            return True
        return tx()

    @classmethod
    @ndb.transactional
    def queue(cls, store_id, categories=None, items=None):
        if not (categories or items):
            return

        key = ndb.Key(cls, store_id)
        queue = key.get()
        assert queue, "No crawl in progress"

        def unseen(bf_data):
            bf = cls.get_bloom(bf_data)
            def inner(url):
                if queue.salt_url(url) in bf:
                    logging.info("%r: skipping already seen URL %s" % (key, url))
                    return False
                else:
                    return True
            return inner

        if categories:
            categories = filter_urls(categories)
            categories = filter(lambda url: url not in queue.category_queue,
                                categories)
            categories = filter(unseen(queue.bloom_categories), categories)
            queue.category_queue += categories

        if items:
            items = filter_urls(items)
            items = filter(lambda url: url not in queue.item_queue,
                           items)
            items = filter(unseen(queue.bloom_items), items)
            queue.item_queue += items

        queue.put()

    @classmethod
    @ndb.transactional
    def peek(cls, store_id):
        queue = ndb.Key(cls, store_id).get()
        if queue:
            if queue.item_queue:
                return (queue.item_queue[0],
                        PAGE_TYPE.ITEM,
                        queue.get_cookies())
            if queue.category_queue:
                return (queue.category_queue[0],
                        PAGE_TYPE.CATEGORY,
                        queue.get_cookies())
        return None, None, None

    @classmethod
    @ndb.transactional
    def save_cookies(cls, store_id, cookies):
        queue = ndb.Key(cls, store_id).get()
        if queue:
            queue.cookies = {cookie.key: cookie.value
                             for cookie in cookies.itervalues()}
            logging.debug("%r: updated cookies: %s"
                          % (queue.key, queue.cookies))
            queue.put()

    @classmethod
    @ndb.transactional
    def pop(cls, store_id, url):
        queue = ndb.Key(cls, store_id).get()
        if not queue:
            return
        def ne(_url):
            return _url != url
        mod = False
        if url in queue.category_queue:
            queue.category_queue = filter(ne, queue.category_queue)
            bf = queue.get_bloom(queue.bloom_categories)
            bf.add(queue.salt_url(url))
            queue.bloom_categories = bf.bitmap.mmap
            mod = True
        if url in queue.item_queue:
            queue.item_queue = filter(ne, queue.item_queue)
            bf = queue.get_bloom(queue.bloom_items)
            bf.add(queue.salt_url(url))
            queue.bloom_items = bf.bitmap.mmap
            mod = True
        if not mod:
            return
        if queue.category_queue or queue.item_queue:
            queue.put()
        else:
            queue.key.delete()

    @classmethod
    def get_bloom(cls, bloom_data):
        bloom_args = (100000, .01)
        if bloom_data:
            size, ideal_k = BloomFilter.params_for_capacity(*bloom_args)
            bitmap = Bitmap(size)
            bitmap.mmap = bloom_data
            return BloomFilter(bitmap, ideal_k)
        else:
            bf = BloomFilter.for_capacity(*bloom_args)
            logging.debug("get_bloom(): data size %dkB"
                          % round(len(bf.bitmap.mmap) / 1024))
            return bf

    def get_cookies(self):
        if self.cookies:
            logging.debug("%r: loaded cookies: %s" % (self.key, self.cookies))
            return SimpleCookie({str(k): v.encode('utf-8')
                                 for k, v in self.cookies.iteritems()})
        else:
            return SimpleCookie()

    def scan_indexed(self):
        store_key = ndb.Key(Store, self.key.id())
        query = Item.query(ancestor=store_key) \
                    .filter(Item.removed == None) \
                    .order(Item.url)
        indexed = self.get_bloom(None)
        batch = None
        while True:
            if batch:
                q = query.filter(Item.url > batch[-1].url)
            else:
                q = query
            batch = q.fetch(1000, projection=[Item.url])
            if not batch:
                break
            for item in batch:
                assert item.url
                indexed.add(self.salt_url(item.url))
        return indexed

    def salt_url(self, url):
        return str("%d$%s" % (self.bloom_salt, url))


class Category(ndb.Model):
    store = ndb.StringProperty(required=True)
    added = ndb.DateTimeProperty(auto_now_add=True)
    title = ndb.StringProperty(required=True)
    url = ndb.StringProperty(required=True)
    parent_cat = ndb.KeyProperty()
    removed = ndb.DateTimeProperty()


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
