from collections import namedtuple
import logging
from random import randint

from google.appengine.api.datastore_errors import BadValueError
from google.appengine.ext import ndb

from pyblooming.bloom import BloomFilter
from pyblooming.bitmap import Bitmap


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
    bloom_data = ndb.BlobProperty(compressed=True)
    bloom_rand = ndb.IntegerProperty(required=True)

    @classmethod
    def bloom_val(cls, url, rand):
        return str("%d$%s" % (rand, url))

    @classmethod
    @ndb.transactional
    def queue(cls, store, categories=None, items=None):
        if not (categories or items):
            return

        key = ndb.Key(cls, store)
        queue = key.get()
        bf = queue.get_bloom()

        def unseen(url):
            if queue and cls.bloom_val(url, queue.bloom_rand) in bf:
                logging.warn("%r: ignoring seen URL %s" % (key, url))
                return False
            return True

        if categories:
            categories = filter(unseen, filter_urls(categories))
        if items:
            items = filter(unseen, filter_urls(items))

        if queue:
            if categories:
                categories = filter(lambda url: url not in queue.category_queue,
                                    categories)
                queue.category_queue += categories
            if items:
                items = filter(lambda url: url not in queue.item_queue,
                               items)
                queue.item_queue += items
        else:
            queue = cls(key=key,
                        category_queue=categories or [],
                        item_queue=items or [],
                        bloom_rand=randint(1, 100000))
        queue.put()

    @classmethod
    @ndb.transactional
    def peek(cls, store):
        queue = ndb.Key(cls, store).get()
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
        queue = ndb.Key(cls, store).get()
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
                bf = queue.get_bloom()
                bf.add(cls.bloom_val(url, queue.bloom_rand))
                queue.set_bloom(bf)
                queue.put()
            else:
                queue.key.delete()

    def get_bloom(self):
        if self.bloom_data:
            size, ideal_k = BloomFilter.params_for_capacity(100000, .01)
            bitmap = Bitmap(size)
            bitmap.mmap = self.bloom_data
            return BloomFilter(bitmap, ideal_k)
        else:
            return BloomFilter.for_capacity(100000, .01)

    def set_bloom(self, bf):
        bf.flush()
        self.bloom_data = bf.bitmap.mmap
        logging.debug("%r: bloomfilter data size %dkB"
                      % (self.key, round(len(self.bloom_data) / 1024)))


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
