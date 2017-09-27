from collections import namedtuple
from Cookie import SimpleCookie
import logging
from random import randint
import time

from google.appengine.api import memcache
from google.appengine.api.datastore_errors import BadValueError
from google.appengine.ext import deferred, ndb
from google.appengine.ext.ndb import polymodel

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


class ScrapeJob(polymodel.PolyModel):
    created = ndb.DateTimeProperty(auto_now_add=True)
    modified = ndb.DateTimeProperty(auto_now=True)
    cookies = ndb.JsonProperty()

    def set_cookies(self, cookies):
        self.cookies = {cookie.key: cookie.value
                        for cookie in cookies.itervalues()}
        logging.debug("%r: updated cookies: %s"
                      % (self.key, self.cookies))

    def get_cookies(self):
        if self.cookies:
            logging.debug("%r: loaded cookies: %s" % (self.key, self.cookies))
            return SimpleCookie({str(k): v.encode('utf-8')
                                 for k, v in self.cookies.iteritems()})
        else:
            return SimpleCookie()


class SiteScan(ScrapeJob):
    category_queue = ndb.TextProperty(repeated=True)
    item_queue = ndb.TextProperty(repeated=True)
    # separating item and category blooms as there have been
    # some item URL mixups
    bloom_categories = ndb.BlobProperty(compressed=True)
    bloom_items = ndb.BlobProperty(compressed=True)
    bloom_salt = ndb.IntegerProperty(required=True)

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
            job = cls(key=key, bloom_salt=salt)
            if bf_items:
                job.bloom_items = bf_items.bitmap.mmap
            job.put()
            return True
        return tx()

    @classmethod
    @ndb.transactional
    def queue(cls, store_id, categories=None, items=None):
        if not (categories or items):
            return

        key = ndb.Key(cls, store_id)
        job = key.get()
        assert isinstance(job, cls), "No crawl in progress"

        def unseen(bf_data):
            bf = cls.get_bloom(bf_data)
            def inner(url):
                if job.salt_url(url) in bf:
                    logging.info("%r: skipping already seen URL %s" % (key, url))
                    return False
                else:
                    return True
            return inner

        if categories:
            categories = filter_urls(categories)
            categories = filter(lambda url: url not in job.category_queue,
                                categories)
            categories = filter(unseen(job.bloom_categories), categories)
            job.category_queue += categories

        if items:
            items = filter_urls(items)
            items = filter(lambda url: url not in job.item_queue,
                           items)
            items = filter(unseen(job.bloom_items), items)
            job.item_queue += items

        job.put()

    @classmethod
    @ndb.transactional
    def peek(cls, store_id):
        job = ndb.Key(cls, store_id).get()
        if isinstance(job, cls):
            if job.item_queue:
                return (job.item_queue[0],
                        PAGE_TYPE.ITEM,
                        job.get_cookies())
            if job.category_queue:
                return (job.category_queue[0],
                        PAGE_TYPE.CATEGORY,
                        job.get_cookies())
        return None, None, None

    @classmethod
    @ndb.transactional
    def pop(cls, store_id, url, cookies):
        job = ndb.Key(cls, store_id).get()
        if not isinstance(job, cls):
            return
        def ne(_url):
            return _url != url
        mod = False
        if url in job.category_queue:
            job.category_queue = filter(ne, job.category_queue)
            bf = job.get_bloom(job.bloom_categories)
            bf.add(job.salt_url(url))
            job.bloom_categories = bf.bitmap.mmap
            mod = True
        if url in job.item_queue:
            job.item_queue = filter(ne, job.item_queue)
            bf = job.get_bloom(job.bloom_items)
            bf.add(job.salt_url(url))
            job.bloom_items = bf.bitmap.mmap
            mod = True
        if not mod:
            return
        if job.category_queue or job.item_queue:
            job.set_cookies(cookies)
            job.put()
        else:
            job.key.delete()

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

    def scan_indexed(self):
        store_key = ndb.Key(Store, self.key.id())
        query = Item.query(Item.removed == None,
                           ancestor=store_key) \
                    .order(Item.url)
        indexed = self.get_bloom(None)
        # results in timeout without this kind of manual batch fetching
        batch = None
        while True:
            if batch:
                q = query.filter(Item.url > batch[-1].url)
            else:
                q = query
            batch = q.fetch(1000, projection=(Item.url,))
            if not batch:
                break
            for item in batch:
                assert item.url
                indexed.add(self.salt_url(item.url))
        return indexed

    def salt_url(self, url):
        return str("%d$%s" % (self.bloom_salt, url))


class TableScan(ScrapeJob):
    marker = ndb.KeyProperty(required=True)

    @classmethod
    def initialize(cls, store_id):
        store_key = ndb.Key(Store, store_id)
        marker = Item.query(ancestor=store_key) \
                     .order(Item.key) \
                     .get(keys_only=True)
        @ndb.transactional
        def tx():
            key = ndb.Key(cls, store_id)
            if key.get():
                return False
            job = cls(key=key, marker=marker)
            job.put()
            return True
        return tx()

    @classmethod
    @ndb.transactional
    def advance(cls, store_id, marker, cookies):
        job = ndb.Key(cls, store_id).get()
        if isinstance(job, cls):
            if marker:
                job.marker = marker
                job.set_cookies(cookies)
                job.put()
            else:
                job.key.delete()


class Category(ndb.Model):
    store = ndb.StringProperty(required=True)
    added = ndb.DateTimeProperty(auto_now_add=True)
    title = ndb.StringProperty(required=True)
    url = ndb.StringProperty(required=True)
    parent_cat = ndb.KeyProperty(kind='Category')
    removed = ndb.DateTimeProperty()


class Item(ndb.Model):
    added = ndb.DateTimeProperty(auto_now_add=True)
    checked = ndb.DateTimeProperty(auto_now=True)
    url = ndb.StringProperty(required=True)
    title = ndb.StringProperty(required=True)
    image = ndb.StringProperty(required=True)
    category = ndb.KeyProperty(kind=Category, required=True)
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


class Stat(polymodel.PolyModel):
    created = ndb.DateTimeProperty(auto_now_add=True)


class ItemCounts(Stat):
    """Use store as parent."""
    categories = ndb.JsonProperty()


def get_duplicate_categories():
    distinct = Category.query(group_by=(Category.url,)) \
                       .fetch(projection=(Category.url,))
    dup_count = Category.query().count() - len(distinct)
    logging.info("Found %d duplicate URLs from index" % dup_count)

    by_url = {}
    itr = Category.query() \
                  .iter(batch_size=200,
                        projection=(Category.url,
                                    Category.title,
                                    Category.store,
                                    # to avoid an extra index
                                    Category.parent_cat))
    for cat in itr:
        by_url.setdefault(cat.url, []) \
              .append((cat.key, cat.title, cat.store))

    dups = {url: cats for url, cats in by_url.iteritems()
            if len(cats) > 1}

    def cat_info((cat_key, title, store)):
        return " - '%s' (%d)" % (title, cat_key.id())

    dup_infos = ["\n%s (%d):\n%s" % (url, len(cats), "\n".join(map(cat_info, cats)))
                 for url, cats in dups.iteritems()]
    logging.info("Found %d URLs with multiple categories:\n%s"
                 % (len(dups), "\n".join(dup_infos)))

    return dups


def prune_duplicate_categories():
    """WARNING: This function flushes memcache."""
    from .hk import by_url
    from .search import reindex_items
    from .util import update_category_counts
    from .views import get_categories

    assert not ScrapeJob.query().count(limit=1), \
        "Not pruning as a ScrapeJob exists"

    dups = get_duplicate_categories()

    if not dups:
        return

    @ndb.transactional
    def move_to(item_key, to_cat):
        item = item_key.get()
        if item.category != to_cat:
            prev_cat = item.category
            item.category = to_cat
            item.put()
            logging.info("Moved %r from %r to %r"
                         % (item_key, prev_cat, item.category))
            return True
        else:
            return False

    def move_items(from_cat, to_cat):
        mod = False
        for ikey in Item.query(Item.category == from_cat) \
                        .iter(batch_size=100, keys_only=True):
            mod |= move_to(ikey, to_cat)
        return mod

    @ndb.transactional
    def move_child(child_cat, new_parent):
        assert new_parent != child_cat
        child = child_cat.get()
        if child.parent_cat != new_parent:
            prev_parent = child.parent_cat
            child.parent_cat = new_parent
            child.put()
            logging.info("Moved %r from %r to %r"
                         % (child_cat, prev_parent, child.parent_cat))
            return True
        else:
            return False

    def deduplicate(cat_keys):
        item_counts = \
            {cat_key: Item.query(Item.category == cat_key)
                          .count(limit=1000)
             for cat_key in cat_keys}
        item_counts = sorted(item_counts.iteritems(),
                             key=lambda (ck, c): c,
                             reverse=True)
        logging.debug("item_counts:\n%s"
                      % "\n".join("- %d: %d" % (ck.id(), c)
                                  for ck, c in item_counts))

        active = item_counts[0][0]
        prune = [ck for ck, c in item_counts[1:]]
        mod = False

        for cat_key in prune:
            children = Category.query(Category.parent_cat == cat_key) \
                               .fetch(keys_only=True)
            if children:
                logging.info("Moving children %r to %r"
                             % (children, active))
                for child in children:
                    mod |= move_child(child, active)

        logging.info("Moving items from %r to %r" % (prune, active))
        for cat_key in prune:
            mod |= move_items(cat_key, active)

        if mod:
            time.sleep(2)

        for cat_key in prune:
            assert not Item.query(Item.category == cat_key) \
                           .count(limit=1)
            assert not Category.query(Category.parent_cat == cat_key) \
                               .count(limit=1)
            cat_key.delete()

        logging.info("Deleted %r" % (prune,))

    for url, cats in dups.iteritems():
        logging.debug("Deduplicating %s" % url)
        deduplicate([ck for ck, title, store in cats])
        by_url(url, _invalidate=True)
        # need to invalidate stores immediately as task execution may fail
        stores = {store for ck, title, store in cats}
        for store_id in stores:
            get_categories(store_id=store_id, _invalidate=True)

    memcache.flush_all()

    stores = {cat.store
              for cat in Category.query(group_by=('store',),
                                        projection=('store',))
                                 .fetch()}
    for store_id in stores:
        deferred.defer(update_category_counts,
                       store_id=store_id,
                       _queue='indexing',
                       _countdown=5)

    deferred.defer(reindex_items,
                   _queue='indexing',
                   _countdown=10)
