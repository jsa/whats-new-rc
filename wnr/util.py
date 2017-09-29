import hashlib
import logging
import time
import urllib

from google.appengine.api import memcache as memcache_module, urlfetch
from google.appengine.ext import ndb

from jinja2._markupsafe._native import escape
from jinja2.filters import do_mark_safe
import webapp2

from settings import env

from .models import Category, Item, ItemCounts, Store


# "alias" just to get rid of pydev error...
memcache = memcache_module.Client()


def render(template, data=None, *args, **kw):
    content = env.get_template(template).render(**(data or {}))
    return webapp2.Response(content, *args, **kw)


def not_found(msg):
    return webapp2.Response(msg, 404, content_type="text/plain")


def redir(url):
    rs = webapp2.Response(status=302)
    rs.headers['Location'] = url # urllib.quote(url)
    return rs


def get(url_template, handler):
    return webapp2.Route(url_template, handler, methods=('GET',))


def cache(expiry):
    def outer(fn):
        def inner(*args, **kw):
            rs = fn(*args, **kw)
            if isinstance(rs, webapp2.Response) \
               and rs.status == "200 OK" \
               and 'Cache-Control' not in rs.headers:
                rs.headers['Cache-Control'] = "public, max-age=%d" % expiry
            return rs
        return inner
    return outer


def update_category_counts(store_id):
    from .views import get_categories

    cats, children = set(), {}
    for cat in Category.query(Category.store == store_id,
                              projection=(Category.parent_cat,)) \
                       .iter(batch_size=200):
        cats.add(cat.key)
        if cat.parent_cat:
            children.setdefault(cat.parent_cat, set()) \
                    .add(cat.key)

    def item_count(cat_key):
        ic = Item.query(Item.category == cat_key,
                        Item.removed == None) \
                 .count()
        childs = children.get(cat_key)
        if childs:
            ic += sum(item_counts[ck] for ck in childs)
        return ic

    def debug_children(ck):
        return "%d: %r" % (ck.id(), children.get(ck))

    item_counts = {}
    while cats:
        seen = set(item_counts.iterkeys())
        leaves = {ck for ck in cats
                  if children.get(ck, set()) <= seen}
        assert leaves, "%d categories left:\n%s" \
                       % (len(cats), "\n".join(map(debug_children, cats)))
        item_counts.update({ck: item_count(ck) for ck in leaves})
        cats -= leaves

    # reduce keys to *string* IDs for JSON
    item_counts = {str(ck.id()): c for ck, c in item_counts.iteritems()}

    @ndb.transactional
    def save_counts(store):
        stat = ItemCounts.query(ancestor=store) \
                         .get()
        if stat:
            stat.categories = item_counts
        else:
            stat = ItemCounts(parent=store,
                              categories=item_counts)
        stat.put()
        logging.debug("Saved %d counts to %r"
                      % (len(item_counts), stat.key))

    save_counts(ndb.Key(Store, store_id))

    time.sleep(1)
    get_categories(_invalidate=True)
    stores = {cat.store
              for cat in Category.query(group_by=('store',),
                                        projection=('store',))
                                 .fetch()}
    for store_id in stores:
        get_categories(store_id=store_id, _invalidate=True)


def ok_resp(rs):
    if rs.status_code == 200:
        return rs
    else:
        raise urlfetch.DownloadError(
                  "%d response:\n\n%r\n\n%s"
                  % (rs.status_code,
                     rs.headers,
                     rs.content))


class _none(object):
    pass


def cacheize(timeout, version=""):
    def outer(fn):
        ns = "cacheize#%s(%s.%s)" % (version, fn.__module__, fn.__name__)
        def inner(*args, **kw):
            invalidate = refresh = False

            try:
                del kw['_invalidate']
                invalidate = True
            except KeyError:
                pass

            try:
                del kw['_refresh']
                refresh = True
            except KeyError:
                pass

            key = hashlib.sha512(repr((args, sorted(kw.iteritems())))) \
                         .hexdigest()

            if invalidate:
                memcache.delete(key, namespace=ns)
                return

            if refresh:
                value = None
            else:
                value = memcache.get(key, namespace=ns)

            if value is None:
                value = fn(*args, **kw)
                if value is None:
                    value = _none
                memcache.set(key, value, timeout, namespace=ns)
            if value is _none:
                return
            else:
                return value
        return inner
    return outer


def nubby(key, itr):
    l, seen = [], set()
    for v in itr:
        kv = key(v)
        if kv not in seen:
            seen.add(kv)
            l.append(v)
    return l


def nub(itr):
    return nubby(lambda x: x, itr)


def GET(param):
    return webapp2.get_request().GET.get(param, "")


def asciidict(d):
    def utf(s):
        if isinstance(s, unicode):
            return s.encode('utf-8')
        else:
            return s
    return {utf(k): utf(v) for k, v in d.iteritems() if v}


def unicodedict(d):
    def de_utf(s):
        if isinstance(s, str):
            return s.decode('utf-8')
        else:
            return s
    return {de_utf(k): de_utf(v) for k, v in d.iteritems() if v}


def path():
    return webapp2.get_request().path


def qset(param, value=None, path=None, as_dict=False):
    rq = webapp2.get_request()
    if path is None:
        path = rq.path
    if rq.GET:
        q = unicodedict(rq.GET)
    else:
        q = {}
    if value:
        q[param] = value
    else:
        q.pop(param, None)
    if as_dict:
        return q
    if q:
        return path + "?%s" % urllib.urlencode(asciidict(q))
    else:
        return path


def as_hidden(q):
    html = "".join('<input type="hidden" name="%s" value="%s">'
                   % (escape(param), escape(value))
                   for param, value in unicodedict(q).iteritems())
    return do_mark_safe(html)
