import urllib

from google.appengine.api import memcache as memcache_module, urlfetch
from google.appengine.api.datastore import MAXIMUM_RESULTS
from google.appengine.ext import ndb

from jinja2._markupsafe._native import escape
from jinja2.filters import do_mark_safe
import webapp2

from settings import env


# "alias" just to get rid of pydev error...
memcache = memcache_module.Client()


def asciidict(d):
    return {k.encode('utf-8'): unicode(v).encode('utf-8')
            for k, v in d.iteritems()
            if v}


def render(template, data=None):
    return env.get_template(template).render(**(data or {}))


def get(url_template, handler):
    return webapp2.Route(url_template, handler, methods=('GET',))


def cache(expiry):
    def outer(fn):
        def inner(*args, **kw):
            rs = fn(*args, **kw)
            if isinstance(rs, webapp2.Response) \
               and rs.status == "200 OK":
                rs.headers['Cache-Control'] = \
                    "public, max-age=%d" % expiry
            return rs
        return inner
    return outer


def count_all(query):
    start, count = None, 0
    while True:
        if start:
            q = query.filter(ndb.Model.key > start)
        else:
            q = query
        key = q.get(offset=MAXIMUM_RESULTS,
                    keys_only=True)

        if key:
            count += MAXIMUM_RESULTS
            start = key
        else:
            count += q.count(limit=MAXIMUM_RESULTS)
            break

    return count


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


INVALIDATE_CACHE = object()


def cacheize(timeout):
    def outer(fn):
        ns = "cacheize(%s.%s)" % (fn.__module__, fn.__name__)
        def inner(*args, **kw):
            key = repr((args, kw))
            if INVALIDATE_CACHE in args:
                memcache.delete(key, namespace=ns)
                return
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


def qset(param, value=None, as_dict=False):
    rq = webapp2.get_request()
    if rq.GET:
        q = rq.GET.copy()
    else:
        q = {}
    if value:
        q[param] = value
    else:
        q.pop(param, None)
    if as_dict:
        return q
    if q:
        return "%s?%s" % (rq.path, urllib.urlencode(asciidict(q)))
    else:
        return rq.path


def as_form(q):
    html = "".join('<input type="hidden" name="%s" value="%s">'
                   % (escape(param), escape(value))
                   for param, value in q.iteritems())
    return do_mark_safe(html)
