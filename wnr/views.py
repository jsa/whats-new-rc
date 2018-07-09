# -*- coding: utf-8 -*-
from collections import namedtuple
from datetime import datetime, timedelta
import logging
import re
import time
import urllib

from google.appengine.api import search as g_search, urlfetch
from google.appengine.ext import ndb

import webapp2

from . import get_stores
from .models import Category, Item, ItemCounts, Store
from .search import from_unix, ITEMS_INDEX, parse_history_price
from .util import cache, cacheize, not_found, nub, qset, redir, render


PARAM = namedtuple(
    "QueryParam",
    ('CATEGORY', 'PAGE', 'SEARCH', 'SORT')) \
    (u"🗄", u"📄️", u"🔦", u"🔀")

SORT = namedtuple(
    "SortOrder",
    ('CHEAP', 'DISCOUNT_AMT', 'DISCOUNT_PC', 'EXPENSIVE', 'LATEST')) \
    (u"💸↑", u"💯💲", u"💯➗", u"💸↓", u"️📅↓")


@cache(10)
def about(rq):
    return render("about.html", {'PARAM': PARAM})


def format_price(cur, amt):
    cur = {
        "AUD": "AU$",
        "EUR": u"€",
        "GBP": u"£",
        "USD": "$",
    }.get(cur, cur)
    return "%s %.2f" % (cur, amt)


class ItemView(object):
    @classmethod
    def make_views(cls, docs, categories):
        stores = get_stores()
        return [cls(d, categories, stores) for d in docs]

    def __init__(self, doc, categories, stores):
        self.doc = doc
        store_id = doc.field('store').value
        self.store = {'id': store_id,
                      'title': stores[store_id].title}
        self.photo_url = urllib.quote(
            "/i/%s/%s" % (store_id, doc.field('sku').value))

        added = from_unix(doc.field('added').value)
        since = datetime.utcnow() - added
        if since < timedelta(hours=1):
            self.added = "%d minutes" % (since.seconds / 60)
        elif since < timedelta(hours=23):
            # display "upper limit"
            self.added = "%d hours" % (since.seconds / 3600 + 1)
        elif since < timedelta(days=1, hours=23):
            self.added = "1 day, %d hour(s)" % (since.seconds / 3600 + 1)
        elif added.year != datetime.utcnow().year:
            self.added = added.strftime("%b %d, %Y")
        else:
            self.added = added.strftime("%b %d")

        try:
            doc.field('removed')
            self.removed = True
        except ValueError:
            self.removed = False

        try:
            prices = doc.field('price_history').value
        except ValueError:
            self.price = "(price not available)"
        else:
            ts, cur, amt = parse_history_price(prices.split(" ")[0])
            self.price = format_price(cur, amt)

        try:
            cats = doc.field('categories').value
        except ValueError:
            self.category_path = []
        else:
            cat_ids = map(int, cats.split(" "))
            cat_infos = map(categories.get, cat_ids)
            self.category_path = \
                [(cat_id, cat_info[1])
                 for cat_id, cat_info in zip(cat_ids, cat_infos)
                 if cat_info]

    def __getattr__(self, name):
        return self.doc.field(name).value

    def __unicode__(self):
        fields = ["doc_id='%s'" % self.doc.doc_id] \
                 + ["%s='%s'" % (field.name, field.value)
                    for field in sorted(self.doc.fields,
                                        key=lambda f: f.name)]
        return "ItemView(%s)" % ", ".join(fields)

    def __str__(self):
        return unicode(self).encode('ascii', 'replace')

    def __repr__(self):
        return "ItemView(%r)" % (self.doc,)


@cacheize(24 * 60 * 60)
def get_categories(store_id=None):
    def key_id(key):
        if key:
            return key.id()

    if store_id:
        q = Category.query(Category.store == store_id) \
                    .iter(batch_size=200,
                          projection=(Category.title,
                                      Category.parent_cat,
                                      # included here to avoid an extra index
                                      Category.url))
        return {c.key.id(): (c.title, key_id(c.parent_cat))
                for c in q}
    else:
        q = Category.query() \
                    .iter(batch_size=200,
                          projection=(Category.store,
                                      Category.title,
                                      Category.parent_cat,
                                      # included here to avoid an extra index
                                      Category.url))
        return {c.key.id(): (c.store, c.title, key_id(c.parent_cat))
                for c in q}


class log_latency(object):
    def __init__(self, msg):
        self.msg = msg

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        ms = (time.time() - self.start) * 1000
        logging.debug(self.msg.format(int(ms)))
        del self.start


@cache(30)
def search(rq):
    ###
    # Temporary parameter rename redirect.
    #
    from .util import asciidict, unicodedict
    q = unicodedict(rq.GET)
    _redir = False
    for _from, _to in (('c', PARAM.CATEGORY),
                       ('p', PARAM.PAGE),
                       ('q', PARAM.SEARCH),
                       ('s', PARAM.SORT)):
        val = q.pop(_from, None)
        if val:
            q[_to] = val
            _redir = True
    if _redir:
        logging.info("Redirecting %r -> %r" % (rq.GET, q))
        return redir(rq.path + "?%s" % urllib.urlencode(asciidict(q)))
    #
    ###

    def page_q(page):
        return qset(PARAM.PAGE, page if page >= 2 else None)

    page = rq.GET.pop(PARAM.PAGE, None)
    if page:
        try:
            page = int(page)
        except ValueError:
            return not_found("Invalid page '%s'" % (page,))
        if page < 2:
            return redir(page_q(page))
    else:
        page = 1

    page_size = 72 # divisible by 2, 3, and 4
    page_limit = g_search.MAXIMUM_SEARCH_OFFSET / page_size + 1
    if page > page_limit:
        return redir(page_q(page_limit))

    sort = rq.GET.get(PARAM.SORT)
    if sort == SORT.CHEAP:
        sort = g_search.SortExpression(
                   'us_cents', g_search.SortExpression.ASCENDING)
    elif sort == SORT.DISCOUNT_AMT:
        sort = g_search.SortExpression(
                   'discount_us_cents', g_search.SortExpression.DESCENDING)
    elif sort == SORT.DISCOUNT_PC:
        sort = g_search.SortExpression(
                   'discount_pc', g_search.SortExpression.DESCENDING)
    elif sort == SORT.EXPENSIVE:
        sort = g_search.SortExpression(
                   'us_cents', g_search.SortExpression.DESCENDING)
    elif sort is not None:
        return redir(qset(PARAM.SORT))

    # Default sort is rank descending, and the rank is the added timestamp.
    # (note: rank would be referenced as "_rank")
    # sort = g_search.SortExpression('added', g_search.SortExpression.DESCENDING)

    if sort:
        sort = g_search.SortOptions(
                   [sort], limit=g_search.MAXIMUM_SORTED_DOCUMENTS)

    index = g_search.Index(ITEMS_INDEX)
    opts = g_search.QueryOptions(
               limit=page_size,
               number_found_accuracy=g_search.MAXIMUM_SORTED_DOCUMENTS
                                     if sort else
                                     g_search.MAXIMUM_SEARCH_OFFSET,
               offset=page_size * (page - 1),
               sort_options=sort)
    expr, filters = [], []

    search_q = rq.GET.get(PARAM.SEARCH)
    if search_q:
        search_q = re.sub(r"[^a-z0-9&_~#]", " ", search_q.lower().strip()) \
                     .strip()
    if search_q:
        expr.append(search_q)
        filters.append(('"%s"' % search_q, qset(PARAM.SEARCH)))

    cats = rq.GET.get(PARAM.CATEGORY)
    if cats:
        cats = cats.split(",")
        try:
            cats = map(int, cats)
        except ValueError:
            return not_found("Invalid categories %s" % (cats,))
        cats = nub(cats)
        cat_infos = map(get_categories().get, cats)
        if not all(cat_infos):
            return not_found("Invalid categories %s" % (cats,))
        cats = zip(cats, cat_infos)
        cat_ids = ['"%d"' % c[0] for c in cats]
        expr.append("categories:(%s)" % " OR ".join(cat_ids))
        cat_names = [c[1][1] for c in cats]
        filters.append((" OR ".join(cat_names), qset(PARAM.CATEGORY)))

    with log_latency("Search latency {:,d}ms"):
        rs = index.search(g_search.Query(" ".join(expr), opts), deadline=10)

    # limit to 1000
    num_found = min(rs.number_found, g_search.MAXIMUM_SEARCH_OFFSET)
    max_page = num_found / page_size
    if rs.number_found % page_size:
        max_page += 1
    max_page = max(min(max_page, page_limit), 1)

    if page > max_page:
        return redir(page_q(max_page))

    def paging():
        start_page = min(max(page - 5, 1),
                         max(max_page - 10, 1))
        end_page = min(start_page + 10, max_page)
        pages = [(p, page_q(p), p == page)
                 for p in range(start_page, end_page + 1)]

        if not pages:
            # zero results, not even a single page
            return

        if len(pages) > 4:
            if pages[0][0] > 1:
                pages[0] = (1, page_q(1), False)
                if pages[1][0] > 2:
                    pages[1] = (u"…",) + pages[1][1:]
            if pages[-1][0] < max_page:
                pages[-1] = (max_page, page_q(max_page), False)
                if pages[-2][0] < (max_page - 1):
                    pages[-2] = (u"…",) + pages[-2][1:]

        paging = {'range': pages}

        p_prev = filter(lambda p: p[0] == page - 1, pages)
        if p_prev:
            paging['prev'] = p_prev[0]

        p_next = filter(lambda p: p[0] == page + 1, pages)
        if p_next:
            paging['next'] = p_next[0]

        return paging

    with log_latency("get_categories() latency {:,d}ms"):
        cats = get_categories()

    with log_latency("ItemView latency {:,d}ms"):
        items = ItemView.make_views(rs.results, cats)

    ctx = {
        'items': items,
        'paging': paging(),
        'filters': filters,
        'warnings': [],
        'PARAM': PARAM,
        'SORT': SORT,
    }

    if rs.number_found < g_search.MAXIMUM_SEARCH_OFFSET:
        ctx['total_count'] = "{:,d}".format(rs.number_found)
    else:
        ctx['total_count'] = "{:,d}+".format(g_search.MAXIMUM_SEARCH_OFFSET)
        if rs.number_found >= g_search.MAXIMUM_SORTED_DOCUMENTS:
            ctx['warnings'].append(
                "Sorting may be missing items due to large number of hits")

    with log_latency("Render latency {:,d}ms"):
        return render("search.html", ctx)


@cache(60 * 60)
def item_image(rq, store, sku):
    item = ndb.Key(Store, store, Item, sku).get()
    if not item:
        return not_found("Item not found")

    logging.debug("Request headers: %r" % (rq.headers,))

    method = {
        'GET': urlfetch.GET,
        'HEAD': urlfetch.HEAD,
    }[rq.method]

    headers = {'Referer': urllib.quote(item.url)}

    ua = rq.headers.get('User-Agent')
    if ua:
        headers['User-Agent'] = "%s;" % ua

    for field in ('Accept', 'If-None-Match'):
        value = rq.headers.get(field)
        if value:
            headers[field] = value

    try:
        rs = urlfetch.fetch(item.image,
                            method=method,
                            headers=headers,
                            deadline=10)
    except Exception as e:
        logging.exception("Image %s failed: '%s'" % (rq.method, item.image))
        return webapp2.Response(unicode(e), 500, content_type="text/plain")

    if rs.status_code in (200, 304):
        level = logging.debug
    else:
        level = logging.error
    level("%d (%.1fkB) from %s:\n%r"
          % (rs.status_code,
             len(rs.content) / 1024.,
             item.image,
             rs.headers))

    # Forward a bunch of headers. (Some may be overridden by App Engine.)
    headers = {}
    for field in ('Content-Type', 'Cache-Control', 'Date', 'ETag', 'Expires',
                  'Last-Modified'):
        value = rs.headers.get(field.lower())
        if value:
            headers[field] = value

    rs = webapp2.Response(rs.content,
                          rs.status_code,
                          # missing for HEAD response
                          content_type=headers.get('Content-Type'))
    # webapp2.Response overrides Cache-Control in contructor to 'no-cache'?!
    # Thus, need to set after constructor...
    for field, value in headers.iteritems():
        rs.headers[field] = value
    # delete the stupid webapp2 default values...
    if not headers.get('Content-Type'):
        # should've been available from source response
        del rs.headers['Content-Type']
    if method == urlfetch.HEAD:
        del rs.headers['Content-Length']

    return rs


def batches(itr, batch_size):
    b = []
    for e in itr:
        b.append(e)
        if len(b) == batch_size:
            yield b
            b = []
    if b:
        yield b


@cache(5 * 60)
def categories(rq, store):
    store_info = get_stores().get(store)
    if not store_info:
        return not_found("Unknown store '%s'" % store)

    item_counts = ItemCounts.query(ancestor=ndb.Key(Store, store)) \
                            .get()
    if item_counts:
        item_counts = item_counts.categories
    else:
        item_counts = {}

    cats = get_categories(store).items()
    if item_counts:
        # filter out empty if we have item counts
        cats = filter(lambda (cat_id, cat): item_counts.get(str(cat_id)) > 0,
                      cats)

    children = {}
    for tup in cats:
        parent_id = tup[1][1]
        if parent_id:
            children.setdefault(parent_id, []) \
                    .append(tup)

    def name_sort((cat_id, (title, parent_id))):
        return title

    for childs in children.itervalues():
        childs.sort(key=name_sort)

    def traverse(cat_id, title):
        return {
            'id': cat_id,
            'title': title,
            'children': [traverse(cat_id, title)
                         for cat_id, (title, parent_id)
                         in children.get(cat_id, [])],
        }

    root = filter(lambda (cat_id, (title, parent_id)): not parent_id, cats)
    root.sort(key=name_sort)

    tree = [traverse(cat_id, title) for cat_id, (title, parent_id) in root]

    def add_counts(cat):
        cat['item_count'] = item_counts.get(str(cat['id']))
        for cat in cat['children']:
            add_counts(cat)

    if item_counts:
        for cat in tree:
            add_counts(cat)

    return render("categories.html", {'store': store_info,
                                      'tree': tree,
                                      'PARAM': PARAM})


def warmup(rq):
    # not doing anything else here as it delays the pending request
    return webapp2.Response("", content_type="text/plain")


def shutdown(rq):
    return webapp2.Response("", content_type="text/plain")


def validate_category_tree():
    children, remaining = {}, {}
    for cat_id, (store_id, title, parent_id) in get_categories().iteritems():
        children.setdefault(parent_id, []) \
                .append(cat_id)
        remaining[cat_id] = (title, parent_id)

    def traverse(path=()):
        if path:
            cat_id = path[-1]
            title, parent_id = remaining.pop(cat_id)
            childs = children.pop(cat_id, [])
        else:
            childs = children.pop(None)
        for child_id in childs:
            traverse(path + (child_id,))

    traverse()

    def cat_info((cat_id, (title, parent_id))):
        return "- '%s' (%d, parent %s)" % (title, cat_id, parent_id)

    assert not remaining, \
        "%d unreachable categories:\n%s" \
        % (len(remaining), "\n".join(map(cat_info, remaining.iteritems())))

    logging.info("Category tree seems ok")
