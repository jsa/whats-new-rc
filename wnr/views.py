# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import logging
import re
import time
import urllib

from google.appengine.api import search as g_search, urlfetch
from google.appengine.ext import ndb

import webapp2

from .models import Category, Item, ItemCounts, Store
from .search import from_unix, ITEMS_INDEX, parse_history_price, to_unix
from .util import cache, cacheize, not_found, nub, qset, redir, render


def get_stores():
    from . import hk
    return {hk._store.id: hk._store}


@cache(10)
def about(rq):
    return render("about.html")


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

    def __str__(self):
        return "ItemView(%s)" % str(self.doc)

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
    def page_q(page):
        return qset("p", page if page >= 2 else None)

    page = rq.GET.pop("p", None)
    if page:
        try:
            page = int(page)
        except ValueError:
            return not_found("Invalid page '%s'" % (page,))
        if page < 2:
            return redir(page_q(page))
    else:
        page = 1

    page_size = 60
    page_limit = g_search.MAXIMUM_SEARCH_OFFSET / page_size + 1
    if page > page_limit:
        return redir(page_q(page_limit))

    # Default sort is rank descending, and the rank is the added timestamp.
    # (note: rank would be referenced as "_rank")
    # sort = g_search.SortOptions(
    #            [g_search.SortExpression('added', g_search.SortExpression.DESCENDING)],
    #            limit=g_search.MAXIMUM_SORTED_DOCUMENTS)

    count_accy = 1000
    index = g_search.Index(ITEMS_INDEX)
    opts = g_search.QueryOptions(
               limit=page_size,
               number_found_accuracy=count_accy,
               offset=page_size * (page - 1))
    expr, filters = [], []

    search_q = rq.GET.get("q")
    if search_q:
        search_q = re.sub(r"[^a-z0-9&_~#]", " ", search_q.lower().strip()) \
                     .strip()
    if search_q:
        expr.append(search_q)
        filters.append(('"%s"' % search_q, qset("q")))

    cats = rq.GET.get("c")
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
        filters.append((" OR ".join(cat_names), qset("c")))

    if not expr:
        # basically just to have some query for the search...
        expr = ["added<=%d" % to_unix(datetime.utcnow())]

    with log_latency("Search latency {:,d}ms"):
        rs = index.search(g_search.Query(" ".join(expr), opts), deadline=10)

    max_page = rs.number_found / page_size
    if rs.number_found % page_size:
        max_page += 1
    max_page = min(max_page, page_limit)

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
    }

    if rs.number_found < count_accy:
        ctx['total_count'] = "{:,d}".format(rs.number_found)
    else:
        ctx['total_count'] = "{:,d}+".format(count_accy)

    with log_latency("Render latency {:,d}ms"):
        return render("search.html", ctx)


@cache(60 * 60)
def item_image(rq, store, sku):
    item = ndb.Key(Store, store, Item, sku).get()
    if not item:
        return not_found("Item not found")

    rs = urlfetch.fetch(item.image, headers={'Referer': urllib.quote(item.url)})
    logging.debug("%d (%.1fkB) from %s:\n%r"
                  % (rs.status_code,
                     len(rs.content) / 1024.,
                     item.image,
                     rs.headers))
    return webapp2.Response(rs.content,
                            rs.status_code,
                            content_type=rs.headers['Content-Type'])


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
                                      'tree': tree})


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
