# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import logging
import re
import urllib

from google.appengine.api import search as g_search, urlfetch
from google.appengine.ext import ndb

import webapp2

from .models import Category, Item, Store
from .search import from_unix, ITEMS_INDEX, parse_history_price, to_unix
from .util import cache, cacheize, qset, render


def about(rq):
    return webapp2.Response(render("about.html"))


def not_found(msg):
    return webapp2.Response(msg, 404, content_type="text/plain")


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
        return [cls(d, categories) for d in docs]

    def __init__(self, doc, categories):
        self.doc = doc
        store_id = doc.field('store').value
        self.store = {'id': store_id,
                      'title': "HobbyKing"}
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
            prices = doc.field('price_history').value
        except ValueError:
            self.price = "(price not available)"
            pass
        else:
            ts, cur, amt = parse_history_price(prices.split(" ")[0])
            self.price = format_price(cur, amt)

        try:
            cats = doc.field('categories').value
        except ValueError as e:
            logging.warn(e, exc_info=True)
            self.category_path = []
        else:
            cats = map(int, cats.split(" "))
            cats = [(cat_id, categories.get(int(cat_id))) for cat_id in cats]
            self.category_path = filter(lambda (_, title): bool(title), cats)

    def __getattr__(self, name):
        return self.doc.field(name).value

    def __str__(self):
        return "ItemView(%s)" % str(self.doc)

    def __repr__(self):
        return "ItemView(%r)" % (self.doc,)


def redir(url):
    rs = webapp2.Response(status=302)
    rs.headers['Location'] = url # urllib.quote(url)
    return rs


@cacheize(10 * 60)
def get_categories():
    return {c.key.id(): c.title for c in Category.query()}


@cache(10)
def search(rq):
    def page_q(page):
        return qset("p", page if page >= 2 else None)

    page = rq.GET.get("p")
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
    count_accy = 1000
    index = g_search.Index(ITEMS_INDEX)
    page_limit = g_search.MAXIMUM_SEARCH_OFFSET / page_size + 1
    if page > page_limit:
        return redir(page_q(page_limit))

    # global sort is latest-ness
    # (note: rank would be referenced as "_rank")
    # sort = g_search.SortOptions(
    #            [g_search.SortExpression('added', g_search.SortExpression.DESCENDING)],
    #            limit=g_search.MAXIMUM_SORTED_DOCUMENTS)

    opts = g_search.QueryOptions(
               limit=page_size,
               number_found_accuracy=count_accy,
               offset=page_size * (page - 1))

    expr, filters = [], []

    cats = rq.GET.get("c")
    if cats:
        cats = cats.split(",")
        try:
            cats = map(int, cats)
        except ValueError:
            return not_found("Invalid categories %s" % (cats,))
        cat_titles = get_categories()
        cat_names = [cat_titles.get(cat_id) for cat_id in cats]
        if not all(cat_names):
            return not_found("Invalid categories %s" % (cats,))
        cats = ['"%d"' % cat_id for cat_id in sorted(set(cats))]
        expr.append("categories:(%s)" % " OR ".join(cats))
        filters.append((" OR ".join(cat_names), qset("c")))

    search_q = rq.GET.get("q")
    if search_q:
        search_q = re.sub(r"[^a-z0-9]", " ", search_q.lower().strip()).strip()
    if search_q:
        expr.append("title:(%s)" % search_q)
        filters.append(('"%s"' % search_q, qset("q")))

    if not expr:
        expr = ["added <= %d" % to_unix(datetime.utcnow())]

    rs = index.search(g_search.Query(" ".join(expr), opts), deadline=50)
    max_page = rs.number_found / page_size
    if rs.number_found % page_size:
        max_page += 1
    max_page = min(max_page, page_limit)

    def paging():
        start_page = max(page - 2, 1)
        end_page = min(page + 7, max_page)
        # if end_page <= start_page:
        #     return

        pages = [(p, page_q(p), p == page)
                 for p in range(start_page, end_page + 1)]

        if pages[0][0] > 1:
            pages.insert(0, (1, page_q(1), False))
        if pages[-1][0] < max_page:
            pages.append((max_page, page_q(max_page), False))

        paging = {'range': pages}

        p_prev = filter(lambda p: p[0] == page - 1, pages)
        if p_prev:
            paging['prev'] = p_prev[0]

        p_next = filter(lambda p: p[0] == page + 1, pages)
        if p_next:
            paging['next'] = p_next[0]

        return paging

    ctx = {
        'items': ItemView.make_views(rs.results, get_categories()),
        'paging': paging(),
        'filters': filters,
    }

    if rs.number_found < count_accy:
        ctx['total_count'] = "{:,d}".format(rs.number_found)
    else:
        ctx['total_count'] = "{:,d}+".format(count_accy)

    return webapp2.Response(render("search.html", ctx))


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
