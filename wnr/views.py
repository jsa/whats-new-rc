# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import logging
import re
import urllib

from google.appengine.api import search as g_search, urlfetch
from google.appengine.ext import ndb

import webapp2

from .models import Item, Store
from .search import from_unix, ITEMS_INDEX, parse_history_price, to_unix
from .util import cache, render


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
    def __init__(self, doc):
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


@cache(10)
def search(rq):
    get_q = {k: v for k, v in rq.GET.iteritems() if v}

    def asciidict(d):
        return {k.encode('utf-8'): unicode(v).encode('utf-8')
                for k, v in d.iteritems()}

    def page_q(page):
        if page < 2:
            q = get_q
        else:
            q = dict(get_q, p=page)
        if q:
            return "%s?%s" % (rq.path, urllib.urlencode(asciidict(q)))
        else:
            return rq.path

    page = get_q.pop("p", None)
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

    search_q = get_q.get("q")
    if search_q:
        search_q = re.sub(r"[^a-z0-9]", " ", search_q.lower().strip()).strip()
    if search_q:
        query = "title:(%s)" % search_q
    else:
        query = "added <= %d" % to_unix(datetime.utcnow())

    rs = index.search(g_search.Query(query, opts), deadline=50)
    max_page = rs.number_found / page_size
    if rs.number_found % page_size:
        max_page += 1
    max_page = min(max_page, page_limit)

    def paging():
        start_page = max(page - 2, 1)
        end_page = min(page + 7, max_page)
        if end_page <= start_page:
            return

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
        'rq': rq,
        'items': map(ItemView, rs.results),
        'paging': paging(),
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
