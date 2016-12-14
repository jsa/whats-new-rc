# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import logging
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
    return "%s %s" % (cur, amt)


class ItemView(object):
    def __init__(self, doc):
        self.doc = doc
        store_id = doc.field('store').value
        self.store = {'id': store_id,
                      'title': "HobbyKing"}
        self.photo_url = "/i/%s/%s" % (store_id, doc.field('sku').value)

        added = from_unix(doc.field('added').value)
        since = datetime.utcnow() - added
        if since < timedelta(hours=1):
            self.added = "%d minutes" % (since.seconds / 60)
        if since < timedelta(days=1):
            self.added = "%d hours" % (since.seconds / 3600)
        if since < timedelta(days=2):
            self.added = "1 days, %d hours" % (since.seconds / 3600)
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
        return str(self.doc)

    def __repr__(self):
        return repr(self.doc)


def redir(url):
    rs = webapp2.Response(status=302)
    rs.headers['Location'] = urllib.quote(url)
    return rs


@cache(10)
def search(rq):
    page = rq.GET.get('page')
    if page:
        page = 1
        try:
            page = int(page)
        except ValueError:
            pass
        if page < 2:
            return redir("/")
    else:
        page = 1

    page_size = 60
    count_accy = 1000
    index = g_search.Index(ITEMS_INDEX)
    # global sort is latest-ness
    # (note: rank would be referenced as "_rank")
    # sort = g_search.SortOptions(
    #            [g_search.SortExpression('added', g_search.SortExpression.DESCENDING)],
    #            limit=g_search.MAXIMUM_SORTED_DOCUMENTS)
    opts = g_search.QueryOptions(
               limit=page_size,
               number_found_accuracy=count_accy,
               offset=page_size * (page - 1))
    query = "added <= %d" % to_unix(datetime.utcnow())
    rs = index.search(g_search.Query(query, opts), deadline=50)

    def paging():
        start_page = max(page - 2, 1)
        end_page = min(page + 7, rs.number_found / page_size - 1)
        pages = [(p, "?p=%d" % p if p > 1 else "?", p == page)
                 for p in range(start_page, end_page + 1)]

        paging = {'nav': pages}

        p_prev = filter(lambda p: p[0] == page - 1, pages)
        if p_prev:
            paging['prev'] = p_prev[0]

        p_next = filter(lambda p: p[0] == page + 1, pages)
        if p_next:
            paging['next'] = p_next[0]

        return paging

    ctx = {
        'items': map(ItemView, rs.results),
        'paging': paging,
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
