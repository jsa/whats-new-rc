from datetime import datetime, timedelta

from google.appengine.api import search as g_search

import webapp2

from . import search
from .util import cache, render


not_found = webapp2.Response("Page not found",
                             "404 Not Found",
                             content_type="text/plain")


class ItemView(object):
    def __init__(self, doc):
        self.doc = doc
        store_id = doc.field('store').value
        self.store = {'id': store_id,
                      'title': "HobbyKing"}
        self.photo_url = "/i/%s/%s" % (store_id, doc.field('sku').value)

        added = search.from_unix(doc.field('added'))
        since = datetime.utcnow() - added
        if since < timedelta(hours=1):
            self.added = "%d minutes" % (since.seconds / 60)
        if since < timedelta(days=1):
            self.added = "%d hours" % (since.seconds / 3600)
        if since < timedelta(days=2):
            self.added = "1 days, %d hours" % (since.seconds / 3600)
        else:
            self.added = added.stftime("%b %d")

    def __getattr__(self, name):
        return self.doc.field(name).value

    def __str__(self):
        return str(self.doc)

    def __repr__(self):
        return repr(self.doc)


@cache(60)
def search(rq):
    page = rq.GET.get('page')
    if page:
        page = 1
        try:
            page = int(page)
        except ValueError:
            pass
        if page < 2:
            TODO redir
    else:
        page = 1

    page_size = 60
    index = g_search.Index(search.ITEMS_INDEX)
    # global sort is latest-ness
    # (note: rank would be referenced as "_rank")
    # sort = g_search.SortOptions(
    #            [g_search.SortExpression('added', g_search.SortExpression.DESCENDING)],
    #            limit=g_search.MAXIMUM_SORTED_DOCUMENTS)
    opts = g_search.QueryOptions(
               limit=page_size,
               number_found_accuracy=g_search.MAXIMUM_NUMBER_FOUND_ACCURACY,
               offset=page_size * (page - 1))
    query = "added <= %d" % search.to_unix(datetime.utcnow())
    rs = index.search(g_search.Query(query, opts), deadline=50)

    start_page = max(page - 2, 1)
    end_page = min(page + 7, rs.number_found / page_size - 1)
    pages = [(p, "?p=%d" % p if p > 1 else "?", p == page)
             for p in range(start_page, end_page + 1)]

    paging = {
        'results': map(ItemView, rs.results),
        'nav': pages,
    }

    p_prev = filter(lambda p: p[0] == page - 1, pages)
    if p_prev:
        paging['prev'] = p_prev[0]

    p_next = filter(lambda p: p[0] == page + 1, pages)
    if p_next:
        paging['next'] = p_next[0]

    ctx = {'paging': paging}

    if rs.number_found == g_search.MAXIMUM_NUMBER_FOUND_ACCURACY:
        ctx['total_count'] = "{:,d}".format(rs.number_found)
    else:
        ctx['total_count'] = "{:,d}+".format(g_search.MAXIMUM_NUMBER_FOUND_ACCURACY)

    return webapp2.Response(render("search.html", ctx))
