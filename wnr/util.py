from google.appengine.api import urlfetch
from google.appengine.api.datastore import MAXIMUM_RESULTS

import webapp2

from settings import env


def render(template, data):
    return env.get_template(template).render(**data)


def get(url_template, handler):
    return webapp2.Route(url_template, handler, methods=('GET',))


def count_all(query):
    # context = ndb.get_context()
    # context.set_cache_policy(lambda key: False)
    # context.set_memcache_policy(lambda key: False)
    # context.clear_cache()

    cursor, count = None, 0
    while True:
        e, _cursor, more = \
            query.fetch_page(page_size=1,
                             start_cursor=cursor,
                             offset=MAXIMUM_RESULTS)

        if e and _cursor and more:
            # logging.info("got %r, cursor %s" % (e[-1].key, cursor))
            count += MAXIMUM_RESULTS
            cursor = _cursor
        else:
            count += query.count(limit=MAXIMUM_RESULTS,
                                 start_cursor=cursor)
            break

    return count


def ok_resp(rs):
    if rs.status_code == 200:
        return rs
    else:
        raise urlfetch.DownloadError(
                  "%d from %s:\n\n%r\n\n%s"
                  % (rs.status_code,
                     rs.final_url,
                     rs.headers,
                     rs.content))
