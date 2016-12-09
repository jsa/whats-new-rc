from google.appengine.api import urlfetch

import webapp2

from settings import env


def render(template, data):
    return env.get_template(template).render(**data)


def get(url_template, handler):
    return webapp2.Route(url_template, handler, methods=('GET',))


def ok_resp(rs):
    if rs.status_code != 200:
        raise urlfetch.DownloadError(
                  "%d from %s:\n\n%r\n\n%s"
                  % (rs.status_code,
                     rs.final_url,
                     rs.headers,
                     rs.content))
