import logging

from google.appengine.ext import ndb

import webapp2
from webapp2_extras import routes

from settings import env
from wnr import hk, util, views


# disable in-context cache (the newbie helper)
ndb.get_context().set_cache_policy(False)


def get_stores():
    from wnr import get_stores
    return sorted(get_stores().iteritems(),
                  key=lambda (store_id, info): info.title)


env.globals['GET'] = util.GET
env.globals['path'] = util.path
env.globals['qset'] = util.qset
env.globals['stores'] = get_stores

env.filters['as_hidden'] = util.as_hidden


app = webapp2.WSGIApplication([
    util.get(r"/", views.search),
    util.get(r"/_ah/start", views.warmup),
    util.get(r"/_ah/stop", views.shutdown),
    util.get(r"/about", views.about),
    webapp2.Route(r"/i/<store:\w+>/<sku:.+>", views.item_image, methods=('GET', 'HEAD')),
    util.get(r"/<store:\w+>/categories", views.categories),
    routes.PathPrefixRoute(r"/_hk", hk.routes),
    # get(r"/<store:\w+>", views.store),
], debug=False)

def error_page(rq, rs, exc):
    logging.exception(exc)
    rs.write(env.get_template("error.html").render())
    rs.headers['Content-Type'] = "text/html"
    rs.set_status(500)

app.error_handlers[500] = error_page
