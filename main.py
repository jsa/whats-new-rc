from google.appengine.ext import ndb

import webapp2
from webapp2_extras import routes

from settings import env
from wnr import hk, views
from wnr.util import as_form, GET, get, qset


# disable in-context cache (the newbie helper)
ndb.get_context().set_cache_policy(False)


env.globals['GET'] = GET
env.globals['qset'] = qset

env.filters['as_form'] = as_form


app = webapp2.WSGIApplication([
    get(r"/", views.search),
    get(r"/_cron/cache-categories", views.cache_categories),
    get(r"/about", views.about),
    get(r"/i/<store:\w+>/<sku:.+>", views.item_image),
    routes.PathPrefixRoute(r"/_hk", hk.routes),
    # get(r"/<store:\w+>", views.store),
], debug=False)
