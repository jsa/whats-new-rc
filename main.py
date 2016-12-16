import webapp2
from webapp2_extras import routes

from settings import env
from wnr import views
from wnr.hk.scrape import routes as hk_routes
from wnr.util import as_form, GET, get, qset


env.globals['GET'] = GET
env.globals['qset'] = qset

env.filters['as_form'] = as_form


app = webapp2.WSGIApplication([
    get(r"/", views.search),
    get(r"/about", views.about),
    get(r"/i/<store:\w+>/<sku:.+>", views.item_image),
    routes.PathPrefixRoute(r"/_hk", hk_routes),
    # get(r"/<store:\w+>", views.store),
], debug=False)
