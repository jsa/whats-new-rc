import webapp2
from webapp2_extras import routes

from wnr import views
from wnr.hk.scrape import routes as hk_routes
from wnr.util import get


app = webapp2.WSGIApplication([
    get(r"/", views.search),
    routes.PathPrefixRoute(r"/hk", hk_routes),
    #get(r"/<store:\w+>", views.store),
], debug=False)
