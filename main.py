import webapp2
from webapp2_extras import routes

from wnr import views
from wnr.hk.scrape import routes as hk_routes
from wnr.util import cache, get, render


@cache(60)
def root(rq):
    return webapp2.Response(render("root.html"))


app = webapp2.WSGIApplication([
    get(r"/", root),
    routes.PathPrefixRoute(r"/hk", hk_routes),
    get(r"/<store:\w+>", views.store),
], debug=False)
