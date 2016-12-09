import webapp2
from webapp2_extras import routes

from wnr.hk.scrape import routes as hk_routes
from wnr.util import get, render


def root(rq):
    return webapp2.Response(render("hello.html", {'name': "John Doe"}))


app = webapp2.WSGIApplication([
    get("/", root),
    routes.PathPrefixRoute("/hk", hk_routes),
], debug=False)
