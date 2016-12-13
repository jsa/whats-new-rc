import webapp2

from .util import render


not_found = webapp2.Response("Page not found",
                             "404 Not Found",
                             content_type="text/plain")


def store(rq, store):
    if store != "hk":
        return not_found
    ctx = {
       'store': {'title': "HobbyKing"},
    }
    return webapp2.Response(render("listing.html", ctx))
