import os

import jinja2
import webapp2


PROJECT_DIR = os.path.abspath(os.path.dirname(__file__))


env = jinja2.Environment(
    #extensions=,
    autoescape=True,
    loader=jinja2.FileSystemLoader(
               os.path.join(PROJECT_DIR, "templates")),
    cache_size=-1,
    auto_reload=False)


class Root(webapp2.RequestHandler):
    def get(self):
        self.response.write(
            env.get_template("hello.html")
               .render(name="John Doe"))


app = webapp2.WSGIApplication([
    (r"^/$", Root),
], debug=False)
