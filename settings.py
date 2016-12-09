import jinja2, os

PROJECT_DIR = os.path.abspath(os.path.dirname(__file__))

env = jinja2.Environment(
    #extensions=,
    autoescape=True,
    loader=jinja2.FileSystemLoader(
               os.path.join(PROJECT_DIR, "templates")),
    cache_size=-1,
    auto_reload=False)
