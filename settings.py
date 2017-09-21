import jinja2, logging, os
from google.appengine.api import app_identity

PROJECT_DIR = os.path.abspath(os.path.dirname(__file__))

env = jinja2.Environment(
    #extensions=,
    autoescape=True,
    loader=jinja2.FileSystemLoader(
               os.path.join(PROJECT_DIR, "templates")),
    cache_size=-1,
    auto_reload=False,
    trim_blocks=True)

try:
    env.globals['app_id'] = app_identity.get_application_id()
except Exception as e:
    logging.warn(e, exc_info=True)
    env.globals['app_id'] = None
