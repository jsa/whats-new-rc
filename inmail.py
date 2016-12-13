from datetime import datetime
import email.utils
import logging

from google.appengine.api import mail
from google.appengine.ext import ndb

import webapp2


class Inmail(ndb.Model):
    timestamp = ndb.DateTimeProperty(auto_now_add=True)
    mail_timestamp = ndb.DateTimeProperty(required=True)
    sender = ndb.StringProperty(required=True)
    recipients = ndb.StringProperty(repeated=True)
    subject = ndb.StringProperty()
    raw = ndb.BlobProperty(required=True)


class InfoEmailHandler(webapp2.RequestHandler):
    def post(self):
        raw = self.request.body
        msg = mail.InboundEmailMessage(raw)
        logging.debug("Received %.1fkB from '%s'"
                      % (len(raw) / 1024., msg.sender))
        ts = datetime.utcfromtimestamp(
                 email.utils.mktime_tz(
                     email.utils.parsedate_tz(
                         msg.date)))
        to = sorted({s.strip() for s in msg.to.split(",")})
        ent = Inmail(mail_timestamp=ts,
                     sender=msg.sender,
                     recipients=to,
                     subject=msg.subject,
                     raw=raw)
        # let error (as too large)
        ent.put()
        logging.info("Stored email as %r" % ent.key)


app = webapp2.WSGIApplication([
    #(r"^/_ah/bounce$", BounceHandler),
    #(r"^'/_ah/mail/alert@.+$", AlertEmailHandler),
    (r"^/_ah/mail/info@.+$", InfoEmailHandler),
], debug=False)
