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


class Bounce(ndb.Model):
    timestamp = ndb.DateTimeProperty(auto_now_add=True)
    recipients = ndb.StringProperty(repeated=True)
    post_fields = ndb.TextProperty(repeated=True)


class InfoEmailHandler(webapp2.RequestHandler):
    """See InboundMailHandler"""
    def post(self):
        for name, value in self.request.headers.iteritems():
            logging.info("Header %r: %r" % (name, value))
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
                     # the subject field is undefined if no subject
                     subject=getattr(msg, 'subject', None),
                     raw=raw)
        ent.put()
        logging.info("Stored email to %r" % ent.key)


class BounceHandler(webapp2.RequestHandler):
    """See BounceNotificationHandler"""
    def post(self):
        form = self.request.POST
        to = form['original-to']
        to = sorted({s.strip() for s in to.split(",")})
        flat_form = [x for f in form.iterkeys() for x in (f, form.get(f))]
        log = Bounce(recipients=to,
                     post_fields=flat_form)
        log.put()
        logging.info("Stored bounce to %r" % log.key)


app = webapp2.WSGIApplication([
    (r"^/_ah/bounce$", BounceHandler),
    #(r"^'/_ah/mail/alert@.+$", AlertEmailHandler),
    (r"^/_ah/mail/info@.+$", InfoEmailHandler),
], debug=False)
