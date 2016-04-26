from tornado.web import authenticated
from knimin.handlers.base import BaseHandler
from knimin.handlers.access_decorators import set_access

from knimin import db


@set_access(['Create AG kits'])
class AGViewHandoutHandler(BaseHandler):
    @authenticated
    def get(self):
        kits = db.get_handout_kits()

        self.render('ag_view_handout.html', kits=kits)
