from json import loads

from tornado.web import authenticated
from knimin.handlers.base import BaseHandler
from knimin.handlers.access_decorators import set_access

from knimin import db


@set_access(['Admin'])
class AGViewHandoutHandler(BaseHandler):
    @authenticated
    def get(self):
        kits = db.get_handout_kits()

        self.render('ag_view_handout.html', kits=kits)

    @authenticated
    def post(self):
        kits = loads(self.get_argument('kits'))
        try:
            db.delete_ag_kits(self.current_user, kits)
        except Exception as e:
            self.write('ERROR: %s' % str(e))
            return
        self.write('Success')
