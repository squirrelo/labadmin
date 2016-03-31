from unittest import TestCase, main
from os.path import join, dirname, realpath
from knimin import db


class TestDataAccess(TestCase):
    ext_survey_fp = join(dirname(realpath(__file__)), '..', '..', 'tests',
                         'data', 'external_survey_data.csv')

    def setUp(self):
        # Make sure vioscreen survey exists in DB
        try:
            db.add_external_survey('Vioscreen', 'FFQ', 'http://vioscreen.com')
        except ValueError:
            pass
        self.barcodes = ['000029429', '000018046', '000023299', '000023300']

    def tearDown(self):
        db._clear_table('external_survey_answers', 'ag')

    def test_check_consent(self):
        consent, fail = db.check_consent(['000027561', '000001124', '0000000'])
        self.assertEqual(consent, ['000027561'])
        self.assertEqual(fail, {'0000000': 'Not an AG barcode',
                                '000001124': 'Sample not logged'})

    def test_get_survey_types(self):
        obs = db.get_survey_types()
        exp = {'Animal': ['ag-animal-en-US'],
               'Human': ['ag-human-en-US']}
        self.assertEqual(obs, exp)

    def test_get_records_for_barcodes(self):
        obs = db.get_records_for_barcodes(self.barcodes)
        exp = [24, 802, 1574]
        self.assertEqual(obs, exp)

    def test_get_barcode_surveys(self):
        obs = db.get_barcode_surveys(self.barcodes)
        exp = [['084532330aca5885', '000018046'],
               ['04e29cac1540c30b', '000023299'],
               ['04e29cac1540c30b', '000023300'],
               ['14f508185c954721', '000029429']]
        self.assertEqual(obs, exp)


if __name__ == "__main__":
    main()
