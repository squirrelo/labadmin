from unittest import TestCase, main
from os.path import join, dirname, realpath

from knimin import db
from knimin.lib.redcap_interaction import pulldown
# _batch_grab, _format_animal, _format_human, _format_basic, _geolocate)


class TestRedcap(TestCase):
    def setUp(self):
        self.barcodes = ['000029429', '000018046', '000023299', '000023300']

    def test_pulldown(self):
        with open(join(dirname(realpath(__file__)), 'survey_expected.txt'),
                  'rU') as f:
            exp = ({1: f.read()}, {})
        obs = pulldown(self.barcodes)
        self.assertEqual(obs, exp)

    def test_pulldown_third_party(self):
        # Add survey answers
        with open(self.ext_survey_fp, 'rU') as f:
            obs = db.store_external_survey(
                f, 'Vioscreen', separator=',', survey_id_col='SubjectId',
                trim='-160')
        self.assertEqual(obs, 3)

        # Test without third party
        obs, _ = pulldown(self.barcodes)
        survey = obs[1]
        self.assertFalse('VIOSCREEN' in survey)

        obs, _ = pulldown(self.barcodes, blanks=['BLANK.01'])
        survey = obs[1]
        self.assertFalse('VIOSCREEN' in survey)
        self.assertTrue('BLANK.01' in survey)

        # Test with third party
        obs, _ = pulldown(self.barcodes, external=['Vioscreen'])
        survey = obs[1]
        self.assertTrue('VIOSCREEN' in survey)

        obs, _ = pulldown(self.barcodes, blanks=['BLANK.01'],
                          external=['Vioscreen'])
        survey = obs[1]
        self.assertTrue('VIOSCREEN' in survey)
        self.assertTrue('BLANK.01' in survey)

if __name__ == '__main__':
    main()
