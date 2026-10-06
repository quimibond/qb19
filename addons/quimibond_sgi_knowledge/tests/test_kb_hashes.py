# -*- coding: utf-8 -*-
"""1.2.0, la red de la decisión 3: sembrar, importar y ligar artículos no
cambia la huella de «Mi procedimiento» ni la del MIID. Publicar un CO sí
cambia su revisión en el MIID (es el control de cambios), con el mismo
título y la misma clave anterior."""
from unittest.mock import patch

from odoo.tests import tagged

from odoo.addons.quimibond_sgi.tests.common_calendar import sgi_test_calendar

from .common_kb import KbCase


def _fake_text(self, attachment):
    return "Texto del instructivo de prueba.", "texto de prueba"


@tagged('post_install', '-at_install')
class TestKbHashes(KbCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_test_calendar(cls.env)
        cls.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.mp_sign_required', 'False')
        cls.it = cls._doc('IT-ZK2-05', 'instructivo', 'Arrancar la línea')
        cls.co = cls._doc('CO-ZK2-05', 'control_operacional', 'Control de residuos peligrosos',
                          sgi_previous_code='CO-P-Z99-01')
        cls.activity = cls._activity('Arrancar línea ZK', doc=cls.it, measure_cadence='diaria')

    def _import(self, docs, user=None):
        with patch.object(type(self.env['sgi.knowledge.import']), '_sgi_pdf_text', _fake_text):
            return super()._import(docs, user=user)

    def _miid_hash(self):
        Miid = self.env['sgi.miid']
        miid = Miid._sgi_get(self.company)
        self.env.invalidate_all()
        return Miid._sgi_hash(miid._sgi_snapshot()), miid._sgi_snapshot()

    def test_01_mi_procedimiento_igual(self):
        h0 = self.job._sgi_my_procedure_data()['hash']
        self.Article._sgi_kb_seed()
        self._import(self.it)
        article = self._article('IT-ZK2-05')
        self.assertEqual(self.activity.instruction_article_id, article)
        role = self.activity.role_ids[:1]
        self.assertFalse(role.mp_article_id, "El borrador no se ofrece en la tarjeta.")
        self.env.invalidate_all()
        self.assertEqual(self.job._sgi_my_procedure_data()['hash'], h0,
                         "Sembrar, importar y ligar no cambian la huella de Mi procedimiento.")

    def test_02_miid_igual(self):
        h0, _snap = self._miid_hash()
        self.Article._sgi_kb_seed()
        self._import(self.co | self.it)
        h1, _snap = self._miid_hash()
        self.assertEqual(h1, h0, "Sembrar e importar no cambian la huella del MIID.")

    def test_03_publicar_si_cambia(self):
        _h0, before = self._miid_hash()
        self.assertEqual(before['controls']['CO-ZK2-05'][0], 0)
        self.Article._sgi_kb_seed()
        self._import(self.co)
        article = self._article('CO-ZK2-05')
        self.env['sgi.instruction.publish'].with_user(self.mast).create(
            {'article_id': article.id}).action_publish()
        _h1, after = self._miid_hash()
        self.assertEqual(after['controls']['CO-ZK2-05'][0], 1, "La revisión sube 1.")
        self.assertEqual(after['controls']['CO-ZK2-05'][1:], before['controls']['CO-ZK2-05'][1:],
                         "Mismo título y misma clave anterior.")
