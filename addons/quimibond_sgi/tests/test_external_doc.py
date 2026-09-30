# -*- coding: utf-8 -*-
"""Documento externo (E2.36, 56.21.0): emisor, revisión del emisor,
recepción, proceso dueño y plazo de implantación de 10 días hábiles."""
from datetime import date

from odoo.tests import TransactionCase, freeze_time, tagged, new_test_user

from .common_calendar import sgi_test_calendar
from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestExternalDoc(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        sgi_test_calendar(cls.env)
        cls.owner = new_test_user(cls.env, login='zs_ext_owner', groups='base.group_user')
        emp = cls.env['hr.employee'].create({'name': 'ZS Dueño externo', 'user_id': cls.owner.id})
        cls.process = cls.env['sgi.process'].create({'code': 'ZEX', 'name': 'ZS Calidad', 'owner_id': emp.id})

    def _doc(self, **vals):
        return self.env['documents.document'].create(dict({
            'name': 'ZS Especificación cliente', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'externo', 'sgi_code': 'EXT-Z01', 'sgi_process_id': self.process.id,
            'sgi_ext_issuer': 'Contitech', 'sgi_ext_issuer_revision': 'C'}, **vals))

    def test_01_plazo_de_10_dias_habiles(self):
        doc = self._doc(sgi_ext_received_date=date(2046, 3, 2))  # viernes
        self.assertEqual(doc.sgi_ext_deadline, date(2046, 3, 16), "10 días hábiles: el viernes 16.")
        self.assertEqual(doc.sgi_ext_state, 'por_implantar')
        doc.sgi_ext_implemented_date = date(2046, 3, 10)
        self.assertEqual(doc.sgi_ext_state, 'implantado')

    def test_02_aviso_al_dueno_del_proceso(self):
        doc = self._doc(sgi_ext_received_date=date(2046, 3, 2))
        Cron = self.env['sgi.cron']
        # 57.66.0: el cron usa el «hoy» del SGI (sgi_today, 57.15.0), no
        # context_today: se congela el reloj en vez de parchar context_today.
        with freeze_time('2046-03-14 12:00:00'):
            Cron._sgi_external_doc_notices()
        activity = doc.activity_ids.filtered(lambda a: 'EXT-Z01' in (a.summary or ''))
        self.assertEqual(activity.user_id, self.owner)
        with freeze_time('2046-03-20 12:00:00'):
            Cron._sgi_external_doc_notices()
        self.assertEqual(doc.sgi_ext_state, 'vencido')
        self.assertTrue(doc.activity_ids.filtered(lambda a: (a.summary or '').startswith('Vencido')))

    def test_03_no_externo_no_tiene_plazo(self):
        # IT-{proceso}-{nn} (56.32.0, D-02): «IT-Z09» no cumple la nomenclatura.
        doc = self._doc(sgi_doc_type='instructivo', sgi_code='IT-ZEX-09',
                        sgi_ext_received_date=date(2046, 3, 2))
        self.assertFalse(doc.sgi_ext_deadline)
        self.assertFalse(doc.sgi_ext_state)
