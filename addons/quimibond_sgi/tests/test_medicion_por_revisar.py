# -*- coding: utf-8 -*-
"""57.106.0: «Medición por revisar»: evidencia que no aparece, atribución
débil y pantallas de acción de servidor que no van con su medición."""
from datetime import timedelta

from odoo import fields
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestMedicionPorRevisar(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.process = cls.env['sgi.process'].create({'code': 'ZMR', 'name': 'Proceso medición'})
        cls.partner_model = cls.env['ir.model']._get('res.partner')
        cls.act = cls.env['sgi.process.activity'].create({
            'process_id': cls.process.id, 'name': 'Dar de alta al proveedor',
            'measure_method': 'odoo', 'measure_cadence': 'evento',
            'measure_model_id': cls.partner_model.id, 'measure_user_field': 'create_uid',
            'measure_domain': "[('name', '=', 'ZMR nunca existe')]"})

    def _codes(self):
        self.act._sgi_refresh_spec_gaps()
        return set(self.act.spec_gap_ids.mapped('code'))

    def _age(self, days):
        self.env.cr.execute("UPDATE sgi_process_activity SET create_date = %s WHERE id = %s",
                            (fields.Datetime.now() - timedelta(days=days), self.act.id))
        self.act.invalidate_recordset(['create_date'])

    def test_01_evidencia_que_no_aparece(self):
        self.assertNotIn('measure_never', self._codes(), "Recién creada: espera su plazo.")
        self._age(90)
        self.assertIn('measure_never', self._codes())
        gap = self.act.spec_gap_ids.filtered(lambda g: g.code == 'measure_never')
        self.assertIn('nunca', gap.message)
        self.act.write({'measure_last_date': fields.Datetime.now() - timedelta(days=5)})
        self.assertNotIn('measure_never', self._codes())
        self.act.write({'measure_cadence': 'anual',
                        'measure_last_date': fields.Datetime.now() - timedelta(days=200)})
        self.assertNotIn('measure_never', self._codes(), "La anual espera su ventana de 380 días.")

    def test_02_atribucion_debil(self):
        self.assertNotIn('weak_attribution', self._codes())
        self.act.write({'measure_user_field': 'write_uid'})
        self.assertIn('weak_attribution', self._codes())
        self.act.write({'measure_user_field': 'create_uid', 'measure_count_other_job': 8,
                        'measure_count_generic': 2, 'measure_adherence_pct': 20.0})
        self.assertIn('weak_attribution', self._codes())
        self.act.write({'measure_adherence_pct': 80.0})
        self.assertNotIn('weak_attribution', self._codes())

    def test_03_menu_con_accion_de_servidor(self):
        users_model = self.env['ir.model']._get('res.users')
        server = self.env['ir.actions.server'].create({
            'name': 'Usuarios ZMR', 'model_id': users_model.id, 'state': 'code',
            'code': 'action = {}'})
        menu = self.env['ir.ui.menu'].create({
            'name': 'Usuarios ZMR', 'action': 'ir.actions.server,%d' % server.id})
        self.act.odoo_menu_id = menu
        self.assertFalse(self.act.odoo_action_id)
        self.assertIn('menu_model_mismatch', self._codes())
        server.model_id = self.partner_model
        self.assertNotIn('menu_model_mismatch', self._codes())
