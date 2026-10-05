# -*- coding: utf-8 -*-
"""57.109.0: «Nuevo indicador» con preguntas y vista previa, y la medición
de la actividad sin nombres técnicos."""
from odoo import fields
from odoo.exceptions import AccessError, UserError
from odoo.tests import TransactionCase, new_test_user, tagged


@tagged('post_install', '-at_install')
class TestIndicadorAsistente(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.manager = new_test_user(cls.env, login='ia_mast',
                                    groups='base.group_user,quimibond_sgi.group_sgi_manager')
        cls.plain = new_test_user(cls.env, login='ia_plain',
                                  groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.process = cls.env['sgi.process'].create({'code': 'ZIA', 'name': 'Proceso indicador asistente'})
        cls.source = cls.env['sgi.indicator.source'].create({
            'name': 'Contactos de prueba', 'model_name': 'res.partner',
            'domain': "[('name', '=like', 'ZIA %')]", 'date_field': 'create_date'})
        # La fórmula cuenta solo la compañía de los KPI (como en producción).
        company = cls.env['sgi.indicator']._sgi_kpi_company()
        cls.env['res.partner'].create([{'name': 'ZIA %d' % n, 'is_company': n % 2 == 0,
                                        'company_id': company.id} for n in range(4)])

    def _wizard(self, **vals):
        Wizard = self.env['sgi.indicator.wizard'].with_user(self.manager)
        return Wizard.create(dict({
            'name': 'Contactos que son empresa', 'code': 'ZIA-01', 'process_id': self.process.id,
            'kind': 'pct', 'source_id': self.source.id,
            'filter_domain': "[('is_company', '=', True)]"}, **vals))

    def test_01_crea_el_indicador_con_su_formula(self):
        self.assertEqual(self.env['sgi.indicator.wizard']._sgi_next_code(self.process), 'ZIA-01')
        wizard = self._wizard()
        self.assertTrue(wizard.preview_html, "La vista previa calcula sin guardar nada.")
        self.assertNotIn('No se puede calcular', wizard.preview_html)
        self.assertIn('Porcentaje de contactos de prueba', wizard.formula_sentence)
        self.assertFalse(self.env['sgi.indicator'].search([('code', '=', 'ZIA-01')]))
        action = wizard.action_create()
        indicator = self.env['sgi.indicator'].browse(action['res_id'])
        self.assertEqual((indicator.calc_mode, indicator.uom, indicator.status), ('configurable', '%', 'prueba'))
        self.assertEqual(sorted(indicator.term_ids.mapped('role')), ['denominator', 'numerator'])
        period = fields.Date.context_today(indicator).replace(day=1)
        result = indicator.sgi_recalculate(period_date=period)[0]
        self.assertEqual(result['value'], 50.0, "2 de 4 contactos son empresa.")
        with self.assertRaises(UserError):
            self._wizard(code='ZIA-01').action_create()

    def test_02_reglas(self):
        with self.assertRaises(UserError):
            self._wizard(code='ZIA-02', filter_domain='[]').action_create()
        with self.assertRaises(AccessError):
            self._wizard(code='ZIA-03').with_user(self.plain).action_create()
        count = self._wizard(code='ZIA-04', kind='count', filter_domain='[]')
        action = count.action_create()
        indicator = self.env['sgi.indicator'].browse(action['res_id'])
        self.assertEqual(len(indicator.term_ids), 1)
        records = count.action_view_records()
        self.assertEqual(records['res_model'], 'res.partner')

    def test_03_medicion_de_la_actividad_por_etiqueta(self):
        activity = self.env['sgi.process.activity'].create({
            'process_id': self.process.id, 'name': 'Dar de alta al contacto',
            'measure_method': 'odoo', 'measure_cadence': 'evento',
            'measure_model_id': self.env['ir.model']._get('res.partner').id,
            'measure_domain': "[('name', '=like', 'ZIA %')]"})
        create_uid = self.env['ir.model.fields']._get('res.partner', 'create_uid')
        write_date = self.env['ir.model.fields']._get('res.partner', 'write_date')
        activity.write({'measure_user_field_id': create_uid.id, 'measure_date_field_id': write_date.id})
        self.assertEqual((activity.measure_user_field, activity.measure_date_field), ('create_uid', 'write_date'))
        activity.invalidate_recordset(['measure_preview_html'])
        self.assertIn('registros en los últimos 30 días', activity.measure_preview_html)
