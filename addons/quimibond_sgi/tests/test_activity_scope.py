# -*- coding: utf-8 -*-
"""Líneas de negocio (56.16.0): «Aplica a» en las actividades, filtro que
trae también las generales, diagrama atenuado, «Quién hace qué» por línea y
departamento, «Mi procedimiento» por líneas del puesto o de la persona, e
indicadores medidos por línea con equipo de ventas o etiqueta del pedido."""
from datetime import date, datetime

from odoo.exceptions import ValidationError
from odoo.tests import TransactionCase, tagged, new_test_user

from .common_calendar import sgi_test_calendar
from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestActivityScope(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        sgi_test_calendar(cls.env)
        env = cls.env
        Scope = env['sgi.activity.scope']
        cls.industrial = Scope.create({'name': 'ZS Industrial'})
        cls.confeccion = Scope.create({'name': 'ZS Confección'})
        cls.nacional = Scope.create({'name': 'ZS Nacional'})
        cls.exportacion = Scope.create({'name': 'ZS Exportación'})
        cls.dept = env['hr.department'].create({'name': 'ZS Ventas'})
        Job = env['hr.job']
        cls.job_conf = Job.create({'name': 'ZS VENDEDOR CONFECCION', 'department_id': cls.dept.id,
                                   'sgi_scope_ids': [(6, 0, (cls.confeccion | cls.nacional).ids)]})
        cls.job_all = Job.create({'name': 'ZS VENDEDOR', 'department_id': cls.dept.id})
        cls.user = new_test_user(env, login='zs_scope_user',
                                 groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.emp_conf = env['hr.employee'].create({
            'name': 'ZS Vendedora Confección', 'job_id': cls.job_conf.id, 'user_id': cls.user.id})
        cls.emp_all = env['hr.employee'].create({'name': 'ZS Vendedor', 'job_id': cls.job_all.id})
        cls.process = env['sgi.process'].create({'code': 'ZSC', 'name': 'ZS Pedido a entrega'})
        Stage = env['sgi.process.stage']
        cls.stage_a = Stage.create({'process_id': cls.process.id, 'code': 'A', 'name': 'Captura'})
        cls.stage_e = Stage.create({'process_id': cls.process.id, 'code': 'E', 'name': 'Exportación'})

        def act(name, scopes, stage, job=None):
            roles = [(0, 0, {'role': 'ejecuta', 'job_id': (job or cls.job_all).id})]
            return env['sgi.process.activity'].create({
                'process_id': cls.process.id, 'name': name, 'stage_id': stage.id,
                'scope_ids': [(6, 0, scopes.ids)], 'role_ids': roles,
                'measure_cadence': 'evento'})

        cls.a_industrial = act('Capturar pedido industrial', cls.industrial, cls.stage_a, cls.job_conf)
        cls.a_confeccion = act('Capturar pedido confección', cls.confeccion, cls.stage_a, cls.job_conf)
        cls.a_general = act('Confirmar fecha compromiso', Scope, cls.stage_a, cls.job_conf)
        cls.a_export_1 = act('Armar carpeta de exportación', cls.exportacion, cls.stage_e)
        cls.a_export_2 = act('Validar pedimento', cls.exportacion, cls.stage_e)
        cls.all_acts = (cls.a_industrial | cls.a_confeccion | cls.a_general
                        | cls.a_export_1 | cls.a_export_2)

    def _search(self, *domain, user=None):
        Activity = self.env['sgi.process.activity']
        if user:
            Activity = Activity.with_user(user)
        return Activity.search([('process_id', '=', self.process.id)] + list(domain))

    # ------------------------------------------------------------------
    def test_01_filtro_trae_la_linea_y_las_generales(self):
        conf = self._search(('scope_filter_id', '=', self.confeccion.id), user=self.user)
        self.assertEqual(conf, self.a_confeccion | self.a_general,
                         "Confección: sus actividades más las generales, sin industrial ni exportación.")
        self.assertEqual(self._search(('scope_filter_id', 'in', [self.confeccion.id])), conf)
        self.assertEqual(self._search(('scope_filter_id', 'ilike', 'ZS Confec')), conf,
                         "Escribir el nombre en la búsqueda da lo mismo que elegir la línea.")
        self.assertEqual(self._search(('scope_filter_id', 'not in', [self.confeccion.id])),
                         self.a_industrial | self.a_export_1 | self.a_export_2)
        self.assertEqual(self._search(('scope_ids', '=', False)), self.a_general,
                         "«Aplican a todas las líneas».")
        groups = self.env['sgi.process.activity']._read_group(
            [('process_id', '=', self.process.id)], ['scope_ids'], ['__count'])
        counts = {scope: count for scope, count in groups}
        self.assertEqual(counts[self.exportacion], 2, "Se agrupa por «Aplica a».")
        self.assertIn(self.process, self.env['sgi.process'].search(
            [('activity_scope_ids', 'in', self.exportacion.ids)]),
            "En la lista de procesos: «Tiene actividades de…».")
        self.assertEqual(self.exportacion.activity_count, 2)

    def test_02_c2_real(self):
        """En una copia de producción ya migrada: C2 con «Confección» son 16
        actividades, sin C2.04 ni la etapa E."""
        c2 = self.env['sgi.process'].search([('code', '=', 'C2')], limit=1)
        c204 = self.env['sgi.process.activity'].search(
            [('process_id', '=', c2.id), ('number', '=', 'C2.04')], limit=1)
        confeccion = self.env.ref('quimibond_sgi.sgi_scope_confeccion', raise_if_not_found=False)
        if not (c2 and c204.scope_ids and confeccion):
            self.skipTest("Sin C2 etiquetado (base sin datos de producción).")
        found = self.env['sgi.process.activity'].search(
            [('process_id', '=', c2.id), ('scope_filter_id', '=', confeccion.id)])
        self.assertNotIn(c204, found)
        self.assertFalse(found.filtered(lambda a: a.stage_id.code == 'E'))
        total = self.env['sgi.process.activity'].search_count([('process_id', '=', c2.id)])
        stage_e = self.env['sgi.process.activity'].search_count(
            [('process_id', '=', c2.id), ('stage_id.code', '=', 'E')])
        self.assertEqual(len(found), total - 1 - stage_e)

    def test_03_mi_procedimiento_por_lineas_del_puesto(self):
        detail = self.job_conf._sgi_mp_role_lists()['detail'].activity_id
        self.assertEqual(detail, self.a_confeccion | self.a_general,
                         "El vendedor de confección no ve el pedido industrial.")
        self.assertNotIn(self.a_industrial, self.emp_conf.sgi_mp_role_ids.activity_id)
        data = self.job_conf.with_user(self.user)._sgi_my_procedure_data()
        shown = {e['activity'] for s in data['sections'] for e in s['entries']}
        self.assertNotIn(self.a_industrial, shown)
        self.assertEqual(data['cover']['scopes'], self.confeccion | self.nacional)
        self.assertEqual(self.job_conf.sgi_execute_count, 2, "El conteo también respeta las líneas.")
        # Sin líneas en el puesto: todas.
        self.assertEqual(self.job_all._sgi_mp_role_lists()['detail'].activity_id,
                         self.a_export_1 | self.a_export_2)
        # Quitarle la etiqueta a la actividad la regresa al procedimiento guardado.
        self.a_industrial.scope_ids = False
        self.assertIn(self.a_industrial, self.emp_conf.sgi_mp_role_ids.activity_id)

    def test_04_lineas_de_la_persona(self):
        """La persona con menos líneas que su puesto ve solo las suyas; sin
        líneas propias, las del puesto."""
        self.emp_conf.sgi_scope_ids = self.industrial
        self.assertEqual(self.emp_conf.sgi_mp_role_ids.activity_id,
                         self.a_industrial | self.a_general)
        data = self.job_conf.with_context(sgi_mp_employee_id=self.emp_conf.id)._sgi_my_procedure_data()
        shown = {e['activity'] for s in data['sections'] for e in s['entries']}
        self.assertEqual(shown, {self.a_industrial, self.a_general})
        self.emp_conf.sgi_scope_ids = False
        self.assertEqual(self.emp_conf.sgi_mp_role_ids.activity_id,
                         self.a_confeccion | self.a_general)

    def test_05_diagrama_atenua_lo_que_no_aplica(self):
        Diagram = self.env['sgi.diagram'].with_user(self.user)
        data = Diagram.data('process_flow', self.process.id, {'linea': str(self.confeccion.id)})
        items = {i['res_id']: i for lane in data['lanes'] for i in lane['items']
                 if i['model'] == 'sgi.process.activity'}
        self.assertTrue(items[self.a_industrial.id]['out'])
        self.assertTrue(items[self.a_export_1.id]['out'])
        self.assertFalse(items[self.a_confeccion.id]['out'])
        self.assertFalse(items[self.a_general.id]['out'])
        self.assertIn('linea', [o['name'] for o in data['param_options']])
        data = Diagram.data('process_flow', self.process.id, {'linea': ''})
        self.assertFalse([i for lane in data['lanes'] for i in lane['items'] if i.get('out')],
                         "Sin línea elegida no se atenúa nada.")
        data = Diagram.data('process_flow', self.process.id,
                            {'carriles': 'etapa', 'linea': str(self.exportacion.id)})
        items = {i['res_id']: i for lane in data['lanes'] for i in lane['items']}
        self.assertTrue(items[self.a_confeccion.id]['out'])
        self.assertFalse(items[self.a_export_2.id]['out'])
        # Quién hace qué (matriz): con Exportación, al vendedor de confección
        # solo le queda la actividad general.
        job_key = 'hr.job,%d' % self.job_conf.id
        process_key = 'sgi.process,%d' % self.process.id

        def cell(params):
            matrix = Diagram.data('who_does_what', None, params)['matrix']
            return matrix['cells'].get(job_key, {}).get(process_key, {}).get('value')
        self.assertEqual(cell({}), 3)
        self.assertEqual(cell({'linea': str(self.exportacion.id)}), 1)

    def test_06_quien_hace_que_por_linea_y_departamento(self):
        Stat = self.env['sgi.activity.exec.stat']
        stat = Stat.create({'activity_id': self.a_export_1.id, 'period_start': date(2046, 3, 2),
                            'job_id': self.job_all.id, 'exec_class': 'correcto', 'count': 3})
        self.assertEqual(stat.department_id, self.dept)
        self.assertEqual(stat.scope_ids, self.exportacion)
        found = Stat.with_user(self.user).search([('scope_filter_id', '=', self.confeccion.id),
                                                  ('id', '=', stat.id)])
        self.assertFalse(found, "Exportación no sale al filtrar Confección.")
        groups = Stat._read_group([('id', '=', stat.id)], ['department_id', 'scope_ids'], ['count:sum'])
        self.assertEqual(groups, [(self.dept, self.exportacion, 3)])
        self.a_export_1.scope_ids = self.nacional
        self.assertEqual(stat.scope_ids, self.nacional, "Sigue a la actividad.")


@tagged('post_install', '-at_install')
class TestIndicatorByScope(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        env['ir.config_parameter'].sudo().set_param('quimibond_sgi.kpi_company_id', env.company.id)
        Team = env['crm.team']
        cls.team_conf = Team.create({'name': 'ZS Equipo confección'})
        cls.team_ind = Team.create({'name': 'ZS Equipo industrial'})
        cls.tag_export = env['crm.tag'].create({'name': 'ZS Exportación'})
        Scope = env['sgi.activity.scope']
        cls.conf = Scope.create({'name': 'ZS Confección', 'team_ids': [(6, 0, cls.team_conf.ids)]})
        cls.ind = Scope.create({'name': 'ZS Industrial', 'team_ids': [(6, 0, cls.team_ind.ids)]})
        cls.export = Scope.create({'name': 'ZS Exportación', 'crm_tag_ids': [(6, 0, cls.tag_export.ids)]})
        cls.partner = env['res.partner'].create({'name': 'ZS Cliente Linea'})
        when = datetime(2046, 3, 10, 12, 0)
        Order = env['sale.order']
        for team, tags in ((cls.team_conf, []), (cls.team_conf, [cls.tag_export.id]),
                           (cls.team_ind, [])):
            Order.create({'partner_id': cls.partner.id, 'team_id': team.id,
                          'date_order': when, 'tag_ids': [(6, 0, tags)]})
        cls.Indicator = env['sgi.indicator']
        cls.model_order = env['ir.model']._get('sale.order')

    def _indicator(self, code, model=None, domain=None, date_field='date_order'):
        ind = self.Indicator.create({'code': code, 'name': 'Pedidos %s' % code,
                                     'calc_mode': 'configurable', 'direction': 'higher_better'})
        self.env['sgi.indicator.term'].create({
            'indicator_id': ind.id, 'role': 'numerator',
            'model_id': (model or self.model_order).id,
            'domain': domain or "[('partner_id.name', '=', 'ZS Cliente Linea')]",
            'date_field': date_field})
        return ind

    def test_01_una_medicion_por_linea(self):
        ind = self._indicator('ZS-L1')
        self.assertIn('team_id', ind.scope_support)
        ind.write({'measure_by_scope': True,
                   'measure_scope_ids': [(6, 0, (self.conf | self.ind | self.export).ids)]})
        result = ind.sgi_recalculate(period_date=date(2046, 3, 1), save=True)[0]
        measure = self.env['sgi.indicator.measure'].browse(result['measure_id'])
        self.assertEqual(measure.value, 3.0, "La medición oficial sigue siendo el total.")
        by_scope = {line.scope_id: line.value for line in measure.scope_line_ids}
        self.assertEqual(by_scope, {self.conf: 2.0, self.ind: 1.0, self.export: 1.0},
                         "Por equipo de ventas y, la de exportación, por etiqueta del pedido.")
        line = measure.scope_line_ids.filtered(lambda l: l.scope_id == self.ind)
        self.assertEqual(len(line.detail_ids.split(',')), 1)
        self.assertEqual(line.action_view_evidence()['res_model'], 'sale.order')
        # Recalcular reemplaza el desglose, no lo duplica.
        ind.sgi_recalculate(period_date=date(2046, 3, 1), save=True)
        self.assertEqual(len(measure.scope_line_ids), 3)
        # El usuario SGI lo lee.
        user = new_test_user(self.env, login='zs_scope_kpi',
                             groups='base.group_user,quimibond_sgi.group_sgi_user')
        self.assertEqual(len(self.env['sgi.indicator.measure.scope'].with_user(user).search(
            [('measure_id', '=', measure.id)])), 3)

    def test_02_sin_equipo_ni_etiqueta_no_se_puede(self):
        ind = self._indicator('ZS-L2', model=self.env['ir.model']._get('sgi.indicator.measure'),
                              domain="[]", date_field='period_date')
        self.assertIn('no trae equipo', ind.scope_support)
        with self.assertRaises(ValidationError):
            ind.measure_by_scope = True

    def test_03_sin_marcar_no_hay_desglose(self):
        ind = self._indicator('ZS-L3')
        vals = ind._sgi_measure_vals(date(2046, 3, 1), date(2046, 3, 31))
        self.assertNotIn('scope_line_ids', vals)
        self.assertEqual(vals['value'], 3.0)
