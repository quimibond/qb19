# -*- coding: utf-8 -*-
"""Líneas de negocio con lo que Odoo ya tiene (56.16.0): equipos de venta y
posiciones fiscales en las actividades, departamento calculado de los puestos
que ejecutan, filtro que trae también las generales, diagrama atenuado,
«Quién hace qué» por equipo y departamento, «Mi procedimiento» según los
equipos de venta de cada persona e indicadores desglosados por equipo o por
mercado."""
from datetime import date, datetime

from odoo.exceptions import ValidationError
from odoo.tests import TransactionCase, tagged, new_test_user

from .common_calendar import sgi_test_calendar
from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestBusinessLine(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        sgi_test_calendar(cls.env)
        env = cls.env
        Team = env['crm.team']
        cls.industrial = Team.create({'name': 'ZS Industrial'})
        cls.confeccion = Team.create({'name': 'ZS Confección'})
        cls.export = env['account.fiscal.position'].create({'name': 'ZS Cliente extranjero'})
        cls.dept_ventas = env['hr.department'].create({'name': 'ZS Ventas'})
        cls.dept_embarques = env['hr.department'].create({'name': 'ZS Embarques'})
        Job = env['hr.job']
        cls.job_seller = Job.create({'name': 'ZS VENDEDOR', 'department_id': cls.dept_ventas.id})
        cls.job_ship = Job.create({'name': 'ZS EMBARQUES', 'department_id': cls.dept_embarques.id})
        groups = 'base.group_user,quimibond_sgi.group_sgi_user'
        cls.user_conf = new_test_user(env, login='zs_line_conf', groups=groups)
        cls.user_ind = new_test_user(env, login='zs_line_ind', groups=groups)
        cls.confeccion.member_ids = cls.user_conf
        cls.industrial.member_ids = cls.user_ind
        Employee = env['hr.employee']
        cls.emp_conf = Employee.create({'name': 'ZS Vendedora Confección', 'job_id': cls.job_seller.id,
                                        'user_id': cls.user_conf.id})
        cls.emp_ind = Employee.create({'name': 'ZS Vendedor Industrial', 'job_id': cls.job_seller.id,
                                       'user_id': cls.user_ind.id})
        cls.process = env['sgi.process'].create({'code': 'ZSC', 'name': 'ZS Pedido a entrega'})
        Stage = env['sgi.process.stage']
        stage_a = Stage.create({'process_id': cls.process.id, 'code': 'A', 'name': 'Captura'})
        stage_e = Stage.create({'process_id': cls.process.id, 'code': 'E', 'name': 'Exportación'})

        def act(name, teams, stage, job, positions=None):
            return env['sgi.process.activity'].create({
                'process_id': cls.process.id, 'name': name, 'stage_id': stage.id,
                'sale_team_ids': [(6, 0, teams.ids)],
                'fiscal_position_ids': [(6, 0, (positions or env['account.fiscal.position']).ids)],
                'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': job.id})],
                'measure_cadence': 'evento'})

        cls.a_industrial = act('Capturar pedido industrial', cls.industrial, stage_a, cls.job_seller)
        cls.a_confeccion = act('Capturar pedido confección', cls.confeccion, stage_a, cls.job_seller)
        cls.a_general = act('Confirmar fecha compromiso', Team, stage_a, cls.job_seller)
        cls.a_export_1 = act('Armar carpeta de exportación', cls.industrial, stage_e, cls.job_ship, cls.export)
        cls.a_export_2 = act('Validar pedimento', cls.industrial, stage_e, cls.job_ship, cls.export)

    def _search(self, *domain, user=None):
        Activity = self.env['sgi.process.activity']
        if user:
            Activity = Activity.with_user(user)
        return Activity.search([('process_id', '=', self.process.id)] + list(domain))

    # ------------------------------------------------------------------
    def test_01_filtro_por_equipo_trae_las_generales(self):
        conf = self._search(('team_filter_id', '=', self.confeccion.id), user=self.user_conf)
        self.assertEqual(conf, self.a_confeccion | self.a_general,
                         "Confección: lo suyo más lo general, sin industrial ni exportación.")
        self.assertEqual(self._search(('team_filter_id', 'in', [self.confeccion.id])), conf)
        self.assertEqual(self._search(('team_filter_id', 'ilike', 'ZS Confec')), conf,
                         "Escribir el nombre da lo mismo que elegir el equipo.")
        self.assertEqual(self._search(('team_filter_id', 'not in', [self.confeccion.id])),
                         self.a_industrial | self.a_export_1 | self.a_export_2)
        self.assertEqual(len(self._search(('market_filter_id', '=', self.export.id))), 5,
                         "Mercado: lo de exportación más lo que no dice mercado.")
        other = self.env['account.fiscal.position'].create({'name': 'ZS Nacional'})
        self.assertEqual(self._search(('market_filter_id', '=', other.id)),
                         self.a_industrial | self.a_confeccion | self.a_general)
        self.assertEqual(self._search(('sale_team_ids', '=', False)), self.a_general)
        counts = dict(self.env['sgi.process.activity']._read_group(
            [('process_id', '=', self.process.id)], ['sale_team_ids'], ['__count']))
        self.assertEqual(counts[self.industrial], 3, "Se agrupa por equipo de ventas.")
        self.assertIn(self.process, self.env['sgi.process'].search(
            [('activity_team_ids', 'in', self.confeccion.ids)]))

    def test_02_departamento_sale_de_los_puestos(self):
        self.assertEqual(self.a_export_1.department_ids, self.dept_embarques)
        self.assertEqual(self._search(('department_ids', 'in', self.dept_ventas.ids)),
                         self.a_industrial | self.a_confeccion | self.a_general)
        self.job_ship.department_id = self.dept_ventas
        self.assertEqual(self.a_export_1.department_ids, self.dept_ventas, "Sigue al puesto.")

    def test_03_c2_real(self):
        """En una copia de producción ya migrada: C2 con el equipo Confección
        son 16 actividades, sin C2.04 ni la etapa E."""
        c2 = self.env['sgi.process'].search([('code', '=', 'C2')], limit=1)
        c204 = self.env['sgi.process.activity'].search(
            [('process_id', '=', c2.id), ('number', '=', 'C2.04')], limit=1)
        conf = self.env['crm.team'].search([('name', '=', 'Confección')], limit=1)
        if not (c2 and c204.sale_team_ids and conf):
            self.skipTest("Sin C2 migrado (base sin datos de producción).")
        found = self.env['sgi.process.activity'].search(
            [('process_id', '=', c2.id), ('team_filter_id', '=', conf.id)])
        self.assertNotIn(c204, found)
        self.assertFalse(found.filtered(lambda a: a.stage_id.code == 'E'))

    def test_04_mi_procedimiento_por_equipos_de_la_persona(self):
        self.assertEqual(self.emp_conf.sgi_team_ids, self.confeccion)
        self.assertEqual(self.emp_conf.sgi_mp_role_ids.activity_id, self.a_confeccion | self.a_general,
                         "La vendedora de confección no ve el pedido industrial.")
        self.assertEqual(self.emp_ind.sgi_mp_role_ids.activity_id, self.a_industrial | self.a_general)
        data = self.job_seller.with_user(self.user_conf).with_context(
            sgi_mp_employee_id=self.emp_conf.id)._sgi_my_procedure_data()
        shown = {e['activity'] for s in data['sections'] for e in s['entries']}
        self.assertEqual(shown, {self.a_confeccion, self.a_general})
        self.assertEqual(data['cover']['teams'], self.confeccion)
        # El puesto: los equipos de todas sus personas.
        self.assertEqual(self.job_seller.sgi_team_ids, self.industrial | self.confeccion)
        self.assertEqual(self.job_seller.sgi_execute_count, 3)

    def test_05_cambiar_de_equipo_cambia_el_procedimiento(self):
        self.industrial.member_ids = self.user_ind | self.user_conf
        self.assertIn(self.a_industrial, self.emp_conf.sgi_mp_role_ids.activity_id,
                      "Al entrar a Industrial en Ventas, le aparece lo industrial.")
        self.a_confeccion.sale_team_ids = False
        self.assertIn(self.a_confeccion, self.emp_ind.sgi_mp_role_ids.activity_id)
        # Una persona sin equipo ve todo, y entonces el puesto también.
        emp = self.env['hr.employee'].create({'name': 'ZS Sin equipo', 'job_id': self.job_seller.id})
        self.assertIn(self.a_industrial, emp.sgi_mp_role_ids.activity_id)
        self.assertFalse(self.job_seller.sgi_team_ids)

    def test_06_diagrama_atenua_lo_que_no_aplica(self):
        Diagram = self.env['sgi.diagram'].with_user(self.user_conf)

        def items(params):
            data = Diagram.data('process_flow', self.process.id, params)
            return {i['res_id']: i for lane in data['lanes'] for i in lane['items']
                    if i['model'] == 'sgi.process.activity'}, data
        found, data = items({'equipo': str(self.confeccion.id)})
        self.assertTrue(found[self.a_industrial.id]['out'])
        self.assertTrue(found[self.a_export_1.id]['out'])
        self.assertFalse(found[self.a_confeccion.id]['out'])
        self.assertFalse(found[self.a_general.id]['out'])
        names = [o['name'] for o in data['param_options']]
        self.assertIn('equipo', names)
        self.assertIn('mercado', names)
        found, _data = items({})
        self.assertFalse([i for i in found.values() if i['out']], "Sin elegir nada no se atenúa.")
        found, _data = items({'carriles': 'etapa', 'mercado': str(self.export.id)})
        self.assertFalse(found[self.a_export_2.id]['out'])
        self.assertFalse(found[self.a_confeccion.id]['out'], "Sin mercado capturado aplica a todos.")
        # Matriz «Quién hace qué»: con Confección al vendedor le quedan 2.
        job_key = 'hr.job,%d' % self.job_seller.id
        process_key = 'sgi.process,%d' % self.process.id

        def cell(params):
            matrix = Diagram.data('who_does_what', None, params)['matrix']
            return matrix['cells'].get(job_key, {}).get(process_key, {}).get('value')
        self.assertEqual(cell({}), 3)
        self.assertEqual(cell({'equipo': str(self.confeccion.id)}), 2)

    def test_07_quien_hace_que_por_equipo_y_departamento(self):
        Stat = self.env['sgi.activity.exec.stat']
        stat = Stat.create({'activity_id': self.a_export_1.id, 'period_start': date(2046, 3, 2),
                            'job_id': self.job_ship.id, 'exec_class': 'correcto', 'count': 3})
        self.assertEqual(stat.department_id, self.dept_embarques)
        self.assertEqual(stat.sale_team_ids, self.industrial)
        self.assertFalse(Stat.with_user(self.user_conf).search(
            [('team_filter_id', '=', self.confeccion.id), ('id', '=', stat.id)]))
        groups = Stat._read_group([('id', '=', stat.id)], ['department_id', 'sale_team_ids'], ['count:sum'])
        self.assertEqual(groups, [(self.dept_embarques, self.industrial, 3)])


@tagged('post_install', '-at_install')
class TestIndicatorSplit(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        company = env.company
        env['ir.config_parameter'].sudo().set_param('quimibond_sgi.kpi_company_id', company.id)
        company.country_id = env.ref('base.mx')
        Team = env['crm.team']
        cls.team_conf = Team.create({'name': 'ZS Equipo confección', 'company_id': company.id})
        cls.team_ind = Team.create({'name': 'ZS Equipo industrial', 'company_id': company.id})
        Partner = env['res.partner']
        mx = Partner.create({'name': 'ZS Cliente MX', 'country_id': env.ref('base.mx').id})
        us = Partner.create({'name': 'ZS Cliente US', 'country_id': env.ref('base.us').id})
        when = datetime(2046, 3, 10, 12, 0)
        for team, partner in ((cls.team_conf, mx), (cls.team_conf, mx), (cls.team_ind, us)):
            env['sale.order'].create({'partner_id': partner.id, 'team_id': team.id,
                                      'date_order': when, 'client_order_ref': 'ZS-LINEA'})
        cls.Indicator = env['sgi.indicator']

    def _indicator(self, code, model='sale.order', domain="[('client_order_ref', '=', 'ZS-LINEA')]",
                   date_field='date_order'):
        ind = self.Indicator.create({'code': code, 'name': 'Pedidos %s' % code,
                                     'calc_mode': 'configurable'})
        self.env['sgi.indicator.term'].create({
            'indicator_id': ind.id, 'role': 'numerator',
            'model_id': self.env['ir.model']._get(model).id,
            'domain': domain, 'date_field': date_field})
        return ind

    def _rows(self, ind):
        result = ind.sgi_recalculate(period_date=date(2046, 3, 1), save=True)[0]
        measure = self.env['sgi.indicator.measure'].browse(result['measure_id'])
        return measure, {row.label: row.value for row in measure.split_ids}

    def test_01_por_equipo_de_ventas(self):
        ind = self._indicator('ZS-S1')
        self.assertIn('team_id', ind.split_support)
        ind.write({'measure_split': 'team',
                   'measure_team_ids': [(6, 0, (self.team_conf | self.team_ind).ids)]})
        measure, rows = self._rows(ind)
        self.assertEqual(measure.value, 3.0, "La oficial sigue siendo el total.")
        self.assertEqual(rows, {'ZS Equipo confección': 2.0, 'ZS Equipo industrial': 1.0})
        row = measure.split_ids.filtered(lambda r: r.team_id == self.team_ind)
        self.assertEqual(row.action_view_evidence()['res_model'], 'sale.order')
        self._rows(ind)
        self.assertEqual(len(measure.split_ids), 2, "Recalcular reemplaza, no duplica.")
        user = new_test_user(self.env, login='zs_split_kpi',
                             groups='base.group_user,quimibond_sgi.group_sgi_user')
        self.assertEqual(len(self.env['sgi.indicator.measure.split'].with_user(user).search(
            [('measure_id', '=', measure.id)])), 2)

    def test_02_por_mercado(self):
        ind = self._indicator('ZS-S2')
        ind.measure_split = 'market'
        _measure, rows = self._rows(ind)
        self.assertEqual(rows, {'Nacional': 2.0, 'Exportación': 1.0, 'Cliente sin país': 0.0})

    def test_03_sin_equipo_no_se_puede(self):
        ind = self._indicator('ZS-S3', model='sgi.indicator.measure', domain="[]", date_field='period_date')
        self.assertIn('no llega', ind.split_support)
        with self.assertRaises(ValidationError):
            ind.measure_split = 'team'

    def test_04_sin_desglose(self):
        ind = self._indicator('ZS-S4')
        vals = ind._sgi_measure_vals(date(2046, 3, 1), date(2046, 3, 31))
        self.assertNotIn('split_ids', vals)
        self.assertEqual(vals['value'], 3.0)
