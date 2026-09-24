# -*- coding: utf-8 -*-
"""Actividades específicas: dónde, cómo, criterio de terminado, plazo, si
falla y escalamiento; publicar exige la especificación completa; medición de
aplicables, completas, a tiempo y vencidas."""
from datetime import datetime, timedelta

from odoo import fields
from odoo.exceptions import UserError, ValidationError
from odoo.tests import TransactionCase, tagged

from ..models.sgi_calendar import sgi_add_business_days
from .common_calendar import sgi_test_calendar


@tagged('post_install', '-at_install')
class TestSpec(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.job = cls.env['hr.job'].create({'name': 'PUESTO SPEC'})
        cls.env['hr.employee'].create({'name': 'Emp Spec', 'job_id': cls.job.id})
        cls.Process = cls.env['sgi.process']
        cls.Activity = cls.env['sgi.process.activity']
        cls.Deliverable = cls.env['sgi.deliverable']
        cls.process = cls.Process.create({'code': 'XS', 'name': 'Spec X'})
        cls.partner_model = cls.env['ir.model']._get_id('res.partner')
        sgi_test_calendar(cls.env)

    def _act(self, name, **vals):
        return self.Activity.create(dict({
            'process_id': self.process.id, 'name': name,
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job.id})],
        }, **vals))

    def _complete_act(self, name, **vals):
        """Una actividad sin faltantes de tipo error."""
        entry = self.Deliverable.create({'code': 'X-IN-%s' % name.replace(' ', '-'),
                                         'name': 'Entrada %s' % name})
        return self._act(name, **dict({
            'done_criteria': "Queda registrado", 'on_fail': "Avisar al dueño",
            'exec_channel': 'fisico', 'how_steps': "Hacer → registrar",
            'input_ids': [(0, 0, {'deliverable_id': entry.id, 'max_days': 1})],
        }, **vals))

    def _codes(self, act):
        return set(act.spec_gap_ids.mapped('code'))

    # 1
    def test_01_no_done_blocks_publish(self):
        act = self._complete_act('Registrar la merma', done_criteria=False)
        self.assertIn('no_done', self._codes(act))
        self.assertFalse(act.spec_complete)
        with self.assertRaises(UserError):
            self.process.action_sgi_publish()
        act.done_criteria = "La merma queda en Odoo con su causa"
        self.assertTrue(act.spec_complete)

    # 2
    def test_02_vague_verb_follows_parameter(self):
        act = self._complete_act('Dar seguimiento al cruce')
        self.assertIn('vague_verb', self._codes(act))
        self.env['ir.config_parameter'].set_param('quimibond_sgi.vague_verbs', 'gestionar')
        self.assertNotIn('vague_verb', self._codes(act), "Cambiar el parámetro recalcula.")

    # 3
    def test_03_compare_verb_without_against_is_warning(self):
        act = self._complete_act('Verificar precio')
        self.assertIn('no_check_against', self._codes(act))
        self.assertTrue(act.spec_complete, "Es advertencia, no error.")
        self.process.action_sgi_publish()
        self.assertEqual(self.process.state, 'vigente')

    # 4
    def test_04_escalation_needs_days(self):
        act = self._complete_act('Registrar la entrega')
        with self.assertRaises(ValidationError), self.env.cr.savepoint():
            act.write({'role_ids': [(0, 0, {'role': 'escala', 'relative_role': 'dueno_proceso',
                                            'target_type': 'relative'})]})
        act.write({'role_ids': [(0, 0, {'role': 'escala', 'relative_role': 'dueno_proceso',
                                        'target_type': 'relative', 'after_days': 3})]})
        self.assertNotIn('no_escalation', self._codes(act))
        self.assertIn('a los 3 días hábiles', act._sgi_sentence())

    # 5
    def test_05_due_weekday_needs_weekly_cadence(self):
        act = self._complete_act('Registrar el programa', measure_cadence='mensual',
                                 due_weekday='1')
        self.assertIn('due_mismatch', self._codes(act))
        act.write({'measure_cadence': 'semanal'})
        self.assertNotIn('due_mismatch', self._codes(act))

    def _partner_deliverables(self, tag, applies=None, complete=None):
        entry = self.Deliverable.create({
            'code': 'X-%s-IN' % tag, 'name': 'Entrada %s' % tag, 'odoo_model_id': self.partner_model,
            'measure_domain': "[('ref', '=', 'X-%s')]" % tag, 'measure_date_field': 'create_date'})
        out = self.Deliverable.create({
            'code': 'X-%s-OUT' % tag, 'name': 'Salida %s' % tag, 'odoo_model_id': self.partner_model,
            'measure_domain': "[('ref', '=', 'X-%s'), ('comment', '!=', False)]" % tag,
            'measure_date_field': 'write_date', 'complete_domain': complete or False})
        act = self._complete_act('Registrar %s' % tag, exec_channel='odoo',
                                 odoo_menu_id=self.env.ref('base.menu_administration').id,
                                 input_ids=[(0, 0, {'deliverable_id': entry.id, 'max_days': 1,
                                                    'applies_domain': applies or False})],
                                 output_deliverable_ids=[(6, 0, out.ids)],
                                 measure_method='entregable')
        return act

    # 6
    def test_06_applies_domain_filters_applicable_and_late(self):
        act = self._partner_deliverables(
            'T6', applies="[('country_id.code', '!=', 'MX')]")
        mx, us = self.env.ref('base.mx'), self.env.ref('base.us')
        partners = self.env['res.partner'].create([
            {'name': 'Nacional', 'ref': 'X-T6', 'country_id': mx.id},
            {'name': 'Exportación', 'ref': 'X-T6', 'country_id': us.id}])
        today = fields.Date.context_today(self.env['res.partner'])
        start = today - timedelta(days=today.weekday()) - timedelta(weeks=2)
        self.env.cr.execute("UPDATE res_partner SET create_date = %s WHERE id IN %s",
                            (datetime.combine(start, datetime.min.time()) + timedelta(hours=9),
                             tuple(partners.ids)))
        self.env.invalidate_all()
        counts = act._sgi_week_counts(start)
        self.assertEqual(counts['applicable_count'], 1, "El nacional no aplica.")
        self.assertEqual(counts['late_open_count'], 1, "Solo la exportación queda vencida.")

    # 7
    def test_07_complete_domain(self):
        act = self._partner_deliverables('T7', complete="[('email', '!=', False)]")
        self.env['res.partner'].create([
            {'name': 'A', 'ref': 'X-T7', 'comment': 'listo'},
            {'name': 'B', 'ref': 'X-T7', 'comment': 'listo'},
            {'name': 'C', 'ref': 'X-T7', 'comment': 'listo', 'email': 'c@x.com'}])
        today = fields.Date.context_today(self.env['res.partner'])
        counts = act._sgi_week_counts(today - timedelta(days=today.weekday()))
        self.assertEqual((counts['done_count'], counts['complete_count']), (3, 1))
        self.assertNotIn('measure_no_complete', self._codes(act))

    # 8
    def test_08_business_days_use_calendar(self):
        friday = datetime(2026, 9, 18, 10, 0)
        self.assertEqual(sgi_add_business_days(self.env, friday, 1), datetime(2026, 9, 21).date(),
                         "Una entrada del viernes con 1 día vence el lunes.")

    def _payload(self):
        return {
            'deliverables': [
                {'code': 'X-L-IN', 'name': 'Entrada carga', 'model': 'res.partner',
                 'domain': "[('ref', '=', 'X-L')]"},
                {'code': 'X-L-OUT', 'name': 'Salida carga', 'model': 'res.partner',
                 'domain': "[('ref', '=', 'X-L'), ('comment', '!=', False)]",
                 'complete_domain': "[('email', '!=', False)]",
                 'complete_criteria': "Trae correo"},
            ],
            'processes': [{'code': 'XL2', 'name': 'Carga spec', 'state': 'piloto', 'activities': [{
                'number': 1, 'name': 'Capturar el alta con correo',
                'check_against': "Constancia fiscal",
                'where': {'channel': 'odoo', 'menu': 'base.menu_administration', 'place': 'Oficina'},
                'how_steps': "Abrir → capturar → guardar",
                'done_criteria': "El contacto trae correo", 'on_fail': "Avisar a crédito",
                'cadence': 'mensual', 'due': {'business_day': 5},
                'inputs': [{'code': 'X-L-IN', 'days': 1,
                            'applies_domain': "[('is_company', '=', True)]",
                            'applies_note': "Solo empresas"}],
                'outputs': ['X-L-OUT'],
                'roles': [{'role': 'ejecuta', 'job': self.job.id},
                          {'role': 'escala', 'relative': 'dueno_proceso', 'after_days': 2}],
                'measure': {'method': 'entregable', 'deliverable': 'X-L-OUT'},
            }]}],
            'indicators': [{'code': 'XL2-01', 'process': 'XL2', 'name': 'Altas completas',
                            'formula': "Altas con correo / altas", 'source': "res.partner",
                            'target': 95, 'unit': '%', 'direction': 'up', 'frequency': 'monthly',
                            'baseline_value': None, 'target_date': '2026-12-31'}],
        }

    # 9
    def test_09_load_is_idempotent_with_new_keys(self):
        result = self.Process.load_payload(self._payload())
        self.assertTrue(result['ok'], result['errors'])
        act = self.Activity.search([('process_id.code', '=', 'XL2')])
        self.assertEqual(act.exec_channel, 'odoo')
        self.assertEqual(act.due_business_day, 5)
        self.assertEqual(act.input_ids.applies_note, "Solo empresas")
        self.assertEqual(act.role_ids.filtered(lambda r: r.role == 'escala').after_days, 2)
        self.assertEqual(act.measure_deliverable_id.complete_criteria, "Trae correo")
        self.assertEqual(act.process_id.state, 'piloto')
        indicator = self.env['sgi.indicator'].search([('code', '=', 'XL2-01')])
        self.assertEqual((indicator.target_objective, indicator.direction), (95, 'higher_better'))
        again = self.Process.load_payload(self._payload())
        self.assertFalse(again['changes'], "Segunda carga sin cambios.")

    # 10
    def test_10_unknown_nested_keys_are_errors(self):
        for mutate, path in (
            (lambda p: p['processes'][0]['activities'][0]['due'].update(dia=3),
             'processes[0].activities[0].due.dia'),
            (lambda p: p['processes'][0]['activities'][0]['where'].update(pantalla='x'),
             'processes[0].activities[0].where.pantalla'),
            (lambda p: p['processes'][0]['activities'][0]['roles'][1].update(dias=2),
             'processes[0].activities[0].roles[1].dias'),
        ):
            payload = self._payload()
            mutate(payload)
            result = self.Process.load_payload(payload, dry_run=True)
            self.assertFalse(result['ok'])
            self.assertIn(path, result['errors'][0]['message'])

    # 11
    def test_11_odoo_channel_needs_menu(self):
        act = self._complete_act('Registrar el pedido', exec_channel='odoo')
        self.assertIn('odoo_no_menu', self._codes(act))
        payload = self._payload()
        payload['processes'][0]['activities'][0]['where']['menu'] = 'no_existe.menu_x'
        result = self.Process.load_payload(payload, dry_run=True)
        self.assertFalse(result['ok'])
        self.assertTrue(any('no_existe.menu_x' in e['message'] for e in result['errors']))

    # 12
    def test_12_how_by_steps_or_instruction(self):
        act = self._complete_act('Registrar el ajuste', how_steps=False)
        self.assertIn('no_how', self._codes(act))
        act.how_steps = "Abrir → ajustar"
        self.assertNotIn('no_how', self._codes(act))

    # 13
    def test_13_open_in_odoo_uses_menu_action(self):
        menu = self.env.ref('base.menu_action_res_users')
        act = self._complete_act('Registrar el usuario', exec_channel='odoo',
                                 odoo_menu_id=menu.id)
        self.assertEqual(act.odoo_action_id, menu.action)
        self.assertEqual(act.action_open_odoo()['id'], menu.action.id)

    # 14
    def test_14_channel_percentages(self):
        for n in range(26):
            self._act('Registrar paso %d' % n, exec_channel='odoo' if n < 20 else 'papel')
        self.assertEqual(self.process.channel_odoo_pct, 77)
        self.assertEqual(self.process.channel_manual_pct, 23)

    # P-1: modos genéricos del indicador
    def test_15_indicator_entregable_completo(self):
        act = self._partner_deliverables('T15', complete="[('email', '!=', False)]")
        self.env['res.partner'].create([
            {'name': 'A', 'ref': 'X-T15', 'comment': 'listo'},
            {'name': 'B', 'ref': 'X-T15', 'comment': 'listo', 'email': 'b@x.com'},
            {'name': 'C', 'ref': 'X-T15', 'comment': 'listo', 'email': 'c@x.com'},
            {'name': 'D', 'ref': 'X-T15', 'comment': 'listo', 'email': 'd@x.com'}])
        ind = self.env['sgi.indicator'].create({
            'code': 'XS-COMPLETO', 'name': 'Completos', 'calc_mode': 'entregable_completo',
            'activity_id': act.id, 'process_id': self.process.id})
        today = fields.Date.context_today(self.env['res.partner'])
        self.assertEqual(ind._sgi_compute_value(today, today), 75.0,
                         "3 completos de 4 entregados; el entregable sale de la actividad.")
        self.assertIsNone(ind._sgi_compute_value(today - timedelta(days=30),
                                                 today - timedelta(days=20)),
                          "Sin entregas en el periodo queda pendiente, no 0.")
        # Faltantes: sin actividad ni entregable, o un entregable sin «completo».
        bare = self.env['sgi.indicator'].create({
            'code': 'XS-SIN', 'name': 'Sin entregable', 'calc_mode': 'entregable_completo'})
        self.assertTrue(any('sin entregable' in p for p in bare._sgi_spec_problems()))
        self.assertFalse(any('completo' in p and 'sin' in p for p in ind._sgi_spec_problems()
                             if 'entregable' in p))

    def test_16_indicator_actividad_a_tiempo(self):
        act = self._partner_deliverables('T16')
        ind = self.env['sgi.indicator'].create({
            'code': 'XS-TIEMPO', 'name': 'A tiempo', 'calc_mode': 'actividad_a_tiempo',
            'activity_id': act.id, 'process_id': self.process.id})
        today = fields.Date.context_today(self.env['res.partner'])
        monday = today - timedelta(days=today.weekday())
        # Sin nada medible en la semana: pendiente.
        self.assertIsNone(ind._sgi_compute_value(monday, monday + timedelta(days=6)))
        # Con las filas del cron: 3 a tiempo de 4 medibles en dos semanas.
        Stat = self.env['sgi.activity.week.stat']
        Stat.create([
            {'activity_id': act.id, 'period_start': monday, 'timed_count': 2, 'on_time_count': 2},
            {'activity_id': act.id, 'period_start': monday - timedelta(days=7),
             'timed_count': 2, 'on_time_count': 1}])
        self.assertEqual(ind._sgi_compute_value(monday - timedelta(days=7),
                                                monday + timedelta(days=6)), 75.0)
        self.assertEqual(ind._sgi_compute_value(monday, monday + timedelta(days=6)), 100.0)
        bare = self.env['sgi.indicator'].create({
            'code': 'XS-TIEMPO-SIN', 'name': 'Sin actividad', 'calc_mode': 'actividad_a_tiempo'})
        self.assertIn("«% a tiempo» sin actividad medida", bare._sgi_spec_problems())

    def test_17_load_indicator_with_activity_and_deliverable(self):
        act = self._partner_deliverables('T17', complete="[('email', '!=', False)]")
        out = act.output_deliverable_ids[:1]
        result = self.Process.load_payload({'indicators': [
            {'code': 'XS-L1', 'name': 'A tiempo cargado', 'process': 'XS',
             'calc_mode': 'actividad_a_tiempo', 'activity': act.number,
             'target': 90, 'unit': '%', 'formula': 'f', 'source': 's', 'frequency': 'weekly'},
            {'code': 'XS-L2', 'name': 'Completo cargado', 'process': 'XS',
             'calc_mode': 'entregable_completo', 'deliverable': out.code,
             'target': 95, 'unit': '%', 'formula': 'f', 'source': 's', 'frequency': 'monthly'},
        ]})
        self.assertTrue(result['ok'], result['errors'])
        Indicator = self.env['sgi.indicator']
        self.assertEqual(Indicator.search([('code', '=', 'XS-L1')]).activity_id, act)
        self.assertEqual(Indicator.search([('code', '=', 'XS-L2')]).deliverable_id, out)
        bad = self.Process.load_payload({'indicators': [
            {'code': 'XS-L3', 'name': 'x', 'process': 'XS', 'calc_mode': 'entregable_completo',
             'deliverable': 'NO-EXISTE'}]}, dry_run=True)
        self.assertFalse(bad['ok'])

    # 18 (P-4)
    def _task_deliverables(self, tag, due_field=None, offset_days=0):
        """Entrada y salida sobre project.task: la salida vence contra la
        fecha límite de la tarea (due_field) y no contra días desde que llegó."""
        model = self.env['ir.model']._get_id('project.task')
        entry = self.Deliverable.create({
            'code': 'X-%s-IN' % tag, 'name': 'Entrada %s' % tag, 'odoo_model_id': model,
            'measure_domain': "[('name', 'ilike', 'X-%s')]" % tag,
            'measure_date_field': 'create_date'})
        out = self.Deliverable.create({
            'code': 'X-%s-OUT' % tag, 'name': 'Salida %s' % tag, 'odoo_model_id': model,
            'measure_domain': "[('name', 'ilike', 'X-%s')]" % tag,
            'measure_date_field': 'create_date'})
        return self._complete_act('Cerrar %s' % tag, exec_channel='odoo',
                                  odoo_menu_id=self.env.ref('base.menu_administration').id,
                                  input_ids=[(0, 0, {'deliverable_id': entry.id, 'max_days': 0,
                                                     'due_field': due_field or False,
                                                     'offset_days': offset_days})],
                                  output_deliverable_ids=[(6, 0, out.ids)],
                                  measure_method='entregable')

    def test_18_due_field_and_offset_days(self):
        act = self._task_deliverables('T18', due_field='date_deadline', offset_days=-1)
        self.assertNotIn('no_timing', self._codes(act), "Vencer por campo ya es un plazo.")
        env = self.env
        today = fields.Date.context_today(env['project.task'])
        project = env['project.project'].create({'name': 'X-T18'})
        later = sgi_add_business_days(env, today, 3)
        env['project.task'].create([
            {'name': 'X-T18 a tiempo', 'project_id': project.id,
             'date_deadline': datetime.combine(later, datetime.min.time())},
            {'name': 'X-T18 tarde', 'project_id': project.id,
             'date_deadline': datetime.combine(today - timedelta(days=10), datetime.min.time())},
            {'name': 'X-T18 sin fecha', 'project_id': project.id}])
        counts = act._sgi_week_counts(today - timedelta(days=today.weekday()))
        self.assertEqual(counts['done_count'], 3)
        self.assertEqual((counts['timed_count'], counts['on_time_count']), (2, 1),
                         "Solo cuentan las que traen la fecha; la de ayer vence antes.")
        with self.assertRaises(ValidationError):
            act.input_ids.due_field = 'no_existe'
        with self.assertRaises(ValidationError):
            act.input_ids.due_field = 'name'
        # Hacia atrás en días hábiles: el margen -1 desde un lunes es el viernes.
        monday = today - timedelta(days=today.weekday())
        self.assertEqual(sgi_add_business_days(env, monday, -1), monday - timedelta(days=3))

    # 19 (P-4)
    def test_19_load_due_field_and_offset(self):
        model = self.env['ir.model']._get_id('project.task')
        for code in ('XL9-IN', 'XL9-OUT'):
            self.Deliverable.create({'code': code, 'name': code, 'odoo_model_id': model,
                                     'measure_domain': "[]", 'measure_date_field': 'create_date'})
        payload = {'processes': [{'code': 'XL9', 'name': 'Carga P-4', 'activities': [{
            'number': 1, 'name': 'Cerrar la tarea programada',
            'roles': [{'role': 'ejecuta', 'job': self.job.id}],
            'inputs': [{'code': 'XL9-IN', 'due_field': 'date_deadline', 'offset_days': -2}],
            'outputs': ['XL9-OUT']}]}]}
        result = self.Process.load_payload(payload)
        self.assertTrue(result['ok'], result['errors'])
        line = self.Activity.search([('process_id.code', '=', 'XL9')]).input_ids
        self.assertEqual((line.due_field, line.offset_days, line.max_days),
                         ('date_deadline', -2, 0))
        self.assertFalse(self.Process.load_payload(payload)['changes'], "Idempotente.")
        payload['processes'][0]['activities'][0]['inputs'][0]['offset_days'] = 'dos'
        self.assertFalse(self.Process.load_payload(payload, dry_run=True)['ok'])
