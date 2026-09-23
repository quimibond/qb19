# -*- coding: utf-8 -*-
"""Estructura en vez de texto: numeral calculado, etapas, entregables que
conectan actividades (ligas, flujos, entradas/salidas calculadas), plazo en
quien recibe, medición por el entregable y frase del procedimiento armada."""
from datetime import datetime

from odoo.exceptions import UserError, ValidationError
from odoo.tests import TransactionCase, tagged
from odoo.tools import mute_logger

from .common_calendar import sgi_test_calendar


@tagged('post_install', '-at_install')
class TestStructure(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Job = cls.env['hr.job']
        cls.job_a = Job.create({'name': 'PUESTO ESTRUCTURA A'})
        cls.job_b = Job.create({'name': 'PUESTO ESTRUCTURA B'})
        cls.env['hr.employee'].create([
            {'name': 'Emp A', 'job_id': cls.job_a.id},
            {'name': 'Emp B', 'job_id': cls.job_b.id}])
        cls.Process = cls.env['sgi.process']
        cls.Activity = cls.env['sgi.process.activity']
        cls.Deliverable = cls.env['sgi.deliverable']
        cls.p_ven = cls.Process.create({'code': 'XV', 'name': 'Ventas X'})
        cls.p_pla = cls.Process.create({'code': 'XP', 'name': 'Planeación X'})
        sgi_test_calendar(cls.env)

    def _act(self, process, name, job=None, **vals):
        return self.Activity.create(dict({
            'process_id': process.id, 'name': name,
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': (job or self.job_a).id})],
        }, **vals))

    # ------------------------------------------------------------------
    def test_01_number_is_process_code_plus_step(self):
        first = self._act(self.p_ven, 'Primera')
        second = self._act(self.p_ven, 'Segunda')
        self.assertEqual((first.step, second.step), (1, 2), "Sin paso, el siguiente libre.")
        self.assertEqual(second.number, 'XV.02')
        explicit = self._act(self.p_ven, 'Veinte', number='XV.20')
        self.assertEqual(explicit.step, 20)
        legacy = self._act(self.p_ven, 'Vieja', number='4.2.3.1')
        self.assertEqual(legacy.legacy_number, '4.2.3.1')
        self.assertEqual(legacy.step, 21, "El numeral viejo no manda: toma el siguiente paso.")
        self.p_ven.code = 'XV2'
        self.assertEqual(second.number, 'XV2.02', "Cambiar la clave del proceso renumera solo.")

    def test_02_section_text_becomes_stage(self):
        act = self._act(self.p_ven, 'Con sección', section='D. Inventario')
        self.assertEqual(act.stage_id.code, 'D')
        self.assertEqual(act.stage_id.name, 'Inventario')
        self.assertEqual(act.section, 'D. Inventario')
        again = self._act(self.p_ven, 'Misma etapa', section='D. Inventario')
        self.assertEqual(again.stage_id, act.stage_id, "La etapa se reutiliza.")
        self.assertEqual(len(self.p_ven.stage_ids), 1)

    def test_03_deliverable_connects_activities(self):
        pedido = self.Deliverable.create({'code': 'X-PED', 'name': 'Pedido confirmado'})
        vende = self._act(self.p_ven, 'Confirmar el pedido', output_deliverable_ids=[(6, 0, pedido.ids)])
        planea = self._act(self.p_pla, 'Programar el pedido', job=self.job_b,
                           input_ids=[(0, 0, {'deliverable_id': pedido.id, 'max_days': 2})])
        link = self.env['sgi.activity.link'].search([('deliverable_id', '=', pedido.id)])
        self.assertEqual((link.from_activity_id, link.to_activity_id), (vende, planea),
                         "La liga sale sola de quién entrega y quién recibe.")
        self.assertEqual(link.name, 'Pedido confirmado')
        self.assertEqual(link.max_days, 2, "El plazo es del renglón «recibe».")
        flow = self.env['sgi.process.flow'].search([('deliverable_id', '=', pedido.id)])
        self.assertEqual((flow.from_process_id, flow.to_process_id), (self.p_ven, self.p_pla),
                         "Cruza de proceso: el flujo también sale solo.")
        self.assertIn(pedido, self.p_ven.output_deliverable_ids)
        self.assertIn(pedido, self.p_pla.input_deliverable_ids)
        self.assertEqual(planea.input_deliverable_ids, pedido)
        # Renombrar el entregable renombra la liga y el flujo.
        pedido.name = 'Pedido liberado'
        self.assertEqual(link.name, 'Pedido liberado')
        self.assertEqual(flow.name, 'Pedido liberado')
        # Quitarlo de quien lo recibe borra la liga y el flujo.
        planea.input_ids.unlink()
        self.assertFalse(link.exists())
        self.assertFalse(flow.exists())
        self.assertEqual(pedido.orphan, 'sin_destino')

    def test_04_calculated_links_are_not_edited(self):
        d = self.Deliverable.create({'code': 'X-D', 'name': 'D'})
        a = self._act(self.p_ven, 'A', output_deliverable_ids=[(6, 0, d.ids)])
        b = self._act(self.p_pla, 'B', job=self.job_b, input_deliverable_ids=[(6, 0, d.ids)])
        link = self.env['sgi.activity.link'].search([('deliverable_id', '=', d.id)])
        flow = self.env['sgi.process.flow'].search([('deliverable_id', '=', d.id)])
        with self.assertRaises(UserError):
            link.name = 'Otra cosa'
        with self.assertRaises(UserError):
            link.unlink()
        with self.assertRaises(UserError):
            flow.write({'name': 'Otra cosa'})
        with self.assertRaises(ValidationError), self.env.cr.savepoint():
            link.active = False     # sin motivo
        link.write({'active': False, 'inactive_reason': "Solo aplica a exportación"})
        self.assertFalse(flow.active, "Sin ligas activas, el flujo se apaga.")
        # Volver a sincronizar no revive la liga apagada.
        d._sgi_sync_connections()
        self.assertFalse(link.active)
        self.assertEqual(a.out_link_ids, link.browse(), "La liga apagada no cuenta en la cadena.")
        self.assertTrue(b.exists())

    def test_05_manual_link_replaced_by_deliverable(self):
        a = self._act(self.p_ven, 'A')
        b = self._act(self.p_pla, 'B', job=self.job_b)
        c = self._act(self.p_ven, 'C')
        Link = self.env['sgi.activity.link']
        manual = Link.create({'from_activity_id': a.id, 'to_activity_id': b.id, 'name': 'A mano'})
        other = Link.create({'from_activity_id': a.id, 'to_activity_id': c.id, 'name': 'Otra'})
        manual_flow = self.env['sgi.process.flow'].create({
            'from_process_id': self.p_ven.id, 'to_process_id': self.p_pla.id, 'name': 'A mano'})
        d = self.Deliverable.create({'code': 'X-R', 'name': 'Reemplazo'})
        a.output_deliverable_ids = [(6, 0, d.ids)]
        b.input_deliverable_ids = [(6, 0, d.ids)]
        self.assertFalse(manual.active, "La liga a mano entre las mismas actividades se archiva.")
        self.assertIn('Reemplazo', manual.inactive_reason)
        self.assertFalse(manual_flow.active)
        self.assertTrue(other.active, "La que no cubre el entregable se queda.")

    def test_06_deliverable_measures_its_producer(self):
        d = self.Deliverable.create({
            'code': 'X-M', 'name': 'Contacto dado de alta',
            'odoo_model_id': self.env['ir.model']._get_id('res.partner'),
            'measure_domain': "[('active', '=', True)]", 'measure_date_field': 'create_date',
            'measure_user_field': 'create_uid'})
        act = self._act(self.p_ven, 'Dar de alta', output_deliverable_ids=[(6, 0, d.ids)],
                        measure_method='entregable')
        self.assertEqual(act.measure_deliverable_id, d, "Un solo entregable con modelo: se elige solo.")
        self.assertEqual(act.measure_model_name, 'res.partner')
        self.assertEqual(act.measure_user_field, 'create_uid')
        d.measure_domain = "[('is_company', '=', True)]"
        self.assertEqual(act.measure_domain, "[('is_company', '=', True)]",
                         "Se captura una vez, en el entregable.")
        act._sgi_measure()
        self.assertTrue(act.measure_last_date, "Se mide como un registro de Odoo.")
        with self.assertRaises(ValidationError), self.env.cr.savepoint():
            self.Deliverable.create({'code': 'X-BAD', 'name': 'Malo',
                                     'odoo_model_id': self.env['ir.model']._get_id('res.partner'),
                                     'measure_date_field': 'no_existe'})
        with self.assertRaises(ValidationError), self.env.cr.savepoint():
            act.output_deliverable_ids = [(5, 0, 0)]

    def test_07_deadline_on_receiver_drives_chain(self):
        d = self.Deliverable.create({'code': 'X-P', 'name': 'Con plazo'})
        a = self._act(self.p_ven, 'Entrega', output_deliverable_ids=[(6, 0, d.ids)])
        b = self._act(self.p_pla, 'Recibe', job=self.job_b,
                      input_ids=[(0, 0, {'deliverable_id': d.id, 'max_days': 2})])
        link = a.out_link_ids
        # Semana sin festivos en México (la del 14-sep trae el 16).
        a.measure_last_date = datetime(2026, 10, 5, 10, 0)    # lunes
        b.measure_last_date = datetime(2026, 9, 1, 10, 0)
        self.assertEqual(link._sgi_chain_verdict(datetime(2026, 10, 7, 12, 0)), ('fluye', 2.0))
        self.assertEqual(link._sgi_chain_verdict(datetime(2026, 10, 12, 12, 0)), ('atorado', 5.0),
                         "Fin de semana no cuenta: 5 días hábiles > 2.")
        b.measure_last_date = datetime(2026, 10, 6, 10, 0)
        self.assertEqual(link._sgi_chain_verdict(datetime(2026, 10, 12, 12, 0)), ('fluye', 0.0))
        self.assertIn('Con plazo (2 días hábiles)', b._sgi_sentence())

    def test_08_sentence_and_responsibilities(self):
        d = self.Deliverable.create({'code': 'X-S', 'name': 'Programa semanal'})
        act = self.Activity.create({
            'process_id': self.p_pla.id, 'name': 'Elaborar el programa',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job_a.id}),
                         (0, 0, {'role': 'informa', 'job_id': self.job_b.id})],
            'output_deliverable_ids': [(6, 0, d.ids)]})
        sentence = act._sgi_sentence()
        self.assertIn('Ejecuta: PUESTO ESTRUCTURA A.', sentence)
        self.assertIn('Se entera: PUESTO ESTRUCTURA B.', sentence)
        self.assertIn('Entrega: Programa semanal.', sentence)
        table = dict(self.p_pla._sgi_responsibilities_by_job())
        self.assertIn('PUESTO ESTRUCTURA A', table)
        self.assertEqual(table['PUESTO ESTRUCTURA A'][0][1], [act])

    def test_09_load_deliverables_io_and_measure(self):
        payload = {
            'deliverables': [
                {'code': 'X-PRON', 'name': 'Pronóstico de ventas'},
                {'code': 'X-ALTA', 'name': 'Cliente dado de alta', 'model': 'res.partner',
                 'domain': "[('is_company', '=', True)]", 'date_field': 'create_date',
                 'user_field': 'create_uid'},
            ],
            'processes': [{'code': 'XL', 'name': 'Carga X'}],
            'activities': [
                {'process': 'XL', 'number': 'XL.01', 'name': 'Elaborar el pronóstico',
                 'stage': 'A. Planeación', 'outputs': ['X-PRON', 'X-ALTA'],
                 'measure': {'method': 'entregable', 'deliverable': 'X-ALTA'},
                 'roles': [{'role': 'ejecuta', 'job': self.job_a.id}]},
                {'process': 'XL', 'number': 'XL.02', 'name': 'Usar el pronóstico',
                 'stage': 'A. Planeación', 'inputs': [{'code': 'X-PRON', 'days': 3}],
                 'roles': [{'role': 'ejecuta', 'job': self.job_b.id}]},
            ],
        }
        result = self.Process.load_payload(payload)
        self.assertTrue(result['ok'], result['errors'])
        acts = self.Activity.search([('process_id.code', '=', 'XL')])
        self.assertEqual(set(acts.mapped('number')), {'XL.01', 'XL.02'})
        self.assertEqual(len(acts.stage_id), 1)
        first = acts.filtered(lambda a: a.step == 1)
        second = acts.filtered(lambda a: a.step == 2)
        self.assertEqual(first.next_activity_ids, second)
        self.assertEqual(second.input_ids.max_days, 3)
        self.assertEqual(first.measure_model_name, 'res.partner')
        self.assertEqual(first.measure_domain, "[('is_company', '=', True)]")
        again = self.Process.load_payload(payload)
        self.assertFalse(again['changes'], "Segunda carga sin cambios.")
        # El plazo se actualiza sin duplicar el renglón.
        payload['activities'][1]['inputs'] = [{'code': 'X-PRON', 'days': 1}]
        self.assertTrue(self.Process.load_payload(payload)['ok'])
        self.assertEqual(second.input_ids.max_days, 1)
        bad = dict(payload, activities=[dict(payload['activities'][0], number='4.1')])
        self.assertFalse(self.Process.load_payload(bad, dry_run=True)['ok'],
                         "Un numeral que no es CLAVE.nn es error en la carga.")

    def test_10_legacy_keys_are_errors(self):
        base = {'processes': [{'code': 'XO', 'name': 'Viejo'}]}
        with mute_logger('odoo.sql_db'):
            for payload in (
                dict(base, activities=[{'process': 'XO', 'number': 1, 'name': 'Uno',
                                        'links_to': ['XO.02'],
                                        'roles': [{'role': 'ejecuta', 'job': self.job_a.id}]}]),
                dict(base, flows=[{'from': 'XO', 'to': 'XV', 'name': 'Algo'}]),
                dict(base, links=[{'from': 'XO:XO.01', 'to': 'XO:XO.02'}]),
                {'processes': [{'code': 'XO', 'name': 'Viejo', 'inputs': 'Texto'}]},
            ):
                result = self.Process.load_payload(payload, dry_run=True)
                self.assertFalse(result['ok'], payload)

    def test_11_condition_only_for_approver_or_informed(self):
        act = self._act(self.p_ven, 'Con aprobación')
        Role = self.env['sgi.activity.role']
        Role.create({'activity_id': act.id, 'role': 'aprueba', 'job_id': self.job_b.id,
                     'condition': 'arriba del monto que se fije'})
        with self.assertRaises(ValidationError), self.env.cr.savepoint():
            Role.create({'activity_id': act.id, 'role': 'participa', 'job_id': self.job_b.id,
                         'condition': 'si es exportación'})
        with self.assertRaises(ValidationError), self.env.cr.savepoint():
            act.role_ids.filtered(lambda r: r.role == 'ejecuta').condition = 'si es nacional'

    def test_12_start_and_end_come_from_deliverables(self):
        oc = self.Deliverable.create({'code': 'X-OC', 'name': 'Orden de compra del cliente'})
        ped = self.Deliverable.create({'code': 'X-PD', 'name': 'Pedido capturado'})
        conf = self.Deliverable.create({'code': 'X-CF', 'name': 'Pedido confirmado'})
        self._act(self.p_ven, 'Capturar', input_deliverable_ids=[(6, 0, oc.ids)],
                  output_deliverable_ids=[(6, 0, ped.ids)])
        self._act(self.p_ven, 'Confirmar', input_deliverable_ids=[(6, 0, ped.ids)],
                  output_deliverable_ids=[(6, 0, conf.ids)])
        self.assertEqual(self.p_ven.input_deliverable_ids, oc, "Inicia con lo que llega de fuera.")
        self.assertEqual(self.p_ven.output_deliverable_ids, conf, "Termina con lo que sale.")
        result = self.Process.load_payload({'processes': [
            {'code': 'XV', 'name': 'Ventas X', 'start_trigger': 'Llega la OC'}]}, dry_run=True)
        self.assertTrue(result['ok'], result['errors'])
        self.assertTrue(any('start_trigger' in w['message'] for w in result['warnings']))

    def test_13_unknown_keys_are_errors_with_path(self):
        payload = {'processes': [{'code': 'XK', 'name': 'Llaves', 'activities': [
            {'number': 1, 'name': 'Uno', 'origin_note': 'algo',
             'roles': [{'role': 'ejecuta', 'job': self.job_a.id}]}]}]}
        result = self.Process.load_payload(payload, dry_run=True)
        self.assertFalse(result['ok'])
        message = result['errors'][0]['message']
        self.assertIn('processes[0].activities[0].origin_note', message)
        self.assertIn('Válidas aquí', message)
        self.assertIn('outputs', message, "Trae la lista de llaves válidas.")
        # Indicadores dentro del proceso: así se perdieron los de C2.
        nested = {'processes': [{'code': 'XK', 'name': 'Llaves',
                                 'indicators': [{'code': 'XK-01', 'name': 'I'}]}]}
        result = self.Process.load_payload(nested)
        self.assertFalse(result['ok'])
        self.assertIn('processes[0].indicators', result['errors'][0]['message'])
        self.assertFalse(self.Process.search([('code', '=', 'XK')]),
                         "Con una llave desconocida no se carga nada.")
        bad_role = {'activities': [{'process': 'XV', 'number': 1, 'name': 'R',
                                    'roles': [{'role': 'ejecuta', 'puesto': 'X'}]}]}
        result = self.Process.load_payload(bad_role, dry_run=True)
        self.assertIn('activities[0].roles[0].puesto', result['errors'][0]['message'])

    def test_14_indicator_formula_source_and_employee_responsible(self):
        user = self.env['res.users'].create({'name': 'Dueña X', 'login': 'duena.x.sgi'})
        emp = self.env['hr.employee'].create({'name': 'Dueña X', 'user_id': user.id,
                                              'job_id': self.job_a.id})
        payload = {'indicators': [{
            'code': 'XV-01', 'name': 'OTIF X', 'process': 'XV', 'frequency': 'monthly',
            'formula': 'Entregas completas a tiempo ÷ entregas del mes',
            'source': 'Fecha compromiso contra fecha de entrega',
            'responsible_employee_id': emp.id}]}
        result = self.Process.load_payload(payload)
        self.assertTrue(result['ok'], result['errors'])
        self.assertFalse(result['warnings'], "Con responsable no hay aviso.")
        ind = self.env['sgi.indicator'].search([('code', '=', 'XV-01')])
        self.assertEqual(ind.responsible_id, user)
        self.assertIn('÷', ind.formula)
        self.assertEqual(ind.source, 'Fecha compromiso contra fecha de entrega')
        self.assertFalse(self.Process.load_payload(payload)['changes'])
        no_user = self.env['hr.employee'].create({'name': 'Sin usuario X'})
        payload['indicators'][0]['responsible_employee_id'] = no_user.id
        self.assertFalse(self.Process.load_payload(payload, dry_run=True)['ok'])

    def test_15_replaces_archives_the_old_process(self):
        old = self.Process.create({'code': 'X-VIEJO', 'name': 'Ventas viejo'})
        old_act = self._act(old, 'Actividad vieja', description='Texto original')
        payload = {'processes': [{'code': 'XN', 'name': 'Nuevo', 'replaces': ['X-VIEJO']}]}
        dry = self.Process.load_payload(payload, dry_run=True)
        self.assertTrue(dry['ok'], dry['errors'])
        self.assertEqual(dry['summary'].get('archived', {}).get('process'), 1)
        self.assertEqual(dry['summary'].get('archived', {}).get('activity'), 1)
        self.assertTrue(old.active, "Con dry_run solo se reporta.")
        result = self.Process.load_payload(payload)
        self.assertTrue(result['ok'], result['errors'])
        self.assertFalse(old.active)
        self.assertFalse(old_act.active, "Sus actividades se archivan con él.")
        self.assertEqual(old_act.description, 'Texto original', "El texto se conserva.")
        self.assertTrue(any('Sustituido por XN' in (m.body or '') for m in old.message_ids))
        self.assertFalse(self.Process.load_payload(payload)['changes'], "Una sola vez.")
        missing = {'processes': [{'code': 'XN', 'name': 'Nuevo', 'replaces': ['NO-EXISTE']}]}
        self.assertFalse(self.Process.load_payload(missing, dry_run=True)['ok'])

    def test_16_commitment_date_stamp(self):
        partner = self.env['res.partner'].create({'name': 'Cliente X'})
        order = self.env['sale.order'].create({'partner_id': partner.id})
        self.assertFalse(order.sgi_commitment_set_at)
        order.commitment_date = datetime(2026, 10, 1, 12, 0)
        stamp = order.sgi_commitment_set_at
        self.assertTrue(stamp)
        self.assertEqual(order.sgi_commitment_set_uid, self.env.user)
        order.commitment_date = datetime(2026, 10, 8, 12, 0)
        self.assertEqual(order.sgi_commitment_set_at, stamp, "Solo la primera vez.")
        born = self.env['sale.order'].create({'partner_id': partner.id,
                                              'commitment_date': datetime(2026, 10, 2)})
        self.assertTrue(born.sgi_commitment_set_at, "También al crear con fecha.")
