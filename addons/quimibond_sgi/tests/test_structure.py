# -*- coding: utf-8 -*-
"""Estructura en vez de texto: numeral calculado, etapas, entregables que
conectan actividades (ligas, flujos, entradas/salidas calculadas), frase del
procedimiento armada y claves del texto convertidas en ligas."""
from odoo.tests import TransactionCase, tagged


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
                           input_deliverable_ids=[(6, 0, pedido.ids)])
        link = self.env['sgi.activity.link'].search([('deliverable_id', '=', pedido.id)])
        self.assertEqual((link.from_activity_id, link.to_activity_id), (vende, planea),
                         "La liga sale sola de quién entrega y quién recibe.")
        self.assertEqual(link.name, 'Pedido confirmado')
        flow = self.env['sgi.process.flow'].search([('deliverable_id', '=', pedido.id)])
        self.assertEqual((flow.from_process_id, flow.to_process_id), (self.p_ven, self.p_pla),
                         "Cruza de proceso: el flujo también sale solo.")
        self.assertIn(pedido, self.p_ven.output_deliverable_ids)
        self.assertIn(pedido, self.p_pla.input_deliverable_ids)
        # Renombrar el entregable renombra la liga y el flujo.
        pedido.name = 'Pedido liberado'
        self.assertEqual(link.name, 'Pedido liberado')
        self.assertEqual(flow.name, 'Pedido liberado')
        # Quitarlo de quien lo recibe borra la liga y el flujo.
        planea.input_deliverable_ids = [(5, 0, 0)]
        self.assertFalse(link.exists())
        self.assertFalse(flow.exists())
        self.assertEqual(pedido.orphan, 'sin_destino')

    def test_04_manual_links_untouched(self):
        a = self._act(self.p_ven, 'A')
        b = self._act(self.p_ven, 'B')
        manual = self.env['sgi.activity.link'].create(
            {'from_activity_id': a.id, 'to_activity_id': b.id, 'name': 'A mano'})
        d = self.Deliverable.create({'code': 'X-D', 'name': 'D'})
        a.output_deliverable_ids = [(6, 0, d.ids)]
        b.input_deliverable_ids = [(6, 0, d.ids)]
        self.assertTrue(manual.exists())
        self.assertEqual(len(a.out_link_ids), 2)

    def test_05_sentence_and_responsibilities(self):
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

    def test_06_extract_references_from_text(self):
        fmt = self.env['documents.document'].create({
            'name': 'Alta de cliente', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'formato', 'sgi_code': 'F-P-A77-21', 'sgi_state': 'vigente'})
        act = self._act(self.p_ven, 'Dar de alta al cliente',
                        description='Para un cliente nuevo se llena el F-P-A77-21 y lo '
                                    'revisa el puesto estructura b; ver F-P-A77-99.')
        found = act._sgi_extract_references()[act]
        self.assertEqual(found['formats'], fmt)
        self.assertIn(self.job_b, found['jobs'])
        self.assertEqual(found['missing'], ['F-P-A77-99'])
        act.action_sgi_extract_references()
        self.assertIn(fmt, act.format_document_ids, "La clave del texto queda ligada.")

    def test_07_load_deliverables_and_io(self):
        payload = {
            'deliverables': [{'code': 'X-PRON', 'name': 'Pronóstico de ventas'}],
            'processes': [{'code': 'XL', 'name': 'Carga X'}],
            'activities': [
                {'process': 'XL', 'number': 'XL.01', 'name': 'Elaborar el pronóstico',
                 'stage': 'A. Planeación', 'outputs': ['X-PRON'],
                 'roles': [{'role': 'ejecuta', 'job': self.job_a.id}]},
                {'process': 'XL', 'number': 'XL.02', 'name': 'Usar el pronóstico',
                 'stage': 'A. Planeación', 'inputs': ['X-PRON'],
                 'roles': [{'role': 'ejecuta', 'job': self.job_b.id}]},
            ],
        }
        result = self.Process.load_payload(payload)
        self.assertTrue(result['ok'], result['errors'])
        acts = self.Activity.search([('process_id.code', '=', 'XL')])
        self.assertEqual(set(acts.mapped('number')), {'XL.01', 'XL.02'})
        self.assertEqual(len(acts.stage_id), 1)
        self.assertEqual(acts.filtered(lambda a: a.step == 1).next_activity_ids.step, 2)
        again = self.Process.load_payload(payload)
        self.assertFalse(again['changes'], "Segunda carga sin cambios.")
        bad = dict(payload, activities=[dict(payload['activities'][0], number='4.1')])
        self.assertFalse(self.Process.load_payload(bad, dry_run=True)['ok'],
                         "Un numeral que no es CLAVE.nn es error en la carga.")
