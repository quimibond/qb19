# -*- coding: utf-8 -*-
"""Diagramas con la vista nativa «hierarchy» (19.0.53.2.0): mapa de procesos
(macroproceso → subprocesos) y flujo de actividades (paso anterior por
eslabón, sin ciclos)."""
from odoo.tests import TransactionCase, tagged

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestHierarchy(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.env = cls.env(context=dict(cls.env.context, sgi_skip_role_check=True))
        Process = cls.env['sgi.process']
        cls.macro = Process.create({'code': 'XH0', 'name': 'Macro H'})
        cls.proc = Process.create({'code': 'XH1', 'name': 'Proceso H', 'parent_id': cls.macro.id})
        cls.other = Process.create({'code': 'XH2', 'name': 'Otro H'})
        Activity = cls.env['sgi.process.activity']
        cls.a1 = Activity.create({'process_id': cls.proc.id, 'name': 'Recibir', 'sequence': 10})
        cls.a2 = Activity.create({'process_id': cls.proc.id, 'name': 'Revisar', 'sequence': 20})
        cls.a3 = Activity.create({'process_id': cls.proc.id, 'name': 'Liberar', 'sequence': 30})
        cls.b1 = Activity.create({'process_id': cls.other.id, 'name': 'Entregar', 'sequence': 10})
        Link = cls.env['sgi.activity.link']
        Link.create({'from_activity_id': cls.a1.id, 'to_activity_id': cls.a2.id, 'name': 'Pedido'})
        Link.create({'from_activity_id': cls.a2.id, 'to_activity_id': cls.a3.id, 'name': 'Revisión'})
        # Cruza procesos: no cuenta como padre de flujo.
        Link.create({'from_activity_id': cls.b1.id, 'to_activity_id': cls.a1.id, 'name': 'Insumo'})
        # Regreso (ciclo): el paso posterior nunca es padre.
        Link.create({'from_activity_id': cls.a3.id, 'to_activity_id': cls.a2.id, 'name': 'Rechazo'})

    def test_01_paso_anterior_sin_ciclos_ni_cruces(self):
        self.assertFalse(self.a1.flow_parent_id, "Recibe de otro proceso: arranca el diagrama.")
        self.assertEqual(self.a2.flow_parent_id, self.a1)
        self.assertEqual(self.a3.flow_parent_id, self.a2)
        self.assertEqual(self.a1.flow_child_ids, self.a2)
        self.assertEqual(self.a2.flow_child_count, 1)
        # Al cambiar la secuencia el padre se recalcula.
        self.a1.sequence = 25
        self.assertFalse(self.a2.flow_parent_id, "El paso anterior debe ir antes en la secuencia.")

    def test_02_vistas_hierarchy_cargan(self):
        for model in ('sgi.process', 'sgi.process.activity'):
            views = self.env[model].get_views([(False, 'hierarchy')])
            self.assertIn('hierarchy', views['views'])
        rows = self.env['sgi.process'].hierarchy_read(
            [('id', 'in', (self.macro | self.proc).ids)], {'code': {}, 'child_ids': {}},
            'parent_id', 'child_ids')
        self.assertEqual({r['id'] for r in rows}, {self.macro.id, self.proc.id})
        action = self.proc.action_sgi_view_diagram()
        self.assertEqual((action['type'], action['tag']), ('ir.actions.client', 'sgi_diagram'))
        self.assertEqual(action['context']['sgi_diagram_kind'], 'process_flow')
        self.assertEqual(action['context']['sgi_diagram_res_id'], self.proc.id)
        action = self.proc.action_sgi_view_process_map()
        self.assertEqual(action['context']['sgi_diagram_kind'], 'process_map')
        self.assertEqual(action['context']['sgi_diagram_selected'], 'sgi.process,%d' % self.proc.id)

    def test_03_datos_del_mapa_con_conexiones(self):
        self.env['sgi.process.flow'].create({
            'from_process_id': self.proc.id, 'to_process_id': self.other.id, 'name': 'Lote liberado'})
        data = self.env['sgi.process'].sgi_map_data()
        by_id = {p['id']: p for band in data['bands'] for p in band['processes']}
        self.assertIn(self.proc.id, by_id)
        self.assertEqual(by_id[self.proc.id]['out_count'], 1)
        self.assertEqual(by_id[self.other.id]['in_count'], 1)
        keys = [band['key'] for band in data['bands']]
        self.assertEqual(keys, [k for k in ('estrategico', 'cop', 'soporte') if k in keys],
                         "Bandas en el orden del mapa.")
        flow = [f for f in data['flows'] if f['from_id'] == self.proc.id and f['to_id'] == self.other.id]
        self.assertEqual(len(flow), 1)
        self.assertEqual(flow[0]['name'], 'Lote liberado')
        self.assertEqual(self.macro.child_count, 1)

    def test_04_diagramas_en_html(self):
        """Cada diagrama devuelve carriles, cajas y flechas con claves que se
        corresponden (lo que dibuja el componente sgi_diagram)."""
        Diagram = self.env['sgi.diagram']
        self.env['sgi.process.flow'].create({
            'from_process_id': self.other.id, 'to_process_id': self.proc.id, 'name': 'Insumo aprobado'})
        stage = self.env['sgi.process.stage'].create({'process_id': self.proc.id, 'name': 'Arranque', 'code': 'A'})
        self.a1.stage_id = stage
        doc = self.env['documents.document'].create({
            'name': 'P-H91 Procedimiento H', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento', 'sgi_code': 'P-H91', 'sgi_state': 'vigente',
            'sgi_process_id': self.proc.id})
        fmt = self.env['documents.document'].create({
            'name': 'IT-P-H91-01 Instructivo H', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'instructivo', 'sgi_code': 'IT-P-H91-01', 'sgi_state': 'piloto',
            'sgi_process_id': self.proc.id, 'sgi_parent_document_id': doc.id})
        objective = self.env['sgi.objective'].create({'name': 'Objetivo H'})
        indicator = self.env['sgi.indicator'].create({
            'code': 'XH-KPI', 'name': 'KPI H', 'calc_mode': 'manual', 'process_id': self.proc.id,
            'objective_id': objective.id})
        job = self.env['hr.job'].create({'name': 'PUESTO H'})
        self.a2.write({'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': job.id})]})

        def keys(data):
            return {item['key'] for lane in data['lanes'] for item in lane['items']}

        # 54.3.0: por omisión, diagrama de flujo funcional (carriles por puesto).
        flow = Diagram.data('process_flow', self.proc.id)
        self.assertEqual(flow['layout'], 'swimlanes')
        self.assertEqual(flow['columns'], 3)
        self.assertIn('sgi.process.activity,%d' % self.a1.id, keys(flow))
        self.assertTrue(any(e['label'] == 'Pedido' for e in flow['edges']))
        labels = [lane['label'] for lane in flow['lanes']]
        self.assertIn('PUESTO H', labels, "Un carril por puesto que ejecuta.")
        self.assertIn('Otros procesos (entregan)', labels,
                      "El eslabón que cruza desde XH2 se dibuja como carril de entrada.")
        cols = {item['key']: item['col'] for lane in flow['lanes'] for item in lane['items']}
        self.assertEqual(cols['sgi.process.activity,%d' % self.a2.id], 2, "Columna = orden en el procedimiento.")
        for edge in flow['edges']:
            self.assertIn(edge['from'], keys(flow))
            self.assertIn(edge['to'], keys(flow))
        by_stage = Diagram.data('process_flow', self.proc.id, {'carriles': 'etapa'})
        self.assertEqual(by_stage['layout'], 'columns')
        self.assertIn('Recibe de otros procesos', [lane['label'] for lane in by_stage['lanes']])
        self.assertEqual([n['kind'] for n in flow['nav']][:2], ['process_flow', 'sipoc'])

        sipoc = Diagram.data('sipoc', self.proc.id)
        self.assertEqual([lane['key'] for lane in sipoc['lanes']],
                         ['suppliers', 'inputs', 'process', 'outputs', 'customers'])
        self.assertIn('sgi.process,%d' % self.other.id, keys(sipoc))
        self.assertTrue(sipoc['show_all_edges'])

        tree = Diagram.data('doc_tree', self.proc.id)
        self.assertEqual({item['key'] for lane in tree['lanes'] for item in lane['items']},
                         {'documents.document,%d' % doc.id, 'documents.document,%d' % fmt.id})
        self.assertEqual(tree['edges'], [{'from': 'documents.document,%d' % doc.id,
                                          'to': 'documents.document,%d' % fmt.id, 'label': ''}])

        kpis = Diagram.data('kpi_tree', self.proc.id)
        self.assertIn('sgi.indicator,%d' % indicator.id, keys(kpis))
        self.assertIn('sgi.objective,%d' % objective.id, keys(kpis))
        self.assertEqual(len(kpis['edges']), 2)

        who = Diagram.data('who_does_what')
        self.assertEqual(who['layout'], 'matrix')
        cell = who['matrix']['cells']['hr.job,%d' % job.id]['sgi.process,%d' % self.proc.id]
        self.assertEqual((cell['value'], cell['level']), (1, 3))

        pmap = Diagram.data('process_map', self.proc.id)
        self.assertEqual(pmap['selected'], 'sgi.process,%d' % self.proc.id)
        with self.assertRaises(ValueError):
            Diagram.data('no_existe')
        self.assertTrue(any(p['id'] == self.proc.id for p in Diagram.processes()))
