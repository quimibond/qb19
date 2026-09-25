# -*- coding: utf-8 -*-
"""Diagramas con la vista nativa «hierarchy» (19.0.53.2.0): mapa de procesos
(macroproceso → subprocesos) y flujo de actividades (paso anterior por
eslabón, sin ciclos)."""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestHierarchy(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
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
        self.assertEqual(action['views'][0][1], 'hierarchy')
        self.assertEqual(action['domain'], [('process_id', '=', self.proc.id)])
        action = self.proc.action_sgi_view_process_map()
        self.assertEqual((action['type'], action['tag']), ('ir.actions.client', 'sgi_process_map'))
        self.assertEqual(action['context'], {'sgi_map_process_id': self.proc.id})

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
