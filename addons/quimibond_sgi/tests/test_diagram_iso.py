# -*- coding: utf-8 -*-
"""Catálogo ISO de diagramas (19.0.53.6.0, models/sgi_diagram_iso.py): cada
diagrama devuelve la estructura que dibuja el componente sgi_diagram y las
flechas apuntan a cajas que existen."""
from odoo.tests import TransactionCase, tagged

from .common_documents import sgi_hide_real_documents

ISO_KINDS = ['interaction_matrix', 'context_map', 'roles_map', 'risk_matrix', 'legal_matrix',
             'calibration_map', 'competence_matrix', 'doc_pyramid', 'emergency_map', 'audit_program',
             'management_review', 'nc_flow', 'pdca', 'kpi_tree']


@tagged('post_install', '-at_install')
class TestDiagramIso(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.env = cls.env(context=dict(cls.env.context, sgi_skip_role_check=True))
        Process = cls.env['sgi.process']
        cls.proc = Process.create({'code': 'XI1', 'name': 'Proceso I'})
        cls.other = Process.create({'code': 'XI2', 'name': 'Otro I'})
        cls.flow = cls.env['sgi.process.flow'].create({
            'from_process_id': cls.proc.id, 'to_process_id': cls.other.id, 'name': 'Lote I'})
        cls.risk = cls.env['sgi.risk'].create({
            'name': 'Riesgo I', 'instrument': 'ryo', 'process_id': cls.proc.id,
            'eval_probability': '5', 'eval_impact': '5'})
        cls.party = cls.env['sgi.interested.party'].create({
            'name': 'Cliente I', 'needs': 'Entregas a tiempo', 'process_ids': [(6, 0, cls.proc.ids)],
            'risk_ids': [(6, 0, cls.risk.ids)]})
        cls.doc = cls.env['documents.document'].create({
            'name': 'P-I91 Procedimiento I', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento', 'sgi_code': 'P-I91', 'sgi_state': 'vigente',
            'sgi_process_id': cls.proc.id})

    @staticmethod
    def _keys(data):
        return {item['key'] for lane in data.get('lanes', []) for item in lane['items']}

    def test_01_todos_los_diagramas_responden(self):
        Diagram = self.env['sgi.diagram']
        for kind in ISO_KINDS:
            data = Diagram.data(kind, self.proc.id)
            self.assertEqual(data['kind'], kind)
            self.assertIn(data['layout'], ('bands', 'columns', 'matrix', 'swimlanes', 'cycle'), kind)
            self.assertTrue(data['title'], kind)
            keys = self._keys(data) | {'lane:%s' % lane['key'] for lane in data.get('lanes', [])}
            for edge in data['edges']:
                self.assertIn(edge['from'], keys, "%s: flecha desde caja inexistente" % kind)
                self.assertIn(edge['to'], keys, "%s: flecha hacia caja inexistente" % kind)
            if data['layout'] == 'matrix':
                rows = {r['key'] for r in data['matrix']['rows']}
                cols = {c['key'] for c in data['matrix']['cols']}
                for row_key, cells in data['matrix']['cells'].items():
                    self.assertIn(row_key, rows, kind)
                    for col_key, cell in cells.items():
                        self.assertIn(col_key, cols, kind)
                        self.assertIn(cell['level'], (0, 1, 2, 3), kind)
            for lane in data.get('lanes', []):
                for item in lane['items']:
                    self.assertTrue(item['model'] or item.get('action'),
                                    "%s: la caja %s no abre nada" % (kind, item['key']))

    def test_02_interaccion_y_riesgos_con_accion(self):
        Diagram = self.env['sgi.diagram']
        matrix = Diagram.data('interaction_matrix')
        cell = matrix['matrix']['cells']['sgi.process,%d' % self.proc.id]['sgi.process,%d' % self.other.id]
        self.assertEqual(cell['value'], 1)
        self.assertEqual(cell['action']['res_model'], 'sgi.process.flow')
        self.assertEqual(cell['action']['domain'], [('id', 'in', [self.flow.id])])

        risks = Diagram.data('risk_matrix', self.proc.id, {'instrument': 'ryo'})
        self.assertTrue(risks['per_process'])
        self.assertEqual(risks['matrix']['cells']['i5']['p5']['level'], 3)
        self.assertEqual(risks['matrix']['cells']['i5']['p5']['action']['res_model'], 'sgi.risk')
        self.assertEqual([r['key'] for r in risks['matrix']['rows']][0], 'i5', "Impacto mayor arriba.")
        iper = Diagram.data('risk_matrix', None, {'instrument': 'iper'})
        self.assertEqual(len(iper['matrix']['cols']), 3, "IPER va de 1 a 3.")
        self.assertEqual(iper['param_options'][0]['name'], 'instrument')

    def test_03_contexto_piramide_y_pdca(self):
        Diagram = self.env['sgi.diagram']
        context = Diagram.data('context_map')
        keys = self._keys(context)
        self.assertIn('sgi.interested.party,%d' % self.party.id, keys)
        self.assertIn('sgi.risk,%d' % self.risk.id, keys)
        self.assertEqual(sum(1 for e in context['edges'] if e['from'] == 'sgi.interested.party,%d' % self.party.id), 2)

        pyramid = Diagram.data('doc_pyramid')
        self.assertEqual(pyramid['layout'], 'bands')
        nivel2 = next(lane for lane in pyramid['lanes'] if lane['key'] == 'nivel2')
        box = next(i for i in nivel2['items'] if i['key'] == 'nivel2,%d' % self.proc.id)
        self.assertEqual(box['action']['domain'], [('id', 'in', [self.doc.id])])

        pdca = Diagram.data('pdca', self.proc.id)
        self.assertEqual(pdca['layout'], 'cycle', "PDCA es un ciclo: cuatro cuadrantes con flechas entre ellos.")
        self.assertEqual([lane['key'] for lane in pdca['lanes']], ['plan', 'do', 'check', 'act'])
        self.assertEqual([e['from'] for e in pdca['edges']], ['lane:plan', 'lane:do', 'lane:check', 'lane:act'])
        self.assertEqual(pyramid['shape'], 'pyramid')
        legal = Diagram.data('legal_matrix')
        self.assertEqual(legal['layout'], 'bands', "Por omisión, un tablero de requisitos por sistema.")
        self.assertEqual(Diagram.data('legal_matrix', None, {'vista': 'resumen'})['layout'], 'matrix')
        calib = Diagram.data('calibration_map')
        if calib.get('lanes'):
            self.assertEqual(calib['param_options'][0]['name'], 'agrupar')
            self.assertGreaterEqual(len(calib['lanes']), 12, "Programa de calibración: una columna por mes.")
        self.assertIn('sgi.risk,%d' % self.risk.id, self._keys(pdca))
        self.assertIn('pdca', [n['kind'] for n in pdca['nav']])

        nc = Diagram.data('nc_flow', self.proc.id)
        self.assertEqual(len(nc['lanes']), 6)
        self.assertTrue(nc['per_process'])
