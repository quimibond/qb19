# -*- coding: utf-8 -*-
"""57.114.0: campos de liga entrada ↔ salida que faltaban."""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestLigasTanda5(TransactionCase):

    def test_01_campos_de_liga(self):
        expected = {
            'approval.request': ('sgi_dyd_task_id', 'sgi_alert_id', 'sgi_maintenance_request_id'),
            'stock.picking': ('sgi_dyd_task_id', 'sgi_maintenance_request_id'),
            'project.task': ('sgi_dyd_picking_ids',),
            'account.move': ('sgi_action_line_id',),
        }
        for model, names in expected.items():
            for name in names:
                field = self.env[model]._fields.get(name)
                self.assertTrue(field and field.store, "%s.%s debe existir y guardarse" % (model, name))

    def test_02_la_liga_quita_el_aviso(self):
        Deliverable = self.env['sgi.deliverable']
        task_model = self.env['ir.model']._get('project.task')
        approval_model = self.env['ir.model']._get('approval.request')
        entrada = Deliverable.create({'code': 'ZL5-IN', 'name': 'Tarea del desarrollo (prueba)',
                                      'odoo_model_id': task_model.id})
        salida = Deliverable.create({'code': 'ZL5-OUT', 'name': 'Solicitud de compra (prueba)',
                                     'odoo_model_id': approval_model.id})
        process = self.env['sgi.process'].create({'code': 'ZL5', 'name': 'Proceso ligas'})
        activity = self.env['sgi.process.activity'].create({
            'process_id': process.id, 'number': 'ZL5.01', 'name': 'Pedir a Compras la materia prima',
            'output_deliverable_ids': [(6, 0, salida.ids)],
            'input_ids': [(0, 0, {'deliverable_id': entrada.id, 'max_days': 3})]})
        codes = [code for code, _msg in activity._sgi_spec_problems()]
        self.assertIn('no_match', codes)
        activity.input_ids.match_path = 'sgi_dyd_task_id'
        codes = [code for code, _msg in activity._sgi_spec_problems()]
        self.assertNotIn('no_match', codes)
