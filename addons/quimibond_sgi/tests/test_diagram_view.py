# -*- coding: utf-8 -*-
"""Tipo de vista «sgi_diagram» (19.0.54.0.0): el diagrama como una vista más
del selector de cada acción, y la ficha del proceso con botones inteligentes."""
from odoo.exceptions import ValidationError
from odoo.tests import TransactionCase, tagged

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestDiagramView(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.env = cls.env(context=dict(cls.env.context, sgi_skip_role_check=True))
        cls.proc = cls.env['sgi.process'].create({'code': 'XV1', 'name': 'Proceso V'})
        cls.other = cls.env['sgi.process'].create({'code': 'XV2', 'name': 'Otro V'})

    def test_01_tipo_de_vista_registrado(self):
        View = self.env['ir.ui.view']
        self.assertIn('sgi_diagram', dict(View._fields['type'].selection))
        self.assertIn('sgi_diagram', dict(self.env['ir.actions.act_window.view']._fields['view_mode'].selection))
        self.assertEqual(View._get_view_info()['sgi_diagram']['icon'], 'fa fa-sitemap')
        for model, kind in (('sgi.risk', 'risk_matrix'), ('sgi.process', 'process_map'),
                            ('quality.alert', 'nc_flow'), ('documents.document', 'doc_tree')):
            views = self.env[model].get_views([(False, 'sgi_diagram')])
            arch = views['views']['sgi_diagram']['arch']
            self.assertIn('kind="%s"' % kind, arch, model)
        # Cada diagrama del catálogo existe como método y trae etiqueta.
        catalog = self.env['sgi.diagram'].catalog()
        self.assertEqual({c['kind'] for c in catalog}, self.env['sgi.diagram']._kinds())
        self.assertTrue(all(c['label'] for c in catalog))
        self.assertGreaterEqual(len(catalog), 19)

    def test_02_validacion_de_la_vista(self):
        View = self.env['ir.ui.view']
        View.create({'name': 'ok', 'model': 'sgi.risk', 'arch': '<sgi_diagram kind="risk_matrix" kinds="risk_matrix,pdca"/>'})
        with self.assertRaises(ValidationError):
            View.create({'name': 'mal', 'model': 'sgi.risk', 'arch': '<sgi_diagram kind="no_existe"/>'})
        with self.assertRaises(ValidationError):
            View.create({'name': 'mal2', 'model': 'sgi.risk', 'arch': '<sgi_diagram kind="risk_matrix" foo="1"/>'})
        with self.assertRaises(ValidationError):
            View.create({'name': 'mal3', 'model': 'sgi.risk', 'arch': '<sgi_diagram kind="risk_matrix"><field name="name"/></sgi_diagram>'})

    def test_03_acciones_con_diagrama_y_sin_menu_duplicado(self):
        ref = self.env.ref
        self.assertEqual(ref('quimibond_sgi.sgi_process_action').view_mode.split(',')[0], 'sgi_diagram')
        for xid in ('sgi_risk_action', 'sgi_legal_requirement_action', 'sgi_interested_party_action',
                    'sgi_objective_action', 'sgi_management_review_action', 'sgi_audit_program_action',
                    'sgi_emergency_plan_action', 'sgi_equipment_action_measuring', 'sgi_document_action',
                    'sgi_competence_gap_action', 'sgi_activity_exec_stat_action_who', 'sgi_process_activity_action'):
            self.assertIn('sgi_diagram', ref('quimibond_sgi.' + xid).view_mode.split(','), xid)
        # El submenú Diagramas se retiró: el diagrama es una vista de cada acción.
        stale = self.env['ir.model.data'].search([('module', '=', 'quimibond_sgi'), ('model', '=', 'ir.ui.menu'),
                                                  ('name', '=like', 'menu_sgi_diagram%')])
        self.assertFalse(stale, stale.mapped('name'))
        self.assertTrue(ref('quimibond_sgi.menu_sgi_activities'))
        # Los PDF de la ficha viven en el menú Imprimir.
        for xid in ('action_report_procedure', 'action_report_master_list', 'action_report_risk_matrix'):
            self.assertEqual(ref('quimibond_sgi.' + xid).binding_model_id.model, 'sgi.process', xid)

    def test_04_ficha_del_proceso_con_botones(self):
        self.env['sgi.process.flow'].create({
            'from_process_id': self.other.id, 'to_process_id': self.proc.id, 'name': 'Insumo V'})
        self.assertEqual(self.proc.flow_count, 1)
        action = self.proc.action_open_flows()
        flows = self.env['sgi.process.flow'].search(action['domain'])
        self.assertEqual(flows.mapped('name'), ['Insumo V'])
        views = self.env['sgi.process'].get_views([(False, 'form')])
        arch = views['views']['form']['arch']
        for button in ('action_sgi_view_diagram', 'action_open_indicators', 'action_open_risks',
                       'action_open_documents', 'action_open_flows', 'action_open_ncs', 'action_view_spec_gaps'):
            self.assertIn('name="%s"' % button, arch, button)
        self.assertNotIn('page string="Indicadores"', arch)
        self.assertNotIn('action_print_master_list', arch, "El PDF sale del menú Imprimir, no del encabezado.")
        diagram = self.proc.action_sgi_view_diagram()
        self.assertEqual(diagram['context']['sgi_diagram_kind'], 'process_flow')
        self.assertIn('process_map', diagram['context']['sgi_diagram_kinds'].split(','))
