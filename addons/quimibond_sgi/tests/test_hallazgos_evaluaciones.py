# -*- coding: utf-8 -*-
"""57.53.0 — ficha, búsqueda y menú propios de los hallazgos de auditoría y
de las evaluaciones del cumplimiento legal (antes Odoo armaba una ficha
genérica y no tenían menú)."""
from datetime import date

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestHallazgosEvaluaciones(TransactionCase):

    def test_01_vistas_propias(self):
        cases = {
            'sgi.audit.finding': ('quimibond_sgi.sgi_audit_finding_view_form',
                                  'quimibond_sgi.sgi_audit_finding_view_search'),
            'sgi.legal.evaluation': ('quimibond_sgi.sgi_legal_evaluation_view_form',
                                     'quimibond_sgi.sgi_legal_evaluation_view_search'),
        }
        for model, (form_xmlid, search_xmlid) in cases.items():
            Model = self.env[model]
            self.assertEqual(Model.get_view(view_type='form')['id'], self.env.ref(form_xmlid).id,
                             "%s sin ficha propia." % model)
            self.assertEqual(Model.get_view(view_type='search')['id'], self.env.ref(search_xmlid).id,
                             "%s sin búsqueda propia." % model)

    def test_02_menus_con_su_accion(self):
        pairs = {
            'quimibond_sgi.menu_sgi_audit_findings': 'sgi.audit.finding',
            'quimibond_sgi.menu_sgi_legal_evaluations': 'sgi.legal.evaluation',
        }
        for menu_xmlid, model in pairs.items():
            menu = self.env.ref(menu_xmlid)
            self.assertEqual(menu.action.res_model, model)

    def test_03_nombres_legibles(self):
        process = self.env['sgi.process'].create({'code': 'XHE', 'name': 'Hallazgos prueba'})
        audit = self.env['sgi.audit'].create({'audit_type': 'interna',
                                              'process_ids': [(6, 0, process.ids)]})
        finding = self.env['sgi.audit.finding'].create({
            'audit_id': audit.id, 'finding_type': 'nc_menor', 'process_id': process.id,
            'description': 'Registro sin firma'})
        self.assertIn(audit.folio, finding.display_name)
        self.assertIn("No conformidad menor", finding.display_name)
        requirement = self.env['sgi.legal.requirement'].create({
            'name': 'Licencia ambiental XHE', 'reference': 'LAU-XHE'})
        wizard = self.env['sgi.legal.evaluate'].create({
            'requirement_id': requirement.id, 'result': 'cumple', 'evidence': 'Licencia vigente',
            'next_date': date(2047, 1, 1)})
        wizard.action_confirm()
        evaluation = self.env['sgi.legal.evaluation'].search([('requirement_id', '=', requirement.id)])
        self.assertEqual(len(evaluation), 1)
        self.assertIn('LAU-XHE', evaluation.display_name)
        # La búsqueda «No cumple o parcial» no la trae.
        self.assertFalse(self.env['sgi.legal.evaluation'].search(
            [('id', '=', evaluation.id), ('result', 'in', ('no_cumple', 'parcial'))]))
