# -*- coding: utf-8 -*-
"""57.64.0 — menús de Entregables y Flujos entre procesos en SGI → Sistema,
con sus vistas propias (sin herencias). 57.67.0: con xmlids nuevos; los que
45.0.0 retiró (``SGI_REMOVED_XMLIDS``) no vuelven."""

from odoo.tests import TransactionCase, tagged

from ..models.sgi_cleanup import SGI_REMOVED_XMLIDS

@tagged('post_install', '-at_install')
class TestMenusEntregables(TransactionCase):

    def test_01_menus_con_su_accion(self):
        processes = self.env.ref('quimibond_sgi.menu_sgi_processes')
        for menu_xmlid, action_xmlid, model in (
                ('menu_sgi_deliverable_list', 'sgi_deliverable_list_action', 'sgi.deliverable'),
                ('menu_sgi_process_flows', 'sgi_process_flow_list_action', 'sgi.process.flow')):
            menu = self.env.ref('quimibond_sgi.%s' % menu_xmlid)
            action = self.env.ref('quimibond_sgi.%s' % action_xmlid)
            self.assertEqual(menu.parent_id, processes)
            self.assertEqual(menu.action, action)
            self.assertEqual(action.res_model, model)
            self.assertTrue(action.search_view_id)
            self.assertFalse(action.search_view_id.inherit_id)

    def test_03_sin_xmlids_retirados(self):
        """57.67.0: 57.64.0 volvió a crear ``menu_sgi_deliverables`` y
        ``sgi_deliverable_action`` (retirados en 45.0.0); los menús nuevos no
        usan ningún xmlid retirado."""
        for xmlid in ('menu_sgi_deliverable_list', 'sgi_deliverable_list_action',
                      'menu_sgi_process_flows', 'sgi_process_flow_list_action',
                      'menu_sgi_audit_findings', 'sgi_audit_finding_list_action'):
            self.assertNotIn('quimibond_sgi.%s' % xmlid, SGI_REMOVED_XMLIDS)
            self.assertTrue(self.env.ref('quimibond_sgi.%s' % xmlid, raise_if_not_found=False))

    def test_02_filtro_sin_modelo(self):
        company = self.env.company
        own = self.env['sgi.deliverable'].create(
            {'name': 'ZM entregable propio', 'code': 'ZM-PROPIO', 'company_id': company.id})
        external = self.env['sgi.deliverable'].create(
            {'name': 'ZM entrada externa', 'code': 'ZM-EXT', 'company_id': company.id,
             'boundary': 'entrada_externa'})
        domain = [('odoo_model_id', '=', False), ('boundary', '!=', 'entrada_externa')]
        found = self.env['sgi.deliverable'].search(domain + [('code', 'like', 'ZM-')])
        self.assertIn(own, found)
        self.assertNotIn(external, found)
