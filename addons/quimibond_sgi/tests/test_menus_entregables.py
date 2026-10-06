# -*- coding: utf-8 -*-
"""57.64.0 — menús de Entregables y Flujos entre procesos en SGI → Sistema,
con sus vistas propias (sin herencias). 57.67.0: con xmlids nuevos; los que
45.0.0 retiró (``SGI_REMOVED_XMLIDS``) no vuelven."""
import importlib.util
import os

from odoo.tests import TransactionCase, tagged

from ..models.sgi_cleanup import SGI_REMOVED_XMLIDS

_MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


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

    def test_04_post_migrate_quita_lo_del_xmlid_retirado(self):
        """57.67.0: el registro que quedó con el xmlid retirado (staging pasó
        por 57.64.0) se borra con su xmlid; el menú nuevo sigue."""
        processes = self.env.ref('quimibond_sgi.menu_sgi_processes')
        old_menu = self.env['ir.ui.menu'].create({
            'name': 'Entregables (57.64)', 'parent_id': processes.id,
            'action': 'ir.actions.act_window,%d'
                      % self.env.ref('quimibond_sgi.sgi_deliverable_list_action').id})
        self.env['ir.model.data'].create({
            'module': 'quimibond_sgi', 'name': 'menu_sgi_deliverables',
            'model': 'ir.ui.menu', 'res_id': old_menu.id})
        path = os.path.join(_MODULE_DIR, 'migrations', '19.0.57.67.0', 'post-migrate.py')
        spec = importlib.util.spec_from_file_location('sgi_mig_57_67_0', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.migrate(self.env.cr, '19.0.57.66.0')
        self.assertFalse(old_menu.exists())
        for name in module.OLD_XMLIDS:
            self.assertFalse(self.env.ref('quimibond_sgi.%s' % name, raise_if_not_found=False))
        self.assertTrue(self.env.ref('quimibond_sgi.menu_sgi_deliverable_list').exists())
        # Segunda corrida: nada que borrar.
        module.migrate(self.env.cr, '19.0.57.66.0')
        self.assertTrue(self.env.ref('quimibond_sgi.sgi_deliverable_list_action').exists())
