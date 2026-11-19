# -*- coding: utf-8 -*-
"""19.0.1.2.0 (revisión de vistas del SGI, V-M10): sumas en las líneas y un
encabezado con solo el flujo; las consultas como botón inteligente y las
cargas en el engrane."""
from lxml import etree

from odoo.tests import TransactionCase, tagged

FLOW = {'action_send_to_review', 'action_approve', 'action_revise',
        'action_set_borrador'}


def _arch(env, model, view_type, view_ref):
    views = env[model].get_views([(env.ref(view_ref).id, view_type)])
    return etree.fromstring(views['views'][view_type]['arch'])


@tagged('post_install', '-at_install')
class TestSalesBudgetViews(TransactionCase):

    def test_01_sumas_en_la_lista_de_lineas(self):
        arch = _arch(self.env, 'sgi.sales.budget.line', 'list',
                     'quimibond_ventas_presupuesto.sgi_sales_budget_line_view_list')
        for name in ('qty_budget', 'qty_real', 'amount_budget', 'amount_real', 'amount_ordered'):
            self.assertTrue(arch.xpath("//list/field[@name='%s'][@sum]" % name), name)

    def test_02_encabezado_solo_con_el_flujo(self):
        arch = _arch(self.env, 'sgi.sales.budget', 'form',
                     'quimibond_ventas_presupuesto.sgi_sales_budget_view_form')
        header = {b.get('name') for b in arch.xpath('//header/button')}
        self.assertFalse(header - FLOW, "Fuera del flujo en el encabezado: %s" % (header - FLOW))
        self.assertTrue(arch.xpath("//div[@name='button_box']/button[@name='action_open_analysis']"))
        bindings = self.env['ir.actions.actions'].get_bindings('sgi.sales.budget')
        reports = {b['id'] for b in bindings.get('report', [])}
        actions = {b['id'] for b in bindings.get('action', [])}
        self.assertIn(self.env.ref('quimibond_ventas_presupuesto.action_report_sales_budget').id, reports)
        for xmlid in ('sgi_sales_budget_refresh_actuals_server', 'sgi_sales_budget_reconcile_server',
                      'sgi_sales_budget_import_action'):
            self.assertIn(self.env.ref('quimibond_ventas_presupuesto.' + xmlid).id, actions, xmlid)

    def test_03_ficha_nativa(self):
        """3.23.0: campos y botones inteligentes nativos; nada de HTML hecho a mano."""
        arch = _arch(self.env, 'sgi.sales.budget', 'form',
                     'quimibond_ventas_presupuesto.sgi_sales_budget_view_form')
        for name in ('action_qb_open_lines', 'action_qb_compare_mercado', 'action_qb_compare_mes',
                     'action_qb_compare_cliente', 'action_open_analysis'):
            self.assertTrue(arch.xpath("//div[@name='button_box']/button[@name='%s']" % name), name)
        self.assertTrue(arch.xpath("//field[@name='fulfillment_pct'][@widget='progressbar']"))
        self.assertTrue(arch.xpath("//field[@name='line_ids']"))
        self.assertFalse(arch.xpath("//field[contains(@name, '_html')]"))
        budget = self.env['sgi.sales.budget'].create({'year': 2049})
        for method in ('action_qb_open_lines', 'action_qb_compare_mercado', 'action_qb_compare_mes',
                       'action_qb_compare_cliente', 'action_open_analysis'):
            action = getattr(budget, method)()
            for view_id, view_type in action.get('views', []):
                if view_id:
                    self.env[action['res_model']].get_views([(view_id, view_type)])
