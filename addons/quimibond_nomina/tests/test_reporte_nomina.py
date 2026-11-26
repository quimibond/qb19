# -*- coding: utf-8 -*-
"""Reportes de nómina con formato NOI (1.23.0). El cálculo se prueba con pytest
puro (``tests_puros/nomina``); aquí, lo que necesita la base: catálogo, menú,
asistente y que los dos reportes se generen en PDF (HTML) y Excel con recibos
de la base. No corre en el CI (depende de Enterprise); se corre en Odoo.sh:
``--test-tags /quimibond_nomina``."""
from odoo.exceptions import UserError, ValidationError
from odoo.tests.common import TransactionCase


class TestReporteNomina(TransactionCase):

    def test_catalogo_sembrado(self):
        Concepto = self.env['qb.nomina.concepto.noi']
        sueldo = self.env.ref('quimibond_nomina.concepto_noi_p001')
        self.assertEqual(sueldo.display_name, 'P001 SUELDO')
        self.assertEqual(Concepto.search_count([('tipo', '=', 'deduccion')]) >= 9, True)
        with self.assertRaises(ValidationError):
            Concepto.create({'name': 'X', 'tipo': 'percepcion', 'codigos': 'BASIC', 'codigos_exento': 'FAP'})

    def test_menu_colgado(self):
        menu = self.env.ref('quimibond_nomina.menu_qb_nomina_reportes')
        if self.env['qb.nomina.concepto.noi'].qb_nomina_colgar_menu_reportes():
            self.assertTrue(menu.parent_id)
            self.assertTrue(menu.active)

    def test_sin_lote_avisa(self):
        wiz = self.env['qb.nomina.reporte'].create({'modo': 'lote'})
        with self.assertRaises(UserError):
            wiz.action_generar()

    def test_reportes_con_recibos_de_la_base(self):
        slip = self.env['hr.payslip'].search([('payslip_run_id', '!=', False), ('state', '!=', 'cancel'),
                                              ('line_ids', '!=', False)], limit=1, order='id desc')
        if not slip:
            self.skipTest("La base no tiene recibos calculados en un lote.")
        for reporte in ('nomina', 'percepcion_deduccion'):
            wiz = self.env['qb.nomina.reporte'].create({
                'modo': 'lote', 'payslip_run_ids': [(6, 0, slip.payslip_run_id.ids)], 'reporte': reporte,
                'company_id': slip.company_id.id, 'incluir_borrador': True})
            datos = wiz._qb_datos()
            self.assertTrue(datos['recibos'])
            # Sin diseño de documento en la compañía (base de build), Odoo abre
            # primero el configurador del encabezado; aquí se prueba el reporte.
            accion = wiz.with_context(discard_logo_check=True).action_generar()
            self.assertEqual(accion['type'], 'ir.actions.report')
            html, _tipo = self.env['ir.actions.report']._render_qweb_html(accion['report_name'], wiz.ids)
            self.assertIn(b'Percepci', html)
            wiz.formato = 'xlsx'
            accion = wiz.action_generar()
            self.assertEqual(accion['type'], 'ir.actions.act_url')
            self.assertTrue(wiz.archivo)
