# -*- coding: utf-8 -*-
from datetime import timedelta

from odoo import fields
from odoo.tests import tagged

from .common import CotizadorCase


@tagged('post_install', '-at_install')
class TestLegado(CotizadorCase):
    """Importación de `qb.cotizacion` (qb_capacidad_costeo) sin tocarla."""

    def setUp(self):
        super().setUp()
        if 'qb.cotizacion' not in self.env:
            self.skipTest('qb_capacidad_costeo no está instalado')

    def _old(self, **vals):
        base = {'name': 'COT PRUEBA', 'partner_id': self.partner.id, 'product_id': self.tela.id,
                'volumen': 500, 'uom_name': 'm', 'fx_rate': 18.0,
                'currency_id': self.env.ref('base.USD').id,
                'mp_unit': 6.0, 'energia_unit': 1.0, 'fab_unit': 3.0, 'conv_unit': 0.5,
                'conv_var_unit': 0.1, 'rendimiento': 0.9, 'op_pct': 10.0,
                'costo_variable': 7.1 / 0.9, 'costo_absorbido_sin_op': 10.5 / 0.9,
                'piso_ocioso': 7.1 / 0.9, 'piso_lleno': 10.5 / 0.9 / 0.9,
                'precio_objetivo': 18.0 * 1.2, 'precio_mercado': 14.0,
                'validez_hasta': fields.Date.today() - timedelta(days=3), 'tc_lta': True,
                'tramo_ids': [(0, 0, {'multiplo': 1.0, 'volumen': 500, 'es_base': True,
                                      'precio_mxn': 21.6, 'precio_divisa': 1.2})]}
        base.update(vals)
        return self.env['qb.cotizacion'].sudo().create(base)

    def test_01_importa_estado_revision_y_costo(self):
        Cot = self.env['qb.cotizador.cotizacion']
        before = Cot.with_context(active_test=False).search_count([])
        old1 = self._old(state='done')
        old2 = self._old(state='draft')          # misma pareja: revisión 2, old1 → superseded
        old3 = self._old(product_id=self.hermana.id, state='won')
        self.assertEqual(old1.state, 'superseded')
        n = Cot.importar_legadas()
        self.assertEqual(n, Cot.with_context(active_test=False).search_count([]) - before)
        nuevas = {c.legacy_id: c for c in Cot.search([('legacy_id', 'in', [old1.id, old2.id, old3.id])])}
        self.assertEqual(nuevas[old1.id].state, 'reemplazada')
        self.assertEqual(nuevas[old2.id].state, 'borrador')
        self.assertEqual(nuevas[old2.id].revision, 2)
        self.assertEqual(nuevas[old2.id].revision_anterior_id, nuevas[old1.id])
        self.assertEqual(nuevas[old3.id].state, 'ganada')
        n2 = nuevas[old2.id]
        self.assertEqual(n2.folio, old2.folio)
        self.assertEqual(n2.costo_fuente, 'legado')
        self.assertAlmostEqual(n2.precio_objetivo, 1.2, places=4, msg='El precio vuelve a su divisa')
        self.assertAlmostEqual(n2.fx_rate, 18.0)
        self.assertAlmostEqual(n2.piso_ocioso, 7.1 / 0.9, places=4)
        self.assertAlmostEqual(n2.piso_lleno, 10.5 / 0.9 / 0.9, places=4)
        self.assertTrue(n2.tc_lta)
        self.assertEqual(len(n2.tramo_ids), 1)
        self.assertEqual(n2.create_date, old2.create_date)
        self.assertEqual(old2.state, 'draft', 'El modelo viejo no se toca')
        self.assertEqual(Cot.importar_legadas(), 0, 'Idempotente')

    def test_02_presentada_vencida_entra_como_vencida(self):
        old = self._old(state='done')
        self.env['qb.cotizador.cotizacion'].importar_legadas()
        nueva = self.env['qb.cotizador.cotizacion'].search([('legacy_id', '=', old.id)])
        self.assertEqual(nueva.state, 'vencida')
        self.assertTrue(nueva.approved_date, 'Lo presentado ya estaba aprobado')
        viva = self._old(state='done', product_id=self.hermana.id,
                         validez_hasta=fields.Date.today() + timedelta(days=5))
        self.env['qb.cotizador.cotizacion'].importar_legadas()
        self.assertEqual(self.env['qb.cotizador.cotizacion'].search(
            [('legacy_id', '=', viva.id)]).state, 'presentada')
