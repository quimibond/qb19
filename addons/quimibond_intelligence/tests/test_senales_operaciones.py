# -*- coding: utf-8 -*-
from odoo.tests import tagged

from .common import SenalesCommon


@tagged('post_install', '-at_install', 'quimibond_intelligence')
class TestSenalesOperaciones(SenalesCommon):

    def _op(self, hace):
        prod = self.env['product.product'].create({'name': 'Rollo', 'type': 'consu', 'is_storable': True})
        mo = self.env['mrp.production'].create({'product_id': prod.id, 'product_qty': 1, 'product_uom_id': prod.uom_id.id, 'date_start': self.hace(hace)})
        mo.action_confirm()
        mo.date_start = self.hace(hace)
        return mo

    def test_op_atrasada_y_zombie(self):
        vieja = self._op(100)
        media = self._op(10)
        futura = self._op(-3)
        filas = self.filas('op_atrasada', {'umbrales': {'dias': 7}})
        fv = self.fila_de(filas, 'op_atrasada:mrp.production:%d' % vieja.id)
        fm = self.fila_de(filas, 'op_atrasada:mrp.production:%d' % media.id)
        self.assertEqual(fv['payload']['fecha_base'], str(self.hace(100)))  # senales_actualizar la hará zombie (> 90)
        self.assertEqual(fm['valor'], 10)
        claves = {f['clave'] for f in filas}
        self.assertNotIn('op_atrasada:mrp.production:%d' % futura.id, claves)
        # Sin len(filas): la copia de producción tiene cientos de OP atrasadas.

    def test_existencia_negativa_por_ubicacion(self):
        wh = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1)
        loc = wh.lot_stock_id
        antes = [f for f in self.filas('existencia_negativa') if f['clave'].endswith(':%d' % loc.id)]
        n_antes = antes[0]['valor'] if antes else 0
        prod = self.env['product.product'].create({'name': 'Hilo', 'type': 'consu', 'is_storable': True})
        self.env['stock.quant'].sudo().create({'product_id': prod.id, 'location_id': loc.id, 'quantity': -4})
        f = self.fila_de(self.filas('existencia_negativa'), 'existencia_negativa:stock.location:%d' % loc.id)
        self.assertEqual(f['payload']['grupo'], loc.complete_name)
        self.assertEqual(f['valor'], n_antes + 1)
        # La señal manda solo 30 productos. Con la ubicación ya llena, el nuevo
        # puede quedar fuera de ese recorte; el conteo sí tiene que subir.
        if n_antes < 30:
            self.assertIn(prod.id, [d['id'] for d in f['documentos']])
