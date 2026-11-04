# -*- coding: utf-8 -*-
"""3.1.0: Inspección como estación única y registro de la orden en metros al cerrar el baño.

Casos de aceptación 1 a 11 del diseño ``docs/superpowers/specs/2026-11-03-inspeccion-clasificacion-rollos-design.md``
(el 12, costeo, lo prueba ``qb_costeo``).
"""
from odoo import Command
from odoo.exceptions import UserError
from odoo.tests import tagged
from odoo.tests.common import TransactionCase


@tagged('post_install', '-at_install', 'quimibond_rollos_bitacora')
class TestInspeccionEstacion(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        cls.metro = cls.env.ref('uom.product_uom_meter')
        cls.prod_kg = cls._producto('IZZ951Q22JNT160', cls.kg)
        cls.prod_m = cls._producto('ZZ951Q22JNT160', cls.metro)
        cls.saldo_2 = cls._producto('ZZ951 SALDO SEGUNDA', cls.kg)
        cls.saldo_d = cls._producto('ZZ951 SALDO DESPERDICIO', cls.kg)
        cls.crudo = cls.env['product.product'].create({
            'name': 'Crudo ZZ951', 'default_code': 'ZZ951CRUDO', 'type': 'consu', 'uom_id': cls.kg.id})
        params = cls.env['ir.config_parameter'].sudo()
        params.set_param('quimibond_ficha_tecnica_tela.saldo_segunda', cls.saldo_2.default_code)
        params.set_param('quimibond_ficha_tecnica_tela.saldo_desperdicio', cls.saldo_d.default_code)
        stock = cls.env['stock.warehouse'].search([('company_id', '=', cls.env.company.id)], limit=1).lot_stock_id
        cls.ubic = {}
        for calidad, clave in (('PQ', 'pq'), ('FE', 'fe'), ('SEGUNDA', 'segunda'), ('DESP', 'desperdicio')):
            nombre = 'ZZ951 %s' % calidad
            cls.ubic[calidad] = cls.env['stock.location'].create(
                {'name': nombre, 'location_id': stock.id, 'usage': 'internal'})
            params.set_param('quimibond_rollos_bitacora.ubicacion_%s' % clave, nombre)
        Tara = cls.env['mrp.tara.producto']
        Tara.create({'product_tmpl_id': cls.prod_kg.product_tmpl_id.id, 'operacion': 'rama', 'tara_kg': 1.1})
        Tara.create({'product_tmpl_id': cls.prod_kg.product_tmpl_id.id, 'operacion': 'clasificacion_final',
                     'tara_kg': 1.5})
        cls.bom_kg = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.prod_kg.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': cls.kg.id,
            'consumption': 'flexible',
            'bom_line_ids': [Command.create({'product_id': cls.crudo.id, 'product_qty': 1.1})]})
        cls.bom_m = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.prod_m.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': cls.metro.id,
            'consumption': 'flexible',
            'bom_line_ids': [Command.create({'product_id': cls.prod_kg.id, 'product_qty': 0.0848})],
            'byproduct_ids': [Command.create({'product_id': cls.saldo_2.id, 'product_qty': 0.0, 'cost_share': 0.0}),
                              Command.create({'product_id': cls.saldo_d.id, 'product_qty': 0.0, 'cost_share': 0.0})]})
        Workcenter = cls.env['mrp.workcenter']
        cls.rama = Workcenter.create({'name': 'RAMA ZZ951', 'code': 'RZZ951'})
        cls.revisadora = Workcenter.create({'name': 'INSPECCION ZZ951', 'code': 'IZZ951'})

    @classmethod
    def _producto(cls, codigo, uom):
        return cls.env['product.product'].create({
            'name': codigo, 'default_code': codigo, 'type': 'consu', 'is_storable': True, 'uom_id': uom.id,
            'tracking': 'lot'})

    # ------------------------------------------------------------------ armado
    def _bano(self, rollos_kg=(50.0,), en_kg=False):
        """Orden en metros (2,000 m) y la orden en kg que la abastece, ligadas como el MTO, con rollos pesados en Rama."""
        orden_m = self.env['mrp.production']
        if not en_kg:
            orden_m = orden_m.create({
                'product_id': self.prod_m.id, 'product_qty': 2000, 'product_uom_id': self.metro.id,
                'bom_id': self.bom_m.id})
            orden_m.action_confirm()
        orden_kg = self.env['mrp.production'].create({
            'product_id': self.prod_kg.id, 'product_qty': 169.6, 'bom_id': self.bom_kg.id})
        orden_kg.action_confirm()
        if orden_m:
            terminado = orden_kg.move_finished_ids.filtered(lambda m: m.product_id == self.prod_kg)
            orden_m._clasif_move_a().move_orig_ids = terminado
        wo_rama = self.env['mrp.workorder'].create({
            'name': 'RAMA', 'production_id': orden_kg.id, 'workcenter_id': self.rama.id})
        wo_insp = self.env['mrp.workorder'].create({
            'name': 'INSPECCION', 'production_id': orden_kg.id, 'workcenter_id': self.revisadora.id})
        rollos = self.env['mrp.rollo.bitacora']
        for i, kg in enumerate(rollos_kg, 1):
            nombre = '%s-%04d' % (orden_kg.name.split('/')[-1], i)
            lote = self.env['stock.lot'].create({
                'name': nombre, 'product_id': self.prod_kg.id, 'company_id': self.env.company.id})
            rollos |= self.env['mrp.rollo.bitacora'].create({
                'production_id': orden_kg.id, 'secuencia': i, 'lot_id': lote.id, 'kg_rama': kg,
                'workcenter_rama_id': self.rama.id})
            orden_kg._rama_registrar_movimiento(lote, kg, wo_rama)
        return orden_m, orden_kg, wo_insp, rollos

    def _inspeccionar(self, orden_kg, wo_insp, rollos, kg_bruto, metros, calidad='PQ', termina=True, **vals):
        valores = {
            'production_id': orden_kg.id, 'workorder_id': wo_insp.id, 'workcenter_id': self.revisadora.id,
            'empleado_numero': '1', 'weighing_mode': 'manual', 'motivo_manual': 'Prueba sin báscula',
            'rollo_codigos': ' '.join(rollos.mapped('name')), 'kg_bruto': kg_bruto, 'metros': metros,
            'clasificacion': calidad, 'termina_rollo': termina}
        valores.update(vals)
        wiz = self.env['rollo.inspeccion.estacion.wizard'].create(valores)
        wiz.action_confirmar()
        return wiz

    def _cantidad(self, producto, ubicacion, lote_nombre=None):
        dominio = [('product_id', '=', producto.id), ('location_id', '=', ubicacion.id)]
        if lote_nombre:
            dominio.append(('lot_id.name', '=', lote_nombre))
        return sum(self.env['stock.quant'].search(dominio).mapped('quantity'))

    # ------------------------------------------------------------------ casos
    def test_01_primera_sin_corte_y_cierre_del_bano(self):
        """Casos 1 y 8: un rollo final con lote nuevo; al cerrar la orden en kg, la orden en metros queda registrada."""
        orden_m, orden_kg, wo, rollos = self._bano((50.0,))
        self._inspeccionar(orden_kg, wo, rollos, 50.6, 585.0)
        final = orden_kg.rollo_final_kg_ids
        self.assertEqual(final.name, 'M%s-0001' % orden_m.name.split('/')[-1])
        self.assertAlmostEqual(final.kg_neto, 49.1)
        self.assertIn('METROS: 585.00', final.zpl_etiqueta)
        self.assertEqual(rollos.estado_inspeccion, 'inspeccionado')
        self.assertEqual(final.estado, 'inspeccionado')
        orden_kg.button_mark_done()
        self.assertEqual((orden_kg.state, orden_m.state), ('done', 'done'))
        self.assertEqual(final.estado, 'registrado')
        self.assertAlmostEqual(orden_m.qty_produced, 585.0)
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ'], final.name), 585.0)
        consumo = orden_m.move_raw_ids.filtered(lambda m: m.product_id == self.prod_kg)
        self.assertAlmostEqual(consumo.quantity, 50.0, msg="Consume el rollo de Rama completo")

    def test_02_corte_primera_y_segunda(self):
        """Caso 2: 250 m de primera y 120 m de segunda; la segunda sale en kg como saldo, a 16."""
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        self._inspeccionar(orden_kg, wo, rollos, 26.5, 250.0, 'PQ', termina=False)
        self.assertEqual(rollos.estado_inspeccion, 'pendiente', "Con pedazos pendientes, el rollo sigue sin inspeccionar")
        wiz = self._inspeccionar(orden_kg, wo, rollos, 13.4, 120.0, 'SEGUNDA')
        self.assertAlmostEqual(wiz.kg_ya_cortados, 25.0)
        self.assertAlmostEqual(wiz.merma_kg, 40.0 - 25.0 - 11.9)
        orden_kg.button_mark_done()
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ']), 250.0)
        segunda = orden_kg.rollo_final_kg_ids.filtered(lambda r: r.clasificacion == 'SEGUNDA')
        self.assertEqual(segunda.product_id, self.saldo_2)
        self.assertAlmostEqual(self._cantidad(self.saldo_2, self.ubic['SEGUNDA'], segunda.name), 11.9)
        self.assertAlmostEqual(orden_m.qty_produced, 250.0, msg="La orden en metros cierra con los metros de primera")

    def test_03_04_fe_y_desperdicio(self):
        """Casos 3 y 4: FE en metros a 15; desperdicio en kg (saldo D) a 17."""
        orden_m, orden_kg, wo, rollos = self._bano((40.0, 30.0))
        self._inspeccionar(orden_kg, wo, rollos[0], 41.0, 460.0, 'FE')
        self._inspeccionar(orden_kg, wo, rollos[1], 30.5, 340.0, 'PQ', termina=False)
        self._inspeccionar(orden_kg, wo, rollos[1], 1.5 + 0.8, 5.0, 'DESP')
        orden_kg.button_mark_done()
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['FE']), 460.0)
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ']), 340.0)
        self.assertAlmostEqual(self._cantidad(self.saldo_d, self.ubic['DESP']), 0.8)

    def test_05_06_juntar_mismo_bano_y_otro_bano(self):
        """Caso 5: dos rollos cortos del mismo baño dan un rollo final; caso 6: de otro baño, error."""
        orden_m, orden_kg, wo, rollos = self._bano((12.0, 9.0))
        self._inspeccionar(orden_kg, wo, rollos, 22.4, 245.0)
        self.assertEqual(len(orden_kg.rollo_final_kg_ids), 1)
        self.assertEqual(orden_kg.rollo_final_kg_ids.bitacora_ids, rollos)
        _orden_m2, orden_kg2, wo2, rollos2 = self._bano((10.0,))
        with self.assertRaisesRegex(UserError, 'mismo baño'):
            self._inspeccionar(orden_kg2, wo2, rollos2 | rollos[:1], 20.0, 200.0)
        orden_kg.button_mark_done()
        consumo = orden_m.move_raw_ids.filtered(lambda m: m.product_id == self.prod_kg)
        self.assertAlmostEqual(consumo.quantity, 21.0, msg="Los dos rollos de Rama se consumen")

    def test_07_08_sin_cerrar_no_hay_existencia_y_bloqueo(self):
        """Caso 7: antes de cerrar, el rollo final no existe en inventario; caso 8: con un rollo sin inspeccionar no
        cierra."""
        orden_m, orden_kg, wo, rollos = self._bano((20.0, 20.0))
        self._inspeccionar(orden_kg, wo, rollos[0], 21.4, 230.0)
        final = orden_kg.rollo_final_kg_ids
        self.assertFalse(self.env['stock.lot'].search([('name', '=', final.name)]), "El lote se crea al registrar")
        self.assertFalse(self._cantidad(self.prod_m, self.ubic['PQ']))
        self.assertTrue(orden_kg.inspeccion_completa_sin_cerrar is False)
        with self.assertRaisesRegex(UserError, 'sin inspeccionar'):
            orden_kg.button_mark_done()
        self._inspeccionar(orden_kg, wo, rollos[1], 21.3, 228.0)
        self.assertTrue(orden_kg.inspeccion_completa_sin_cerrar)
        orden_kg.button_mark_done()
        self.assertEqual(orden_m.state, 'done')
        self.assertFalse(orden_kg.inspeccion_completa_sin_cerrar)

    def test_09_10_merma_y_captura_manual(self):
        """Caso 9: merma arriba del límite avisa antes de guardar; caso 10: captura manual sin motivo, error."""
        orden_m, orden_kg, wo, rollos = self._bano((50.0,))
        self.env['ir.config_parameter'].sudo().set_param('quimibond_rollos_bitacora.alerta_merma_inspeccion_pct', '3')
        wiz = self.env['rollo.inspeccion.estacion.wizard'].new({
            'production_id': orden_kg.id, 'workorder_id': wo.id, 'rollo_codigos': rollos.name, 'kg_bruto': 46.5,
            'metros': 520.0, 'clasificacion': 'PQ', 'termina_rollo': True})
        self.assertAlmostEqual(wiz.merma_kg, 5.0)
        self.assertIn('arriba del límite', wiz.alertas)
        with self.assertRaisesRegex(UserError, 'motivo'):
            self._inspeccionar(orden_kg, wo, rollos, 50.6, 585.0, motivo_manual=False)

    def test_11_falla_el_registro_no_cierra_nada(self):
        """Caso 11: si el registro falla (la ubicación de la segunda no existe), el baño no se cierra y el mensaje dice
        por qué."""
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        self._inspeccionar(orden_kg, wo, rollos, 26.5, 250.0, 'PQ', termina=False)
        self._inspeccionar(orden_kg, wo, rollos, 13.4, 120.0, 'SEGUNDA')
        self.env['ir.config_parameter'].sudo().set_param('quimibond_rollos_bitacora.ubicacion_segunda', 'ZZ951 NO EXISTE')
        with self.assertRaisesRegex(UserError, 'No existe la ubicación'):
            orden_kg.button_mark_done()
        self.env.invalidate_all()
        self.assertNotEqual(orden_kg.state, 'done')
        self.assertNotEqual(orden_m.state, 'done')

    def test_12_no_cortar_y_juntar_a_la_vez(self):
        orden_m, orden_kg, wo, rollos = self._bano((20.0, 20.0))
        self._inspeccionar(orden_kg, wo, rollos[0], 10.0, 100.0, termina=False)
        with self.assertRaisesRegex(UserError, 'cortar y juntar'):
            self._inspeccionar(orden_kg, wo, rollos, 30.0, 300.0)

    def test_13_bano_para_venta_en_kg(self):
        """3.2.0: sin orden en metros, los rollos finales quedan como el acabado en kg con su peso real (lote K…): PQ a
        14, FE a 15; la segunda como saldo en kg a 16."""
        _m, orden_kg, wo, rollos = self._bano((40.0, 30.0), en_kg=True)
        self._inspeccionar(orden_kg, wo, rollos[0], 41.0, 460.0, 'PQ')
        self._inspeccionar(orden_kg, wo, rollos[1], 20.5, 230.0, 'FE', termina=False)
        self._inspeccionar(orden_kg, wo, rollos[1], 10.5, 110.0, 'SEGUNDA')
        primero = orden_kg.rollo_final_kg_ids.sorted('secuencia')[:1]
        self.assertEqual(primero.name, 'K%s-0001' % orden_kg.name.split('/')[-1])
        orden_kg.button_mark_done()
        self.assertEqual(orden_kg.state, 'done')
        self.assertAlmostEqual(orden_kg.qty_produced, 39.5 + 19.0, msg="La orden en kg cierra con el peso real de PQ y FE")
        self.assertAlmostEqual(self._cantidad(self.prod_kg, self.ubic['PQ'], primero.name), 39.5)
        self.assertAlmostEqual(self._cantidad(self.prod_kg, self.ubic['FE']), 19.0)
        self.assertAlmostEqual(self._cantidad(self.saldo_2, self.ubic['SEGUNDA']), 9.0)
        self.assertEqual(set(orden_kg.rollo_final_kg_ids.mapped('estado')), {'registrado'})

    def test_14_bano_sin_primera_ni_fe(self):
        """3.2.0: todo en segunda y desperdicio: la orden en metros se cancela y una orden de saldos saca los saldos en
        kg, consumiendo los rollos de Rama completos."""
        orden_m, orden_kg, wo, rollos = self._bano((20.0,))
        self._inspeccionar(orden_kg, wo, rollos, 18.5, 200.0, 'SEGUNDA', termina=False)
        self._inspeccionar(orden_kg, wo, rollos, 3.0, 20.0, 'DESP')
        orden_kg.button_mark_done()
        self.assertEqual(orden_kg.state, 'done')
        self.assertEqual(orden_m.state, 'cancel')
        finales = orden_kg.rollo_final_kg_ids
        self.assertEqual(set(finales.mapped('estado')), {'registrado'})
        orden_s = finales.production_saldo_id
        self.assertEqual(orden_s.state, 'done')
        self.assertAlmostEqual(self._cantidad(self.saldo_2, self.ubic['SEGUNDA']), 17.0)
        self.assertAlmostEqual(self._cantidad(self.saldo_d, self.ubic['DESP']), 1.5)
        consumo = orden_s.move_raw_ids.filtered(lambda m: m.product_id == self.prod_kg)
        self.assertAlmostEqual(consumo.quantity, 20.0)

