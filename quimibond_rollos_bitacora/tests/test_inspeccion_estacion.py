# -*- coding: utf-8 -*-
"""3.1.0: Inspección como estación única y registro de la orden en metros al cerrar el baño.

Casos de aceptación 1 a 11 del diseño ``docs/superpowers/specs/2026-11-03-inspeccion-clasificacion-rollos-design.md``
(el 12, costeo, lo prueba ``qb_costeo``).

3.3.0 (revisión del flujo, 2026-11-05), pruebas 15 a 20: dos baños para una orden en metros (el primero deja pendiente,
el segundo lo cierra), baño de saldos con otro baño abierto, anular una captura, alerta de metros contra la ficha,
motivo de la captura manual en Rama y en el desperdicio, lote reutilizable y mínimo de kg pesados para cerrar.
"""
from unittest.mock import patch

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
        cls.defecto = cls.env['calidad.defecto.tela'].create({'clave': 'ZZ951', 'name': 'Defecto de prueba'})
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
        if calidad in ('SEGUNDA', 'FE'):
            valores['defecto_ids'] = [Command.set(self.defecto.ids)]  # 3.3.1: obligatorio en Segunda y FE
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
        # 3.3.0: assertRaises (no assertRaisesRegex) abre un savepoint y lo revierte, como la transacción de la pantalla;
        # assertRaisesRegex de unittest no revierte nada y la prueba fallaba aunque el cierre sí es atómico.
        with self.assertRaises(UserError) as error:
            orden_kg.button_mark_done()
        self.assertIn('No existe la ubicación', str(error.exception))
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

    # ------------------------------------------------------------------ 3.3.0
    def _dos_banos(self, kg_a=(40.0,), kg_b=(40.0,)):
        """Orden en metros de 2,000 m (169.6 kg) abastecida por dos baños de 84.8 kg, como la división de la
        programación de tintorería."""
        orden_m = self.env['mrp.production'].create({
            'product_id': self.prod_m.id, 'product_qty': 2000, 'product_uom_id': self.metro.id, 'bom_id': self.bom_m.id})
        orden_m.action_confirm()
        banos = []
        for rollos_kg in (kg_a, kg_b):
            orden_kg = self.env['mrp.production'].create({
                'product_id': self.prod_kg.id, 'product_qty': 84.8, 'bom_id': self.bom_kg.id})
            orden_kg.action_confirm()
            terminado = orden_kg.move_finished_ids.filtered(lambda m: m.product_id == self.prod_kg)
            raw = orden_m._clasif_move_a()
            raw.move_orig_ids = raw.move_orig_ids | terminado
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
            banos.append((orden_kg, wo_insp, rollos))
        return orden_m, banos

    def test_15_dos_banos_una_orden_en_metros(self):
        orden_m, ((kg_a, wo_a, rollos_a), (kg_b, wo_b, rollos_b)) = self._dos_banos()
        raiz = orden_m.name.split('/')[-1]
        self._inspeccionar(kg_a, wo_a, rollos_a, 40.6, 460.0)
        # El segundo baño se inspecciona antes de que cierre el primero: mismo consecutivo de lotes.
        self._inspeccionar(kg_b, wo_b, rollos_b, 40.5, 455.0)
        self.assertEqual(kg_b.rollo_final_kg_ids.name, 'M%s-0002' % raiz)

        kg_a.button_mark_done()
        self.assertEqual(kg_a.state, 'done')
        self.assertEqual(orden_m.state, 'done', "El primer baño cierra su parte de la orden en metros")
        self.assertAlmostEqual(orden_m.qty_produced, 460.0)
        pendiente = orden_m.production_group_id.production_ids.filtered(lambda p: p.state not in ('done', 'cancel'))
        self.assertEqual(len(pendiente), 1, "Queda un pendiente para el segundo baño")
        kg_por_m = self.bom_m.bom_line_ids.product_qty  # la lista guarda los kg por metro con su precisión
        self.assertAlmostEqual(pendiente.product_qty, 84.8 / kg_por_m, delta=1.0,
                               msg="El pendiente es lo planeado del segundo baño")
        self.assertIn(kg_b, pendiente._ordenes_previas(), "El segundo baño abastece al pendiente")

        kg_b.button_mark_done()
        self.assertEqual(kg_b.state, 'done', "El segundo baño ya puede cerrar")
        self.assertEqual(pendiente.state, 'done')
        self.assertAlmostEqual(pendiente.qty_produced, 455.0)
        self.assertEqual(kg_b.rollo_final_kg_ids.production_m_id, pendiente)
        self.assertEqual(set((kg_a | kg_b).rollo_final_kg_ids.mapped('estado')), {'registrado'})
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ']), 915.0)
        self.assertFalse(orden_m.production_group_id.production_ids.filtered(lambda p: p.state not in ('done', 'cancel')),
                         "El último baño cierra sin dejar pendiente")

    def test_16_bano_de_saldos_no_cancela_con_otro_bano_abierto(self):
        orden_m, ((kg_a, wo_a, rollos_a), (kg_b, wo_b, rollos_b)) = self._dos_banos()
        self._inspeccionar(kg_a, wo_a, rollos_a, 30.5, 320.0, 'SEGUNDA', termina=False)
        self._inspeccionar(kg_a, wo_a, rollos_a, 9.0, 80.0, 'DESP')
        kg_a.button_mark_done()
        self.assertEqual(kg_a.state, 'done')
        self.assertNotEqual(orden_m.state, 'cancel', "El otro baño la sigue abasteciendo")
        self.assertAlmostEqual(self._cantidad(self.saldo_2, self.ubic['SEGUNDA']), 29.0)
        self._inspeccionar(kg_b, wo_b, rollos_b, 40.5, 455.0)
        kg_b.button_mark_done()
        self.assertEqual(orden_m.state, 'done')
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ']), 455.0)

    def test_17_anular_captura(self):
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        self._inspeccionar(orden_kg, wo, rollos, 26.5, 250.0, 'PQ', termina=False)
        self._inspeccionar(orden_kg, wo, rollos, 13.4, 120.0, 'SEGUNDA')
        self.assertEqual(rollos.estado_inspeccion, 'inspeccionado')
        segunda = orden_kg.rollo_final_kg_ids.filtered(lambda r: r.clasificacion == 'SEGUNDA')
        segunda.action_anular()
        self.assertFalse(segunda.exists())
        self.assertEqual(rollos.estado_inspeccion, 'pendiente', "El rollo de Rama vuelve a quedar abierto")
        with self.assertRaisesRegex(UserError, 'sin inspeccionar'):
            orden_kg.button_mark_done()
        self._inspeccionar(orden_kg, wo, rollos, 13.4, 120.0, 'FE')
        orden_kg.button_mark_done()
        self.assertEqual(orden_m.state, 'done')
        with self.assertRaisesRegex(UserError, 'registrado'):
            orden_kg.rollo_final_kg_ids[:1].action_anular()

    def test_18_alerta_de_metros_contra_la_ficha(self):
        orden_m, orden_kg, wo, rollos = self._bano((50.0,))
        Wizard = type(self.env['rollo.inspeccion.estacion.wizard'])
        with patch.object(Wizard, '_rendimiento_acabado', return_value=11.8):
            wiz = self.env['rollo.inspeccion.estacion.wizard'].new({
                'production_id': orden_kg.id, 'workorder_id': wo.id, 'rollo_codigos': rollos.name,
                'kg_bruto': 50.6, 'metros': 58.5, 'clasificacion': 'PQ', 'termina_rollo': True})
            self.assertAlmostEqual(wiz.metros_teoricos, 49.1 * 11.8)
            self.assertIn('teóricos', wiz.alertas, "585 capturado como 58.5 avisa")
            wiz.metros = 580.0
            self.assertFalse(wiz.alertas and 'teóricos' in wiz.alertas)
            self._inspeccionar(orden_kg, wo, rollos, 50.6, 58.5)
        final = orden_kg.rollo_final_kg_ids
        self.assertTrue(final.alerta_metros)

    def test_19_motivo_manual_en_rama_y_desperdicio(self):
        orden_kg = self.env['mrp.production'].create({
            'product_id': self.prod_kg.id, 'product_qty': 100.0, 'bom_id': self.bom_kg.id})
        orden_kg.action_confirm()
        wo_rama = self.env['mrp.workorder'].create({
            'name': 'RAMA', 'production_id': orden_kg.id, 'workcenter_id': self.rama.id})
        valores = {'production_id': orden_kg.id, 'workorder_id': wo_rama.id, 'empleado_numero': '1',
                   'weighing_mode': 'manual', 'kg_bruto': 31.1}
        with self.assertRaisesRegex(UserError, 'motivo'):
            self.env['rollo.pesaje.rama.wizard'].create(valores).action_confirmar()
        self.env['rollo.pesaje.rama.wizard'].create(dict(valores, motivo_manual='Báscula sin conexión')).action_confirmar()
        rollo = orden_kg.rollo_bitacora_ids
        self.assertAlmostEqual(rollo.kg_rama, 30.0)
        self.assertEqual(rollo.motivo_manual_rama, 'Báscula sin conexión')
        desp = {'production_id': orden_kg.id, 'workorder_id': wo_rama.id, 'empleado_numero': '1',
                'weighing_mode': 'manual', 'kg_desperdicio': 1.2}
        with self.assertRaisesRegex(UserError, 'motivo'):
            self.env['rollo.desperdicio.rama.wizard'].create(desp).action_confirmar()

    def test_20_lote_reutilizable_y_minimo_para_cerrar(self):
        Produccion = self.env['mrp.production']
        lote = Produccion._insp_lote(self.prod_m, 'MZZ951-PRUEBA', self.env.company)
        self.assertEqual(Produccion._insp_lote(self.prod_m, 'MZZ951-PRUEBA', self.env.company), lote,
                         "Un lote sin movimientos se reutiliza si el cierre se repite")
        orden_m, orden_kg, wo, rollos = self._bano((50.0,))
        self._inspeccionar(orden_kg, wo, rollos, 50.6, 585.0)
        self.env['ir.config_parameter'].sudo().set_param('quimibond_rollos_bitacora.cierre_minimo_pct', '50')
        with self.assertRaisesRegex(UserError, 'mínimo'):
            orden_kg.button_mark_done()
        self.env['ir.config_parameter'].sudo().set_param('quimibond_rollos_bitacora.cierre_minimo_pct', '')
        orden_kg.button_mark_done()
        self.assertEqual(orden_kg.state, 'done')
        self.assertTrue(orden_kg.message_ids.filtered(lambda m: 'kg planeados' in (m.body or '')))

    # ------------------------------------------------------------------ integración con la programación de tintorería
    def _programar_dos_banos(self):
        """Orden en metros de 2,000 m abastecida por una orden en kg de 169.6 kg que la programación de tintorería
        (quimibond_tintoreria_programacion) divide con la división nativa en dos baños de 84.8 kg en un jet de 100 kg.
        Las fichas técnicas se simulan como en las pruebas de la programación."""
        if 'tintoreria.programacion' not in self.env:
            self.skipTest('quimibond_tintoreria_programacion no está instalado')
        htj = self.env['mrp.workcenter'].create({'name': 'TINTORERIA ZZ951', 'code': 'TZZ951'})
        bom = self.env['mrp.bom'].create({
            'product_tmpl_id': self.prod_kg.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': self.kg.id,
            'consumption': 'flexible', 'sequence': 1,
            'operation_ids': [
                Command.create({'name': 'Tintorería', 'workcenter_id': htj.id, 'sequence': 10}),
                Command.create({'name': 'RAMA', 'workcenter_id': self.rama.id, 'sequence': 20}),
                Command.create({'name': 'INSPECCION', 'workcenter_id': self.revisadora.id, 'sequence': 30})],
            'bom_line_ids': [Command.create({'product_id': self.crudo.id, 'product_qty': 1.1})]})

        class _Config:
            def _ft_value(self, codigo):
                return {'capacidad': 100.0}.get(codigo, 0.0)

        prod_kg = self.prod_kg
        Workcenter = self.env.registry['mrp.workcenter']
        self.patch(Workcenter, '_ft_config_at', lambda wc, day=None: _Config() if wc.id == htj.id else False)
        self.patch(Workcenter, '_ft_capacidad_carga',
                   lambda wc, rendimiento, day=None, product=None: (0.0, 'sin capacidad capturada'))
        self.patch(self.env.registry['product.product'], '_ft_rendimiento', lambda producto: (11.79, 'prueba'))
        self.patch(self.env.registry['mrp.production'], '_es_producto_acabado_kg',
                   lambda orden: orden.product_id == prod_kg)
        self.patch(self.env.registry['mrp.production'], '_minutos_tintoreria',
                   lambda orden, wc, kg_bano=None: (240.0, 'ficha', False))

        orden_m = self.env['mrp.production'].create({
            'product_id': self.prod_m.id, 'product_qty': 2000, 'product_uom_id': self.metro.id, 'bom_id': self.bom_m.id})
        orden_m.action_confirm()
        orden_kg = self.env['mrp.production'].create({
            'product_id': self.prod_kg.id, 'product_qty': 169.6, 'bom_id': bom.id})
        orden_kg.action_confirm()
        self.assertTrue(orden_kg.requiere_programacion_tintoreria)
        # Liga MTO como la deja el abastecimiento: el consumo de la orden en metros nace de la orden en kg.
        raw = orden_m._clasif_move_a()
        raw.write({'move_orig_ids': [Command.set(orden_kg.move_finished_ids.filtered(
            lambda m: m.product_id == self.prod_kg).ids)], 'created_production_id': orden_kg.id})
        programacion = orden_kg._programar_tintoreria(htj)
        banos = programacion.production_ids.sorted('id')
        self.assertEqual(len(banos), 2)
        self.assertEqual(banos.mapped('product_qty'), [84.8, 84.8])
        self.assertEqual(orden_m._ordenes_previas(), banos, "Los dos baños abastecen a la orden en metros")
        for bano in banos:
            self.assertEqual(bano._insp_orden_metros(), orden_m)
        return orden_m, banos

    def _rama_e_inspeccion(self, bano, kg_bruto_rama, capturas):
        """Pesa un rollo en Rama con el asistente y lo inspecciona en una o varias capturas (kg_bruto, metros, calidad)."""
        wo_rama = bano.workorder_ids.filtered(lambda w: w.workcenter_id == self.rama)
        wo_insp = bano.workorder_ids.filtered(lambda w: w.workcenter_id == self.revisadora)
        self.assertTrue(wo_rama and wo_insp, "La división copia las operaciones de la lista a cada baño")
        self.env['rollo.pesaje.rama.wizard'].create({
            'production_id': bano.id, 'workorder_id': wo_rama.id, 'empleado_numero': '1',
            'weighing_mode': 'manual', 'motivo_manual': 'Prueba sin báscula', 'kg_bruto': kg_bruto_rama,
        }).action_confirmar()
        rollos = bano.rollo_bitacora_ids
        self.assertEqual(rollos.lot_id.name, '%s-0001' % bano.name.split('/')[-1])
        for i, (kg_bruto, metros, calidad) in enumerate(capturas, 1):
            self._inspeccionar(bano, wo_insp, rollos, kg_bruto, metros, calidad, termina=i == len(capturas))
        return rollos

    def test_21_programacion_divide_y_cada_bano_cierra(self):
        orden_m, (bano_1, bano_2) = self._programar_dos_banos()
        raiz = orden_m.name.split('/')[-1]  # al crear el pendiente Odoo renombra la orden a …-001
        self._rama_e_inspeccion(bano_1, 41.1, [(40.6, 460.0, 'PQ')])
        bano_1.button_mark_done()
        self.assertEqual(bano_1.state, 'done')
        self.assertEqual(orden_m.state, 'done')
        self.assertAlmostEqual(orden_m.qty_produced, 460.0)
        pendiente = orden_m.production_group_id.production_ids.filtered(lambda p: p.state not in ('done', 'cancel'))
        self.assertEqual(len(pendiente), 1)
        self.assertEqual(bano_2._insp_orden_metros(), pendiente, "El segundo baño ya abastece al pendiente")
        # El segundo baño se inspecciona ya con el primero cerrado: el consecutivo sigue sobre la raíz de la orden.
        self._rama_e_inspeccion(bano_2, 41.1, [(40.5, 455.0, 'PQ')])
        self.assertEqual(bano_2.rollo_final_kg_ids.name, 'M%s-0002' % raiz)
        self.assertEqual(bano_2.rollo_final_kg_ids.production_m_id, pendiente)
        bano_2.button_mark_done()
        self.assertEqual(bano_2.state, 'done')
        self.assertEqual(pendiente.state, 'done')
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ']), 915.0)
        self.assertFalse(orden_m.production_group_id.production_ids.filtered(lambda p: p.state not in ('done', 'cancel')))
        self.assertEqual(bano_1.rollo_final_kg_ids.name, 'M%s-0001' % raiz)
        self.assertEqual(set((bano_1 | bano_2).rollo_final_kg_ids.mapped('estado')), {'registrado'})

    def test_22_programacion_cierre_en_orden_inverso_y_bano_de_saldos(self):
        orden_m, (bano_1, bano_2) = self._programar_dos_banos()
        # El segundo baño sale todo segunda y desperdicio, y cierra antes que el primero.
        self._rama_e_inspeccion(bano_2, 41.1, [(30.5, 320.0, 'SEGUNDA'), (9.0, 80.0, 'DESP')])
        bano_2.button_mark_done()
        self.assertEqual(bano_2.state, 'done')
        self.assertNotIn(orden_m.state, ('cancel',), "El primer baño sigue abasteciendo a la orden en metros")
        self.assertAlmostEqual(self._cantidad(self.saldo_2, self.ubic['SEGUNDA']), 29.0)
        self.assertEqual(bano_1._insp_orden_metros(), orden_m)
        self._rama_e_inspeccion(bano_1, 41.1, [(26.6, 300.0, 'PQ'), (14.0, 150.0, 'FE')])
        bano_1.button_mark_done()
        self.assertEqual(bano_1.state, 'done')
        self.assertEqual(orden_m.state, 'done')
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ']), 300.0)
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['FE']), 150.0)
        self.assertFalse(orden_m.production_group_id.production_ids.filtered(lambda p: p.state not in ('done', 'cancel')))

    # ------------------------------------------------------------------ errores de la prueba punta a punta en main (3.3.1)
    def test_23_ultimo_bano_cierra_aunque_el_anterior_tuviera_saldos(self):
        """E1: el pendiente heredaba la línea sin lote del saldo del primer baño y el último no cerraba («Debe
        proporcionar un número de serie o lote para el producto SALDO…»)."""
        orden_m, ((kg_a, wo_a, rollos_a), (kg_b, wo_b, rollos_b)) = self._dos_banos()
        self._inspeccionar(kg_a, wo_a, rollos_a, 30.6, 345.0, 'PQ', termina=False)
        self._inspeccionar(kg_a, wo_a, rollos_a, 9.9, 100.0, 'SEGUNDA')
        kg_a.button_mark_done()
        self.assertEqual(orden_m.state, 'done')
        pendiente = orden_m.production_group_id.production_ids.filtered(lambda p: p.state not in ('done', 'cancel'))
        saldos = self.env['mrp.rollo.final']._productos_saldo()
        movs = pendiente.move_finished_ids.filtered(lambda m: m.product_id in saldos and m.state not in ('done', 'cancel'))
        self.assertFalse(movs.move_line_ids, "El pendiente no hereda líneas de saldo")
        self._inspeccionar(kg_b, wo_b, rollos_b, 40.5, 455.0)
        kg_b.button_mark_done()
        self.assertEqual(kg_b.state, 'done')
        self.assertEqual(pendiente.state, 'done')
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ']), 800.0)
        self.assertAlmostEqual(self._cantidad(self.saldo_2, self.ubic['SEGUNDA']), 8.4)

    def test_24_orden_de_saldos_no_lanza_abastecimiento(self):
        """E2: el consumo de la orden de saldos tomaba la regla bajo pedido de la ruta del producto en kg y creaba una orden
        en kg nueva que nadie pidió."""
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        stock = orden_kg.location_dest_id
        produccion = self.prod_kg.property_stock_production
        tipo = self.env['stock.picking.type'].search(
            [('code', '=', 'mrp_operation'), ('company_id', '=', self.env.company.id)], limit=1)
        ruta = self.env['stock.route'].create({
            'name': 'ZZ951 bajo pedido a producción', 'product_selectable': True,
            'rule_ids': [Command.create({
                'name': 'ZZ951 stock → producción', 'action': 'pull', 'procure_method': 'make_to_order',
                'location_src_id': stock.id, 'location_dest_id': produccion.id, 'picking_type_id': tipo.id})]})
        fabricar = self.env.ref('mrp.route_warehouse0_manufacture')
        self.prod_kg.route_ids = [Command.link(ruta.id), Command.link(fabricar.id)]
        antes = self.env['mrp.production'].search([('product_id', '=', self.prod_kg.id)])
        self._inspeccionar(orden_kg, wo, rollos, 30.5, 320.0, 'SEGUNDA', termina=False)
        self._inspeccionar(orden_kg, wo, rollos, 9.0, 80.0, 'DESP')
        orden_kg.button_mark_done()
        self.assertEqual(orden_kg.state, 'done')
        self.assertEqual(orden_m.state, 'cancel')
        nuevas = self.env['mrp.production'].search([('product_id', '=', self.prod_kg.id)]) - antes
        self.assertFalse(nuevas, "La orden de saldos no debe crear órdenes en kg: %s" % nuevas.mapped('name'))
        orden_s = orden_kg.rollo_final_kg_ids.production_saldo_id
        self.assertEqual(orden_s.state, 'done')
        self.assertEqual(set(orden_s.move_raw_ids.mapped('procure_method')), {'make_to_stock'})

    def test_25_orden_de_saldos_entra_a_costo_cero(self):
        """E4 (Dirección, 2026-11-05): los saldos se venden a 1–2 $/kg; en la orden de saldos entran a costo cero y el costo
        del baño no queda como inventario de Segunda (en la prueba de main entró a 131.79 $/kg)."""
        categoria = self.env['product.category'].create({
            'name': 'ZZ951 costo promedio', 'property_cost_method': 'average'})
        (self.crudo | self.prod_kg | self.saldo_2 | self.saldo_d).write({'categ_id': categoria.id})
        self.crudo.write({'is_storable': True, 'standard_price': 50.0})
        stock = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1).lot_stock_id
        self.env['stock.quant']._update_available_quantity(self.crudo, stock, 500.0)
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        self._inspeccionar(orden_kg, wo, rollos, 30.5, 320.0, 'SEGUNDA', termina=False)
        self._inspeccionar(orden_kg, wo, rollos, 9.0, 80.0, 'DESP')
        orden_kg.button_mark_done()
        self.assertEqual(orden_kg.state, 'done')
        orden_s = orden_kg.rollo_final_kg_ids.production_saldo_id
        self.assertEqual(orden_s.state, 'done')
        consumo = sum(orden_s.move_raw_ids.filtered(lambda m: m.state == 'done').mapped('value'))
        self.assertGreater(consumo, 0.0, "El baño tiene costo: sale del inventario de la tela en kg")
        terminados = orden_s.move_finished_ids.filtered(lambda m: m.state == 'done')
        self.assertTrue(terminados)
        self.assertFalse(any(terminados.mapped('value')), "Segunda y Desperdicio entran a costo cero")
        self.assertAlmostEqual(self.saldo_2.standard_price, 0.0)

    def test_26_segunda_y_fe_piden_defecto(self):
        """E12c: Segunda y FE necesitan al menos un defecto (evidencia de calidad); PQ y Desperdicio no."""
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        for calidad in ('SEGUNDA', 'FE'):
            with self.assertRaises(UserError) as error:
                self._inspeccionar(orden_kg, wo, rollos, 13.4, 120.0, calidad, termina=False, defecto_ids=[])
            self.assertIn('defecto', str(error.exception))
        self.assertFalse(orden_kg.rollo_final_kg_ids, "Sin defecto no se registra el rollo")
        self._inspeccionar(orden_kg, wo, rollos, 13.4, 120.0, 'FE', termina=False)
        self._inspeccionar(orden_kg, wo, rollos, 26.5, 250.0, 'PQ')
        self.assertEqual(orden_kg.rollo_final_kg_ids.filtered(lambda r: r.clasificacion == 'FE').defecto_ids, self.defecto)
