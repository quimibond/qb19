# -*- coding: utf-8 -*-
"""3.1.0: Inspección como estación única y registro de la orden en metros al cerrar el baño.

Casos de aceptación 1 a 11 del diseño ``docs/superpowers/specs/2026-11-03-inspeccion-clasificacion-rollos-design.md``
(el 12, costeo, lo prueba ``qb_costeo``).

3.3.0 (revisión del flujo, 2026-11-05), pruebas 15 a 20: dos baños para una orden en metros (el primero deja pendiente,
el segundo lo cierra), baño de saldos con otro baño abierto, anular una captura, alerta de metros contra la ficha,
motivo de la captura manual en Rama y en el desperdicio, lote reutilizable y mínimo de kg pesados para cerrar.
"""
from unittest.mock import patch

from psycopg2 import IntegrityError

from odoo import Command, fields
from odoo.exceptions import UserError
from odoo.tests import tagged
from odoo.tests.common import TransactionCase
from odoo.tools import mute_logger


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
    def _consumir_crudo(self, orden_kg):
        """3.3.2: la orden en kg no cierra si consume menos crudo que lo pesado en Rama. Se deja crudo en existencia en la
        ubicación de la orden y se reserva, como en test_27: el consumo sale de la reserva, igual que en la planta."""
        self.crudo.write({'is_storable': True})
        planeado = sum(orden_kg._moves_crudo().mapped('product_uom_qty'))
        self.env['stock.quant']._update_available_quantity(self.crudo, orden_kg.location_src_id, planeado)
        orden_kg.action_assign()

    def _cerrar_bano(self, orden_kg, usuario=None):
        """Cierra el baño como en planta (3.16.0, Empaque siempre activo): cierra la orden en kg, que pasa sus rollos a la
        orden en metros; Empaque confirma lo que Inspección clasificó y la orden en metros se registra al cerrarse."""
        (orden_kg.with_user(usuario) if usuario else orden_kg).button_mark_done()
        for orden_m in orden_kg.rollo_final_kg_ids.production_m_id.filtered(
                lambda o: o.state not in ('done', 'cancel') and o._insp_orden_m_en_empaque()):
            self._empaque_confirma_todos(orden_m)
            orden_m.button_mark_done()

    def _bano(self, rollos_kg=(50.0,), en_kg=False, con_crudo=True):
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
        if con_crudo:
            self._consumir_crudo(orden_kg)
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
        self.assertEqual(final.name, '%s-F001' % orden_kg.numero_bano)  # 3.18.0: número de baño + F + rollo
        self.assertAlmostEqual(final.kg_neto, 49.1)
        self.assertIn('585.00-', final.zpl_etiqueta)
        self.assertEqual(rollos.estado_inspeccion, 'inspeccionado')
        self.assertEqual(final.estado, 'inspeccionado')
        self._cerrar_bano(orden_kg)
        self.assertEqual((orden_kg.state, orden_m.state), ('done', 'done'))
        self.assertEqual(final.estado, 'registrado')
        self.assertAlmostEqual(orden_m.qty_produced, 585.0)
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ'], final.name), 585.0)
        consumo = orden_m.move_raw_ids.filtered(lambda m: m.product_id == self.prod_kg)
        self.assertAlmostEqual(consumo.quantity, 49.1, msg="3.18.0: consume el rollo final con los kg de Inspección")

    def test_02_corte_primera_y_segunda(self):
        """Caso 2: 250 m de primera y 120 m de segunda; 3.15.0: la segunda es la tela en metros, con su lote, a 16 (diseño §8)."""
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        self._inspeccionar(orden_kg, wo, rollos, 26.5, 250.0, 'PQ', termina=False)
        self.assertEqual(rollos.estado_inspeccion, 'pendiente', "Con rollos pendientes, el rollo sigue sin inspeccionar")
        wiz = self._inspeccionar(orden_kg, wo, rollos, 13.4, 120.0, 'SEGUNDA')
        self.assertAlmostEqual(wiz.kg_ya_cortados, 25.0)
        self.assertAlmostEqual(wiz.merma_kg, 40.0 - 25.0 - 11.9)
        self._cerrar_bano(orden_kg)
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ']), 250.0)
        segunda = orden_kg.rollo_final_kg_ids.filtered(lambda r: r.clasificacion == 'SEGUNDA')
        self.assertEqual(segunda.product_id, self.prod_m)
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['SEGUNDA'], segunda.name), 120.0)
        self.assertFalse(self._cantidad(self.saldo_2, self.ubic['SEGUNDA']), "La Segunda ya no sale como saldo en kg")
        self.assertAlmostEqual(orden_m.qty_produced, 370.0, msg="La orden en metros cierra con primera y Segunda")

    def test_03_04_fe_y_desperdicio(self):
        """Casos 3 y 4: FE en metros a 15; desperdicio en kg (saldo D) a 17."""
        orden_m, orden_kg, wo, rollos = self._bano((40.0, 30.0))
        self._inspeccionar(orden_kg, wo, rollos[0], 41.0, 460.0, 'FE')
        self._inspeccionar(orden_kg, wo, rollos[1], 30.5, 340.0, 'PQ', termina=False)
        self._inspeccionar(orden_kg, wo, rollos[1], 1.5 + 0.8, 5.0, 'DESP')
        self._cerrar_bano(orden_kg)
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
        self._cerrar_bano(orden_kg)
        consumo = orden_m.move_raw_ids.filtered(lambda m: m.product_id == self.prod_kg)
        self.assertAlmostEqual(consumo.quantity, 20.9, msg="3.18.0: consume los rollos finales con los kg de Inspección")

    def test_07_08_sin_cerrar_no_hay_existencia_y_bloqueo(self):
        """Caso 7: antes de cerrar, el rollo final no existe en inventario; caso 8: con un rollo sin inspeccionar no
        cierra."""
        orden_m, orden_kg, wo, rollos = self._bano((20.0, 20.0))
        self._inspeccionar(orden_kg, wo, rollos[0], 21.4, 230.0)
        final = orden_kg.rollo_final_kg_ids
        # 3.7.0: el lote de Rama (producto en kg) se llama igual que el rollo final (producto en metros): se busca el del B.
        self.assertFalse(self.env['stock.lot'].search([
            ('name', '=', final.name), ('product_id', '=', orden_m.product_id.id)]), "El lote se crea al registrar")
        self.assertFalse(self._cantidad(self.prod_m, self.ubic['PQ']))
        self.assertTrue(orden_kg.inspeccion_completa_sin_cerrar is False)
        with self.assertRaisesRegex(UserError, 'sin inspeccionar'):
            orden_kg.button_mark_done()
        self._inspeccionar(orden_kg, wo, rollos[1], 21.3, 228.0)
        self.assertTrue(orden_kg.inspeccion_completa_sin_cerrar)
        self._cerrar_bano(orden_kg)
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
        orden_kg.button_mark_done()  # 3.16.0: el registro ocurre al cerrar la orden en metros, tras Empaque
        self._empaque_confirma_todos(orden_m)
        with self.assertRaises(UserError) as error:
            orden_m.button_mark_done()
        self.assertIn('No existe la ubicación', str(error.exception))
        self.env.invalidate_all()
        self.assertNotEqual(orden_m.state, 'done')
        self.assertEqual(set(orden_kg.rollo_final_kg_ids.mapped('estado')), {'clasificado'})

    def test_12_no_cortar_y_juntar_a_la_vez(self):
        orden_m, orden_kg, wo, rollos = self._bano((20.0, 20.0))
        self._inspeccionar(orden_kg, wo, rollos[0], 10.0, 100.0, termina=False)
        with self.assertRaisesRegex(UserError, "ya se dividió"):
            self._inspeccionar(orden_kg, wo, rollos, 30.0, 300.0)

    def test_13_bano_para_venta_en_kg(self):
        """3.2.0: sin orden en metros, los rollos finales quedan como el acabado en kg con su peso real (lote K…): PQ a
        14, FE a 15; la segunda como saldo en kg a 16."""
        _m, orden_kg, wo, rollos = self._bano((40.0, 30.0), en_kg=True)
        self._inspeccionar(orden_kg, wo, rollos[0], 41.0, 460.0, 'PQ')
        self._inspeccionar(orden_kg, wo, rollos[1], 20.5, 230.0, 'FE', termina=False)
        self._inspeccionar(orden_kg, wo, rollos[1], 10.5, 110.0, 'SEGUNDA')
        primero = orden_kg.rollo_final_kg_ids.sorted('secuencia')[:1]
        self.assertEqual(primero.name, '%s-F001' % orden_kg.numero_bano)
        self._cerrar_bano(orden_kg)
        self.assertEqual(orden_kg.state, 'done')
        self.assertAlmostEqual(orden_kg.qty_produced, 39.5 + 19.0, msg="La orden en kg cierra con el peso real de PQ y FE")
        self.assertAlmostEqual(self._cantidad(self.prod_kg, self.ubic['PQ'], primero.name), 39.5)
        self.assertAlmostEqual(self._cantidad(self.prod_kg, self.ubic['FE']), 19.0)
        self.assertAlmostEqual(self._cantidad(self.saldo_2, self.ubic['SEGUNDA']), 9.0)
        self.assertEqual(set(orden_kg.rollo_final_kg_ids.mapped('estado')), {'registrado'})

    def test_14_bano_sin_primera_ni_fe(self):
        """3.2.0: todo desperdicio (3.15.0: la Segunda ya es tela en metros): la orden en metros se cancela y una orden
        de saldos saca el desperdicio en kg, consumiendo los rollos de Rama completos."""
        orden_m, orden_kg, wo, rollos = self._bano((20.0,))
        self._inspeccionar(orden_kg, wo, rollos, 18.5, 200.0, 'DESP', termina=False)
        self._inspeccionar(orden_kg, wo, rollos, 3.0, 20.0, 'DESP')
        self._cerrar_bano(orden_kg)
        self.assertEqual(orden_kg.state, 'done')
        self.assertEqual(orden_m.state, 'cancel')
        finales = orden_kg.rollo_final_kg_ids
        self.assertEqual(set(finales.mapped('estado')), {'registrado'})
        orden_s = finales.production_saldo_id
        self.assertEqual(orden_s.state, 'done')
        self.assertAlmostEqual(self._cantidad(self.saldo_d, self.ubic['DESP']), 18.5)
        consumo = orden_s.move_raw_ids.filtered(lambda m: m.product_id == self.prod_kg)
        self.assertAlmostEqual(consumo.quantity, 18.5, msg="3.18.0: los kg de Inspección")

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
            self._consumir_crudo(orden_kg)
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
        self._inspeccionar(kg_a, wo_a, rollos_a, 40.6, 460.0)
        self._cerrar_bano(kg_a)
        self.assertEqual(kg_a.state, 'done')
        self.assertEqual(orden_m.state, 'done', "El primer baño cierra su parte de la orden en metros")
        self.assertAlmostEqual(orden_m.qty_produced, 460.0)
        pendiente = orden_m.production_group_id.production_ids.filtered(lambda p: p.state not in ('done', 'cancel'))
        self.assertEqual(len(pendiente), 1, "Queda un pendiente para el segundo baño")
        kg_por_m = self.bom_m.bom_line_ids.product_qty  # la lista guarda los kg por metro con su precisión
        self.assertAlmostEqual(pendiente.product_qty, 84.8 / kg_por_m, delta=1.0,
                               msg="El pendiente es lo planeado del segundo baño")
        self.assertIn(kg_b, pendiente._ordenes_previas(), "El segundo baño abastece al pendiente")

        # 3.5.0: cada baño tiene su número y su serie de lotes, aunque abastezcan la misma orden en metros.
        self._inspeccionar(kg_b, wo_b, rollos_b, 40.5, 455.0)
        self.assertNotEqual(kg_a.numero_bano, kg_b.numero_bano)
        self.assertEqual(kg_b.rollo_final_kg_ids.name, '%s-F001' % kg_b.numero_bano)
        self._cerrar_bano(kg_b)
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
        self._inspeccionar(kg_a, wo_a, rollos_a, 30.5, 320.0, 'DESP', termina=False)
        self._inspeccionar(kg_a, wo_a, rollos_a, 9.0, 80.0, 'DESP')
        self._cerrar_bano(kg_a)
        self.assertEqual(kg_a.state, 'done')
        self.assertNotEqual(orden_m.state, 'cancel', "El otro baño la sigue abasteciendo")
        self.assertAlmostEqual(self._cantidad(self.saldo_d, self.ubic['DESP']), 36.5)
        self._inspeccionar(kg_b, wo_b, rollos_b, 40.5, 455.0)
        self._cerrar_bano(kg_b)
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
        self._cerrar_bano(orden_kg)
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

    def _registrar_bascula(self):
        """3.3.9: con una báscula IoT registrada, capturar a mano pide motivo (sin báscula se llena solo)."""
        caja = False
        if 'iot.box' in self.env:
            Caja = self.env['iot.box'].sudo()
            valores_caja = {'name': 'Caja prueba ZZ951'}
            if 'identifier' in Caja._fields:
                valores_caja['identifier'] = 'ZZ951'
            caja = Caja.create(valores_caja)
        valores = {'name': 'Báscula ZZ951', 'type': 'scale', 'identifier': 'ZZ951-scale'}
        if caja:
            valores['iot_id'] = caja.id
        return self.env['iot.device'].sudo().create(valores)

    def test_19_motivo_manual_en_rama_y_desperdicio(self):
        self._registrar_bascula()
        orden_kg = self.env['mrp.production'].create({
            'product_id': self.prod_kg.id, 'product_qty': 100.0, 'bom_id': self.bom_kg.id})
        orden_kg.action_confirm()
        wo_rama = self.env['mrp.workorder'].create({
            'name': 'RAMA', 'production_id': orden_kg.id, 'workcenter_id': self.rama.id})
        valores = {'production_id': orden_kg.id, 'workorder_id': wo_rama.id, 'empleado_numero': '1',
                   'weighing_mode': 'manual', 'kg_bruto': 31.1}
        with self.assertRaisesRegex(UserError, 'motivo'):
            self.env['rollo.pesaje.rama.wizard'].create(valores).action_confirmar()
        self.env['rollo.pesaje.rama.wizard'].create(
            dict(valores, motivo_manual='Báscula sin conexión', metros_rama=480.0)).action_confirmar()
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
        self._cerrar_bano(orden_kg)
        self.assertEqual(orden_kg.state, 'done')
        self.assertTrue(orden_kg.message_ids.filtered(lambda m: 'kg planeados' in (m.body or '')))

    # ------------------------------------------------------------------ integración con la programación de tintorería
    def _programar_dos_banos(self, otra_orden_kg=False):
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
            'bom_line_ids': [Command.create({'product_id': self.crudo.id, 'product_qty': 1.1})],
            # Como la lista en kg de las fichas (2.25.0): el desperdicio de Rama sale de subproducto en cero.
            'byproduct_ids': [Command.create({'product_id': self.saldo_d.id, 'product_qty': 0.0, 'cost_share': 0.0})]})

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
        # Como en planta, todos los terminados de la orden en kg (también el subproducto) quedan ligados al consumo.
        origenes = orden_kg.move_finished_ids
        if otra_orden_kg:
            # Programación 1.8.2: otra orden en kg también abastece la orden en metros (p. ej. se subió la cantidad de la
            # orden en metros después de abastecer).
            otra = self.env['mrp.production'].create({'product_id': self.prod_kg.id, 'product_qty': 20.0, 'bom_id': bom.id})
            otra.action_confirm()
            origenes |= otra.move_finished_ids
        raw.write({'move_orig_ids': [Command.set(origenes.ids)], 'created_production_id': orden_kg.id})
        programacion = orden_kg._programar_tintoreria(htj)
        banos = programacion.production_ids.sorted('id')
        self.assertEqual(len(banos), 2)
        self.assertEqual(banos.mapped('product_qty'), [84.8, 84.8])
        if otra_orden_kg:
            return orden_m, banos
        # Programación 1.8.0 (Dirección, 2026-11-06): un baño = una orden en metros. La orden en metros se divide en la
        # misma proporción que los baños y cada parte queda ligada a su baño.
        m1, m2 = banos[0]._insp_orden_metros(), banos[1]._insp_orden_metros()
        self.assertEqual(m1, orden_m, "La primera parte es la orden en metros original")
        self.assertTrue(m2 and m2 != m1)
        self.assertEqual([m1.product_qty, m2.product_qty], [1000.0, 1000.0])
        self.assertEqual(m1._ordenes_previas(), banos[0])
        self.assertEqual(m2._ordenes_previas(), banos[1])
        return orden_m, banos

    def _rama_e_inspeccion(self, bano, kg_bruto_rama, capturas):
        """Pesa un rollo en Rama con el asistente y lo inspecciona en una o varias capturas (kg_bruto, metros, calidad)."""
        wo_rama = bano.workorder_ids.filtered(lambda w: w.workcenter_id == self.rama)
        wo_insp = bano.workorder_ids.filtered(lambda w: w.workcenter_id == self.revisadora)
        self.assertTrue(wo_rama and wo_insp, "La división copia las operaciones de la lista a cada baño")
        self.env['rollo.pesaje.rama.wizard'].create({
            'production_id': bano.id, 'workorder_id': wo_rama.id, 'empleado_numero': '1',
            'weighing_mode': 'manual', 'motivo_manual': 'Prueba sin báscula', 'kg_bruto': kg_bruto_rama,
            'metros_rama': 600.0,
        }).action_confirmar()
        rollos = bano.rollo_bitacora_ids
        self.assertEqual(rollos.lot_id.name, '%s-0001' % bano.numero_bano)  # 3.7.0: la J ya va en el número
        for i, (kg_bruto, metros, calidad) in enumerate(capturas, 1):
            self._inspeccionar(bano, wo_insp, rollos, kg_bruto, metros, calidad, termina=i == len(capturas))
        return rollos

    def test_21_programacion_divide_y_cada_bano_cierra(self):
        """Programación 1.8.0: cada baño cierra su propia orden en metros completa, con sus metros reales, sin pendiente;
        cerrar el primero no toca la orden del segundo y no nace ninguna orden en kg."""
        orden_m, (bano_1, bano_2) = self._programar_dos_banos()
        m2 = bano_2._insp_orden_metros()
        antes = self.env['mrp.production'].search([('product_id', '=', self.prod_kg.id)])
        self._rama_e_inspeccion(bano_1, 41.1, [(40.6, 460.0, 'PQ')])
        self._cerrar_bano(bano_1)
        self.assertEqual(bano_1.state, 'done')
        self.assertEqual(orden_m.state, 'done')
        self.assertAlmostEqual(orden_m.qty_produced, 460.0)
        self.assertEqual(m2.state, 'confirmed', "La orden en metros del segundo baño no cambia")
        self.assertAlmostEqual(m2.product_qty, 1000.0)
        self.assertEqual(bano_2._insp_orden_metros(), m2)
        self._rama_e_inspeccion(bano_2, 41.1, [(40.5, 455.0, 'PQ')])
        self.assertTrue(bano_1.numero_bano and bano_2.numero_bano and bano_1.numero_bano != bano_2.numero_bano)
        self.assertEqual(bano_2.rollo_final_kg_ids.name, '%s-0001' % bano_2.numero_bano)
        self.assertEqual(bano_2.rollo_final_kg_ids.production_m_id, m2)
        self._cerrar_bano(bano_2)
        self.assertEqual(bano_2.state, 'done')
        self.assertEqual(m2.state, 'done')
        self.assertAlmostEqual(m2.qty_produced, 455.0)
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ']), 915.0)
        self.assertFalse((orden_m | m2).production_group_id.production_ids.filtered(
            lambda p: p.state not in ('done', 'cancel')), "No queda ningún pendiente")
        self.assertEqual(set((bano_1 | bano_2).rollo_final_kg_ids.mapped('estado')), {'registrado'})
        nuevas = self.env['mrp.production'].search([('product_id', '=', self.prod_kg.id)]) - antes
        self.assertFalse(nuevas, "Cerrar los baños no crea órdenes en kg: %s" % nuevas.mapped('name'))

    def test_22_programacion_cierre_en_orden_inverso_y_bano_de_saldos(self):
        """Programación 1.8.0: el segundo baño sale todo Desperdicio y cierra primero: cancela solo su orden en
        metros y saca los saldos aparte; el primer baño cierra la suya."""
        orden_m, (bano_1, bano_2) = self._programar_dos_banos()
        m2 = bano_2._insp_orden_metros()
        self._rama_e_inspeccion(bano_2, 41.1, [(30.5, 320.0, 'DESP'), (9.0, 80.0, 'DESP')])
        self._cerrar_bano(bano_2)
        self.assertEqual(bano_2.state, 'done')
        self.assertEqual(m2.state, 'cancel', "Sin tela en metros se cancela la orden en metros de ese baño")
        self.assertEqual(orden_m.state, 'confirmed', "La orden en metros del primer baño no cambia")
        self.assertAlmostEqual(self._cantidad(self.saldo_d, self.ubic['DESP']), 36.5)
        self.assertEqual(bano_1._insp_orden_metros(), orden_m)
        self._rama_e_inspeccion(bano_1, 41.1, [(26.6, 300.0, 'PQ'), (14.0, 150.0, 'FE')])
        self._cerrar_bano(bano_1)
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
        self._cerrar_bano(kg_a)
        self.assertEqual(orden_m.state, 'done')
        pendiente = orden_m.production_group_id.production_ids.filtered(lambda p: p.state not in ('done', 'cancel'))
        saldos = self.env['mrp.rollo.final']._productos_saldo()
        movs = pendiente.move_finished_ids.filtered(lambda m: m.product_id in saldos and m.state not in ('done', 'cancel'))
        self.assertFalse(movs.move_line_ids, "El pendiente no hereda líneas de saldo")
        self._inspeccionar(kg_b, wo_b, rollos_b, 40.5, 455.0)
        self._cerrar_bano(kg_b)
        self.assertEqual(kg_b.state, 'done')
        self.assertEqual(pendiente.state, 'done')
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ']), 800.0)
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['SEGUNDA']), 100.0)

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
        self._inspeccionar(orden_kg, wo, rollos, 30.5, 320.0, 'DESP', termina=False)
        self._inspeccionar(orden_kg, wo, rollos, 9.0, 80.0, 'DESP')
        self._cerrar_bano(orden_kg)
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
        self._inspeccionar(orden_kg, wo, rollos, 30.5, 320.0, 'DESP', termina=False)
        self._inspeccionar(orden_kg, wo, rollos, 9.0, 80.0, 'DESP')
        self._cerrar_bano(orden_kg)
        self.assertEqual(orden_kg.state, 'done')
        orden_s = orden_kg.rollo_final_kg_ids.production_saldo_id
        self.assertEqual(orden_s.state, 'done')
        consumo = sum(orden_s.move_raw_ids.filtered(lambda m: m.state == 'done').mapped('value'))
        self.assertGreater(consumo, 0.0, "El baño tiene costo: sale del inventario de la tela en kg")
        terminados = orden_s.move_finished_ids.filtered(lambda m: m.state == 'done')
        self.assertTrue(terminados)
        self.assertFalse(any(terminados.mapped('value')), "El desperdicio entra a costo cero")
        self.assertAlmostEqual(self.saldo_d.standard_price, 0.0)

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

    def test_27_no_cierra_con_menos_crudo_que_lo_pesado_en_rama(self):
        """3.3.2: con el consumo flexible un baño cerraba sin crudo en existencia (en main: 19 y 0 kg) y el kg salía casi
        gratis. Ahora no cierra si el crudo consumido es menor que lo pesado en Rama; con existencia sí."""
        self.crudo.write({'is_storable': True})
        orden_m, orden_kg, wo, rollos = self._bano((40.0,), con_crudo=False)
        self._inspeccionar(orden_kg, wo, rollos, 40.6, 460.0)
        with self.assertRaises(UserError) as error:
            orden_kg.button_mark_done()
        self.assertIn('crudo', str(error.exception))
        self.assertNotEqual(orden_kg.state, 'done')
        stock = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1).lot_stock_id
        self.env['stock.quant']._update_available_quantity(self.crudo, stock, 500.0)
        orden_kg.action_assign()
        self._cerrar_bano(orden_kg)
        self.assertEqual(orden_kg.state, 'done')
        self.assertEqual(orden_m.state, 'done')
        # Con el parámetro, además pide un porcentaje del crudo planeado.
        self.env['ir.config_parameter'].sudo().set_param('quimibond_rollos_bitacora.crudo_minimo_pct', '90')
        orden_m2, orden_kg2, wo2, rollos2 = self._bano((40.0,))
        crudo2 = orden_kg2._moves_crudo()
        crudo2.write({'quantity': 45.0, 'picked': True})  # más que Rama, pero lejos de los 186.56 kg planeados
        self._inspeccionar(orden_kg2, wo2, rollos2, 40.6, 460.0)
        with self.assertRaisesRegex(UserError, 'planeados'):
            orden_kg2.button_mark_done()

    def test_28_operador_sin_permisos_de_administrador(self):
        """Un operador de Manufactura (sin administrador ni supervisor) pesa en Rama, inspecciona y cierra los baños, incluida la
        orden de saldos y el pendiente; «Anular captura» es solo del supervisor."""
        operador = self.env['res.users'].create({
            'name': 'Operador ZZ951', 'login': 'operador.zz951', 'email': 'operador.zz951@example.com',
            'company_id': self.env.company.id, 'company_ids': [Command.set(self.env.company.ids)],
            'group_ids': [Command.set([self.env.ref('mrp.group_mrp_user').id, self.env.ref('base.group_user').id])]})
        self.assertFalse(operador.has_group('mrp.group_mrp_manager'))
        orden_m, ((kg_a, wo_a, rollos_a), (kg_b, wo_b, rollos_b)) = self._dos_banos()
        Wizard = self.env['rollo.inspeccion.estacion.wizard'].with_user(operador)
        base = {'workcenter_id': self.revisadora.id, 'empleado_numero': '1', 'weighing_mode': 'manual',
                'motivo_manual': 'Prueba sin báscula'}
        Wizard.create(dict(base, production_id=kg_a.id, workorder_id=wo_a.id, rollo_codigos=rollos_a.name,
                           kg_bruto=30.6, metros=345.0, clasificacion='PQ', termina_rollo=False)).action_confirmar()
        Wizard.create(dict(base, production_id=kg_a.id, workorder_id=wo_a.id, rollo_codigos=rollos_a.name,
                           kg_bruto=9.9, metros=100.0, clasificacion='SEGUNDA', termina_rollo=True,
                           defecto_ids=[Command.set(self.defecto.ids)])).action_confirmar()
        segunda = kg_a.rollo_final_kg_ids.filtered(lambda r: r.clasificacion == 'SEGUNDA')
        with self.assertRaises(UserError):
            segunda.with_user(operador).action_anular()
        self._cerrar_bano(kg_a, operador)
        self.assertEqual(kg_a.state, 'done')
        Wizard.create(dict(base, production_id=kg_b.id, workorder_id=wo_b.id, rollo_codigos=rollos_b.name,
                           kg_bruto=40.5, metros=455.0, clasificacion='PQ', termina_rollo=True)).action_confirmar()
        self._cerrar_bano(kg_b, operador)
        self.assertEqual(kg_b.state, 'done')
        self.assertFalse(orden_m.production_group_id.production_ids.filtered(lambda p: p.state not in ('done', 'cancel')))
        # Baño de solo saldos (orden de saldos) también con el operador.
        orden_m2, orden_kg2, wo2, rollos2 = self._bano((40.0,))
        Wizard.create(dict(base, production_id=orden_kg2.id, workorder_id=wo2.id, rollo_codigos=rollos2.name,
                           kg_bruto=40.5, metros=400.0, clasificacion='DESP', termina_rollo=True)).action_confirmar()
        self._cerrar_bano(orden_kg2, operador)
        self.assertEqual(orden_kg2.rollo_final_kg_ids.production_saldo_id.state, 'done')

    def test_29_quimico_faltante_avisa_y_no_bloquea(self):
        """3.3.4: si a un químico o al engomado le falta existencia para su consumo teórico, la orden en kg cierra con lo
        que hay y deja una nota y una actividad con el faltante (el crudo es el único que bloquea)."""
        quimico = self.env['product.product'].create({
            'name': 'Engomado ZZ951', 'default_code': 'ENGZZ951', 'type': 'consu', 'is_storable': True,
            'uom_id': self.kg.id})
        self.bom_kg.write({'bom_line_ids': [Command.create({'product_id': quimico.id, 'product_qty': 0.04})]})
        stock = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1).lot_stock_id
        self.env['stock.quant']._update_available_quantity(quimico, stock, 2.0)  # alcanza para 2 de 6.78 kg
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        orden_kg.action_assign()
        self._inspeccionar(orden_kg, wo, rollos, 40.6, 460.0)
        self._cerrar_bano(orden_kg)
        self.assertEqual(orden_kg.state, 'done', "El faltante de químico no detiene el cierre")
        self.assertEqual(orden_m.state, 'done')
        consumo = orden_kg.move_raw_ids.filtered(lambda m: m.product_id == quimico and m.state == 'done')
        self.assertAlmostEqual(sum(consumo.mapped('quantity')), 2.0, delta=0.01, msg="Se consumió lo que había")
        nota = orden_kg.message_ids.filtered(lambda m: 'ENGZZ951' in (m.body or ''))
        self.assertTrue(nota, "La orden deja la nota con el faltante")
        actividad = self.env['mail.activity'].search([('res_model', '=', 'mrp.production'), ('res_id', '=', orden_kg.id)])
        self.assertTrue(actividad.filtered(lambda a: 'Faltante' in (a.summary or '')), "Queda la actividad para Almacén")

    def test_30_sin_faltante_no_hay_aviso(self):
        """3.3.4: con la existencia completa no se deja nota ni actividad de faltante."""
        quimico = self.env['product.product'].create({
            'name': 'Engomado ZZ952', 'default_code': 'ENGZZ952', 'type': 'consu', 'is_storable': True,
            'uom_id': self.kg.id})
        self.bom_kg.write({'bom_line_ids': [Command.create({'product_id': quimico.id, 'product_qty': 0.04})]})
        stock = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1).lot_stock_id
        self.env['stock.quant']._update_available_quantity(quimico, stock, 20.0)
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        orden_kg.action_assign()
        self._inspeccionar(orden_kg, wo, rollos, 40.6, 460.0)
        self._cerrar_bano(orden_kg)
        self.assertEqual(orden_kg.state, 'done')
        self.assertFalse(orden_kg.message_ids.filtered(lambda m: 'Faltante de componentes' in (m.subject or '')))

    # ------------------------------------------------------------------ 3.3.5 (propuesta de Consolti, 2026-11-05)
    def test_31_desarrollo_va_a_la_31(self):
        """La primera y la FE de una orden de desarrollo van al almacén de desarrollos; la Segunda sigue a 16 y el desperdicio a 17."""
        stock = self.ubic['PQ'].location_id
        desarrollos = self.env['stock.location'].create({'name': 'ZZ951 DESARROLLOS', 'location_id': stock.id,
                                                         'usage': 'internal'})
        self.env['ir.config_parameter'].sudo().set_param('quimibond_rollos_bitacora.ubicacion_desarrollo',
                                                         'ZZ951 DESARROLLOS')
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        orden_m.es_desarrollo = True
        self._inspeccionar(orden_kg, wo, rollos, 26.5, 250.0, 'PQ', termina=False)
        self._inspeccionar(orden_kg, wo, rollos, 13.4, 120.0, 'SEGUNDA')
        self._cerrar_bano(orden_kg)
        self.assertEqual(orden_m.state, 'done')
        pq = orden_kg.rollo_final_kg_ids.filtered(lambda r: r.clasificacion == 'PQ')
        self.assertAlmostEqual(self._cantidad(self.prod_m, desarrollos, pq.name), 250.0)
        self.assertFalse(self._cantidad(self.prod_m, self.ubic['PQ']), "Nada de desarrollo entra a 14")
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['SEGUNDA']), 120.0)

    def test_32_consecutivo_sin_huecos_al_anular(self):
        orden_m, orden_kg, wo, rollos = self._bano((40.0, 41.0, 42.0))
        for rollo, bruto in zip(rollos, (41.5, 42.5, 43.5)):
            self._inspeccionar(orden_kg, wo, rollo, bruto, 400.0)
        segundo = orden_kg.rollo_final_kg_ids.filtered(lambda r: r.secuencia == 2)
        segundo.action_anular()
        self._inspeccionar(orden_kg, wo, rollos[1], 42.5, 410.0)
        self.assertEqual(sorted(orden_kg.rollo_final_kg_ids.mapped('secuencia')), [1, 2, 3])
        self.assertIn('%s-F002' % orden_kg.numero_bano, orden_kg.rollo_final_kg_ids.mapped('name'))

    def test_33_inspeccion_en_paralelo_a_rama(self):
        """Inspección se abre con Rama todavía abierta si hay rollos por inspeccionar; no termina hasta que Rama termina."""
        orden_m, orden_kg, wo_insp, rollos = self._bano((40.0,))
        wo_rama = orden_kg.workorder_ids.filtered(lambda w: w.workcenter_id == self.rama)
        # En Odoo 19 la operación queda «bloqueada» mientras la anterior no produce (qty_ready); aquí se fija a mano.
        wo_insp.state = 'blocked'
        accion = wo_insp.action_inspeccionar_rollo()
        self.assertEqual(accion['res_model'], 'rollo.inspeccion.estacion.wizard')
        self._inspeccionar(orden_kg, wo_insp, rollos, 41.5, 400.0)
        with self.assertRaisesRegex(UserError, 'no está lista'):
            wo_insp.action_inspeccionar_rollo()  # sin rollos pendientes, se espera a Rama como siempre
        with self.assertRaisesRegex(UserError, 'Rama siga abierta'):
            wo_insp.button_finish()

    def test_34_no_se_captura_en_orden_cancelada(self):
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        orden_kg.action_cancel()
        with self.assertRaisesRegex(UserError, 'cerrada o cancelada'):
            self._inspeccionar(orden_kg, wo, rollos, 41.5, 400.0)

    def test_35_reserva_sin_lote_de_quimico_no_bloquea(self):
        """3.3.6: una línea sin lote de un químico con lote (registro sin lote ni existencia en la ubicación) no detiene
        el cierre: se quita y queda como faltante."""
        quimico = self.env['product.product'].create({
            'name': 'Engomado ZZ953', 'default_code': 'ENGZZ953', 'type': 'consu', 'is_storable': True,
            'uom_id': self.kg.id, 'tracking': 'lot'})
        self.bom_kg.write({'bom_line_ids': [Command.create({'product_id': quimico.id, 'product_qty': 0.04})]})
        stock = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1).lot_stock_id
        lote = self.env['stock.lot'].create({'name': 'ZZ953-1', 'product_id': quimico.id,
                                             'company_id': self.env.company.id})
        self.env['stock.quant']._update_available_quantity(quimico, stock, 2.0, lot_id=lote)
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        orden_kg.action_assign()
        movimiento = orden_kg.move_raw_ids.filtered(lambda m: m.product_id == quimico)
        self.env['stock.move.line'].create({
            'move_id': movimiento.id, 'product_id': quimico.id, 'quantity': 0.5,
            'location_id': movimiento.location_id.id, 'location_dest_id': movimiento.location_dest_id.id})
        self.assertTrue(movimiento.move_line_ids.filtered(lambda ml: not ml.lot_id))
        self._inspeccionar(orden_kg, wo, rollos, 40.6, 460.0)
        self._cerrar_bano(orden_kg)
        self.assertEqual(orden_kg.state, 'done', "La línea sin lote no detiene el cierre")
        hechos = orden_kg.move_raw_ids.filtered(lambda m: m.product_id == quimico and m.state == 'done')
        self.assertAlmostEqual(sum(hechos.mapped('quantity')), 2.0, delta=0.01, msg="Solo se consume lo del lote")
        self.assertFalse(hechos.move_line_ids.filtered(lambda ml: not ml.lot_id))
        self.assertTrue(orden_kg.message_ids.filtered(lambda m: 'ENGZZ953' in (m.body or '')), "Queda como faltante")

    def test_36_historial_de_pesaje(self):
        """3.3.8: Rama, desperdicio e Inspección dejan copia en el «Historial de Pesaje» de la orden (si está el módulo
        de pesaje de Tejido); al anular el rollo final, su pesada sale del historial."""
        if 'mrp.weighing.log' not in self.env:
            self.skipTest('Sin pesaje_rollos_tejido no hay historial de pesaje.')
        Log = self.env['mrp.weighing.log']
        orden_m, orden_kg, wo, rollos = self._bano((50.0,))
        wo_rama = orden_kg.workorder_ids.filtered(lambda w: w.workcenter_id == self.rama)
        self.env['rollo.pesaje.rama.wizard'].create({
            'production_id': orden_kg.id, 'workorder_id': wo_rama.id, 'empleado_numero': '1',
            'weighing_mode': 'manual', 'motivo_manual': 'Prueba', 'kg_bruto': 31.1, 'metros_rama': 480.0}).action_confirmar()
        nuevo = orden_kg.rollo_bitacora_ids - rollos
        log_rama = Log.search([('production_id', '=', orden_kg.id), ('roll_number', '=', nuevo.lot_id.name)])
        self.assertEqual(len(log_rama), 1)
        self.assertEqual((log_rama.weight_bruto, log_rama.tara, log_rama.weight_neto, log_rama.workcenter_id),
                         (31.1, 1.1, 30.0, self.rama))
        self.assertEqual(log_rama.lot_id, nuevo.lot_id)
        self.assertFalse(log_rama.rollo_circular)
        self.assertFalse(orden_kg.roll_count if 'roll_count' in orden_kg._fields else 0,
                         'El historial no mueve el contador de rollos de Tejido')
        # El baño de prueba no trae subproducto de desperdicio: aquí solo importa la copia en el historial.
        with patch.object(type(orden_kg), '_rama_registrar_desperdicio', lambda *args, **kwargs: None):
            self.env['rollo.desperdicio.rama.wizard'].create({
                'production_id': orden_kg.id, 'workorder_id': wo_rama.id, 'empleado_numero': '1',
                'weighing_mode': 'manual', 'motivo_manual': 'Prueba', 'kg_desperdicio': 1.2}).action_confirmar()
        log_desp = Log.search([('production_id', '=', orden_kg.id), ('roll_number', '=', 'DESPERDICIO RAMA')])
        self.assertEqual((log_desp.weight_bruto, log_desp.tara, log_desp.weight_neto), (1.2, 0.0, 1.2))
        self._inspeccionar(orden_kg, wo, rollos, 50.6, 585.0)
        final = orden_kg.rollo_final_kg_ids
        log_insp = Log.search([('production_id', '=', orden_kg.id), ('roll_number', '=', final.name)])
        self.assertEqual((log_insp.weight_bruto, log_insp.tara, log_insp.weight_neto, log_insp.workcenter_id),
                         (50.6, 1.5, final.kg_neto, self.revisadora))
        nombre_final = final.name
        final.action_anular()
        self.assertFalse(Log.search([('production_id', '=', orden_kg.id), ('roll_number', '=', nombre_final)]))
        self.assertEqual(len(Log.search([('production_id', '=', orden_kg.id)])), 2, 'Rama y desperdicio se quedan')

    def test_37_sin_bascula_abre_en_manual(self):
        """3.3.9: sin báscula IoT registrada, Rama, desperdicio e Inspección abren en captura manual con el motivo
        «Sin báscula IoT»; con una báscula registrada abren en automático y sin motivo."""
        from ..wizard.pesaje_manual import MOTIVO_SIN_BASCULA
        Dispositivo = self.env['iot.device'].sudo()
        Dispositivo.search([('type', '=', 'scale')]).unlink()
        modelos = ('rollo.pesaje.rama.wizard', 'rollo.desperdicio.rama.wizard', 'rollo.inspeccion.estacion.wizard')
        for modelo in modelos:
            valores = self.env[modelo].default_get(['weighing_mode', 'motivo_manual'])
            self.assertEqual((valores['weighing_mode'], valores['motivo_manual']), ('manual', MOTIVO_SIN_BASCULA), modelo)
        self._registrar_bascula()
        for modelo in modelos:
            valores = self.env[modelo].default_get(['weighing_mode', 'motivo_manual'])
            self.assertEqual(valores['weighing_mode'], 'iot', modelo)
            self.assertFalse(valores.get('motivo_manual'), modelo)

    def test_38_etiquetas_preliminar_y_final(self):
        """3.4.0: al confirmar sale la etiqueta preliminar de 68 x 38 mm (artículo, orden, fecha, revisadora, kg, metros,
        calidad y código del lote); con el parámetro `inspeccion_imprime` = final sale la de 76 x 102 mm."""
        orden_m, orden_kg, wo, rollos = self._bano((50.0, 40.0))
        valores = {
            'production_id': orden_kg.id, 'workorder_id': wo.id, 'workcenter_id': self.revisadora.id,
            'empleado_numero': '1', 'weighing_mode': 'manual', 'motivo_manual': 'Prueba sin báscula',
            'rollo_codigos': rollos[0].name, 'kg_bruto': 50.6, 'metros': 585.0, 'clasificacion': 'FE',
            'defecto_ids': [Command.set(self.defecto.ids)], 'termina_rollo': True}
        Wizard = self.env['rollo.inspeccion.estacion.wizard'].with_context(discard_logo_check=True)  # sin layout en la base de prueba
        accion = Wizard.create(valores).action_confirmar()
        self.assertEqual(accion['report_name'], 'quimibond_rollos_bitacora.report_etiqueta_rollo_preliminar')
        self.assertTrue(accion.get('close_on_report_download'))
        final = orden_kg.rollo_final_kg_ids
        zpl = final.zpl_etiqueta_preliminar
        self.assertTrue(zpl.startswith('^XA^PW544^LL304'))
        for texto in ('ARTÍCULO: %s' % self.prod_m.default_code, 'ORDEN: %s' % orden_kg.name, 'FECHA: ',
                      'CENTRO: %s' % self.revisadora.name, 'KG: %.2f' % final.kg_neto, 'M: 585.00', '^FDFE^FS'):
            self.assertIn(texto, zpl)
        self.assertIn('^FD%s^FS' % self.env['mrp.rollo.bitacora']._code128_zpl(final.name)[0], zpl)
        self.assertTrue(final.zpl_etiqueta.startswith('^XA^PW608^LL816'))  # la final se guarda igual

        self.env['ir.config_parameter'].sudo().set_param('quimibond_rollos_bitacora.inspeccion_imprime', 'final')
        accion = Wizard.create(
            dict(valores, rollo_codigos=rollos[1].name, kg_bruto=40.6, metros=470.0, clasificacion='PQ',
                 defecto_ids=[])).action_confirmar()
        self.assertEqual(accion['report_name'], 'quimibond_rollos_bitacora.report_etiqueta_rollo_final')

        # Los dos reportes, para varios rollos a la vez (Imprimir en la lista de rollos finales).
        finales = orden_kg.rollo_final_kg_ids
        texto, _fmt = self.env['ir.actions.report']._render_qweb_text(
            'quimibond_rollos_bitacora.report_etiqueta_rollo_preliminar', finales.ids)
        self.assertEqual(texto.decode().count('^XA^PW544'), 2)

    def test_39_numero_de_bano(self):
        """3.7.0: el número de baño es el nombre de la orden sin el prefijo («TL/OP-KG/J00001» → «J00001»); se fija una sola
        vez, no se copia al duplicar, es único y da nombre a los lotes: Rama «<baño>-0001» e Inspección «<baño>-F001»."""
        _m, orden_kg, wo, rollos = self._bano((40.0,))
        self.assertFalse(orden_kg.numero_bano)
        orden_kg._asignar_numero_bano()
        numero = orden_kg.numero_bano
        self.assertEqual(numero, orden_kg.name.rsplit('/', 1)[-1])
        orden_kg._asignar_numero_bano()
        self.assertEqual(orden_kg.numero_bano, numero, "No cambia una vez asignado")
        self.assertFalse(orden_kg.copy().numero_bano, "Una copia no hereda el número")
        otra = self._bano((30.0,))[1]
        otra._asignar_numero_bano()
        self.assertNotEqual(otra.numero_bano, numero)
        # La llave única se viola a propósito: el ERROR de odoo.sql_db se silencia.
        with self.assertRaises(IntegrityError), mute_logger('odoo.sql_db'), self.cr.savepoint():
            otra.numero_bano = numero
            otra.flush_recordset()
        # Si el número del nombre ya lo tiene otra orden (una vieja numerada con la secuencia de 3.5.0), se usa el nombre
        # completo con guiones.
        tercera = self._bano((20.0,))[1]
        tercera.numero_bano = False
        self.env['mrp.production'].search([('id', '=', otra.id)]).numero_bano = tercera.name.rsplit('/', 1)[-1]
        tercera._asignar_numero_bano()
        self.assertEqual(tercera.numero_bano, tercera.name.replace('/', '-'))
        self._inspeccionar(orden_kg, wo, rollos, 40.6, 460.0)
        self.assertEqual(orden_kg.rollo_final_kg_ids.name, '%s-F001' % numero)
        # Empezar una operación del baño le asigna el número (Tintorería lo usa al cargar el jet).
        bano = self._bano((30.0,))[1]
        self.assertFalse(bano.numero_bano)
        wo_listo = bano.workorder_ids.filtered(lambda w: w.state in ('ready', 'progress'))[:1]
        self.assertTrue(wo_listo)
        wo_listo.button_start()
        self.assertTrue(bano.numero_bano)

    def test_40_operador_por_referencia_de_nomina(self):
        """3.5.1: el número tecleado se busca como la referencia S-<número> (nómina semanal); otra nómina se teclea completa.
        El mismo número en dos nóminas ya no confunde a la estación."""
        Emp = self.env['hr.employee']
        if 'registration_number' not in Emp._fields:
            self.skipTest('Sin «Referencia de empleado» (nómina Enterprise): corre en Odoo.sh')
        semanal = Emp.create({'name': 'Operadora semanal', 'registration_number': 'S-85'})
        quincenal = Emp.create({'name': 'Mecanico quincenal', 'registration_number': 'Q-85'})
        self.assertEqual(Emp._qb_operador('85'), semanal)
        self.assertEqual(Emp._qb_operador(' 085 '), semanal)
        self.assertEqual(Emp._qb_operador('Q-85'), quincenal)
        self.assertEqual(Emp._qb_operador('q85'), quincenal)
        self.assertFalse(Emp._qb_operador('86'))
        self.assertFalse(Emp._qb_operador(''))
        for modelo in ('rollo.pesaje.rama.wizard', 'rollo.desperdicio.rama.wizard', 'rollo.inspeccion.estacion.wizard'):
            self.assertEqual(self.env[modelo].new({'empleado_numero': '85'}).empleado_nombre, 'Operadora semanal', modelo)
            self.assertEqual(self.env[modelo].new({'empleado_numero': 'Q-85'}).empleado_nombre, 'Mecanico quincenal', modelo)

    def test_41_crudo_por_categoria_de_tejido(self):
        """3.5.1: el crudo es el componente de una categoría de tejido aunque otro pese más (IWJ032: LAVADO001 a 2.2 kg por
        kg, detergente de la abridora en «Aux. acabado»); sin categoría de tejido sigue ganando el de más kg."""
        _m, orden_kg, _wo, _rollos = self._bano((40.0,))
        self.assertEqual(orden_kg._moves_crudo().product_id, self.crudo)
        Categoria = self.env['product.category']
        lavado = self.env['product.product'].create({
            'name': 'Lavado ZZ951', 'type': 'consu', 'uom_id': self.kg.id,
            'categ_id': Categoria.create({'name': 'Aux. acabado'}).id})
        self.env['stock.move'].create({
            'raw_material_production_id': orden_kg.id, 'product_id': lavado.id, 'product_uom_qty': 400.0,
            'product_uom': self.kg.id, 'location_id': orden_kg.location_src_id.id,
            'location_dest_id': orden_kg.production_location_id.id, 'company_id': orden_kg.company_id.id})
        self.assertEqual(orden_kg._moves_crudo().product_id, lavado, "Sin categoría de tejido gana el de más kg")
        self.crudo.categ_id = Categoria.create({'name': 'Tejido Circular'})
        self.assertEqual(orden_kg._moves_crudo().product_id, self.crudo)


    def test_42_no_se_cancela_orden_con_rollos_de_rama(self):
        """3.5.2 (ronda 5 de pruebas): un operador no cancela una orden en kg con rollos pesados en Rama (la tela existe);
        un administrador de Fabricación sí, con nota; la orden en metros que abastecía recibe el aviso de que se quedó sin
        abasto, y los rollos de esa orden ya no salen en «Pendientes de inspección»."""
        operador = self.env['res.users'].create({
            'name': 'Operador ZZ960', 'login': 'operador.zz960', 'email': 'operador.zz960@example.com',
            'company_id': self.env.company.id, 'company_ids': [Command.set(self.env.company.ids)],
            'group_ids': [Command.set([self.env.ref('mrp.group_mrp_user').id, self.env.ref('base.group_user').id])]})
        orden_m, orden_kg, wo, rollos = self._bano((40.0, 35.0))
        with self.assertRaisesRegex(UserError, 'no se cancela'):
            orden_kg.with_user(operador).action_cancel()
        self.assertNotEqual(orden_kg.state, 'cancel')
        self.assertTrue(self.env.user.has_group('mrp.group_mrp_manager'))
        orden_kg.action_cancel()
        self.assertEqual(orden_kg.state, 'cancel')
        self.assertTrue(orden_kg.message_ids.filtered(lambda m: 'no entraron al inventario' in (m.body or '')))
        self.assertTrue(orden_m.message_ids.filtered(lambda m: 'sin 169.60 kg de abasto' in (m.body or '')))
        pendientes = self.env['mrp.rollo.bitacora'].search(
            [('estado_inspeccion', '=', 'pendiente'), ('production_id.state', '!=', 'cancel')])
        self.assertFalse(pendientes & rollos)

    def test_43_orden_sin_rollos_se_cancela(self):
        """Una orden en kg sin pesar se cancela como siempre, también por un operador."""
        operador = self.env['res.users'].create({
            'name': 'Operador ZZ961', 'login': 'operador.zz961', 'email': 'operador.zz961@example.com',
            'company_id': self.env.company.id, 'company_ids': [Command.set(self.env.company.ids)],
            'group_ids': [Command.set([self.env.ref('mrp.group_mrp_user').id, self.env.ref('base.group_user').id])]})
        orden_kg = self.env['mrp.production'].create({
            'product_id': self.prod_kg.id, 'product_qty': 100, 'bom_id': self.bom_kg.id})
        orden_kg.action_confirm()
        orden_kg.with_user(operador).action_cancel()
        self.assertEqual(orden_kg.state, 'cancel')

    def test_44_rama_rechaza_operador_inexistente(self):
        """3.5.3 (ronda 7 de pruebas en main): Rama y el desperdicio de Rama registraban «EMPLEADO NO ENCONTRADO» con un
        número que no existe. Donde existe el número de trabajador (la base de Quimibond) ahora lo rechazan, como Inspección.
        Aquí se simula ese campo con el nombre del empleado."""
        orden_kg = self.env['mrp.production'].create({
            'product_id': self.prod_kg.id, 'product_qty': 100.0, 'bom_id': self.bom_kg.id})
        orden_kg.action_confirm()
        wo_rama = self.env['mrp.workorder'].create({
            'name': 'RAMA', 'production_id': orden_kg.id, 'workcenter_id': self.rama.id})
        base = {'production_id': orden_kg.id, 'workorder_id': wo_rama.id, 'weighing_mode': 'manual',
                'motivo_manual': 'Prueba sin báscula'}
        with patch('odoo.addons.quimibond_rollos_bitacora.models.hr_employee.CAMPO_NUMERO', 'name'):
            with self.assertRaisesRegex(UserError, 'No existe el empleado 9999'):
                self.env['rollo.pesaje.rama.wizard'].create(dict(base, empleado_numero='9999', kg_bruto=31.1)).action_confirmar()
            with self.assertRaisesRegex(UserError, 'No existe el empleado 9999'):
                self.env['rollo.desperdicio.rama.wizard'].create(
                    dict(base, empleado_numero='9999', kg_desperdicio=1.2)).action_confirmar()
        self.assertFalse(orden_kg.rollo_bitacora_ids)

    def test_45_metros_de_rama_obligatorios_con_motivo(self):
        """3.6.0 (Dirección, 2026-11-06): Rama captura los metros del cuentametros con cada rollo; sin metros solo registra
        con el motivo. Los metros quedan en el rollo, en el resumen del baño, en el rendimiento y en la etiqueta."""
        orden_kg = self.env['mrp.production'].create({
            'product_id': self.prod_kg.id, 'product_qty': 100.0, 'bom_id': self.bom_kg.id})
        orden_kg.action_confirm()
        wo_rama = self.env['mrp.workorder'].create({
            'name': 'RAMA', 'production_id': orden_kg.id, 'workcenter_id': self.rama.id})
        base = {'production_id': orden_kg.id, 'workorder_id': wo_rama.id, 'empleado_numero': '1',
                'weighing_mode': 'manual', 'motivo_manual': 'Prueba sin báscula', 'kg_bruto': 31.1}
        Wizard = self.env['rollo.pesaje.rama.wizard']
        with self.assertRaisesRegex(UserError, 'cuentametros'):
            Wizard.create(base).action_confirmar()
        with self.assertRaisesRegex(UserError, 'negativos'):
            Wizard.create(dict(base, metros_rama=-5.0)).action_confirmar()
        self.assertFalse(orden_kg.rollo_bitacora_ids)
        Wizard.create(dict(base, metros_rama=480.0, motivo_sin_metros='no aplica')).action_confirmar()
        rollo = orden_kg.rollo_bitacora_ids
        self.assertEqual(rollo.metros_rama, 480.0)
        self.assertFalse(rollo.motivo_sin_metros_rama, 'Con metros no se guarda motivo')
        self.assertAlmostEqual(rollo.rendimiento_rama, 480.0 / rollo.kg_rama, places=4)
        self.assertIn('M: 480.00', rollo.zpl_etiqueta_rama)
        Wizard.create(dict(base, motivo_sin_metros='Cuentametros descompuesto')).action_confirmar()
        sin_metros = orden_kg.rollo_bitacora_ids - rollo
        self.assertEqual(sin_metros.metros_rama, 0.0)
        self.assertEqual(sin_metros.motivo_sin_metros_rama, 'Cuentametros descompuesto')
        self.assertEqual(sin_metros.rendimiento_rama, 0.0)
        self.assertNotIn('M: ', sin_metros.zpl_etiqueta_rama)
        self.assertEqual(orden_kg.rollos_rama_metros, 480.0)

    def test_46_bano_del_flujo_kg_no_termina_rama_ni_cierra_sin_rollos(self):
        """3.6.1 (prueba de Dirección en main, 2026-11-06): con las 5 operaciones en «Hecho» y sin pesar nada, el baño
        cerraba con los kg planeados. En el tipo «Fabricación Tela en kg» (OP-KG), Rama no termina y la orden no cierra
        sin rollos pesados; con un rollo pesado Rama sí termina. Una orden con Rama de otro tipo no cambia."""
        almacen = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1)
        tipo_kg = almacen.manu_type_id.copy({'name': 'Fabricación Tela en kg ZZ966', 'sequence_code': 'OP-KG'})
        orden_kg = self.env['mrp.production'].create({
            'product_id': self.prod_kg.id, 'product_qty': 100.0, 'bom_id': self.bom_kg.id,
            'picking_type_id': tipo_kg.id})
        orden_kg.action_confirm()
        wo_rama = self.env['mrp.workorder'].create({
            'name': 'RAMA', 'production_id': orden_kg.id, 'workcenter_id': self.rama.id})
        self.assertTrue(orden_kg._es_bano_flujo_kg())
        with self.assertRaisesRegex(UserError, 'no tiene rollos pesados en Rama'):
            wo_rama.button_finish()
        self.assertNotEqual(wo_rama.state, 'done')
        with self.assertRaisesRegex(UserError, 'no tiene rollos pesados en Rama'):
            orden_kg.button_mark_done()
        self.assertNotEqual(orden_kg.state, 'done')
        self.env['rollo.pesaje.rama.wizard'].create({
            'production_id': orden_kg.id, 'workorder_id': wo_rama.id, 'empleado_numero': '1', 'weighing_mode': 'manual',
            'motivo_manual': 'Prueba sin báscula', 'kg_bruto': 31.1, 'metros_rama': 480.0}).action_confirmar()
        wo_rama.button_finish()
        self.assertEqual(wo_rama.state, 'done')
        # Otro tipo de operación con Rama (flujo viejo, costeo): sin cambio.
        otra = self.env['mrp.production'].create({
            'product_id': self.prod_kg.id, 'product_qty': 100.0, 'bom_id': self.bom_kg.id})
        otra.action_confirm()
        self.env['mrp.workorder'].create({'name': 'RAMA', 'production_id': otra.id, 'workcenter_id': self.rama.id})
        self.assertFalse(otra._es_bano_flujo_kg())
        otra._validar_rollos_pesados()

    def test_47_ultimo_bano_con_mas_kg_no_crea_orden_en_kg(self):
        """3.6.2 (ronda 12 en main, 2026-11-06): al cerrar el último baño con más kg que la parte que Odoo le dejó al
        pendiente, el cierre subía la demanda del acabado en kg de la orden en metros y Odoo abastecía la diferencia con
        una orden en kg nueva (TL/OP-KG/00066, 20.6 kg, con crudo reservado). Ajustar la demanda no debe abastecer."""
        orden_m, ((kg_a, wo_a, rollos_a), (kg_b, wo_b, rollos_b)) = self._dos_banos(kg_b=(45.0, 45.0))
        # Ruta bajo pedido como la del acabado en kg en planta: una demanda mayor se abastece con una orden en kg.
        stock = kg_a.location_dest_id
        produccion = self.prod_kg.property_stock_production
        tipo = self.env['stock.picking.type'].search(
            [('code', '=', 'mrp_operation'), ('company_id', '=', self.env.company.id)], limit=1)
        ruta = self.env['stock.route'].create({
            'name': 'ZZ967 bajo pedido a producción', 'product_selectable': True,
            'rule_ids': [Command.create({
                'name': 'ZZ967 stock → producción', 'action': 'pull', 'procure_method': 'make_to_order',
                'location_src_id': stock.id, 'location_dest_id': produccion.id, 'picking_type_id': tipo.id})]})
        self.prod_kg.route_ids = [Command.link(ruta.id), Command.link(self.env.ref('mrp.route_warehouse0_manufacture').id)]
        orden_m._clasif_move_a().procure_method = 'make_to_order'
        antes = self.env['mrp.production'].search([('product_id', '=', self.prod_kg.id)])
        # El primer baño rinde mucho (700 m con 40 kg): al pendiente le toca menos demanda de la que pesará el segundo.
        self._inspeccionar(kg_a, wo_a, rollos_a, 41.5, 700.0)
        self._cerrar_bano(kg_a)
        pendiente = orden_m.production_group_id.production_ids.filtered(lambda p: p.state not in ('done', 'cancel'))
        self.assertEqual(len(pendiente), 1)
        self.assertLess(pendiente._clasif_move_a().product_uom_qty, 90.0, "El pendiente quedó con menos kg de los pesados")
        self._inspeccionar(kg_b, wo_b, rollos_b[:1], 45.5, 600.0)
        self._inspeccionar(kg_b, wo_b, rollos_b[1:], 45.5, 600.0)
        self._cerrar_bano(kg_b)
        self.assertEqual(kg_b.state, 'done')
        self.assertEqual(pendiente.state, 'done')
        self.assertAlmostEqual(pendiente.qty_produced, 1200.0)
        nuevas = self.env['mrp.production'].search([('product_id', '=', self.prod_kg.id)]) - antes
        self.assertFalse(nuevas, "Cerrar el último baño no debe crear órdenes en kg: %s" % nuevas.mapped('name'))

    def test_48_desperdicio_sin_metros(self):
        """3.6.2 (ronda 11 en main): el Desperdicio sale en kg y puede ir sin metros; Primera, FE y Segunda los piden."""
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        with self.assertRaisesRegex(UserError, 'Captura los metros del rollo final'):
            self._inspeccionar(orden_kg, wo, rollos, 30.5, 0.0, termina=False)
        self._inspeccionar(orden_kg, wo, rollos, 30.5, 320.0, termina=False)
        Wizard = type(self.env['rollo.inspeccion.estacion.wizard'])
        with patch.object(Wizard, '_rendimiento_acabado', return_value=11.8):
            self._inspeccionar(orden_kg, wo, rollos, 9.0, 0.0, 'DESP')
        desp = orden_kg.rollo_final_kg_ids.filtered(lambda r: r.clasificacion == 'DESP')
        self.assertEqual(desp.metros, 0.0)
        self.assertTrue(desp.metros_teoricos, "La ficha da metros teóricos")
        self.assertFalse(desp.alerta_metros, "3.16.1: sin metros capturados no hay alerta de metros (prueba del 9-nov)")
        self._cerrar_bano(orden_kg)
        self.assertEqual(orden_kg.state, 'done')
        self.assertAlmostEqual(self._cantidad(self.saldo_d, self.ubic['DESP']), 7.5)

    def test_49_cancelar_un_bano_cancela_su_orden_en_metros(self):
        """Programación 1.8.0 (revisión 2026-11-06): un baño cancelado antes de Rama se lleva su orden en metros; la del
        otro baño no cambia y sigue cerrando con sus metros reales."""
        orden_m, (bano_1, bano_2) = self._programar_dos_banos()
        m2 = bano_2._insp_orden_metros()
        bano_2.action_cancel()
        self.assertEqual(bano_2.state, 'cancel')
        self.assertEqual(m2.state, 'cancel', "La orden en metros del baño cancelado no queda colgada")
        self.assertEqual(orden_m.state, 'confirmed')
        self._rama_e_inspeccion(bano_1, 41.1, [(40.6, 460.0, 'PQ')])
        self._cerrar_bano(bano_1)
        self.assertEqual(orden_m.state, 'done')
        self.assertAlmostEqual(orden_m.qty_produced, 460.0)

    def test_50_secuencias_j(self):
        """3.7.0: las secuencias de OP-KG y OP-M llevan el prefijo «J» y arrancan en 00001; es idempotente."""
        from odoo.addons.quimibond_rollos_bitacora.hooks import configurar_secuencias_j
        almacen = self.env['stock.warehouse'].search([], limit=1)
        tipo = self.env['stock.picking.type'].create({
            'name': 'Fabricación Tela en kg (prueba J)', 'code': 'mrp_operation', 'sequence_code': 'OP-KG',
            'warehouse_id': almacen.id})
        prefijo = tipo.sequence_id.prefix
        self.assertFalse((prefijo or '').endswith('J'))
        self.assertIn(tipo, configurar_secuencias_j(self.env))
        self.assertEqual(tipo.sequence_id.prefix, '%sJ' % prefijo)
        self.assertTrue(tipo.sequence_id.next_by_id().endswith('/J00001'))
        configurar_secuencias_j(self.env)
        self.assertEqual(tipo.sequence_id.prefix, '%sJ' % prefijo, "Idempotente: no agrega otra J")
        self.assertTrue(tipo.sequence_id.next_by_id().endswith('/J00002'), "Ni reinicia el consecutivo")

    def test_51_piso_configuracion_y_consumo(self):
        """3.8.0: configurar_piso crea «Químicos-Colorantes Piso», repunta las rutas de surtido y la orden en kg consume
        de ahí el producto cuya ruta sale del Piso; el crudo (sin esa ruta) sigue en la Ubic. 2. Idempotente."""
        from odoo.addons.quimibond_rollos_bitacora.hooks import (
            configurar_piso, NOMBRE_PISO, PARAM_UBICACION_PISO)
        almacen = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1)
        tipo_kg = almacen.manu_type_id.copy({'name': 'Fabricación Tela en kg ZZ967', 'sequence_code': 'OP-KG'})
        ubic2 = self.env['stock.location'].create({'name': 'ZZ2 PRODUCCIÓN', 'usage': 'internal',
                                                   'location_id': almacen.lot_stock_id.id})
        ruta = self.env['stock.route'].create({'name': 'Surtido Engomado ZZ967 (Ubic. 1 → Producción)'})
        compania = almacen.company_id.id
        produccion_virtual = self.env['stock.location'].search([
            ('usage', '=', 'production'), ('company_id', 'in', [compania, False])], limit=1)
        self.assertTrue(produccion_virtual, "La base de prueba tiene una ubicación virtual de producción")
        regla_surte = self.env['stock.rule'].create({
            'name': 'ZZ surte', 'route_id': ruta.id, 'action': 'pull', 'picking_type_id': almacen.in_type_id.id,
            'company_id': compania,
            'location_src_id': almacen.lot_stock_id.id, 'location_dest_id': ubic2.id})
        regla_consumo = self.env['stock.rule'].create({
            'name': 'ZZ consumo', 'route_id': ruta.id, 'action': 'pull', 'picking_type_id': tipo_kg.id,
            'company_id': compania,
            'location_src_id': ubic2.id, 'location_dest_id': produccion_virtual.id})
        padre = self.env['stock.location'].search([('complete_name', '=', 'Toluca/Stock')], limit=1)
        if not padre:
            # Base de prueba sin «Toluca/Stock»: el padre se da por nombre completo, como en la planta.
            padre = almacen.lot_stock_id
            padre.name = 'Stock'
            padre.location_id.name = 'Toluca'
        piso = configurar_piso(self.env)
        self.assertEqual(piso.name, NOMBRE_PISO)
        self.assertEqual(piso.location_id, padre)
        self.assertEqual(self.env['ir.config_parameter'].sudo().get_param(PARAM_UBICACION_PISO), str(piso.id))
        self.assertEqual(regla_surte.location_dest_id, piso)
        self.assertEqual(regla_consumo.location_src_id, piso)
        self.assertIn('→ Piso)', ruta.name)
        self.assertEqual(configurar_piso(self.env), piso, "Idempotente: no crea otra ubicación")
        engomado = self.env['product.product'].create({
            'name': 'Engomado ZZ967', 'default_code': 'ENGZZ967', 'type': 'consu', 'is_storable': True,
            'uom_id': self.kg.id, 'route_ids': [Command.link(ruta.id)]})
        orden_kg = self.env['mrp.production'].create({
            'product_id': self.prod_kg.id, 'product_qty': 100.0, 'bom_id': self.bom_kg.id,
            'picking_type_id': tipo_kg.id})
        datos = orden_kg._get_move_raw_values(engomado, 1.0, self.kg)
        self.assertEqual(datos['location_id'], piso.id, "El engomado sale del Piso")
        crudo = orden_kg._get_move_raw_values(self.prod_kg, 1.0, self.kg)
        self.assertNotEqual(crudo['location_id'], piso.id, "Lo que no pasa por el Piso sigue en la ubicación de la orden")

    def test_52_sobrante_del_piso_regresa_a_su_origen(self):
        """3.9.0: lo que se surtió al Piso y la orden en kg no consumió regresa a la ubicación de donde salió (transferencia
        interna ligada a la orden); lo consumido no se devuelve. Sin sobrante no se crea nada."""
        almacen = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1)
        tipo_kg = almacen.manu_type_id.copy({'name': 'Fabricación Tela en kg ZZ968', 'sequence_code': 'OP-KG'})
        piso = self.env['stock.location'].create({
            'name': 'Piso ZZ968', 'usage': 'internal', 'location_id': almacen.lot_stock_id.id})
        origen = self.env['stock.location'].create({
            'name': 'Origen ZZ968', 'usage': 'internal', 'location_id': almacen.lot_stock_id.id})
        produccion = self.env['stock.location'].search([
            ('usage', '=', 'production'), ('company_id', 'in', [almacen.company_id.id, False])], limit=1)
        quimico = self.env['product.product'].create({
            'name': 'Químico ZZ968', 'default_code': 'QZZ968', 'type': 'consu', 'is_storable': True,
            'uom_id': self.kg.id})
        self.env['stock.quant']._update_available_quantity(quimico, origen, 10.0)
        surtido = self.env['stock.move'].create({
            'product_id': quimico.id, 'product_uom': self.kg.id, 'product_uom_qty': 10.0,
            'location_id': origen.id, 'location_dest_id': piso.id})
        surtido._action_confirm()
        surtido._action_assign()
        surtido.picked = True
        surtido._action_done()
        self.assertEqual(surtido.state, 'done')
        orden = self.env['mrp.production'].create({
            'product_id': self.prod_kg.id, 'product_qty': 100.0, 'bom_id': self.bom_kg.id,
            'picking_type_id': tipo_kg.id})
        consumo = self.env['stock.move'].create({
            'product_id': quimico.id, 'product_uom': self.kg.id, 'product_uom_qty': 10.0,
            'location_id': piso.id, 'location_dest_id': produccion.id,
            'raw_material_production_id': orden.id, 'move_orig_ids': [Command.link(surtido.id)]})
        consumo._action_confirm()
        consumo._action_assign()
        consumo.write({'quantity': 6.0, 'picked': True})
        consumo._action_done()
        self.assertEqual(consumo.state, 'done')
        transferencias = orden._devolver_sobrante_piso_orden(piso)
        self.assertEqual(len(transferencias), 1)
        self.assertEqual(transferencias.state, 'done')
        self.assertEqual(transferencias.location_dest_id, origen)
        Quant = self.env['stock.quant']
        self.assertAlmostEqual(Quant._get_available_quantity(quimico, origen, strict=True), 4.0)
        self.assertAlmostEqual(Quant._get_available_quantity(quimico, piso, strict=True), 0.0)
        self.assertFalse(orden._devolver_sobrante_piso_orden(piso), "Sin sobrante no se crea otra transferencia")

    def test_53_valores_de_registro_con_clasificacion_en_empaque(self):
        """3.10.0: mientras Empaque no clasifica, el rollo final se registra con lo de Inspección; con la clasificación
        de Empaque manda lo de Empaque (el peso solo si Empaque pesó) y un cambio de calidad deja el rollo reetiquetado."""
        almacen = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1)
        tipo_kg = almacen.manu_type_id.copy({'name': 'Fabricación Tela en kg ZZ969', 'sequence_code': 'OP-KG'})
        orden = self.env['mrp.production'].create({
            'product_id': self.prod_kg.id, 'product_qty': 100.0, 'bom_id': self.bom_kg.id,
            'picking_type_id': tipo_kg.id})
        rollo = self.env['mrp.rollo.final'].create({
            'name': 'ZZ969-0001', 'secuencia': 1, 'production_id': orden.id, 'clasificacion': 'PQ',
            'kg_bruto': 21.0, 'tara': 1.0, 'kg_neto': 20.0, 'metros': 100.0})
        self.assertEqual(rollo.estado, 'inspeccionado')
        self.assertEqual((rollo.clasificacion_registro, rollo.kg_registro, rollo.metros_registro), ('PQ', 20.0, 100.0))
        self.assertFalse(rollo.reetiquetado)
        # Empaque confirma la calidad y pesa: manda su peso y sus metros; no hay reetiquetado.
        rollo.write({'clasificacion_final': 'PQ', 'kg_neto_final': 19.5, 'metros_final': 98.0, 'estado': 'clasificado'})
        self.assertEqual((rollo.clasificacion_registro, rollo.kg_registro, rollo.metros_registro), ('PQ', 19.5, 98.0))
        self.assertFalse(rollo.reetiquetado)
        # Calidad cambia a FE y Empaque no pesó (kg en cero): se queda el peso de Inspección.
        rollo.write({'clasificacion_final': 'FE', 'kg_neto_final': 0.0, 'metros_final': 97.0})
        self.assertEqual((rollo.clasificacion_registro, rollo.kg_registro, rollo.metros_registro), ('FE', 20.0, 97.0))
        self.assertTrue(rollo.reetiquetado, "Empaque cambió la calidad: hay que reimprimir la etiqueta final")

    # ------------------------------------------------------------------ 3.11.0: Clasificación final en Empaque
    def _clasificar_en_empaque(self, orden_m, rollo, calidad=None, metros=None, motivo=None, pesar=False,
                               kg_bruto=0.0, defectos=True):
        """Estación de Empaque (3.12.0): el rollo se escanea por su lote y se confirma o se cambia."""
        wiz = self.env['rollo.empaque.clasificacion.wizard'].create({
            'production_id': orden_m.id, 'empleado_numero': '1', 'codigo': rollo.name,
            'weighing_mode': 'manual', 'motivo_manual': 'Prueba sin báscula', 'pesar': pesar, 'kg_bruto': kg_bruto})
        cambios = {}
        if calidad:
            cambios['clasificacion'] = calidad
            if calidad in ('SEGUNDA', 'FE') and defectos:
                cambios['defecto_ids'] = [Command.set(self.defecto.ids)]
        if metros is not None:
            cambios['metros'] = metros
        if motivo:
            cambios['motivo_ajuste'] = motivo
        if cambios:
            wiz.write(cambios)
        return wiz.action_confirmar()

    def _empaque_confirma_todos(self, orden_m):
        """Empaque confirma tal cual lo que Inspección clasificó, rollo por rollo."""
        for rollo in orden_m.rollo_final_m_ids.filtered(lambda r: r.estado == 'inspeccionado'):
            self._clasificar_en_empaque(orden_m, rollo)

    def test_54_empaque_activo_el_cierre_de_kg_no_registra_la_orden_en_metros(self):
        """A3.2: con el parámetro en 1 el cierre de la orden en kg pasa los rollos a la orden en metros sin registrarlos;
        la orden en metros los registra al cerrar, con la calidad, el peso y los metros de registro."""
        orden_m, orden_kg, wo, rollos = self._bano((50.0,))
        self._inspeccionar(orden_kg, wo, rollos, 50.6, 585.0)
        final = orden_kg.rollo_final_kg_ids
        orden_kg.button_mark_done()
        self.assertEqual(orden_kg.state, 'done')
        self.assertNotEqual(orden_m.state, 'done', "La orden en metros queda abierta para Empaque")
        self.assertEqual(final.estado, 'inspeccionado')
        self.assertEqual(final.production_m_id, orden_m)
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ'], final.name), 0.0)
        with self.assertRaisesRegex(UserError, 'por clasificar'):
            orden_m.button_mark_done()
        self._clasificar_en_empaque(orden_m, final)
        orden_m.button_mark_done()
        self.assertEqual(orden_m.state, 'done')
        self.assertEqual(final.estado, 'registrado')
        self.assertAlmostEqual(orden_m.qty_produced, 585.0)
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ'], final.name), 585.0)
        consumo = orden_m.move_raw_ids.filtered(lambda m: m.product_id == self.prod_kg)
        self.assertAlmostEqual(consumo.quantity, 49.1, msg="3.18.0: consume el rollo final con los kg de Inspección")

    def test_55_empaque_cambia_calidad_y_metros_antes_de_cerrar_la_orden_en_metros(self):
        """A3.2: Empaque reclasifica un rollo de primera a FE con otros metros; la orden en metros lo registra en 15."""
        orden_m, orden_kg, wo, rollos = self._bano((50.0,))
        self._inspeccionar(orden_kg, wo, rollos, 50.6, 585.0)
        final = orden_kg.rollo_final_kg_ids
        orden_kg.button_mark_done()
        final.write({'clasificacion_final': 'FE', 'metros_final': 580.0, 'estado': 'clasificado'})
        orden_m.button_mark_done()
        self.assertEqual((orden_m.state, final.estado), ('done', 'registrado'))
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['FE'], final.name), 580.0)
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ']), 0.0)

    def test_56_empaque_rescata_un_bano_que_inspeccion_dejo_en_saldos(self):
        """A3.2: Inspección lo deja todo en Segunda, pero Empaque lo confirma como primera: la orden en metros no se
        cancela, se registra la tela."""
        orden_m, orden_kg, wo, rollos = self._bano((20.0,))
        self._inspeccionar(orden_kg, wo, rollos, 18.5, 200.0, 'SEGUNDA')
        final = orden_kg.rollo_final_kg_ids
        orden_kg.button_mark_done()
        self.assertEqual(orden_kg.state, 'done')
        self.assertNotEqual(orden_m.state, 'cancel', "Con Empaque activo la orden en metros espera la clasificación")
        final.write({'clasificacion_final': 'PQ', 'metros_final': 200.0, 'estado': 'clasificado'})
        orden_m.button_mark_done()
        self.assertEqual(orden_m.state, 'done')
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ'], final.name), 200.0)

    def test_57_empaque_activo_sin_primera_ni_fe_saca_los_saldos_al_cerrar_la_orden_en_metros(self):
        """A3.2: si Empaque confirma que todo es desperdicio, al cerrar la orden en metros se cancela y sale el desperdicio."""
        orden_m, orden_kg, wo, rollos = self._bano((20.0,))
        self._inspeccionar(orden_kg, wo, rollos, 18.5, 200.0, 'DESP', termina=False)
        self._inspeccionar(orden_kg, wo, rollos, 3.0, 20.0, 'DESP')
        orden_kg.button_mark_done()
        self.assertNotEqual(orden_m.state, 'cancel')
        self._empaque_confirma_todos(orden_m)
        orden_m.button_mark_done()
        self.assertEqual(orden_m.state, 'cancel')
        finales = orden_kg.rollo_final_kg_ids
        self.assertEqual(set(finales.mapped('estado')), {'registrado'})
        self.assertAlmostEqual(self._cantidad(self.saldo_d, self.ubic['DESP']), 18.5)

    def test_58_empaque_peso_de_empaque_manda_en_segunda(self):
        """A3.2 (3.15.0): la Segunda sale en metros; el peso de Empaque, si pesó, queda como referencia (kg_registro)."""
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        self._inspeccionar(orden_kg, wo, rollos, 26.5, 250.0, 'PQ', termina=False)
        self._inspeccionar(orden_kg, wo, rollos, 13.4, 120.0, 'SEGUNDA')
        orden_kg.button_mark_done()
        segunda = orden_kg.rollo_final_kg_ids.filtered(lambda r: r.clasificacion == 'SEGUNDA')
        segunda.write({'clasificacion_final': 'SEGUNDA', 'kg_neto_final': 11.5, 'metros_final': 120.0,
                       'estado': 'clasificado'})
        self._empaque_confirma_todos(orden_m)
        orden_m.button_mark_done()
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['SEGUNDA'], segunda.name), 120.0)
        self.assertAlmostEqual(segunda.kg_registro, 11.5)

    def test_59_orden_en_metros_no_cierra_con_el_bano_abierto(self):
        """A3.2: con Empaque activo, la orden en metros no registra mientras la orden en kg sigue abierta."""
        orden_m, orden_kg, wo, rollos = self._bano((50.0,))
        self._inspeccionar(orden_kg, wo, rollos, 50.6, 585.0)
        # Un rollo ya ligado a la orden en metros sin que el baño haya cerrado (caso de datos mal capturados).
        orden_kg.rollo_final_kg_ids.write({'production_m_id': orden_m.id})
        with self.assertRaises(UserError):
            orden_m.button_mark_done()

    # ------------------------------------------------------------------ 3.12.0: estación de Empaque (A3.3)
    def _bano_en_empaque(self, kg=50.6, metros=585.0, calidad='PQ'):
        orden_m, orden_kg, wo, rollos = self._bano((50.0,))
        self._inspeccionar(orden_kg, wo, rollos, kg, metros, calidad)
        orden_kg.button_mark_done()
        return orden_m, orden_kg, orden_kg.rollo_final_kg_ids

    def test_60_empaque_confirma_la_calidad_de_inspeccion(self):
        """A3.3: Empaque escanea el rollo y lo confirma tal cual: queda clasificado, sin reetiquetar, con la etiqueta final."""
        orden_m, orden_kg, final = self._bano_en_empaque()
        self.assertTrue(orden_m.clasificacion_empaque_pendiente)
        accion = self._clasificar_en_empaque(orden_m, final)
        self.assertEqual(final.estado, 'clasificado')
        self.assertEqual(final.clasificacion_final, 'PQ')
        self.assertAlmostEqual(final.metros_final, 585.0)
        self.assertFalse(final.reetiquetado)
        self.assertFalse(final.motivo_ajuste_final)
        self.assertEqual(final.operador_final.split(' - ')[0], '1')
        self.assertIn('585.00-', final.zpl_etiqueta)
        self.assertTrue(accion, "Sale la etiqueta final")

    def test_61_cambio_de_calidad_pide_motivo_y_reetiqueta(self):
        """A3.3: cambiar la calidad o los metros pide motivo; queda reetiquetado y la etiqueta lleva lo definitivo."""
        orden_m, orden_kg, final = self._bano_en_empaque()
        with self.assertRaisesRegex(UserError, 'motivo'):
            self._clasificar_en_empaque(orden_m, final, calidad='FE', metros=580.0)
        self.assertEqual(final.estado, 'inspeccionado')
        self._clasificar_en_empaque(orden_m, final, calidad='FE', metros=580.0, motivo='Indicación de Calidad')
        self.assertEqual(final.clasificacion_registro, 'FE')
        self.assertAlmostEqual(final.metros_registro, 580.0)
        self.assertTrue(final.reetiquetado)
        self.assertEqual(final.motivo_ajuste_final, 'Indicación de Calidad')
        self.assertEqual(final.defecto_final_ids, self.defecto)
        self.assertIn('580.00-', final.zpl_etiqueta)
        self.assertIn('KILOS', final.zpl_etiqueta, "FE sale como PNC (kilos y libras) con el código de sus defectos")
        self.assertEqual(final.zpl_etiqueta.count('^BCN'), 2)
        self.assertIn('Indicación de Calidad', ' '.join(orden_m.message_ids.mapped('body')))
        orden_m.button_mark_done()
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['FE'], final.name), 580.0)

    def test_62_peso_de_empaque_opcional_con_la_tara_compartida(self):
        """A3.3: si Empaque pesa, el peso (con la misma tara que Inspección) manda; si no pesa, manda el de Inspección."""
        orden_m, orden_kg, final = self._bano_en_empaque(kg=50.6, metros=585.0)
        neto_insp = final.kg_neto
        self._clasificar_en_empaque(orden_m, final, pesar=True, kg_bruto=50.0)
        self.assertAlmostEqual(final.tara_final, 1.5, msg="Misma tara que Inspección («Clasificación final»)")
        self.assertAlmostEqual(final.kg_neto_final, 48.5)
        self.assertAlmostEqual(final.kg_registro, 48.5)
        self.assertFalse(final.reetiquetado, "Cambiar solo el peso no cambia la calidad")
        otro_m, otro_kg, otro = self._bano_en_empaque(kg=30.6, metros=300.0)
        self._clasificar_en_empaque(otro_m, otro)
        self.assertAlmostEqual(otro.kg_registro, otro.kg_neto, msg="Sin pesar manda el peso de Inspección")
        self.assertNotAlmostEqual(final.kg_registro, neto_insp)

    def test_63_validaciones_de_la_estacion(self):
        """A3.3: rollo que no es de la orden, Segunda o FE sin defecto, rollo ya registrado."""
        orden_m, orden_kg, final = self._bano_en_empaque()
        Wizard = self.env['rollo.empaque.clasificacion.wizard']
        base = {'production_id': orden_m.id, 'empleado_numero': '1', 'weighing_mode': 'manual',
                'motivo_manual': 'Prueba sin báscula'}
        ajeno = Wizard.create(dict(base, codigo='NO-EXISTE-0001'))
        with self.assertRaisesRegex(UserError, 'no está en'):
            ajeno.action_confirmar()
        with self.assertRaisesRegex(UserError, 'defecto'):
            self._clasificar_en_empaque(orden_m, final, calidad='SEGUNDA', metros=585.0, motivo='Calidad',
                                        defectos=False)
        self._clasificar_en_empaque(orden_m, final)
        orden_m.button_mark_done()
        registrado = Wizard.create(dict(base, codigo=final.name))
        with self.assertRaises(UserError):
            registrado.action_confirmar()

    def test_64_boton_de_la_orden_con_rollos_por_clasificar(self):
        """A3.3: el botón abre la estación solo con rollos por clasificar (sin interruptor desde 3.16.0)."""
        orden_m, orden_kg, final = self._bano_en_empaque()
        accion = orden_m.action_clasificar_empaque()
        self.assertEqual(accion['res_model'], 'rollo.empaque.clasificacion.wizard')
        self.assertEqual(accion['context']['default_production_id'], orden_m.id)
        orden_m.action_cancel()
        self.assertFalse(orden_m.clasificacion_empaque_pendiente)
        with self.assertRaises(UserError):
            orden_m.action_clasificar_empaque()

    # ------------------------------------------------------------------ 3.13.0: desperdicio de Empaque (A3.3b)
    def _pesar_desperdicio_empaque(self, orden_m, kg, **extra):
        valores = {'production_id': orden_m.id, 'empleado_numero': '1', 'kg_desperdicio': kg,
                   'weighing_mode': 'manual', 'motivo_manual': 'Prueba sin báscula'}
        valores.update(extra)
        return self.env['rollo.empaque.desperdicio.wizard'].create(valores).action_confirmar()

    def test_65_desperdicio_de_empaque_se_suma_al_de_inspeccion(self):
        """A3.3b: el peso de Empaque se suma al saldo Desperdicio de los rollos que Inspección clasificó como tal."""
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        self._inspeccionar(orden_kg, wo, rollos, 30.6, 320.0, 'PQ', termina=False)
        self._inspeccionar(orden_kg, wo, rollos, 9.0, 40.0, 'DESP')
        orden_kg.button_mark_done()
        self._pesar_desperdicio_empaque(orden_m, 2.5)
        self.assertAlmostEqual(orden_m.desperdicio_empaque_kg, 2.5)
        self._empaque_confirma_todos(orden_m)
        orden_m.button_mark_done()
        self.assertEqual(orden_m.state, 'done')
        desp_rollo = orden_kg.rollo_final_kg_ids.filtered(lambda r: r.clasificacion == 'DESP')
        self.assertAlmostEqual(self._cantidad(self.saldo_d, self.ubic['DESP'], desp_rollo.name), 7.5)
        self.assertAlmostEqual(self._cantidad(self.saldo_d, self.ubic['DESP']), 7.5 + 2.5,
                               msg="Rollo de Desperdicio más lo pesado en Empaque, en el mismo producto y ubicación")

    def test_66_desperdicio_de_empaque_sin_rollos_de_desperdicio(self):
        """A3.3b: aunque Inspección no clasifique Desperdicio, lo que Empaque pesa sale como Saldo Desperdicio."""
        orden_m, orden_kg, final = self._bano_en_empaque()
        self._pesar_desperdicio_empaque(orden_m, 3.0)
        self._clasificar_en_empaque(orden_m, final)
        orden_m.button_mark_done()
        self.assertEqual(orden_m.state, 'done')
        self.assertAlmostEqual(self._cantidad(self.saldo_d, self.ubic['DESP']), 3.0)
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ'], final.name), 585.0)

    def test_67_desperdicio_de_empaque_con_un_bano_de_puros_saldos(self):
        """A3.3b: si todo el baño es desperdicio, la orden en metros se cancela y el desperdicio sale en la orden de saldos."""
        orden_m, orden_kg, wo, rollos = self._bano((20.0,))
        self._inspeccionar(orden_kg, wo, rollos, 18.5, 200.0, 'DESP')
        orden_kg.button_mark_done()
        self._pesar_desperdicio_empaque(orden_m, 2.0)
        self._empaque_confirma_todos(orden_m)
        orden_m.button_mark_done()
        self.assertEqual(orden_m.state, 'cancel')
        self.assertAlmostEqual(self._cantidad(self.saldo_d, self.ubic['DESP']), 19.0)

    def test_68_validaciones_y_repesar_del_desperdicio_de_empaque(self):
        """A3.3b: sin peso no registra, un nuevo peso sustituye al anterior, solo con el parámetro y la orden abierta."""
        orden_m, orden_kg, final = self._bano_en_empaque()
        with self.assertRaisesRegex(UserError, 'peso'):
            self._pesar_desperdicio_empaque(orden_m, 0.0)
        self._pesar_desperdicio_empaque(orden_m, 2.0)
        self._pesar_desperdicio_empaque(orden_m, 2.4)
        self.assertAlmostEqual(orden_m.desperdicio_empaque_kg, 2.4)
        self.assertIn('antes 2.000', ' '.join(orden_m.message_ids.mapped('body')))
        self.assertTrue(orden_m.action_pesar_desperdicio_empaque())

    # ------------------------------------------------------------------ 3.14.0: operación CLASIFICACION (A3.4b)
    def _con_operacion_clasificacion(self):
        """La lista en metros con la operación CLASIFICACION en EMPAQUE (fichas 2.28.0)."""
        empaque = self.env['mrp.workcenter'].create({'name': 'EMPAQUE ZZ951', 'code': 'EZZ951'})
        self.env['mrp.routing.workcenter'].create({
            'bom_id': self.bom_m.id, 'name': 'CLASIFICACION', 'workcenter_id': empaque.id, 'time_cycle_manual': 60.0})
        return empaque

    def test_69_botones_de_empaque_en_la_operacion_de_la_orden_en_metros(self):
        """A3.4b: con la operación CLASIFICACION en la orden en metros, CLASIFICAR y PESAR DESPERDICIO abren las
        estaciones de Empaque desde la operación, solo mientras hay rollos por clasificar."""
        self._con_operacion_clasificacion()
        orden_m, orden_kg, wo, rollos = self._bano((50.0,))
        wo_c = orden_m.workorder_ids
        self.assertEqual(len(wo_c), 1)
        self.assertTrue(wo_c.es_clasificacion)
        self.assertFalse(wo_c.empaque_pendiente, "Antes de que el baño cierre no hay nada que clasificar")
        self._inspeccionar(orden_kg, wo, rollos, 50.6, 585.0)
        orden_kg.button_mark_done()
        self.assertTrue(wo_c.empaque_pendiente)
        accion = wo_c.action_clasificar_rollo()
        self.assertEqual(accion['res_model'], 'rollo.empaque.clasificacion.wizard')
        self.assertEqual(accion['context']['default_workorder_id'], wo_c.id)
        accion = wo_c.action_pesar_desperdicio_empaque()
        self.assertEqual(accion['res_model'], 'rollo.empaque.desperdicio.wizard')
        self.assertEqual(accion['context']['default_workorder_id'], wo_c.id)
        with self.assertRaisesRegex(UserError, 'no es de Clasificación final'):
            wo.action_pesar_desperdicio_empaque()

    def test_70_la_operacion_no_termina_sin_clasificar_y_la_orden_cierra(self):
        """A3.4b: la operación CLASIFICACION no termina con rollos por clasificar; clasificados, la orden en metros
        cierra con su operación y registra la tela."""
        self._con_operacion_clasificacion()
        orden_m, orden_kg, wo, rollos = self._bano((50.0,))
        self._inspeccionar(orden_kg, wo, rollos, 50.6, 585.0)
        orden_kg.button_mark_done()
        wo_c = orden_m.workorder_ids
        final = orden_kg.rollo_final_kg_ids
        with self.assertRaisesRegex(UserError, 'por clasificar'):
            wo_c.button_finish()
        self.assertEqual(wo_c.state not in ('done', 'cancel'), True)
        self._clasificar_en_empaque(orden_m, final)
        self.assertEqual(final.estado, 'clasificado')
        orden_m.button_mark_done()
        self.assertEqual(orden_m.state, 'done')
        self.assertEqual(wo_c.state, 'done')
        self.assertEqual(final.estado, 'registrado')
        self.assertFalse(wo_c.empaque_pendiente)
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ'], final.name), 585.0)

    def test_72_orden_en_metros_cancelada_registra_en_una_nueva(self):
        """3.14.1 (revisión de código, 2026-11-06): si la orden en metros se cancela después de inspeccionar (pedido
        cancelado), el baño cierra y registra la tela en una orden en metros nueva, en metros; antes no cerraba nunca."""
        orden_m, orden_kg, wo, rollos = self._bano((50.0,))
        self._inspeccionar(orden_kg, wo, rollos, 50.6, 585.0)
        antes = self.env['mrp.production'].search([('product_id', '=', self.prod_kg.id)])
        orden_m.action_cancel()
        self.assertEqual(orden_m.state, 'cancel')
        orden_kg.button_mark_done()
        self.assertEqual(orden_kg.state, 'done')
        final = orden_kg.rollo_final_kg_ids
        nueva = final.production_m_id
        self.assertTrue(nueva and nueva != orden_m)
        self.assertEqual(nueva.product_id, self.prod_m)
        self.assertEqual(final.estado, 'inspeccionado', "3.16.0: la orden nueva espera a Empaque")
        self._empaque_confirma_todos(nueva)
        nueva.button_mark_done()
        self.assertEqual(nueva.state, 'done')
        self.assertEqual(final.estado, 'registrado')
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ'], final.name), 585.0)
        self.assertTrue(any('orden en metros nueva' in (m.body or '') for m in orden_kg.message_ids))
        nuevas = self.env['mrp.production'].search([('product_id', '=', self.prod_kg.id)]) - antes
        self.assertFalse(nuevas, "Registrar en la orden nueva no debe crear órdenes en kg: %s" % nuevas.mapped('name'))

    def test_73_dos_crudos_cuentan_los_dos(self):
        """3.14.1 (revisión de código, 2026-11-06): con dos crudos de tejido en el baño, los dos cuentan como crudo (antes
        solo el de más kg, y el otro se trataba como químico)."""
        _m, orden_kg, _wo, _rollos = self._bano((40.0,))
        tejido = self.env['product.category'].create({'name': 'Tejido Circular ZZ973'})
        self.crudo.categ_id = tejido
        crudo_2 = self.env['product.product'].create({
            'name': 'Crudo dos ZZ973', 'type': 'consu', 'is_storable': True, 'uom_id': self.kg.id, 'categ_id': tejido.id})
        self.env['stock.move'].create({
            'raw_material_production_id': orden_kg.id, 'product_id': crudo_2.id, 'product_uom_qty': 30.0,
            'product_uom': self.kg.id, 'location_id': orden_kg.location_src_id.id,
            'location_dest_id': orden_kg.production_location_id.id, 'company_id': orden_kg.company_id.id})
        self.assertEqual(set(orden_kg._moves_crudo().mapped('product_id').ids), {self.crudo.id, crudo_2.id})

    def test_74_no_dividir_la_orden_en_metros_avisa(self):
        """Programación 1.8.2 (revisión 2026-11-08): si la orden en metros no se puede dividir (aquí la abastece otra
        orden en kg), se queda como estaba pero deja nota en los baños y en la orden en metros; antes no avisaba."""
        orden_m, banos = self._programar_dos_banos(otra_orden_kg=True)
        self.assertEqual(banos[0]._insp_orden_metros(), orden_m)
        self.assertEqual(banos[1]._insp_orden_metros(), orden_m, "No se dividió: los dos baños abastecen la misma")
        for orden in orden_m | banos:
            self.assertTrue(any('No se dividió la orden en metros' in (m.body or '') for m in orden.message_ids),
                            "Sin nota en %s" % orden.name)

    # ------------------------------------------------------------------ 3.15.0: Segunda en metros (José, 2026-11-08)
    def test_75_bano_solo_segunda_y_desperdicio_registra_segunda_en_metros(self):
        """3.15.0 (diseño §2, §8 paso 13 y §16): un baño sin primera ni FE pero con Segunda ya no se cancela: la orden en
        metros cierra con la Segunda en metros, con su lote, a 16; el desperdicio sale en kg como subproducto a 17."""
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        self._inspeccionar(orden_kg, wo, rollos, 30.5, 320.0, 'SEGUNDA', termina=False)
        self._inspeccionar(orden_kg, wo, rollos, 9.0, 80.0, 'DESP')
        orden_kg.button_mark_done()
        self.assertEqual(orden_kg.state, 'done')
        self._empaque_confirma_todos(orden_m)
        orden_m.button_mark_done()
        self.assertEqual(orden_m.state, 'done', "La Segunda es tela en metros: la orden en metros no se cancela")
        segunda = orden_kg.rollo_final_kg_ids.filtered(lambda r: r.clasificacion == 'SEGUNDA')
        self.assertEqual(segunda.product_id, self.prod_m)
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['SEGUNDA'], segunda.name), 320.0)
        self.assertFalse(self._cantidad(self.saldo_2, self.ubic['SEGUNDA']))
        self.assertAlmostEqual(self._cantidad(self.saldo_d, self.ubic['DESP']), 7.5)
        self.assertFalse(orden_kg.rollo_final_kg_ids.production_saldo_id, "No hace falta orden de saldos")

    def test_76_empaque_cambia_a_segunda_y_entra_en_metros_a_16(self):
        """3.15.0: Inspección clasifica PQ y Empaque lo cambia a Segunda: al cerrar la orden en metros entra en metros a 16."""
        orden_m, orden_kg, wo, rollos = self._bano((50.0,))
        self._inspeccionar(orden_kg, wo, rollos, 50.6, 585.0)
        orden_kg.button_mark_done()
        final = orden_kg.rollo_final_kg_ids
        final.write({'clasificacion_final': 'SEGUNDA', 'metros_final': 580.0, 'estado': 'clasificado'})
        orden_m.button_mark_done()
        self.assertEqual(orden_m.state, 'done')
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['SEGUNDA'], final.name), 580.0)
        self.assertFalse(self._cantidad(self.prod_m, self.ubic['PQ']))

    def test_76c_etiqueta_y_lote_del_desperdicio_de_rama(self):
        """3.17.0 (Dirección, 2026-11-10): el desperdicio de Rama también imprime etiqueta con código de barras, y el lote de
        los dos desperdicios lleva el número de la orden y el mes («J00002-11-2026»)."""
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        with self.assertRaises(UserError):
            orden_kg._zpl_etiqueta_desperdicio_rama()
        lote = orden_kg._lote_desperdicio(self.saldo_d)
        numero = orden_kg.numero_bano or orden_kg.name.rsplit('/', 1)[-1]
        self.assertRegex(lote.name, r'^%s-\d\d-\d{4}$' % numero)
        self.assertEqual(orden_m._lote_desperdicio(self.saldo_d).name.rsplit('-', 2)[1:],
                         lote.name.rsplit('-', 2)[1:], 'Mismo mes en los dos')
        orden_kg.write({'desperdicio_kg': 1.2, 'desperdicio_lote_id': lote.id,
                        'desperdicio_fecha': fields.Datetime.now()})
        zpl = orden_kg._zpl_etiqueta_desperdicio_rama()
        for texto in ('PRODUCTO: %s' % self.saldo_d.default_code, 'NOMBRE: %s' % self.saldo_d.name, 'LOTE: %s' % lote.name,
                      'CANTIDAD: 1.20 %s' % self.saldo_d.uom_id.name, '^BC'):
            self.assertIn(texto, zpl)
        accion = orden_kg.action_imprimir_etiqueta_desperdicio_rama()
        self.assertTrue(accion.get('type'))
        self.assertIn('report_etiqueta_desperdicio_rama', str(accion))

    def test_76d_anular_cierra_la_ventana_sin_recargar_el_rollo(self):
        """3.17.0: el botón Anular captura cierra la ventana sin recargar el rollo ya borrado (si no, Odoo avisa que no
        encuentra el registro aunque la captura sí se anuló)."""
        vista = self.env['mrp.rollo.final'].get_view(view_type='form')['arch']
        boton = vista[vista.index('name="action_anular"'):]
        self.assertIn('close="1"', boton[:boton.index('/>') if '/>' in boton[:400] else 400])

    def test_76b_etiqueta_del_desperdicio_de_empaque(self):
        """3.15.0 (José 2026-11-08): la etiqueta del desperdicio lleva fecha, producto, nombre, lote, cantidad con su unidad
        y el código de barras del lote; sale al pesar."""
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        self._inspeccionar(orden_kg, wo, rollos, 40.6, 460.0)
        orden_kg.button_mark_done()
        with self.assertRaises(UserError):
            orden_m._zpl_etiqueta_desperdicio_empaque()
        accion = self._pesar_desperdicio_empaque(orden_m, 2.5)
        self.assertTrue(accion.get('type'), "El pesaje devuelve la acción de imprimir la etiqueta")
        zpl = orden_m._zpl_etiqueta_desperdicio_empaque()
        lote = orden_m._lote_desperdicio(self.saldo_d).name
        for texto in ('PRODUCTO: %s' % self.saldo_d.default_code, 'NOMBRE: %s' % self.saldo_d.name, 'LOTE: %s' % lote,
                      'CANTIDAD: 2.50 %s' % self.saldo_d.uom_id.name, '^BC'):
            self.assertIn(texto, zpl)
        self.assertRegex(zpl, r'FECHA: \d\d/\d\d/\d{4} \d\d:\d\d')

    # ------------------------------------------------------------------ 3.18.0: cadena por la Ubic. 10 y traspasos
    def test_77_finales_a_la_10_consumo_y_traspasos_a_calidad(self):
        """3.18.0 (Dirección, 2026-11-10): al cerrar la orden en kg los rollos finales entran a la Ubic. 10 con su lote
        («J…-F001») y los kg de Inspección; la orden en metros consume esos lotes; al cerrarla, la tela y los saldos entran
        a la Ubic. 10 y transferencias internas los pasan a 14 / 17."""
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        self._inspeccionar(orden_kg, wo, rollos, 30.6, 320.0, 'PQ', termina=False)
        self._inspeccionar(orden_kg, wo, rollos, 9.0, 40.0, 'DESP')
        salida = orden_kg.location_dest_id
        orden_kg.button_mark_done()
        pq, desp = orden_kg.rollo_final_kg_ids.sorted('secuencia')
        self.assertEqual((pq.name, desp.name), ('%s-F001' % orden_kg.numero_bano, '%s-F002' % orden_kg.numero_bano))
        self.assertEqual(pq.lot_kg_id.name, pq.name)
        self.assertEqual(pq.lot_kg_id.product_id, self.prod_kg)
        # Los rollos finales (no los de Rama) están en la Ubic. 10 con los kg de Inspección.
        self.assertAlmostEqual(self._cantidad(self.prod_kg, salida, pq.name), pq.kg_neto)
        self.assertAlmostEqual(self._cantidad(self.prod_kg, salida, desp.name), desp.kg_neto)
        self.assertFalse(self._cantidad(self.prod_kg, salida, rollos.lot_id.name), "El lote de Rama ya no entra a la 10")
        self._empaque_confirma_todos(orden_m)
        self._pesar_desperdicio_empaque(orden_m, 2.5)
        orden_m.button_mark_done()
        self.assertEqual(orden_m.state, 'done')
        consumo = orden_m.move_raw_ids.filtered(lambda m: m.product_id == self.prod_kg).move_line_ids
        self.assertEqual(sorted(consumo.mapped('lot_id.name')), sorted([pq.name, desp.name]))
        self.assertAlmostEqual(sum(consumo.mapped('quantity')), pq.kg_neto + desp.kg_neto)
        # La tela y el desperdicio quedaron en su calidad y la 10 sin saldo de lo producido.
        self.assertAlmostEqual(self._cantidad(self.prod_m, self.ubic['PQ'], pq.name), 320.0)
        self.assertFalse(self._cantidad(self.prod_m, salida))
        self.assertAlmostEqual(self._cantidad(self.saldo_d, self.ubic['DESP']), desp.kg_neto + 2.5)
        self.assertFalse(self._cantidad(self.saldo_d, salida))
        traspasos = self.env['stock.picking'].search([('origin', 'ilike', orden_m.name), ('state', '=', 'done')])
        self.assertEqual(sorted(traspasos.mapped('location_dest_id').ids), sorted([self.ubic['PQ'].id, self.ubic['DESP'].id]))
        self.assertEqual(set(traspasos.mapped('location_id').ids), {salida.id})
        self.assertEqual(set(traspasos.picking_type_id.mapped('code')), {'internal'})

    def test_77b_saldos_pasan_por_la_10_y_a_su_ubicacion(self):
        """3.18.0: el baño sin primera ni FE (orden de saldos) también entra a la Ubic. 10 y pasa al 17 con transferencia."""
        orden_m, orden_kg, wo, rollos = self._bano((20.0,))
        self._inspeccionar(orden_kg, wo, rollos, 20.5, 100.0, 'DESP')
        salida = orden_kg.location_dest_id
        self._cerrar_bano(orden_kg)
        desp = orden_kg.rollo_final_kg_ids
        self.assertEqual(desp.estado, 'registrado')
        self.assertAlmostEqual(self._cantidad(self.saldo_d, self.ubic['DESP']), desp.kg_neto)
        self.assertFalse(self._cantidad(self.saldo_d, salida))
        orden_s = desp.production_saldo_id
        consumo = orden_s.move_raw_ids.move_line_ids
        self.assertEqual(consumo.mapped('lot_id.name'), [desp.name])
        self.assertTrue(self.env['stock.picking'].search_count([
            ('origin', 'ilike', orden_s.name), ('state', '=', 'done'), ('location_dest_id', '=', self.ubic['DESP'].id)]))

    def test_78_desperdicio_de_rama_entra_a_la_10_y_pasa_a_la_17(self):
        """3.18.0: el subproducto de desperdicio de Rama conserva el destino de la orden (Ubic. 10) y al cerrar la orden en
        kg una transferencia lo pasa a la 17."""
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        wo_rama = orden_kg.workorder_ids.filtered(lambda w: w.name == 'RAMA')
        salida = orden_kg.location_dest_id
        # Como en la planta: la lista trae el subproducto de desperdicio con cantidad 0 y destino el de la orden.
        self.env['stock.move'].create({
            'product_id': self.saldo_d.id, 'product_uom': self.saldo_d.uom_id.id, 'product_uom_qty': 0.0,
            'location_id': orden_kg.move_finished_ids[:1].location_id.id, 'location_dest_id': salida.id,
            'production_id': orden_kg.id, 'company_id': orden_kg.company_id.id, 'origin': orden_kg.name,
            'cost_share': 0.0})._action_confirm()
        orden_kg._rama_registrar_desperdicio(wo_rama, 1.2, 'Operador', False)
        movimiento = orden_kg.move_finished_ids.filtered(lambda m: m.product_id != self.prod_kg and m.quantity)
        self.assertEqual(movimiento.location_dest_id, salida, "Entra a la 10, no se fuerza la 17")
        producto = movimiento.product_id
        self._inspeccionar(orden_kg, wo, rollos, 40.6, 460.0)
        self._cerrar_bano(orden_kg)
        self.assertAlmostEqual(self._cantidad(producto, self.ubic['DESP']), 1.2)
        self.assertFalse(self._cantidad(producto, salida))
        self.assertTrue(self.env['stock.picking'].search_count([
            ('origin', 'ilike', orden_kg.name), ('state', '=', 'done'), ('location_dest_id', '=', self.ubic['DESP'].id)]))

    def test_79_requisicion_de_quimicos_del_piso_a_la_2(self):
        """3.18.0: la requisición de químicos y colorantes (REQ-TIN) toma del Piso y surte a la Ubic. 2 (el destino del tipo);
        la orden en kg los consume de la 2, y solo el engomado sigue saliendo del Piso. Idempotente."""
        from odoo.addons.quimibond_rollos_bitacora.hooks import NOMBRE_RUTA_QUIMICOS, configurar_piso
        almacen = self.env['stock.warehouse'].search([('company_id', '=', self.env.company.id)], limit=1)
        compania = almacen.company_id.id
        padre = self.env['stock.location'].search([('complete_name', '=', 'Toluca/Stock')], limit=1)
        if not padre:
            padre = almacen.lot_stock_id
            padre.name = 'Stock'
            padre.location_id.name = 'Toluca'
        ubic2 = self.env['stock.location'].create({'name': 'ZZ979 PRODUCCIÓN', 'usage': 'internal', 'location_id': padre.id})
        tipo_req = almacen.int_type_id.copy({
            'name': 'Requisición ZZ979', 'sequence_code': 'ZZREQ', 'default_location_dest_id': ubic2.id})
        tipo_kg = almacen.manu_type_id.copy({'name': 'Fabricación Tela en kg ZZ979', 'sequence_code': 'OP-KG'})
        produccion_virtual = self.env['stock.location'].search([
            ('usage', '=', 'production'), ('company_id', 'in', [compania, False])], limit=1)
        ruta = self.env['stock.route'].create({'name': 'Surtido Químicos y Colorantes ZZ979 (Ubic. 1 → Piso)'})
        regla_surte = self.env['stock.rule'].create({
            'name': 'ZZ surte', 'route_id': ruta.id, 'action': 'pull', 'picking_type_id': tipo_req.id,
            'company_id': compania, 'location_src_id': almacen.lot_stock_id.id, 'location_dest_id': padre.id,
            'location_dest_from_rule': True})
        regla_consumo = self.env['stock.rule'].create({
            'name': 'ZZ consumo', 'route_id': ruta.id, 'action': 'pull', 'picking_type_id': tipo_kg.id,
            'company_id': compania, 'location_src_id': padre.id, 'location_dest_id': produccion_virtual.id})
        piso = configurar_piso(self.env)
        self.assertEqual(regla_surte.location_src_id, piso, "La requisición sale del Piso")
        self.assertEqual(regla_surte.location_dest_id, ubic2)
        self.assertFalse(regla_surte.location_dest_from_rule, "El destino del movimiento es el del tipo (Ubic. 2)")
        self.assertEqual(regla_consumo.location_src_id, ubic2, "La orden en kg consume de la Ubic. 2")
        self.assertEqual(ruta.name, NOMBRE_RUTA_QUIMICOS)
        configurar_piso(self.env)  # idempotente
        self.assertEqual((regla_surte.location_src_id, regla_consumo.location_src_id), (piso, ubic2))
        quimico = self.env['product.product'].create({
            'name': 'Químico ZZ979', 'default_code': 'QZZ979', 'type': 'consu', 'is_storable': True,
            'uom_id': self.kg.id, 'route_ids': [Command.link(ruta.id)]})
        orden_kg = self.env['mrp.production'].create({
            'product_id': self.prod_kg.id, 'product_qty': 100.0, 'bom_id': self.bom_kg.id, 'picking_type_id': tipo_kg.id})
        datos = orden_kg._get_move_raw_values(quimico, 1.0, self.kg)
        self.assertNotEqual(datos['location_id'], piso.id, "El químico ya no se consume del Piso")

    def test_77c_banos_anteriores_a_3_18_0_siguen_con_los_rollos_de_rama(self):
        """3.18.0: un baño que cerró antes (sus rollos finales no tienen lote en kg) sigue consumiendo los rollos de Rama con
        sus kg de Rama; con lote en kg, los lotes de los rollos finales con los kg de Inspección."""
        orden_m, orden_kg, wo, rollos = self._bano((40.0,))
        self._inspeccionar(orden_kg, wo, rollos, 40.6, 460.0)
        final = orden_kg.rollo_final_kg_ids
        Produccion = self.env['mrp.production']
        self.assertEqual(Produccion._insp_consumo_lotes(final), [(rollos.lot_id, 40.0)])
        lote = Produccion._insp_lote(self.prod_kg, final.name, self.env.company)
        final.lot_kg_id = lote
        self.assertEqual(Produccion._insp_consumo_lotes(final), [(lote, final.kg_neto)])
