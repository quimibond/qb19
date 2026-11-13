# -*- coding: utf-8 -*-
from odoo import Command
from odoo.exceptions import UserError
from odoo.tests import tagged
from odoo.tests.common import TransactionCase


@tagged('post_install', '-at_install', 'quimibond_rollos_bitacora')
class TestLiberacionLab(TransactionCase):
    """3.20.0: laboratorio libera la orden en metros para Empaque con su número de empleado."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        kg = cls.env.ref('uom.product_uom_kgm')
        metro = cls.env.ref('uom.product_uom_meter')
        cls.prod_a = cls._producto('IWJ038Q22JNT160', kg)
        cls.prod_b = cls._producto('WJ038Q22JNT160', metro)
        bom_a = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.prod_a.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': kg.id})
        cls.orden_a = cls.env['mrp.production'].create({
            'product_id': cls.prod_a.id, 'product_qty': 100, 'bom_id': bom_a.id})
        bom_b = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.prod_b.product_tmpl_id.id, 'product_qty': 1, 'product_uom_id': metro.id,
            'bom_line_ids': [Command.create({'product_id': cls.prod_a.id, 'product_qty': 0.0635})]})
        cls.orden_b = cls.env['mrp.production'].create({
            'product_id': cls.prod_b.id, 'product_qty': 100, 'bom_id': bom_b.id})
        centro = cls.env['mrp.workcenter'].create({'name': 'EMPAQUE LIB', 'code': 'EMPLIB'})
        cls.workorder = cls.env['mrp.workorder'].create({
            'name': 'CLASIFICACION', 'production_id': cls.orden_b.id, 'workcenter_id': centro.id})
        # «Referencia de empleado» es de nómina Enterprise: en Community no existe y el operador queda solo con su número.
        Emp = cls.env['hr.employee']
        cls.con_referencia = 'registration_number' in Emp._fields
        if cls.con_referencia:
            Emp.create({'name': 'LABORATORISTA PRUEBA', 'registration_number': 'S-77'})

    @classmethod
    def _producto(cls, codigo, uom):
        return cls.env['product.product'].create({
            'name': codigo, 'default_code': codigo, 'type': 'consu', 'is_storable': True,
            'uom_id': uom.id, 'tracking': 'lot'})

    def _final(self, secuencia, clasificacion):
        return self.env['mrp.rollo.final'].create({
            'name': 'LIBTEST-%04d' % secuencia, 'secuencia': secuencia, 'production_id': self.orden_a.id,
            'production_m_id': self.orden_b.id, 'clasificacion': clasificacion, 'metros': 500.0,
            'kg_bruto': 34.0, 'kg_neto': 33.4, 'siglas_operador': 'RI'})

    def _liberar(self, numero='77'):
        wiz = self.env['rollo.liberacion.lab.wizard'].with_context(
            default_production_id=self.orden_b.id, default_workorder_id=self.workorder.id).create(
            {'production_id': self.orden_b.id, 'workorder_id': self.workorder.id, 'empleado_numero': numero})
        return wiz.action_confirmar()

    def test_01_con_rollos_pq_la_liberacion_esta_pendiente(self):
        self._final(1, 'PQ')
        self.assertTrue(self.orden_b.liberacion_lab_pendiente)
        self.assertTrue(self.workorder.liberacion_pendiente)

    def test_02_sin_rollos_pq_no_hay_liberacion(self):
        """Una orden sin rollos PQ por revisar: Empaque clasifica sin liberación."""
        self._final(1, 'FE')
        self._final(2, 'SEGUNDA')
        self.assertFalse(self.orden_b.liberacion_lab_pendiente)
        accion = self.orden_b.action_clasificar_empaque()
        self.assertEqual(accion['res_model'], 'rollo.empaque.clasificacion.wizard')
        with self.assertRaises(UserError):
            self.orden_b.action_liberar_empaque()

    def test_03_clasificar_se_bloquea_hasta_liberar(self):
        self._final(1, 'PQ')
        with self.assertRaisesRegex(UserError, 'laboratorio'):
            self.orden_b.action_clasificar_empaque()
        with self.assertRaisesRegex(UserError, 'laboratorio'):
            self.workorder.action_clasificar_rollo()

    def test_04_la_orden_no_cierra_sin_liberar(self):
        self._final(1, 'PQ')
        with self.assertRaisesRegex(UserError, 'laboratorio'):
            self.orden_b.button_mark_done()
        with self.assertRaisesRegex(UserError, 'laboratorio'):
            self.workorder._validar_clasificacion_empaque()

    def test_05_liberar_con_numero_de_empleado(self):
        self._final(1, 'PQ')
        self._final(2, 'PQ')
        self._final(3, 'FE')
        accion = self.orden_b.with_context(default_workorder_id=self.workorder.id).action_liberar_empaque()
        self.assertEqual(accion['res_model'], 'rollo.liberacion.lab.wizard')
        wiz = self.env['rollo.liberacion.lab.wizard'].create(
            {'production_id': self.orden_b.id, 'workorder_id': self.workorder.id, 'empleado_numero': '77'})
        self.assertEqual(wiz.empleado_nombre, 'LABORATORISTA PRUEBA' if self.con_referencia
                         else 'EMPLEADO NO ENCONTRADO')
        self.assertEqual(wiz.rollos_pq_count, 2, 'Solo los PQ pasan por laboratorio')
        wiz.action_confirmar()
        self.assertTrue(self.orden_b.liberada_lab)
        self.assertEqual(self.orden_b.liberada_lab_operador,
                         '77 - LABORATORISTA PRUEBA' if self.con_referencia else '77')
        self.assertTrue(self.orden_b.liberada_lab_fecha)
        self.assertFalse(self.orden_b.liberacion_lab_pendiente)
        self.assertIn('liberada para Empaque', self.orden_b.message_ids[0].body)

    def test_06_liberada_clasificar_abre(self):
        self._final(1, 'PQ')
        self._liberar()
        accion = self.orden_b.action_clasificar_empaque()
        self.assertEqual(accion['res_model'], 'rollo.empaque.clasificacion.wizard')
        self.assertFalse(self.workorder.liberacion_pendiente)

    def test_07_no_se_libera_dos_veces(self):
        self._final(1, 'PQ')
        self._liberar()
        with self.assertRaisesRegex(UserError, 'ya fue liberada'):
            self.orden_b.action_liberar_empaque()
        with self.assertRaisesRegex(UserError, 'ya fue liberada'):
            self._liberar()

    def test_08_la_liberacion_no_se_copia(self):
        self._final(1, 'PQ')
        self._liberar()
        copia = self.orden_b.copy()
        self.assertFalse(copia.liberada_lab)

    def test_09_orden_cerrada_no_se_libera(self):
        self._final(1, 'PQ')
        self.orden_b.state = 'cancel'
        with self.assertRaises(UserError):
            self.orden_b.action_liberar_empaque()

    def test_10_rollos_ya_clasificados_en_empaque_no_cuentan(self):
        """Órdenes en curso al desplegar la versión: un PQ que Empaque ya clasificó no queda pendiente de laboratorio."""
        final = self._final(1, 'PQ')
        self.assertTrue(self.orden_b.liberacion_lab_pendiente)
        final.write({'estado': 'clasificado'})
        self.orden_b.invalidate_recordset()
        self.assertFalse(self.orden_b.liberacion_lab_pendiente)
