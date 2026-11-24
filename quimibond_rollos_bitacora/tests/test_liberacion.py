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
        cls.defecto = cls.env['calidad.defecto.tela'].create({'clave': 'ZZ952', 'name': 'Defecto de prueba lab'})
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

    def test_11_el_wizard_ofrece_solo_ordenes_pendientes_y_toma_la_operacion(self):
        wiz = self.env['rollo.liberacion.lab.wizard'].create({'production_id': self.orden_b.id, 'empleado_numero': '1'})
        self.assertNotIn(self.orden_b, wiz.production_domain_ids)  # sin rollos PQ
        self._final(1, 'PQ')
        wiz = self.env['rollo.liberacion.lab.wizard'].create({'production_id': self.orden_b.id, 'empleado_numero': '1'})
        self.assertIn(self.orden_b, wiz.production_domain_ids)
        self.assertNotIn(self.orden_a, wiz.production_domain_ids)
        self.assertEqual(wiz.workorder_id, self.workorder)
        self._liberar()
        wiz = self.env['rollo.liberacion.lab.wizard'].create({'production_id': self.orden_b.id, 'empleado_numero': '1'})
        self.assertNotIn(self.orden_b, wiz.production_domain_ids)  # ya liberada

    # ------------------------------------------------------------------ 3.21.0: calidad final de laboratorio
    def _wizard_lab(self, numero='77'):
        return self.env['rollo.liberacion.lab.wizard'].create(
            {'production_id': self.orden_b.id, 'workorder_id': self.workorder.id, 'empleado_numero': numero})

    def _linea(self, wiz, rollo):
        return wiz.linea_ids.filtered(lambda l: l.rollo_id == rollo)

    def test_12_una_linea_por_rollo_pq_con_pq_por_omision(self):
        r1, r2 = self._final(1, 'PQ'), self._final(2, 'PQ')
        self._final(3, 'FE')
        wiz = self._wizard_lab()
        self.assertEqual(wiz.linea_ids.rollo_id, r1 | r2)
        self.assertEqual(set(wiz.linea_ids.mapped('clasificacion')), {'PQ'})

    def test_13_confirmar_pq_registra_la_calidad_final(self):
        r1 = self._final(1, 'PQ')
        self._wizard_lab().action_confirmar()
        self.assertEqual(r1.clasificacion_lab, 'PQ')
        self.assertFalse(r1.defecto_lab_ids)
        self.assertFalse(r1.motivo_lab)
        self.assertTrue(r1.fecha_lab)
        self.assertTrue(r1.operador_lab.startswith('77'))

    def test_14_cambio_a_fe_o_segunda_pide_defectos_y_causa(self):
        r1, r2 = self._final(1, 'PQ'), self._final(2, 'PQ')
        wiz = self._wizard_lab()
        self._linea(wiz, r1).write({'clasificacion': 'FE'})
        with self.assertRaisesRegex(UserError, 'al menos un defecto'):
            wiz.action_confirmar()
        self._linea(wiz, r1).write({'defecto_ids': [Command.set(self.defecto.ids)]})
        with self.assertRaisesRegex(UserError, 'causa del cambio'):
            wiz.action_confirmar()
        self.assertFalse(self.orden_b.liberada_lab, 'Un error no libera la orden')
        self._linea(wiz, r1).write({'motivo': 'Barras en el ancho'})
        self._linea(wiz, r2).write({'clasificacion': 'SEGUNDA', 'defecto_ids': [Command.set(self.defecto.ids)],
                                    'motivo': 'Manchas'})
        wiz.action_confirmar()
        self.assertEqual((r1.clasificacion_lab, r2.clasificacion_lab), ('FE', 'SEGUNDA'))
        self.assertEqual(r1.defecto_lab_ids, self.defecto)
        self.assertEqual(r1.motivo_lab, 'Barras en el ancho')
        self.assertEqual(r1.clasificacion, 'PQ', 'La calidad de Inspección no se toca')
        self.assertIn('Barras en el ancho', self.orden_b.message_ids[0].body)
        self.assertTrue(self.orden_b.liberada_lab)

    def test_15_si_cambian_los_rollos_pendientes_se_pide_reabrir(self):
        self._final(1, 'PQ')
        wiz = self._wizard_lab()
        self.assertEqual(len(wiz.linea_ids), 1)
        self._final(2, 'PQ')
        with self.assertRaisesRegex(UserError, 'cambiaron'):
            wiz.action_confirmar()

    def test_16_empaque_arranca_con_la_calidad_de_laboratorio(self):
        r_fe, r_pq = self._final(1, 'PQ'), self._final(2, 'PQ')
        wiz = self._wizard_lab()
        self._linea(wiz, r_fe).write({'clasificacion': 'FE', 'defecto_ids': [Command.set(self.defecto.ids)],
                                      'motivo': 'Barras'})
        wiz.action_confirmar()
        Empaque = self.env['rollo.empaque.clasificacion.wizard']
        base = {'production_id': self.orden_b.id, 'empleado_numero': '77'}
        e_fe = Empaque.create(dict(base, codigo=r_fe.name))
        self.assertEqual(e_fe.clasificacion_insp, 'PQ')
        self.assertEqual(e_fe.clasificacion_lab, 'FE')
        self.assertEqual(e_fe.clasificacion, 'FE', 'Arranca con la calidad de laboratorio')
        self.assertEqual(e_fe.defecto_ids, self.defecto)
        self.assertEqual(e_fe.motivo_ajuste, 'Barras')
        self.assertTrue(e_fe.cambia)
        # 3.24.0: si Empaque deja la del laboratorio (FE), el rollo se reetiqueta: la etiqueta impresa es la de Inspección (PQ)
        self.assertFalse(e_fe.cambia_lab)
        r_fe.write({'clasificacion_final': 'FE'})
        self.assertTrue(r_fe.reetiquetado, 'Se compara contra Inspección, no contra el laboratorio')
        r_fe.write({'clasificacion_final': False})
        # Empaque puede cambiarla antes de confirmar
        e_fe.clasificacion = 'SEGUNDA'
        self.assertEqual(e_fe.clasificacion, 'SEGUNDA')
        e_pq = Empaque.create(dict(base, codigo=r_pq.name))
        self.assertEqual(e_pq.clasificacion_lab, 'PQ')
        self.assertEqual(e_pq.clasificacion, 'PQ')
        self.assertFalse(e_pq.motivo_ajuste)
        self.assertFalse(e_pq.cambia)
        # un rollo que no pasó por laboratorio arranca con la de Inspección
        r_otro = self._final(3, 'FE')
        e_otro = Empaque.create(dict(base, codigo=r_otro.name))
        self.assertFalse(e_otro.clasificacion_lab)
        self.assertEqual(e_otro.clasificacion, 'FE')

    def test_17_numero_repetido_entre_nominas_avisa_las_referencias(self):
        if not self.con_referencia:
            self.skipTest('Sin referencia de empleado (Community sin nómina).')
        Emp = self.env['hr.employee']
        if 'x_studio_nmero_de_trabajador' not in Emp._fields:
            self.skipTest('Sin número de trabajador (base sin Studio).')
        Emp.create({'name': 'QUINCENAL PRUEBA', 'registration_number': 'Q-920', 'x_studio_nmero_de_trabajador': '920'})
        Emp.create({'name': 'CONFIANZA PRUEBA', 'registration_number': 'C-920', 'x_studio_nmero_de_trabajador': '920'})
        self._final(1, 'PQ')
        wiz = self._wizard_lab('920')
        self.assertIn('Q-920', wiz.empleado_nombre)
        self.assertIn('C-920', wiz.empleado_nombre)
        with self.assertRaisesRegex(UserError, 'teclea tu referencia'):
            wiz.action_confirmar()
        wiz.empleado_numero = 'Q-920'
        self.assertEqual(wiz.empleado_nombre, 'QUINCENAL PRUEBA')
        wiz.action_confirmar()
        self.assertEqual(self.orden_b.liberada_lab_operador, 'Q-920 - QUINCENAL PRUEBA')
