# -*- coding: utf-8 -*-
"""2.30.0 (auditoría del 10-nov-2026): claves de los cálculos protegidas, ancho de venta con su unidad y rendimiento
anterior al escribirlo a mano."""
from odoo.exceptions import UserError
from odoo.tests import tagged

from .test_tiempo_ficha import TiempoFichaCase


@tagged('post_install', '-at_install')
class TestAuditoria230(TiempoFichaCase):

    def test_01_clave_del_calculo_no_se_renombra_ni_se_borra(self):
        tipo = self.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_inspeccion')
        param = self._param(tipo, 'velocidad_inspeccion', 'm/h', 'articulo')
        self.assertTrue(param._ft_es_del_calculo())
        with self.assertRaises(UserError, msg="Renombrar la clave dejaría el tiempo de inspección en cero"):
            param.code = 'velocidad_insp'
        with self.assertRaises(UserError):
            param.active = False
        with self.assertRaises(UserError):
            param.unlink()
        param.write({'name': 'Velocidad de inspección (1 empalme)', 'notas': 'Cambiar el nombre sí se puede'})
        self.assertEqual(param.code, 'velocidad_inspeccion')

    def test_02_clave_que_no_usa_ningun_calculo_se_maneja_libre(self):
        tipo = self.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_inspeccion')
        param = self._param(tipo, 'zz_prueba_230', 'm', 'articulo')
        self.assertFalse(param._ft_es_del_calculo())
        param.code = 'zz_prueba_230b'
        param.unlink()

    def test_03_ancho_de_venta_con_su_unidad(self):
        self.acabado.write({'peso_acabado': 50, 'peso_acabado_tol_unit': 'g/m2', 'ancho_acabado': 160,
                            'ancho_acabado_tol_unit': 'cm'})
        en_cm = self.acabado.rendimiento_teorico
        self.assertAlmostEqual(en_cm, round(1000 / (50 * 1.6), 4), places=4)
        self.acabado.write({'ancho_acabado': 1.6, 'ancho_acabado_tol_unit': 'm'})
        self.assertAlmostEqual(self.acabado.rendimiento_teorico, en_cm, places=4,
                               msg="1.6 m y 160 cm son el mismo ancho")

    def test_04_rendimiento_escrito_a_mano_guarda_el_anterior(self):
        self.assertEqual(self.acabado.rendimiento_tela_acabada, 8.0)
        self.acabado.rendimiento_tela_acabada = 9.5
        self.assertEqual(self.acabado.rendimiento_anterior, 8.0)
        self.assertTrue(self.acabado.rendimiento_manual)

    def test_05_clave_del_tipo_de_maquina_no_se_renombra(self):
        tipo = self.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_inspeccion')
        with self.assertRaises(UserError):
            tipo.code = 'insp'
        with self.assertRaises(UserError):
            tipo.unlink()
        tipo.name = 'Inspección (prueba 2.30.0)'
        self.assertEqual(tipo.code, 'inspeccion')

    def _rama_con(self, ancho=None, merma=None):
        tipo = self.env.ref('quimibond_ficha_tecnica_tela.ft_maquina_tipo_acabado')
        self._param(tipo, 'ancho_rama', 'm', 'articulo')
        self._param(tipo, 'merma', '%', 'articulo')
        rama = self.env['mrp.workcenter'].create({'name': 'RAMA 230 prueba', 'code': 'R230', 'ft_tipo_id': tipo.id})
        proceso = self.env['ficha.tecnica.proceso'].create({'articulo': 'IZZ931Q22JNT165', 'workcenter_id': rama.id})
        if ancho:
            proceso._ft_set_value('ancho_rama', ancho)
        if merma:
            proceso._ft_set_value('merma', merma)
        self.acabado.invalidate_recordset(['rendimiento_produccion', 'rendimiento_produccion_calculo'])
        return proceso

    def test_06_rendimiento_de_produccion_con_peso_ancho_de_rama_y_merma(self):
        """Ingeniería (10-nov-2026): 1000 ÷ (g/m² × ancho de rama) × (1 − merma), por kg que entra a acabado."""
        self._rama_con(ancho=1.65, merma=10)
        esperado = round(1000 / (53 * 1.65) * 0.9, 4)
        self.assertAlmostEqual(self.acabado.rendimiento_produccion, esperado, places=4)
        self.assertIn('1 − 10 %', self.acabado.rendimiento_produccion_calculo)
        self.acabado.action_ft_calcular_rendimiento_produccion()
        self.assertAlmostEqual(self.acabado.rendimiento_tela_acabada, esperado, places=4)
        self.assertEqual(self.acabado.rendimiento_fuente, 'produccion')
        self.assertEqual(self.acabado.rendimiento_anterior, 8.0)
        self.assertFalse(self.acabado._ft_aplicar_rendimiento_produccion(), "Sin cambio no vuelve a escribir")
        self.acabado.rendimiento_medido = 12.0
        with self.assertRaises(UserError, msg="Con rendimiento de producción lo medido es solo control"):
            self.acabado.action_ft_usar_rendimiento_medido()

    def test_07_sin_merma_no_hay_rendimiento_de_produccion(self):
        self._rama_con(ancho=1.65)
        self.assertFalse(self.acabado.rendimiento_produccion)
        self.assertIn('la merma de la rama', self.acabado.rendimiento_produccion_calculo)
        self.acabado.action_ft_calcular_rendimiento_produccion()
        self.assertEqual(self.acabado.rendimiento_tela_acabada, 8.0, "Sin todos los datos se queda el de la ficha")
