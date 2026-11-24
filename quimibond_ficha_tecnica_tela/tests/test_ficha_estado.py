# -*- coding: utf-8 -*-
"""2.33.0: estado de la ficha de acabado; la lista de materiales sale de la ficha, en orden."""
from odoo.exceptions import UserError
from odoo.service.model import call_kw
from odoo.tests import tagged

from ..models.ruta_familia import CODIGO_KG
from .test_tiempo_ficha import TiempoFichaCase


@tagged('post_install', '-at_install')
class TestFichaEstado(TiempoFichaCase):

    def _armar(self):
        call_kw(self.env['ficha.tecnica.ruta'], 'action_ft_armar_desde_listas', [[]], {})

    def test_01_borrador_no_genera_listas(self):
        self.acabado.estado = 'borrador'
        with self.assertRaises(UserError):
            self.acabado.action_ft_generar_listas()
        self.assertIn('el ancho', self.acabado.ft_faltantes)
        with self.assertRaises(UserError, msg="Sin ancho ni ruta no se envía a revisión"):
            self.acabado.action_ft_enviar_revision()
        self.assertEqual(self.acabado.estado, 'borrador')

    def test_02_enviar_a_revision_genera_las_listas(self):
        self._armar()
        self.acabado.write({'estado': 'borrador', 'ancho_acabado': 165, 'consumo_crudo': 1.1})
        self.assertFalse(self.acabado.ft_faltantes, self.acabado.ft_faltantes)
        self.acabado.action_ft_enviar_revision()
        self.assertEqual(self.acabado.estado, 'revision')
        self.assertTrue(self.acabado.ft_lista_kg_id and self.acabado.ft_lista_m_id)
        self.assertTrue(self.acabado.ft_lista_kg_line_ids, "La ficha muestra los componentes de la lista en kg")
        self.assertTrue(self.acabado.ft_revision_tela_id, "Y queda revisada")

    def test_03_con_pendientes_no_se_activa(self):
        self.p_insp.unlink()  # Inspección sin proceso del artículo: no lo corrige la ficha
        self.env.invalidate_all()
        with self.assertRaises(UserError):
            self.acabado.action_ft_activar()
        self.assertEqual(self.acabado.estado, 'revision')

    def test_04_activar_desde_la_ficha(self):
        (self.bom | self.bom_m).action_archive()
        self.acabado.action_ft_activar()
        self.assertEqual(self.acabado.estado, 'activa')
        self.assertTrue(self.bom.active and self.bom_m.active, "Activa las dos listas")
        self.assertEqual(self.tela.product_tmpl_id.ft_ficha_estado, 'activa')
        # Modificar: regresa a revisión y la lista sigue fabricando
        self.acabado.action_ft_modificar()
        self.assertEqual(self.acabado.estado, 'revision')
        self.assertTrue(self.bom.active)
        with self.assertRaises(UserError, msg="Con las listas en uso no vuelve a borrador"):
            self.acabado.action_ft_volver_borrador()

    def test_05_la_lista_del_flujo_no_se_activa_a_mano(self):
        self.bom.code = CODIGO_KG
        self.bom.action_archive()
        with self.assertRaises(UserError):
            self.bom.action_unarchive()
        self.acabado.estado = 'activa'
        self.bom.action_unarchive()
        self.assertTrue(self.bom.active, "Con la ficha activa sí")

    def test_06_obsoleta_archiva_ficha_y_listas(self):
        self.acabado.action_ft_obsoleta()
        self.assertEqual(self.acabado.estado, 'obsoleta')
        self.assertFalse(self.acabado.active)
        self.assertFalse(self.bom.active or self.bom_m.active)
        self.acabado.action_ft_volver_borrador()
        self.assertEqual(self.acabado.estado, 'borrador')
        self.assertTrue(self.acabado.active)
