# -*- coding: utf-8 -*-
from unittest.mock import patch

from odoo import Command
from odoo.exceptions import UserError
from odoo.tests import tagged
from odoo.tests.common import TransactionCase

RENDIMIENTO_PATCH = ('odoo.addons.quimibond_tintoreria_programacion.models.mrp_production.'
                     'MrpProduction._rendimiento_acabado')


@tagged('post_install', '-at_install', 'quimibond_tintoreria_programacion')
class TestProgramacionTintoreria(TransactionCase):
    """Caso del cliente: rendimiento 9 (banda B), HTJ con 600 kg en banda B, MO de 1000 kg
    -> 2 MOs de 500 kg en el mismo HTJ."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        cls.htj = cls.env['mrp.workcenter'].create({'name': 'Tintoreria TEST 1', 'code': 'HTJT1'})
        cls.env['tintoreria.capacidad.rendimiento'].create({
            'workcenter_id': cls.htj.id,
            'capacidad_kg': 1000, 'capacidad_grupo_a': 900,
            'capacidad_grupo_b': 600, 'capacidad_grupo_c': 450,
            'relacion_bano': 10,
        })
        cls.htj_sin_banda = cls.env['mrp.workcenter'].create({'name': 'Tintoreria TEST 3', 'code': 'HTJT3'})
        cls.env['tintoreria.capacidad.rendimiento'].create({
            'workcenter_id': cls.htj_sin_banda.id,
            'capacidad_kg': 600, 'capacidad_grupo_a': 600, 'relacion_bano': 6,
        })
        cls.producto_k = cls._producto('WT105Q56KNT155AF')
        cls.componente = cls._producto('COMP-TEST', storable=False)
        cls.bom = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.producto_k.product_tmpl_id.id,
            'product_qty': 1,
            'product_uom_id': cls.kg.id,
            'operation_ids': [Command.create({
                'name': 'Tintorería', 'workcenter_id': cls.htj.id,
            })],
            'bom_line_ids': [Command.create({'product_id': cls.componente.id, 'product_qty': 0.1})],
        })

    @classmethod
    def _producto(cls, codigo, storable=True):
        return cls.env['product.product'].create({
            'name': codigo, 'default_code': codigo, 'type': 'consu',
            'is_storable': storable, 'uom_id': cls.kg.id,
        })

    def _crear_mo(self, kg=1000.0):
        mo = self.env['mrp.production'].create({
            'product_id': self.producto_k.id,
            'product_qty': kg,
            'product_uom_id': self.kg.id,
            'bom_id': self.bom.id,
        })
        mo.action_confirm()
        return mo

    # ------------------------------------------------------------------
    def test_codigo_k_posicion_9(self):
        mo = self._crear_mo(100)
        self.assertTrue(mo._es_producto_acabado_kg())
        self.assertTrue(mo.requiere_programacion_tintoreria)
        self.assertTrue(mo.tintoreria_pendiente)
        otro = self.env['mrp.production'].new({'product_id': self._producto('WT105Q56JNT155AF').id})
        self.assertFalse(otro._es_producto_acabado_kg())

    def test_plan_reparto_simetrico(self):
        mo = self._crear_mo(1000)
        with patch(RENDIMIENTO_PATCH, return_value=9.0):
            plan = mo._calcular_plan_tintoreria(self.htj)
        self.assertEqual(plan['banda'], 'B')
        self.assertEqual(plan['capacidad_kg'], 600)
        self.assertEqual(plan['n_banos'], 2)
        self.assertEqual(plan['montos'], [500.0, 500.0])

    def test_plan_un_solo_bano(self):
        mo = self._crear_mo(400)
        with patch(RENDIMIENTO_PATCH, return_value=9.0):
            plan = mo._calcular_plan_tintoreria(self.htj)
        self.assertEqual(plan['n_banos'], 1)
        self.assertEqual(plan['montos'], [400.0])

    def test_plan_redondeo_en_ultimo_bano(self):
        mo = self._crear_mo(1000.01)
        with patch(RENDIMIENTO_PATCH, return_value=9.0):
            plan = mo._calcular_plan_tintoreria(self.htj)
        self.assertEqual(plan['n_banos'], 2)
        self.assertAlmostEqual(sum(plan['montos']), 1000.01, places=2)

    def test_htj_sin_capacidad_para_banda(self):
        mo = self._crear_mo(1000)
        with patch(RENDIMIENTO_PATCH, return_value=9.0), self.assertRaises(UserError):
            mo._calcular_plan_tintoreria(self.htj_sin_banda)

    def test_rendimiento_fuera_de_bandas(self):
        mo = self._crear_mo(1000)
        with patch(RENDIMIENTO_PATCH, return_value=6.5), self.assertRaises(UserError):
            mo._calcular_plan_tintoreria(self.htj)

    def test_programar_divide_y_marca(self):
        mo = self._crear_mo(1000)
        with patch(RENDIMIENTO_PATCH, return_value=9.0):
            programacion = mo._programar_tintoreria(self.htj)
        ordenes = programacion.production_ids
        self.assertEqual(len(ordenes), 2)
        self.assertEqual(sorted(ordenes.mapped('product_qty')), [500.0, 500.0])
        self.assertTrue(all(ordenes.mapped('tintoreria_programada')))
        self.assertFalse(any(ordenes.mapped('tintoreria_pendiente')))
        for orden in ordenes:
            self.assertEqual(orden._workorder_tintoreria().workcenter_id, self.htj)
        self.assertEqual(programacion.n_banos, 2)
        self.assertEqual(programacion.kg_total, 1000)

    def test_programar_un_bano_no_divide(self):
        mo = self._crear_mo(400)
        with patch(RENDIMIENTO_PATCH, return_value=9.0):
            programacion = mo._programar_tintoreria(self.htj)
        self.assertEqual(programacion.production_ids, mo)
        self.assertTrue(mo.tintoreria_programada)

    def test_no_se_puede_programar_dos_veces(self):
        mo = self._crear_mo(400)
        with patch(RENDIMIENTO_PATCH, return_value=9.0):
            mo._programar_tintoreria(self.htj)
            with self.assertRaises(UserError):
                mo._programar_tintoreria(self.htj)

    def test_sin_programar_no_inicia_operacion(self):
        mo = self._crear_mo(400)
        with self.assertRaises(UserError):
            mo.workorder_ids[:1].button_start()
        with self.assertRaises(UserError):
            mo.action_start()

    def test_programada_si_inicia_operacion(self):
        mo = self._crear_mo(400)
        with patch(RENDIMIENTO_PATCH, return_value=9.0):
            mo._programar_tintoreria(self.htj)
        mo.workorder_ids[:1].button_start()
        self.assertEqual(mo.workorder_ids[:1].state, 'progress')

    def test_wizard(self):
        mo = self._crear_mo(1000)
        wizard = self.env['tintoreria.programacion.wizard'].with_context(
            active_model='mrp.production', active_ids=mo.ids).create({})
        self.assertEqual(len(wizard.line_ids), 1)
        with patch(RENDIMIENTO_PATCH, return_value=9.0):
            wizard.line_ids.workcenter_id = self.htj
            self.assertEqual(wizard.line_ids.n_banos, 2)
            wizard.action_confirmar()
        self.assertTrue(mo.tintoreria_programada)
