# -*- coding: utf-8 -*-
"""57.160.0: el acabado en proceso (K, kg) en C1: se genera con los demás artículos, sigue el estado del
desarrollo sin venderse y su orden se valida contra la ficha de acabado."""
from odoo.tests import TransactionCase, tagged

from ..models.sgi_dev_product_proceso import proceso_code


@tagged('post_install', '-at_install')
class TestDevProceso(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Project = cls.env['project.project']
        cls.Clave = cls.env['ficha.tecnica.clave.codigo']
        cls.kg = cls.env.ref('uom.product_uom_kgm')
        cls.m = cls.env.ref('uom.product_uom_meter')
        cls.partner = cls.env['res.partner'].create({'name': 'Cliente acabado en proceso', 'is_company': True})

    def _clave(self, kind, code):
        return self.Clave.search([('kind', '=', kind), ('code', '=', code)], limit=1)

    def _dev(self, **vals):
        return self.Project.create(dict({
            'name': 'k', 'sgi_is_ft': True, 'partner_id': self.partner.id, 'sgi_dev_product_name': 'Jersey',
            'sgi_dev_code_composicion_id': self._clave('composicion', 'W').id,
            'sgi_dev_code_dibujo_id': self._clave('dibujo', 'J').id,
            'sgi_dev_code_hilo_id': self._clave('hilo', 'Q').id,
            'sgi_dev_code_color_id': self._clave('color', 'NG').id,
            'sgi_dev_code_peso': 53, 'sgi_dev_code_galga': 18, 'sgi_dev_code_ancho': 160,
            'sgi_dev_code_ancho_crudo': 185}, **vals))

    def test_01_codigo(self):
        self.assertEqual(proceso_code('WJ053Q22JNT160'), 'WJ053Q22KNT160')
        self.assertEqual(proceso_code('WJ053Q22JNG160AF'), 'WJ053Q22KNG160AF')
        self.assertEqual(proceso_code('WJ044Q22HNT235'), '', "Solo del acabado (J)")
        self.assertEqual(proceso_code('CORTO'), '')

    def test_02_generar_por_claves(self):
        dev = self._dev()
        self.assertEqual(dev.sgi_dev_code_proceso_code, 'WJ053Q21KNG160')
        dev.action_sgi_dev_generate_products()
        proceso = dev.sgi_dev_product_proceso_id
        self.assertEqual(proceso.default_code, 'WJ053Q21KNG160')
        self.assertEqual((proceso.uom_id, proceso.sgi_dev_role, proceso.sgi_dev_project_id, proceso.sgi_dev_state),
                         (self.kg, 'proceso', dev, 'desarrollo'))
        self.assertFalse(proceso.sale_ok)
        self.assertTrue(any('WJ053Q21KNG160' in (m.body or '') for m in dev.message_ids), "Queda en el chatter")
        dev.action_sgi_dev_generate_products()
        self.assertEqual(self.env['product.product'].search_count([('default_code', '=', 'WJ053Q21KNG160')]), 1,
                         "Volver a generar no lo duplica")
        self.assertIn(proceso.product_tmpl_id, dev._sgi_dev_products())
        dev._sgi_dev_products()._sgi_dev_set_state('pilotaje')
        self.assertEqual(proceso.sgi_dev_state, 'pilotaje')
        self.assertFalse(proceso.sale_ok, "El acabado en proceso nunca se vende")
        self.assertTrue(dev.sgi_dev_product_id.sale_ok)

    def test_03_existente_se_liga(self):
        existing = self.env['product.product'].create({'name': 'K que ya existía', 'default_code': 'WJ053Q21KNG160',
                                                       'uom_id': self.kg.id})
        dev = self._dev()
        dev.action_sgi_dev_generate_products()
        self.assertEqual(dev.sgi_dev_product_proceso_id, existing)
        self.assertFalse(existing.sgi_dev_state, "Lo que no nació del proyecto no cambia de estado")

    def test_04_desde_base_con_acabado_en_proceso(self):
        P = self.env['product.product']
        crudo = P.create({'name': 'Crudo base K', 'default_code': 'WJ044Q22HNT185', 'type': 'consu',
                          'uom_id': self.kg.id})
        tenido = P.create({'name': 'Teñido base K', 'default_code': 'WJ044Q22INT185', 'type': 'consu',
                           'uom_id': self.kg.id})
        proceso = P.create({'name': 'Proceso base K', 'default_code': 'WJ053Q22KNT160', 'type': 'consu',
                            'uom_id': self.kg.id})
        acabado = P.create({'name': 'Acabado base K', 'default_code': 'WJ053Q22JNT160', 'type': 'consu',
                            'uom_id': self.m.id})
        Bom = self.env['mrp.bom']
        Bom.create({'product_tmpl_id': tenido.product_tmpl_id.id, 'product_qty': 1,
                    'bom_line_ids': [(0, 0, {'product_id': crudo.id, 'product_qty': 1.0})]})
        Bom.create({'product_tmpl_id': proceso.product_tmpl_id.id, 'product_qty': 1,
                    'bom_line_ids': [(0, 0, {'product_id': tenido.id, 'product_qty': 1.05})]})
        Bom.create({'product_tmpl_id': acabado.product_tmpl_id.id, 'product_qty': 1,
                    'bom_line_ids': [(0, 0, {'product_id': proceso.id, 'product_qty': 0.1})]})
        dev = self._dev(sgi_dev_analysis_result='nuevo', sgi_dev_base_product_id=acabado.id)
        self.assertEqual([lv['role'] for lv in dev._sgi_dev_base_chain()], ['crudo', 'tenido', 'proceso', 'acabado'])
        dev.action_sgi_dev_generate_products()
        new_t, new_k, new_a = dev.sgi_dev_product_tenido_id, dev.sgi_dev_product_proceso_id, dev.sgi_dev_product_id
        self.assertEqual((new_t.default_code, new_k.default_code, new_a.default_code),
                         ('WJ044Q22ING185', 'WJ053Q22KNG160', 'WJ053Q22JNG160'))
        self.assertEqual(new_k.uom_id, self.kg)
        bom_k = Bom._bom_find(new_k)[new_k]
        self.assertEqual(bom_k.bom_line_ids.product_id, new_t, "El acabado en proceso consume el teñido nuevo")
        bom_a = Bom._bom_find(new_a)[new_a]
        self.assertEqual(bom_a.bom_line_ids.product_id, new_k, "Inspección consume el acabado en proceso nuevo")

    def test_05_validar_corrida_con_la_ficha_de_acabado(self):
        dev = self._dev()
        dev.action_sgi_dev_generate_products()
        proceso, acabado = dev.sgi_dev_product_proceso_id, dev.sgi_dev_product_id
        tejido = self.env['ficha.tecnica.tejido'].create({'articulo': dev.sgi_dev_product_crudo_id.default_code,
                                                         'product_proceso_id': dev.sgi_dev_product_crudo_id.id})
        ficha = self.env['ficha.tecnica.acabado'].create({'articulo': acabado.default_code, 'tejido_id': tejido.id,
                                                         'product_acabado_id': acabado.id})
        self.assertEqual(ficha.product_proceso_id, proceso, "La ficha liga sola el acabado en proceso")
        self.assertEqual(dev._sgi_dev_find_sheet('acabado', proceso), ficha)
        mo = self.env['mrp.production'].new({'product_id': proceso.id, 'sgi_dev_project_id': dev.id})
        self.assertEqual(mo._sgi_dev_run_sheet(), ('acabado', ficha))
        self.assertIn('no está validada', mo._sgi_dev_run_sheet_problems()[0])
