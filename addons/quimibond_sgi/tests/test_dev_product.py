# -*- coding: utf-8 -*-
"""57.119.0 (C1, bloque 3): artículo en desarrollo y generador de código."""
from odoo import fields
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestDevProduct(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Project = cls.env['project.project']
        cls.Clave = cls.env['ficha.tecnica.clave.codigo']
        cls.partner = cls.env['res.partner'].create({'name': 'Cliente desarrollo', 'is_company': True})
        cls.dev = cls.Project.create({
            'name': 'x', 'sgi_is_ft': True, 'partner_id': cls.partner.id, 'sgi_dev_product_name': 'Jersey',
            'sgi_dev_code_composicion_id': cls._clave(cls, 'composicion', 'W').id,
            'sgi_dev_code_dibujo_id': cls._clave(cls, 'dibujo', 'J').id,
            'sgi_dev_code_hilo_id': cls._clave(cls, 'hilo', 'Q').id,
            'sgi_dev_code_color_id': cls._clave(cls, 'color', 'NT').id,
            'sgi_dev_code_ancho_crudo': 185,
        })

    def _clave(self, kind, code):
        return self.Clave.search([('kind', '=', kind), ('code', '=', code)], limit=1)

    def _stage(self, key):
        return self.env.ref('quimibond_sgi.sgi_dev_stage_%s' % key)

    def test_01_code_from_table_and_build(self):
        self.dev.action_sgi_dev_load_lines()
        lines = self.dev.sgi_dev_line_ids
        lines.filtered(lambda l: l.caracteristica_code == 'masa').write({'spec_nominal': 53})
        lines.filtered(lambda l: l.caracteristica_code == 'ancho').write({'spec_nominal': 1.60})
        lines.filtered(lambda l: l.caracteristica_code == 'galga').write({'spec_nominal': 18})
        self.assertFalse(self.dev.sgi_dev_code_acabado_code, "Sin peso, galga y ancho no hay código.")
        self.dev.action_sgi_dev_code_from_table()
        self.assertEqual((self.dev.sgi_dev_code_peso, self.dev.sgi_dev_code_galga, self.dev.sgi_dev_code_ancho),
                         (53, 18, 160))
        self.assertEqual(self.dev.sgi_dev_code_acabado_code, 'WJ053Q21JNT160')
        self.assertEqual(self.dev.sgi_dev_code_crudo, 'WJ053Q21HNT185')
        self.assertFalse(self.dev.sgi_dev_code_tenido, "Color natural: sin teñido.")
        self.dev.write({'sgi_dev_code_color_id': self._clave('color', 'NG').id,
                        'sgi_dev_code_acabado_id': self._clave('acabado', 'AF').id})
        self.assertTrue(self.dev.sgi_dev_code_tenido)
        self.assertEqual(self.dev.sgi_dev_code_acabado_code, 'WJ053Q21JNG160AF')
        self.assertEqual(self.dev.sgi_dev_code_tenido_code, 'WJ053Q21ING185')
        self.assertEqual(self.dev.sgi_dev_code_crudo, 'WJ053Q21HNT185', "El crudo de hilo natural va en natural.")
        self.dev.write({'sgi_dev_code_hilo_id': self._clave('hilo', 'R').id})
        self.assertEqual(self.dev.sgi_dev_code_crudo, 'WJ053R21HNG185', "El hilo reciclado trae el color.")

    def test_02_generate_products_and_states(self):
        self.dev.write({'sgi_dev_code_peso': 53, 'sgi_dev_code_galga': 18, 'sgi_dev_code_ancho': 160,
                        'sgi_dev_code_color_id': self._clave('color', 'NG').id})
        self.dev.action_sgi_dev_generate_products()
        crudo, tenido, acabado = (self.dev.sgi_dev_product_crudo_id, self.dev.sgi_dev_product_tenido_id,
                                  self.dev.sgi_dev_product_id)
        self.assertEqual((crudo.default_code, tenido.default_code, acabado.default_code),
                         ('WJ053Q21HNT185', 'WJ053Q21ING185', 'WJ053Q21JNG160'))
        self.assertEqual(acabado.name, 'JERSEY DE 53 G/M2')
        self.assertEqual((crudo.uom_id, acabado.uom_id),
                         (self.env.ref('uom.product_uom_kgm'), self.env.ref('uom.product_uom_meter')))
        self.assertEqual(set((crudo | tenido | acabado).mapped('sgi_dev_state')), {'desarrollo'})
        self.assertFalse(acabado.sale_ok, "En desarrollo no se vende.")
        self.assertTrue(acabado.is_storable)
        self.assertEqual(acabado.sgi_dev_project_id, self.dev)
        self.assertTrue(self.dev.name.endswith('WJ053Q21JNG160'), "El nombre toma el código del artículo.")
        # Volver a generar no duplica.
        self.dev.action_sgi_dev_generate_products()
        self.assertEqual(self.env['product.product'].search_count([('default_code', '=', 'WJ053Q21JNG160')]), 1)
        # Venta bloqueada en desarrollo, con aviso en pilotaje.
        order = self.env['sale.order'].create({
            'partner_id': self.partner.id,
            'order_line': [(0, 0, {'product_id': acabado.id, 'product_uom_qty': 10})]})
        with self.assertRaises(UserError):
            order.action_confirm()
        self.dev.write({'stage_id': self._stage('pilotaje').id})
        self.assertEqual(acabado.sgi_dev_state, 'pilotaje')
        self.assertTrue(acabado.sale_ok)
        order.action_confirm()
        self.assertEqual(order.state, 'sale')
        self.assertTrue(any('pilotaje' in (m.body or '') for m in order.message_ids))
        self.dev.write({'stage_id': self._stage('liberado').id})
        self.assertEqual(acabado.sgi_dev_state, 'liberado')

    def test_03_existing_code_is_linked_and_closing_archives(self):
        existing = self.env['product.product'].create({'name': 'Ya existía', 'default_code': 'WJ053Q21HNT185'})
        self.dev.write({'sgi_dev_code_peso': 53, 'sgi_dev_code_galga': 18, 'sgi_dev_code_ancho': 160})
        self.dev.action_sgi_dev_generate_products()
        self.assertEqual(self.dev.sgi_dev_product_crudo_id, existing)
        self.assertFalse(existing.sgi_dev_state, "El artículo existente no cambia de estado.")
        self.assertFalse(self.dev.sgi_dev_product_tenido_id, "Natural: no hay teñido.")
        acabado = self.dev.sgi_dev_product_id
        bom = self.env['mrp.bom'].create({'product_tmpl_id': acabado.product_tmpl_id.id, 'product_qty': 1})
        self.dev.write({'sgi_dev_not_feasible_reason_id': self.env['sgi.dev.option'].create(
            {'kind': 'motivo_no_factible', 'name': 'Costo'}).id})
        self.dev.action_sgi_dev_close_not_feasible()
        self.assertEqual(acabado.sgi_dev_state, 'archivado')
        self.assertFalse(acabado.active)
        self.assertFalse(bom.active)
        self.assertTrue(existing.active, "Lo que no nació del proyecto no se archiva.")

    def test_04_generic_sample_block_by_parameter(self):
        generic = self.env['product.product'].create({'name': 'MUESTRAPILOTOTEJIDO', 'default_code': 'MUESTRA PILOTO TEJIDO',
                                                      'type': 'consu', 'is_storable': True})
        Param = self.env['ir.config_parameter'].sudo()
        Param.set_param('quimibond_sgi.dev_block_generic_sample_from', '')
        mo = self.env['mrp.production'].create({'product_id': generic.id, 'product_qty': 5})
        self.assertTrue(mo.exists(), "Sin fecha no hay bloqueo.")
        Param.set_param('quimibond_sgi.dev_block_generic_sample_from', '2099-01-01')
        self.env['mrp.production'].create({'product_id': generic.id, 'product_qty': 5})
        Param.set_param('quimibond_sgi.dev_block_generic_sample_from', fields.Date.to_string(fields.Date.context_today(mo)))
        with self.assertRaises(UserError):
            self.env['mrp.production'].create({'product_id': generic.id, 'product_qty': 5})
        with self.assertRaises(UserError):
            mo.action_confirm()
        Param.set_param('quimibond_sgi.dev_block_generic_sample_from', 'no es fecha')
        self.env['mrp.production'].create({'product_id': generic.id, 'product_qty': 5})
