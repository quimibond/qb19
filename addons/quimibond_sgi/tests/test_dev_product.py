# -*- coding: utf-8 -*-
"""57.119.0 (C1, bloque 3): artículo en desarrollo y generador de código."""
from odoo import fields
from odoo.exceptions import UserError, ValidationError
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
        # 57.142.0: los dígitos de galga se eligen dentro del rango de la galga (18 = 21 a 30).
        self.assertEqual((self.dev.sgi_dev_code_galga_digits, self.dev.sgi_dev_code_galga_range), (21, '21 a 30'))
        self.dev.write({'sgi_dev_code_galga_digits': 22})
        self.assertEqual(self.dev.sgi_dev_code_acabado_code, 'WJ053Q22JNT160')
        with self.assertRaises(ValidationError, msg="Fuera del rango no se guarda"):
            self.dev.write({'sgi_dev_code_galga_digits': 35})
        self.dev.write({'sgi_dev_code_galga': 22})
        self.assertEqual(self.dev.sgi_dev_code_galga_digits, 31, "Al cambiar la galga se propone el primero de su rango")
        self.dev.write({'sgi_dev_code_galga': 18})
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
        self.dev.write({'sgi_dev_analysis_result': 'nuevo'})
        self.dev.action_sgi_dev_review_approve()
        self.dev.write({'stage_id': self._stage('pilotaje').id})
        self.assertEqual(acabado.sgi_dev_state, 'pilotaje')
        self.assertTrue(acabado.sale_ok)
        order.action_confirm()
        self.assertEqual(order.state, 'sale')
        self.assertTrue(any('pilotaje' in (m.body or '') for m in order.message_ids))
        self.dev.write({'stage_id': self._stage('liberado').id})
        self.assertEqual(acabado.sgi_dev_state, 'liberado')

    def _base_chain(self):
        """Cadena real del base: acabado 53 g a 160 cm ← teñido 44 g a 185 cm ← crudo 44 g a 185 cm."""
        P = self.env['product.product']
        kg, m = self.env.ref('uom.product_uom_kgm'), self.env.ref('uom.product_uom_meter')
        hilo = P.create({'name': 'Hilo poliéster', 'default_code': 'HPES100', 'type': 'consu', 'uom_id': kg.id})
        agua = P.create({'name': 'Agua', 'default_code': 'AGUA', 'type': 'consu', 'uom_id': kg.id})
        formula = P.create({'name': 'Fórmula natural', 'default_code': 'NATURAL005', 'type': 'consu', 'uom_id': kg.id})
        engomado = P.create({'name': 'Engomado', 'default_code': 'ENGOMADO002', 'type': 'consu', 'uom_id': kg.id})
        # La familia real lleva 22 en la galga 18 (rango 21 a 30): el proyecto propone 21 y debe respetar 22.
        crudo = P.create({'name': 'Crudo base', 'default_code': 'WJ044Q22HNT185', 'type': 'consu', 'uom_id': kg.id})
        tenido = P.create({'name': 'Teñido base', 'default_code': 'WJ044Q22INT185', 'type': 'consu', 'uom_id': kg.id})
        acabado = P.create({'name': 'Acabado base', 'default_code': 'WJ053Q22JNT160', 'type': 'consu', 'uom_id': m.id})
        Bom = self.env['mrp.bom']
        wc = self.env['mrp.workcenter'].create({'name': 'Circular prueba'})
        Bom.create({'product_tmpl_id': crudo.product_tmpl_id.id, 'product_qty': 1,
                    'bom_line_ids': [(0, 0, {'product_id': hilo.id, 'product_qty': 1.0})],
                    'operation_ids': [(0, 0, {'name': 'Tejer', 'workcenter_id': wc.id, 'time_cycle_manual': 60})]})
        Bom.create({'product_tmpl_id': tenido.product_tmpl_id.id, 'product_qty': 1,
                    'bom_line_ids': [(0, 0, {'product_id': crudo.id, 'product_qty': 1.0}),
                                     (0, 0, {'product_id': agua.id, 'product_qty': 33.0}),
                                     (0, 0, {'product_id': formula.id, 'product_qty': 0.032})]})
        Bom.create({'product_tmpl_id': acabado.product_tmpl_id.id, 'product_qty': 1,
                    'bom_line_ids': [(0, 0, {'product_id': tenido.id, 'product_qty': 0.1}),
                                     (0, 0, {'product_id': engomado.id, 'product_qty': 0.0047})]})
        return crudo, tenido, acabado, engomado, formula

    def test_05_generate_from_base_product(self):
        # 57.141.0: cambia solo el color → el crudo se liga, teñido y acabado se crean desde el base.
        crudo, tenido, acabado, engomado, formula = self._base_chain()
        dev = self.Project.create({
            'name': 'base', 'sgi_is_ft': True, 'partner_id': self.partner.id, 'sgi_dev_product_name': 'Jersey cobre',
            'sgi_dev_analysis_result': 'nuevo', 'sgi_dev_base_product_id': acabado.id,
            'sgi_dev_code_composicion_id': self._clave('composicion', 'W').id,
            'sgi_dev_code_dibujo_id': self._clave('dibujo', 'J').id,
            'sgi_dev_code_hilo_id': self._clave('hilo', 'Q').id,
            'sgi_dev_code_color_id': self._clave('color', 'NG').id,
            'sgi_dev_code_peso': 53, 'sgi_dev_code_galga': 18, 'sgi_dev_code_ancho': 160, 'sgi_dev_code_ancho_crudo': 185})
        chain = dev._sgi_dev_base_chain()
        self.assertEqual([lv['role'] for lv in chain], ['crudo', 'tenido', 'acabado'])
        self.assertEqual(dev._sgi_dev_changed_keys(chain)[0], {'color'}, "La galga 18 es la misma aunque el base lleve 22")
        dev.action_sgi_dev_generate_products()
        self.assertEqual(dev.sgi_dev_product_crudo_id, crudo, "El crudo no cambia con el color: se liga el del base")
        self.assertFalse(crudo.sgi_dev_project_id, "Y el del base no se toca")
        new_t, new_a = dev.sgi_dev_product_tenido_id, dev.sgi_dev_product_id
        self.assertEqual((new_t.default_code, new_a.default_code), ('WJ044Q22ING185', 'WJ053Q22JNG160'),
                         "Código del nivel del base con solo el color sustituido; la galga conserva el 22 del base")
        self.assertTrue(dev.name.endswith('WJ053Q22JNG160'), "El nombre toma el código del acabado")
        self.assertEqual((new_t.uom_id, new_a.uom_id), (tenido.uom_id, acabado.uom_id))
        self.assertEqual(new_t.sgi_dev_project_id, dev)
        bom_t = self.env['mrp.bom']._bom_find(new_t)[new_t]
        self.assertEqual(len(bom_t.bom_line_ids), 3, "Lista del teñido copiada completa")
        line_crudo = bom_t.bom_line_ids.filtered(lambda l: l.product_id == crudo)
        self.assertTrue(line_crudo and not line_crudo.sgi_dev_pending, "El insumo crudo sigue igual y no está pendiente")
        self.assertTrue(all(bom_t.bom_line_ids.filtered(lambda l: l.product_id != crudo).mapped('sgi_dev_pending')),
                        "Agua y fórmula dependen del color: por capturar")
        bom_a = self.env['mrp.bom']._bom_find(new_a)[new_a]
        self.assertEqual(len(bom_a.bom_line_ids), 2)
        line_in = bom_a.bom_line_ids.filtered(lambda l: l.product_id == new_t)
        self.assertEqual(line_in.product_qty, 0.1, "El insumo se reemplazó por el teñido nuevo con la misma cantidad")
        self.assertFalse(line_in.sgi_dev_pending)
        self.assertFalse(bom_a.bom_line_ids.filtered(lambda l: l.product_id == engomado).sgi_dev_pending)
        self.assertEqual(dev.sgi_dev_bom_pending_count, 2)
        self.assertEqual(dev.action_sgi_dev_bom_pending()['domain'], [('id', 'in', bom_t.ids)])
        last = dev.message_ids[:1].body
        self.assertIn('WJ044Q22ING185', last)
        self.assertIn('por capturar', last)
        # Volver a generar no duplica ni toca lo ligado.
        dev.action_sgi_dev_generate_products()
        self.assertEqual(self.env['product.product'].search_count([('default_code', '=', 'WJ044Q22ING185')]), 1)
        # Si alguien corrige el código del acabado a mano, el nombre del proyecto lo sigue.
        new_a.write({'default_code': 'WJ053Q23JNG160'})
        self.assertTrue(dev.name.endswith('WJ053Q23JNG160'))
        # Capturados los pendientes, el contador baja.
        bom_t.bom_line_ids.write({'sgi_dev_pending': False})
        dev.invalidate_recordset(['sgi_dev_bom_pending_count'])
        self.assertEqual(dev.sgi_dev_bom_pending_count, 0)

    def test_06_generate_from_base_peso_y_ancho_crudo(self):
        # Cambian peso y ancho crudo: los tres niveles cambian; el peso de crudo y teñido se estima en proporción.
        crudo, tenido, acabado, engomado, formula = self._base_chain()
        dev = self.Project.create({
            'name': 'base2', 'sgi_is_ft': True, 'partner_id': self.partner.id, 'sgi_dev_product_name': 'Jersey 60',
            'sgi_dev_analysis_result': 'nuevo', 'sgi_dev_base_product_id': acabado.id,
            'sgi_dev_code_color_id': self._clave('color', 'NT').id,
            'sgi_dev_code_peso': 60, 'sgi_dev_code_ancho': 160, 'sgi_dev_code_ancho_crudo': 200})
        dev.action_sgi_dev_generate_products()
        self.assertEqual(dev.sgi_dev_product_crudo_id.default_code, 'WJ050Q22HNT200', "44 × 60 / 53 ≈ 50, ancho crudo 200")
        self.assertEqual(dev.sgi_dev_product_tenido_id.default_code, 'WJ050Q22INT200')
        self.assertEqual(dev.sgi_dev_product_id.default_code, 'WJ060Q22JNT160')
        bom_c = self.env['mrp.bom']._bom_find(dev.sgi_dev_product_crudo_id)[dev.sgi_dev_product_crudo_id]
        self.assertTrue(bom_c.operation_ids, "Las operaciones del crudo se copian")
        self.assertTrue(all(bom_c.bom_line_ids.mapped('sgi_dev_pending')), "El hilo depende del peso: por capturar")
        bom_a = self.env['mrp.bom']._bom_find(dev.sgi_dev_product_id)[dev.sgi_dev_product_id]
        self.assertTrue(bom_a.bom_line_ids.filtered(lambda l: l.product_id == dev.sgi_dev_product_tenido_id).sgi_dev_pending,
                        "La cantidad del insumo depende del peso: por capturar")
        self.assertIn('estimado', dev.message_ids[:1].body)

    def test_07_linea_no_genera(self):
        crudo, tenido, acabado, engomado, formula = self._base_chain()
        dev = self.Project.create({'name': 'linea', 'sgi_is_ft': True, 'partner_id': self.partner.id,
                                   'sgi_dev_product_name': 'Línea', 'sgi_dev_analysis_result': 'linea',
                                   'sgi_dev_base_product_id': acabado.id})
        with self.assertRaises(UserError, msg="Producto de línea: se cotiza el de línea"):
            dev.action_sgi_dev_generate_products()
        self.assertFalse(dev.sgi_dev_product_id)

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
