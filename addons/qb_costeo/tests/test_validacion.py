# -*- coding: utf-8 -*-
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestValidacion(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        kg = cls.env.ref('uom.product_uom_kgm')
        cls.kg = kg
        cocina = cls.env['product.category'].create({
            'name': 'Cocina',
            'parent_id': cls.env['product.category'].create(
                {'name': 'Producto en Proceso'}).id})
        cls.crudo = cls.env['product.product'].create({
            'name': 'WK284R46HNT166', 'default_code': 'WK284R46HNT166',
            'type': 'consu', 'uom_id': kg.id, 'standard_price': 41.37})
        cls.negro = cls.env['product.product'].create({
            'name': 'NEGRO016', 'default_code': 'NEGRO016', 'type': 'consu',
            'uom_id': kg.id, 'standard_price': 55.66, 'categ_id': cocina.id})
        cls.aux = cls.env['product.product'].create({
            'name': 'AUX01', 'default_code': 'AUX01', 'type': 'consu',
            'uom_id': kg.id, 'standard_price': 20, 'categ_id': cocina.id})
        cls.agua = cls.env['product.product'].create({
            'name': 'AGUA', 'default_code': 'AGUA', 'type': 'consu',
            'uom_id': cls.env.ref('uom.product_uom_litre').id,
            'standard_price': 0})
        cls.tenido = cls.env['product.product'].create({
            'name': 'WK284R46ING166', 'default_code': 'WK284R46ING166',
            'type': 'consu', 'uom_id': kg.id})
        cls.bom = cls.env['mrp.bom'].create({
            'product_tmpl_id': cls.tenido.product_tmpl_id.id,
            'product_qty': 1, 'product_uom_id': kg.id,
            'bom_line_ids': [
                (0, 0, {'product_id': cls.crudo.id, 'product_qty': 1,
                        'product_uom_id': kg.id}),
                (0, 0, {'product_id': cls.negro.id, 'product_qty': 0.632,
                        'product_uom_id': kg.id}),
                (0, 0, {'product_id': cls.aux.id, 'product_qty': 0.05,
                        'product_uom_id': kg.id}),
                (0, 0, {'product_id': cls.agua.id, 'product_qty': 29,
                        'product_uom_id': cls.agua.uom_id.id}),
            ]})
        cls.V = cls.env['qb.producto.validacion']

    def _abiertas(self):
        return self.V.search([('product_id', '=', self.tenido.id),
                              ('estado', '=', 'abierta')])

    def test_colorante_fuera_de_rango(self):
        abiertas = self.V.revisar(self.tenido)
        reglas = {(v.regla, v.componente_id.default_code or False)
                  for v in abiertas}
        # 63 % de preparación en una línea (máx. 30) y 68 % en total (máx. 40).
        self.assertIn(('receta_implausible', 'NEGRO016'), reglas)
        self.assertIn(('receta_implausible', False), reglas)
        # El agua a 0 no dispara precio_cero.
        self.assertNotIn(('precio_cero', 'AGUA'), reglas)
        linea = abiertas.filtered(lambda v: v.componente_id == self.negro)
        self.assertAlmostEqual(linea.valor, 63.0, delta=0.5)  # la receta redondea a 0.63

    def test_se_cierra_sola_al_corregir(self):
        self.V.revisar(self.tenido)
        self.assertEqual(len(self._abiertas()), 2)
        self.bom.bom_line_ids.filtered(
            lambda l: l.product_id == self.negro).product_qty = 0.0632
        self.V.revisar(self.tenido)
        self.assertFalse(self._abiertas())
        corregidas = self.V.search([('product_id', '=', self.tenido.id),
                                    ('estado', '=', 'corregida')])
        self.assertEqual(len(corregidas), 2)
        self.assertTrue(all(corregidas.mapped('resuelta_el')))
        # Si vuelve a fallar, la misma fila se reabre (no se duplica).
        self.bom.bom_line_ids.filtered(
            lambda l: l.product_id == self.negro).product_qty = 0.632
        self.V.revisar(self.tenido)
        self.assertEqual(len(self.V.search(
            [('product_id', '=', self.tenido.id)])), 2)
        self.assertEqual(len(self._abiertas()), 2)

    def test_aceptar_con_motivo(self):
        abiertas = self.V.revisar(self.tenido)
        with self.assertRaises(UserError):
            abiertas.action_aceptar()
        abiertas.write({'motivo': 'receta especial confirmada por planta'})
        abiertas.action_aceptar()
        self.assertFalse(self._abiertas())
        # Mismo valor: sigue aceptada.
        self.V.revisar(self.tenido)
        self.assertFalse(self._abiertas())
        # Cambia más de 10 %: se reabre.
        self.bom.bom_line_ids.filtered(
            lambda l: l.product_id == self.negro).product_qty = 0.9
        self.V.revisar(self.tenido)
        self.assertTrue(self._abiertas())

    def test_precio_cero_y_fuera_de_banda(self):
        self.crudo.standard_price = 0
        abiertas = self.V.revisar(self.tenido)
        self.assertTrue(abiertas.filtered(
            lambda v: v.regla == 'precio_cero'
            and v.componente_id == self.crudo))
        # Última compra del negro a 20 contra promedio 55.66 → fuera de banda.
        partner = self.env['res.partner'].create({'name': 'Proveedor'})
        po = self.env['purchase.order'].create({
            'partner_id': partner.id,
            'order_line': [(0, 0, {
                'product_id': self.negro.id, 'product_qty': 10,
                'price_unit': 20, 'product_uom_id': self.kg.id,
                'name': 'negro', 'date_planned': '2026-09-01'})]})
        po.button_confirm()
        abiertas = self.V.revisar(self.tenido)
        banda = abiertas.filtered(lambda v: v.regla == 'precio_fuera_banda')
        self.assertEqual(banda.componente_id, self.negro)
        self.assertGreater(banda.valor, 100)

    def test_intermedio_con_receta_no_se_valida_por_precio(self):
        """Un crudo con receta propia cuesta 0 hasta que se produce: su costo
        sale de su receta, no de su precio."""
        self.crudo.standard_price = 0
        self.env['mrp.bom'].create({
            'product_tmpl_id': self.crudo.product_tmpl_id.id,
            'product_qty': 1, 'product_uom_id': self.kg.id,
            'bom_line_ids': [(0, 0, {
                'product_id': self.aux.id, 'product_qty': 1,
                'product_uom_id': self.kg.id})]})
        abiertas = self.V.revisar(self.tenido)
        self.assertFalse(abiertas.filtered(
            lambda v: v.regla == 'precio_cero'
            and v.componente_id == self.crudo))

    def test_categoria_excluida_y_preparacion_no_se_validan(self):
        maquila = self.env['product.category'].create({'name': 'Maquila'})
        self.tenido.categ_id = maquila
        self.assertFalse(self.V.revisar(self.tenido))
        # La preparación de color (Cocina) tampoco: sus líneas suman 100 %.
        self.env['mrp.bom'].create({
            'product_tmpl_id': self.negro.product_tmpl_id.id,
            'product_qty': 1, 'product_uom_id': self.kg.id,
            'bom_line_ids': [(0, 0, {
                'product_id': self.aux.id, 'product_qty': 1,
                'product_uom_id': self.kg.id})]})
        self.assertFalse(self.V.revisar(self.negro))

    def test_ultima_compra_en_otra_unidad(self):
        """Comprado por gramo a 0.05 = 50/kg: dentro de la banda de 55.66."""
        gramo = self.env.ref('uom.product_uom_gram')
        partner = self.env['res.partner'].create({'name': 'Proveedor g'})
        po = self.env['purchase.order'].create({
            'partner_id': partner.id,
            'order_line': [(0, 0, {
                'product_id': self.negro.id, 'product_qty': 1000,
                'price_unit': 0.05, 'product_uom_id': gramo.id,
                'name': 'negro', 'date_planned': '2026-09-01'})]})
        po.button_confirm()
        precios = self.V._ultimo_precio_compra()
        self.assertAlmostEqual(precios[self.negro.id], 50.0, 2)
        abiertas = self.V.revisar(self.tenido)
        self.assertFalse(abiertas.filtered(
            lambda v: v.regla == 'precio_fuera_banda'))

    def test_revisar_todo_cubre_recetas_activas(self):
        abiertas = self.V.revisar()
        self.assertTrue(abiertas.filtered(
            lambda v: v.product_id == self.tenido))
