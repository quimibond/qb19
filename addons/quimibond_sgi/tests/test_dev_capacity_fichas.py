# -*- coding: utf-8 -*-
"""57.198.0: «Capacidad a utilizar» con la ruta de la app de fichas y la capacidad teórica de cada máquina.

Dirección General 2026-11-04: la ruta es la de la familia del crudo (una fila por máquina), las telas de color van al jet
de colores y la capacidad de cada máquina es la teórica (calendario ÷ tiempo de la ficha); sin dato no se inventa."""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestDevCapacityFichas(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Product = cls.env['product.product']
        kg = cls.env.ref('uom.product_uom_kgm')
        cls.crudo = Product.create({'name': 'Crudo capacidad prueba', 'default_code': 'WX035Q22HNT200',
                                    'is_storable': True, 'uom_id': kg.id})
        Wc = cls.env['mrp.workcenter']
        cls.wc_dev = Wc.create({'name': 'DEVANADORA PRUEBA CAP'})
        cls.wc_jet = Wc.create({'name': 'JET FAMILIA PRUEBA CAP'})
        cls.wc_color = Wc.create({'name': 'JET COLOR PRUEBA CAP'})
        cls.wc_rama = Wc.create({'name': 'RAMA PRUEBA CAP'})
        cls.wc_insp = Wc.create({'name': 'INSPECCION PRUEBA CAP'})
        cls.wc_emp = Wc.create({'name': 'EMPAQUE PRUEBA CAP'})
        cls.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.dev_capacity_color_jet_id', cls.wc_color.id)
        cls.env['ficha.tecnica.ruta'].create({'crudo_id': cls.crudo.id, 'paso_ids': [
            (0, 0, {'paso': 'devanado', 'sequence': 10, 'workcenter_id': cls.wc_dev.id}),
            (0, 0, {'paso': 'clasificacion', 'sequence': 10, 'workcenter_id': cls.wc_emp.id}),
            (0, 0, {'paso': 'tintoreria', 'sequence': 20, 'workcenter_id': cls.wc_jet.id}),
            (0, 0, {'paso': 'rama', 'sequence': 30, 'workcenter_id': cls.wc_rama.id}),
            (0, 0, {'paso': 'inspeccion', 'sequence': 40, 'workcenter_id': cls.wc_insp.id}),
        ]})
        cls.partner = cls.env['res.partner'].create({'name': 'CLIENTE CAPACIDAD PRUEBA', 'is_company': True,
                                                     'country_id': cls.env.ref('base.mx').id})
        cls.Project = cls.env['project.project'].with_context(sgi_dev_no_autoload=True)

    def _dev(self, code):
        product = self.env['product.product'].create({'name': 'Tela %s' % code, 'default_code': code})
        return self.Project.create({'name': code, 'sgi_is_ft': True, 'partner_id': self.partner.id,
                                    'sgi_dev_product_crudo_id': self.crudo.id, 'sgi_dev_product_id': product.id,
                                    'sgi_dev_consumption_monthly': 1000, 'sgi_dev_consumption_uom': 'm'})

    def test_01_color_goes_to_color_jet(self):
        dev = self._dev('WX038Q22JVH165')
        self.assertEqual(dev._sgi_dev_capacity_route_steps(), [
            ('devanado', self.wc_dev), ('tintoreria', self.wc_color), ('rama', self.wc_rama),
            ('inspeccion', self.wc_insp)], "Ruta de la familia en kg, sin la Clasificación final, y el color al jet 4.")

    def test_02_natural_and_white_keep_family_jet(self):
        for code in ('WX038Q22JNT165', 'WX038Q22JBL165'):
            steps = dict(self._dev(code)._sgi_dev_capacity_route_steps())
            self.assertEqual(steps['tintoreria'], self.wc_jet, "%s va al jet de su familia." % code)

    def test_03_rows_per_machine_never_invent(self):
        dev = self._dev('WX038Q22JVH165')
        cap = dev._sgi_dev_start_approval_capacity()
        names = [row['name'] for row in cap['rows']]
        for wc in (self.wc_dev, self.wc_color, self.wc_rama, self.wc_insp):
            self.assertTrue(any(wc.name in name and 'capacidad teórica' in name for name in names), wc.name)
        self.assertFalse(cap['no_route'], "Con ruta de fichas el artículo sí tiene ruta.")
        self.assertTrue(any(self.wc_rama.name in name for name in cap['missing']),
                        "Sin tiempo en la ficha no hay capacidad: la fila lo dice y el aviso la lista.")
        rama = next(row for row in cap['rows'] if self.wc_rama.name in row['name'])
        self.assertEqual(rama['monthly_pct'], "Sin capacidad capturada")

    def test_04_calendar_hours(self):
        dev = self._dev('WX038Q22JNT165')
        self.assertTrue(self.wc_rama.resource_calendar_id, "El centro trae el calendario de la compañía.")
        self.assertGreater(dev._sgi_dev_capacity_hours(self.wc_rama), 0)

    def test_05_only_crudo_operations_with_fichas_route(self):
        dev = self._dev('WX038Q22JVH165')
        Bom = self.env['mrp.bom']
        Bom.create({'product_tmpl_id': self.crudo.product_tmpl_id.id, 'product_qty': 1,
                    'operation_ids': [(0, 0, {'name': 'TEJIDO PRUEBA', 'workcenter_id': self.wc_dev.id})]})
        Bom.create({'product_tmpl_id': dev.sgi_dev_product_id.product_tmpl_id.id, 'product_qty': 1,
                    'operation_ids': [(0, 0, {'name': 'ACABADO VIEJO PRUEBA', 'workcenter_id': self.wc_rama.id})]})
        names = [op.name for _product, op in dev._sgi_dev_capacity_operations()]
        self.assertEqual(names, ['TEJIDO PRUEBA'], "El acabado lo pone la ruta de fichas, no la lista vieja.")
