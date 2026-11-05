# -*- coding: utf-8 -*-
"""57.152.0: la aprobación para iniciar conserva los campos del formato anterior F-P-D01-09 / F-P-D01-19.

Dirección General 2026-10-13: encabezado con clave del formato por línea, datos generales y del
proyecto, características con nominal y tolerancias separadas, información requerida, requisitos
legales con su leyenda, capacidad a utilizar sin inventar capacidades, los dos recuadros de
aprobación y versión en inglés para el cliente extranjero."""
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestDevStartApprovalFormat(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Project = cls.env['project.project'].with_context(sgi_dev_no_autoload=True)
        mx = cls.env.ref('base.mx')
        cls.partner = cls.env['res.partner'].create({
            'name': 'CLIENTE FORMATO PRUEBA', 'is_company': True, 'country_id': mx.id,
            'street': 'Stivia 2106', 'city': 'León', 'phone': '4770000000', 'email': 'compras@cliente.example.com'})
        cls.contact = cls.env['res.partner'].create({'name': 'Vianey Prueba', 'parent_id': cls.partner.id,
                                                     'email': 'vianey@cliente.example.com'})
        Option = cls.env['sgi.dev.option']
        cls.market = Option.create({'kind': 'mercado', 'name': 'Automotriz prueba', 'name_en': 'Automotive test'})
        cls.legal = Option.create({'kind': 'requisito_legal', 'name': 'REACH prueba'})
        cls.Car = cls.env['ficha.tecnica.caracteristica']

    def _dev(self, **vals):
        return self.Project.create(dict({
            'name': 'x', 'sgi_is_ft': True, 'partner_id': self.partner.id, 'sgi_dev_contact_id': self.contact.id,
            'sgi_dev_product_name': 'Interlock 140', 'sgi_dev_use': 'Tejido interlock de 140 g/m2',
            'sgi_dev_market_id': self.market.id, 'sgi_dev_legal_ids': [(6, 0, self.legal.ids)],
            'sgi_dev_consumption_monthly': 150000, 'sgi_dev_consumption_uom': 'm', 'sgi_dev_sample_m': 5,
            'sgi_dev_requester': 'Vianey de Dios'}, **vals))

    def _line(self, dev, code, **vals):
        car = self.Car.search([('code', '=', code)], limit=1)
        self.assertTrue(car, "El catálogo de características trae «%s»." % code)
        return self.env['sgi.dev.characteristic'].create(dict({'project_id': dev.id, 'caracteristica_id': car.id}, **vals))

    def test_01_format_by_line(self):
        dev = self._dev()
        self.assertEqual(dev._sgi_dev_start_approval_format()['ref'], 'format_ref_dev_start_tejido')
        self.assertEqual(dev._sgi_dev_start_approval_format()['code'], 'F-P-D01-09',
                         "Sin documento vigente sale la clave del mapeo.")
        dev.sgi_dev_line_key = 'entretelas'
        self.assertEqual(dev._sgi_dev_start_approval_format()['code'], 'F-P-D01-19')

    def test_02_general_and_project_data(self):
        dev = self._dev()
        data = dev._sgi_dev_start_approval_data()
        self.assertEqual(data['customer'], 'CLIENTE FORMATO PRUEBA')
        self.assertEqual(data['email'], 'vianey@cliente.example.com', "El correo es el del contacto.")
        self.assertEqual(data['phone'], '4770000000', "El contacto no tiene teléfono: sale el del cliente.")
        self.assertIn('Stivia 2106', data['address'])
        self.assertEqual(data['requester'], 'Vianey de Dios')
        self.assertEqual(data['market'], 'Automotriz prueba')
        self.assertEqual(data['monthly'], '150,000 m', "Consumo mensual con su unidad (antes salía vacío).")
        self.assertEqual(data['sample_size'], '5 m')
        self.assertTrue(data['request_date'], "Sin fecha de solicitud, la de creación.")
        self.assertEqual(data['description'], 'Tejido interlock de 140 g/m2')

    def test_03_characteristics_separate_nominal_and_tolerance(self):
        dev = self._dev()
        self._line(dev, 'masa', spec_nominal=140, spec_tol_minus=5, spec_tol_plus=5)
        self._line(dev, 'ancho', spec_nominal=1.65, spec_tol_minus=0.02, spec_tol_plus=0.05)
        rows = {r['nominal']: r for r in dev._sgi_dev_start_approval_line_rows()}
        self.assertEqual(rows['140']['tolerance'], '± 5')
        self.assertEqual(rows['1.65']['tolerance'], '−0.02 / +0.05')
        self.assertEqual(rows['140']['unit'], self.Car.search([('code', '=', 'masa')], limit=1).unit)

    def test_04_required_docs_by_line(self):
        dev = self._dev()
        dev.action_sgi_dev_load_required_docs()
        names = dev.sgi_dev_required_doc_ids.option_id.mapped('name')
        self.assertIn('PPAP', names)
        self.assertNotIn('Caducidad de materiales', names, "Caducidad es solo de entretelas.")
        self.assertEqual(set(dev.sgi_dev_required_doc_ids.mapped('moment')), {'na'})
        count = len(dev.sgi_dev_required_doc_ids)
        dev.action_sgi_dev_load_required_docs()
        self.assertEqual(len(dev.sgi_dev_required_doc_ids), count, "Volver a cargar no duplica.")
        dev.sgi_dev_line_key = 'entretelas'
        dev.action_sgi_dev_load_required_docs()
        self.assertIn('Caducidad de materiales', dev.sgi_dev_required_doc_ids.option_id.mapped('name'))
        dev.sgi_dev_required_doc_ids.filtered(lambda d: d.option_id.name == 'PPAP').moment = 'diseno'
        rows = {r['name']: r['moment'] for r in dev._sgi_dev_start_approval_required_rows()}
        self.assertEqual(rows['PPAP'], 'diseno')

    def test_05_capacity_never_invents(self):
        dev = self._dev()
        cap = dev._sgi_dev_start_approval_capacity()
        self.assertTrue(cap['no_route'])
        self.assertEqual(len(cap['rows']), 1, "Sin ruta solo va la materia prima.")
        self.assertEqual(cap['rows'][0]['sample_qty'], '5 m', "Sin rendimiento no hay equivalente en kg.")
        self.assertEqual(cap['rows'][0]['sample_pct'], "Sin capacidad capturada",
                         "La materia prima no tiene capacidad en Odoo: no se inventa.")
        self._line(dev, 'masa', spec_nominal=140)
        self._line(dev, 'ancho', spec_nominal=1.65)
        self.assertAlmostEqual(dev._sgi_dev_yield_m_per_kg(), 1000 / (140 * 1.65), places=4)
        cap = dev._sgi_dev_start_approval_capacity()
        self.assertIn('kg', cap['rows'][0]['monthly_qty'], "Con rendimiento, la cantidad dice su equivalente en kg.")
        self.assertIn('ruta', dev._sgi_dev_start_approval_capacity_warning())

    def test_06_english_for_foreign_customer(self):
        dev = self._dev()
        self.assertFalse(dev._sgi_dev_start_approval_is_en())
        self.partner.country_id = self.env.ref('base.us')
        self.assertTrue(dev._sgi_dev_start_approval_is_en())
        self.assertEqual(dev._sgi_dev_start_approval_labels()['customer'], 'Customer')
        self.assertEqual(dev._sgi_dev_start_approval_data()['market'], 'Automotive test')

    def test_07_generate_warns_and_renders_every_block(self):
        dev = self._dev()
        dev.action_sgi_dev_load_required_docs()
        action = dev.action_sgi_dev_start_approval()
        self.assertEqual(action['tag'], 'display_notification', "Sin ruta avisa al generar.")
        self.assertEqual(dev.sgi_dev_start_approval_by_id, self.env.user)
        self.assertEqual(dev._sgi_dev_start_approval_approvals()['generated_by'], self.env.user.name,
                         "Quien generó sale en el recuadro de PNT Quimibond.")
        html = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_sgi.report_dev_start_approval_document', dev.ids)[0].decode()
        for text in ('Datos generales', 'Datos del proyecto', 'Información requerida', 'Capacidad a utilizar',
                     'Requisitos legales y regulatorios', 'REACH prueba', 'debe ser solicitada por el cliente',
                     'Aprobación del inicio del proyecto (cliente)', 'Aprobación del inicio del proyecto PNT Quimibond',
                     'F-P-D01-09', 'Norma a aplicar', 'Tolerancias'):
            self.assertIn(text, html, text)
