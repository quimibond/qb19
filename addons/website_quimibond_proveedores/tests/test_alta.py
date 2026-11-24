from odoo import http
from odoo.tests import HttpCase, tagged

from .common import PDF

RFC = 'NUE050607AB3'


@tagged('post_install', '-at_install')
class TestAltaProveedor(HttpCase):
    """Alta de proveedor en línea: llega a Compras y nada se crea hasta aprobar."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.website = cls.env['website'].search([], limit=1)
        cls.website.qb_diseno_b2b = True
        cls.website.qb_prov_alta_user_id = cls.env.ref('base.user_admin')

    def _enviar(self, **cambios):
        if not self.session:
            self.authenticate(None, None)       # sesión de visitante, para el token del formulario
        datos = {
            'csrf_token': http.Request.csrf_token(self),
            'name': 'Hilos Nuevos SA de CV', 'rfc': RFC, 'giro': 'hilo', 'contacto': 'Laura Pérez',
            'email': 'laura@hilosnuevos.test', 'telefono': '55 0000 0000', 'banco': 'Banco de prueba',
            'clabe': '002010077777777771', 'titular': 'Hilos Nuevos SA de CV', 'acepta': '1',
        }
        datos.update(cambios)
        archivos = {campo: ('%s.pdf' % campo, PDF, 'application/pdf') for campo in ('constancia', 'opinion', 'caratula')}
        if datos.pop('sin_caratula', None):
            archivos.pop('caratula')
        return self.url_open('/proveedores/alta', data=datos, files=archivos)

    def test_ingles(self):
        """Con la versión en inglés del sitio, las páginas públicas salen en inglés."""
        Lang = self.env['res.lang'].with_context(active_test=False)
        espanol = Lang._activate_lang('es_MX') or Lang._create_lang('es_MX')
        self.website.language_ids = [(4, espanol.id)]
        self.website.default_lang_id = espanol
        self.website.qb_ingles = True
        for ruta, textos in (
                ('/en/proveedores', ('Become a Quimibond supplier.', 'Issued within the last three months.',
                                     'Do not offer gifts or payments to Quimibond staff.', 'Suppliers | Quimibond')),
                ('/en/proveedores/alta', ('Legal name', 'Yarns and fibers', 'SAT compliance opinion (positive)',
                                          'Supplier registration')),
                ('/en/proveedores/alta/gracias', ('We received your request',))):
            pagina = self.url_open(ruta)
            self.assertEqual(pagina.status_code, 200, ruta)
            for texto in textos:
                self.assertIn(texto, pagina.text, '%s: %s' % (ruta, texto))
        # Los avisos del formulario también, y lo que la persona escribió se queda como está.
        if not self.session:
            self.authenticate(None, None)
        respuesta = self.url_open('/en/proveedores/alta', data={
            'csrf_token': http.Request.csrf_token(self), 'name': 'Otro', 'rfc': 'X', 'giro': 'hilo',
            'contacto': 'Laura', 'email': 'laura@hilosnuevos.test', 'acepta': '1'}, files={})
        self.assertIn('The RFC does not have the right format', respuesta.text)
        self.assertIn('This document is missing.', respuesta.text)
        self.assertIn('value="Otro"', respuesta.text)

    def test_paginas(self):
        for ruta in ('/proveedores', '/proveedores/alta', '/proveedores/alta/gracias'):
            pagina = self.url_open(ruta)
            self.assertEqual(pagina.status_code, 200, ruta)
            self.assertNotIn('Odoo', pagina.text.split('<main')[1].split('</main>')[0], ruta)

    def test_alta(self):
        Alta = self.env['qb.proveedor.alta']
        for cambios, mensaje in (
            ({'rfc': 'XX'}, 'El RFC no tiene el formato correcto'),
            ({'email': 'sin-arroba'}, 'Escriba un correo válido'),
            ({'clabe': '002010077777777772'}, 'La CLABE debe tener 18 dígitos'),
            ({'acepta': ''}, 'Marque la casilla'),
            ({'sin_caratula': True}, 'Falta este documento'),
        ):
            pagina = self._enviar(**cambios)
            self.assertIn(mensaje, pagina.text)
            self.assertIn('Hilos Nuevos SA de CV', pagina.text, 'el formulario conserva lo capturado')
        self.assertFalse(Alta.search([('rfc', '=', RFC)]))
        # Un robot que llena el campo trampa recibe las gracias y no deja nada.
        self.assertIn('Recibimos su solicitud', self._enviar(empresa_web='http://spam.test').text)
        self.assertFalse(Alta.search([('rfc', '=', RFC)]))

        pagina = self._enviar(rfc='nue-050607 ab3')
        self.assertIn('Recibimos su solicitud', pagina.text)
        alta = Alta.search([('rfc', '=', RFC)])
        self.assertEqual(len(alta), 1)
        self.assertEqual((alta.state, alta.giro, alta.contacto), ('nueva', 'hilo', 'Laura Pérez'))
        self.assertTrue(alta.constancia and alta.opinion and alta.caratula)
        self.assertTrue(alta.acepta_codigo and alta.acepta_fecha)
        self.assertEqual(alta.activity_ids.user_id, self.env.ref('base.user_admin'))
        self.assertFalse(self.env['res.partner'].search([('vat', '=', RFC)]), 'no hay contacto hasta aprobar')

        # La misma empresa no duplica la solicitud abierta.
        self._enviar()
        self.assertEqual(Alta.search_count([('rfc', '=', RFC)]), 1)

        alta.action_aprobar()
        socio = alta.partner_id
        self.assertEqual((alta.state, socio.vat, socio.supplier_rank), ('aprobada', RFC, 1))
        self.assertEqual(socio.child_ids.email, 'laura@hilosnuevos.test')
        self.assertFalse(socio.bank_ids, 'la cuenta bancaria la captura Cuentas por pagar')

    def test_aprobar_liga_el_contacto_que_ya_existe(self):
        existente = self.env['res.partner'].create({'name': 'Hilos Nuevos', 'is_company': True, 'vat': RFC})
        self._enviar()
        alta = self.env['qb.proveedor.alta'].search([('rfc', '=', RFC)])
        self.assertEqual(alta.partner_existente_id, existente)
        alta.action_aprobar()
        self.assertEqual(alta.partner_id, existente)
        self.assertEqual(existente.supplier_rank, 1)
