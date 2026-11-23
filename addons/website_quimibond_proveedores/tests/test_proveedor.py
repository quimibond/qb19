from datetime import date

from odoo import Command, http
from odoo.tests import HttpCase, tagged

from ..controllers.proveedor import estado_pago, fecha_programada
from .common import PDF, RFC_ALFA, RFC_BETA, RFC_EMPRESA, cfdi_xml


@tagged('post_install', '-at_install')
class TestPortalProveedor(HttpCase):
    """Portal de proveedores: cada proveedor ve y factura solo lo suyo."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.website = cls.env['website'].search([], limit=1)
        cls.website.qb_diseno_b2b = True
        cls.website.qb_prov_dia_pago = False
        cls.env.company.country_id = cls.env.ref('base.mx')
        cls.env.company.vat = RFC_EMPRESA
        cls.producto = cls.env['product.product'].create({
            'name': 'Hilo de prueba', 'is_storable': True, 'standard_price': 100.0, 'purchase_method': 'receive',
        })
        Socio = cls.env['res.partner']
        cls.alfa = Socio.create({'name': 'Proveedor Alfa SA', 'is_company': True, 'vat': RFC_ALFA})
        cls.beta = Socio.create({'name': 'Proveedor Beta SA', 'is_company': True, 'vat': RFC_BETA})
        cls.contacto = Socio.create({'name': 'Ventas Alfa', 'parent_id': cls.alfa.id, 'email': 'ventas@alfa.test'})
        cls.usuario = cls.env['res.users'].with_context(no_reset_password=True).create({
            'name': 'Ventas Alfa', 'login': 'ventas@alfa.test', 'password': 'ventas@alfa.test',
            'partner_id': cls.contacto.id, 'group_ids': [Command.set([cls.env.ref('base.group_portal').id])],
        })
        cls.orden_a = cls._orden(cls.alfa, 'COT-ALFA-1')
        cls.orden_b = cls._orden(cls.beta, 'COT-BETA-9')

    @classmethod
    def _orden(cls, socio, referencia):
        orden = cls.env['purchase.order'].create({
            'partner_id': socio.id, 'partner_ref': referencia,
            'order_line': [Command.create({'product_id': cls.producto.id, 'product_qty': 5, 'price_unit': 100.0})],
        })
        orden.button_confirm()
        return orden

    def _recibir(self, orden):
        entrada = orden.picking_ids
        entrada.move_ids.quantity = 5
        entrada.move_ids.picked = True
        entrada.button_validate()
        self.assertEqual(orden.invoice_status, 'to invoice')

    def _xml(self, uuid, **kw):
        # La moneda de la compañía de prueba no siempre es MXN: el CFDI va en la de la orden.
        kw.setdefault('moneda', self.orden_a.currency_id.name)
        return cfdi_xml(uuid, **kw)

    def _subir(self, orden, xml, pdf=PDF):
        return self.url_open(
            '/mi-cuenta/proveedor/orden/%s/factura' % orden.id,
            data={'csrf_token': http.Request.csrf_token(self)},
            files={'xml': ('factura.xml', xml, 'text/xml'), 'pdf': ('factura.pdf', pdf, 'application/pdf')})

    # -- reglas de fecha -------------------------------------------------------

    def test_fecha_programada(self):
        hoy = date(2026, 11, 20)                                                # viernes
        self.assertEqual(fecha_programada(date(2026, 11, 25), hoy), date(2026, 11, 25))
        self.assertFalse(fecha_programada(date(2026, 11, 10), hoy))            # ya venció y no hay día fijo
        self.assertEqual(fecha_programada(date(2026, 11, 25), hoy, '4'), date(2026, 11, 27))   # siguiente viernes
        self.assertEqual(fecha_programada(date(2026, 11, 27), hoy, '4'), date(2026, 11, 27))
        self.assertEqual(fecha_programada(date(2026, 11, 10), hoy, '4'), hoy)   # vencida: el próximo día de pago
        self.assertFalse(fecha_programada(False, hoy, '4'))

    # -- acceso ----------------------------------------------------------------

    def test_publico_va_a_iniciar_sesion(self):
        respuesta = self.url_open('/mi-cuenta/proveedor', allow_redirects=False)
        self.assertIn(respuesta.status_code, (302, 303))
        self.assertIn('/web/login', respuesta.headers['Location'])

    def test_proveedor_ve_solo_lo_suyo(self):
        self.authenticate('ventas@alfa.test', 'ventas@alfa.test')
        # Quien solo es proveedor entra directo a su portal.
        inicio = self.url_open('/mi-cuenta', allow_redirects=False)
        self.assertIn(inicio.status_code, (302, 303))
        self.assertTrue(inicio.headers['Location'].endswith('/mi-cuenta/proveedor'))

        resumen = self.url_open('/mi-cuenta/proveedor')
        self.assertEqual(resumen.status_code, 200)
        self.assertIn('Proveedor Alfa SA', resumen.text)
        self.assertIn(self.orden_a.name, resumen.text)
        for ruta in ('/mi-cuenta/proveedor', '/mi-cuenta/proveedor/ordenes', '/mi-cuenta/proveedor/pagos',
                     '/mi-cuenta/proveedor/orden/%s' % self.orden_a.id):
            for sufijo in ('', '?proveedor=%s' % self.beta.id):
                pagina = self.url_open(ruta + sufijo)
                self.assertEqual(pagina.status_code, 200, ruta)
                self.assertNotIn('COT-BETA-9', pagina.text, ruta)
                self.assertNotIn(self.orden_b.name, pagina.text, ruta)
                self.assertNotIn('Proveedor Beta', pagina.text, ruta)
        # La orden de otro proveedor no existe para este usuario: ni verla ni facturarla.
        self.assertEqual(self.url_open('/mi-cuenta/proveedor/orden/%s' % self.orden_b.id).status_code, 404)
        self.assertEqual(self.url_open(
            '/mi-cuenta/proveedor/orden/%s?proveedor=%s' % (self.orden_b.id, self.beta.id)).status_code, 404)
        self._recibir(self.orden_b)
        ajena = self._subir(self.orden_b, self._xml('AAAAAAAA-0000-4000-8000-000000000001', emisor=RFC_BETA))
        self.assertEqual(ajena.status_code, 404)
        self.assertFalse(self.env['qb.proveedor.factura'].search([('purchase_id', '=', self.orden_b.id)]))

    def test_cliente_sin_compras_no_entra(self):
        socio = self.env['res.partner'].create({'name': 'Solo Cliente', 'email': 'solo@cliente.test'})
        self.env['res.users'].with_context(no_reset_password=True).create({
            'name': 'Solo Cliente', 'login': 'solo@cliente.test', 'password': 'solo@cliente.test',
            'partner_id': socio.id, 'group_ids': [Command.set([self.env.ref('base.group_portal').id])],
        })
        self.authenticate('solo@cliente.test', 'solo@cliente.test')
        respuesta = self.url_open('/mi-cuenta/proveedor', allow_redirects=False)
        self.assertIn(respuesta.status_code, (302, 303))
        self.assertTrue(respuesta.headers['Location'].endswith('/mi-cuenta'))
        self.assertNotIn('Portal de proveedores', self.url_open('/mi-cuenta').text)

    # -- subir factura ---------------------------------------------------------

    def test_subir_factura(self):
        Factura = self.env['qb.proveedor.factura']
        self._recibir(self.orden_a)
        self.authenticate('ventas@alfa.test', 'ventas@alfa.test')
        self.assertIn('Subir factura', self.url_open('/mi-cuenta/proveedor/orden/%s' % self.orden_a.id).text)

        # No es de su empresa, no es para la empresa, no es factura, no es XML: nada se guarda.
        uuid = 'AAAAAAAA-0000-4000-8000-000000000002'
        for xml, mensaje in (
            (self._xml(uuid, emisor=RFC_BETA), 'no es el de su empresa'),
            (self._xml(uuid, receptor='OTR010101AAA'), 'no está a nombre de'),
            (self._xml(uuid, tipo='P'), 'no es una factura'),
            (b'%PDF-1.4 esto no es xml', 'No pudimos recibir la factura'),
        ):
            pagina = self._subir(self.orden_a, xml)
            self.assertEqual(pagina.status_code, 200)
            self.assertIn(mensaje, pagina.text)
        self.assertIn('Suba también la factura en PDF', self._subir(self.orden_a, self._xml(uuid), pdf=b'hola').text)
        self.assertFalse(Factura.search([('purchase_id', '=', self.orden_a.id)]))

        # La buena: 5 x 100 recibidos, factura por 500 antes de impuestos.
        pagina = self._subir(self.orden_a, self._xml(uuid, subtotal=500, total=580, folio='777'))
        self.assertEqual(pagina.status_code, 200)
        self.assertIn('Recibimos su factura', pagina.text)
        self.assertIn('Coincide con lo recibido', pagina.text)
        factura = Factura.search([('purchase_id', '=', self.orden_a.id)])
        self.assertEqual(len(factura), 1)
        self.assertEqual((factura.name, factura.uuid, factura.state), ('F-777', uuid, 'recibida'))
        self.assertTrue(factura.cuadra)
        self.assertEqual(factura.partner_id, self.alfa)
        self.assertEqual(factura.usuario_id, self.usuario)
        self.assertTrue(factura.xml_file and factura.pdf_file)
        self.assertFalse(self.orden_a.invoice_ids, 'subir la factura no crea asientos')

        # Repetida, y otra mientras la primera está en revisión.
        self.assertIn('ya se había subido', self._subir(self.orden_a, self._xml(uuid)).text)
        pagos = self.url_open('/mi-cuenta/proveedor/pagos')
        self.assertIn('F-777', pagos.text)
        self.assertIn('qb-estado--revision', pagos.text)
        self.assertIn('Factura en revisión', self.url_open('/mi-cuenta/proveedor/ordenes').text)

        # Cuentas por pagar la rechaza: el proveedor ve el motivo y puede subir otra.
        factura.motivo_rechazo = 'El uso del CFDI no es el acordado'
        factura.action_rechazar()
        pagos = self.url_open('/mi-cuenta/proveedor/pagos')
        self.assertIn('Rechazada', pagos.text)
        self.assertIn('El uso del CFDI no es el acordado', pagos.text)

        # Importe distinto a lo recibido: se recibe con aviso.
        otra = 'AAAAAAAA-0000-4000-8000-000000000003'
        pagina = self._subir(self.orden_a, self._xml(otra, subtotal=450, total=522, folio='778'))
        self.assertIn('no coincide con lo recibido', pagina.text)
        segunda = Factura.search([('uuid', '=', otra)])
        self.assertFalse(segunda.cuadra)
        self.assertEqual(segunda.por_facturar, 500.0)

    def test_factura_contable_y_estado_de_pago(self):
        self._recibir(self.orden_a)
        factura = self.env['qb.proveedor.factura']._recibir(
            self.orden_a, self._xml('AAAAAAAA-0000-4000-8000-000000000004', folio='900'), 'f.xml', PDF, 'f.pdf', self.usuario)
        factura.action_crear_factura()
        move = factura.move_id
        self.assertEqual(factura.state, 'aceptada')
        self.assertEqual((move.move_type, move.state, move.ref), ('in_invoice', 'draft', 'F-900'))
        self.assertEqual(move.amount_untaxed, 500.0)
        self.assertEqual(len(self.env['ir.attachment'].search([('res_model', '=', 'account.move'), ('res_id', '=', move.id)])), 2)

        hoy = date(2026, 11, 20)
        self.assertEqual(estado_pago(move, hoy)[0], 'revision')
        move.invoice_date_due = date(2026, 11, 25)
        move.action_post()
        self.assertEqual(estado_pago(move, hoy)[::2], ('programada', date(2026, 11, 25)))
        self.assertEqual(estado_pago(move, hoy, '4')[::2], ('programada', date(2026, 11, 27)))
        self.assertEqual(estado_pago(move, date(2026, 12, 1))[0], 'proceso')

        self.authenticate('ventas@alfa.test', 'ventas@alfa.test')
        pagos = self.url_open('/mi-cuenta/proveedor/pagos')
        self.assertIn('F-900', pagos.text)
        self.assertIn('Pago programado', pagos.text)
        self.assertNotIn('qb-estado--revision', pagos.text)

        self.env['account.payment.register'].with_context(
            active_model='account.move', active_ids=move.ids).create({})._create_payments()
        self.assertEqual(estado_pago(move, hoy)[0], 'pagada')
        self.assertIn('Pagada', self.url_open('/mi-cuenta/proveedor/pagos').text)

    # -- vista previa y apagado ------------------------------------------------

    def test_vista_previa_solo_lectura(self):
        self._recibir(self.orden_b)
        self.authenticate('admin', 'admin')
        previa = self.url_open('/mi-cuenta/proveedor?proveedor=%s' % self.beta.id)
        self.assertEqual(previa.status_code, 200)
        self.assertIn('Vista previa', previa.text)
        self.assertIn(self.orden_b.name, previa.text)
        detalle = self.url_open('/mi-cuenta/proveedor/orden/%s?proveedor=%s' % (self.orden_b.id, self.beta.id))
        self.assertEqual(detalle.status_code, 200)
        self.assertNotIn('name="xml"', detalle.text)
        self.assertEqual(self._subir(self.orden_b, self._xml('AAAAAAAA-0000-4000-8000-000000000005', emisor=RFC_BETA)).status_code, 404)
        sin = self.url_open('/mi-cuenta/proveedor', allow_redirects=False)
        self.assertIn(sin.status_code, (302, 303))

    def test_apagado(self):
        self.website.qb_diseno_b2b = False
        self.authenticate('ventas@alfa.test', 'ventas@alfa.test')
        self.assertEqual(self.url_open('/mi-cuenta/proveedor').status_code, 404)
        self.assertEqual(self.url_open('/proveedores').status_code, 404)
