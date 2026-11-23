from odoo import Command
from odoo.tests import HttpCase, tagged


@tagged('post_install', '-at_install')
class TestMiCuenta(HttpCase):
    """Mi cuenta: cada cliente ve solo lo de su empresa."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.website = cls.env['website'].search([], limit=1)
        cls.website.qb_diseno_b2b = True
        cls.env.company.country_id = cls.env.ref('base.mx')
        cls.producto = cls.env['product.product'].create({
            'name': 'Scrim de prueba', 'is_storable': True, 'list_price': 10.0,
        })
        Socio = cls.env['res.partner']
        cls.empresa_a = Socio.create({'name': 'Cliente Alfa SA', 'is_company': True})
        cls.empresa_b = Socio.create({'name': 'Cliente Beta SA', 'is_company': True})
        cls.contacto_a = Socio.create({'name': 'Compras Alfa', 'parent_id': cls.empresa_a.id, 'email': 'compras@alfa.test'})
        cls.usuario_a = cls.env['res.users'].with_context(no_reset_password=True).create({
            'name': 'Compras Alfa', 'login': 'compras@alfa.test', 'password': 'compras@alfa.test',
            'partner_id': cls.contacto_a.id, 'group_ids': [Command.set([cls.env.ref('base.group_portal').id])],
        })
        cls.pedido_a = cls._pedido(cls.empresa_a, 'OC-ALFA-1')
        cls.pedido_b = cls._pedido(cls.empresa_b, 'OC-BETA-9')

    @classmethod
    def _pedido(cls, socio, referencia):
        pedido = cls.env['sale.order'].create({
            'partner_id': socio.id, 'client_order_ref': referencia,
            'order_line': [Command.create({'product_id': cls.producto.id, 'product_uom_qty': 5})],
        })
        pedido.action_confirm()
        return pedido

    def test_etapas(self):
        from odoo.addons.website_quimibond.controllers.cuenta import etapa_pedido
        etapa = etapa_pedido(self.pedido_a)
        self.assertEqual(etapa['clave'], 'preparacion')
        self.assertEqual(etapa['paso'], 1)
        salida = self.pedido_a.picking_ids
        salida.move_ids.quantity = 5
        salida.move_ids.picked = True
        salida.button_validate()
        self.assertEqual(self.pedido_a.delivery_status, 'full')
        self.assertEqual(etapa_pedido(self.pedido_a)['clave'], 'entregado')

    def test_publico_va_a_iniciar_sesion(self):
        respuesta = self.url_open('/mi-cuenta', allow_redirects=False)
        self.assertIn(respuesta.status_code, (302, 303))
        self.assertIn('/web/login', respuesta.headers['Location'])

    def test_cliente_ve_solo_lo_suyo(self):
        self.authenticate('compras@alfa.test', 'compras@alfa.test')
        inicio = self.url_open('/my', allow_redirects=False)
        self.assertIn(inicio.status_code, (302, 303))
        self.assertTrue(inicio.headers['Location'].endswith('/mi-cuenta'))
        self.assertEqual(self.url_open('/my?todo=1').status_code, 200)

        cuenta = self.url_open('/mi-cuenta')
        self.assertEqual(cuenta.status_code, 200)
        self.assertIn('Cliente Alfa SA', cuenta.text)
        self.assertIn(self.pedido_a.name, cuenta.text)
        self.assertIn('OC-ALFA-1', cuenta.text)
        self.assertIn('En preparación', cuenta.text)
        self.assertNotIn('OC-BETA-9', cuenta.text)
        self.assertNotIn('Cliente Beta', cuenta.text)

        for ruta in ('/mi-cuenta/pedidos', '/mi-cuenta/estado-de-cuenta', '/mi-cuenta/pedido/%s' % self.pedido_a.id):
            pagina = self.url_open(ruta)
            self.assertEqual(pagina.status_code, 200, ruta)
            self.assertNotIn('OC-BETA-9', pagina.text, ruta)
        # El pedido de otra empresa no existe para este usuario, ni pidiéndolo con ?cliente=.
        self.assertEqual(self.url_open('/mi-cuenta/pedido/%s' % self.pedido_b.id).status_code, 404)
        ajeno = self.url_open('/mi-cuenta?cliente=%s' % self.empresa_b.id)
        self.assertNotIn('OC-BETA-9', ajeno.text)
        self.assertNotIn('Cliente Beta', ajeno.text)
        self.assertEqual(self.url_open('/mi-cuenta/pedido/%s?cliente=%s' % (self.pedido_b.id, self.empresa_b.id)).status_code, 404)

    def test_factura_y_saldo(self):
        factura = self.pedido_a._create_invoices()
        factura.action_post()
        ajena = self.pedido_b._create_invoices()
        ajena.action_post()
        self.authenticate('compras@alfa.test', 'compras@alfa.test')
        estado = self.url_open('/mi-cuenta/estado-de-cuenta')
        self.assertEqual(estado.status_code, 200)
        self.assertIn(factura.name, estado.text)
        self.assertIn('Por pagar', estado.text)
        self.assertIn('PDF y XML', estado.text)
        self.assertNotIn(ajena.name, estado.text)
        self.assertIn(factura.name, self.url_open('/mi-cuenta').text)

    def test_vista_previa_solo_gerente_de_ventas(self):
        self.authenticate('admin', 'admin')
        previa = self.url_open('/mi-cuenta?cliente=%s' % self.empresa_b.id)
        self.assertEqual(previa.status_code, 200)
        self.assertIn('Vista previa', previa.text)
        self.assertIn('OC-BETA-9', previa.text)
        # Sin cliente, el usuario interno va a su Odoo.
        sin = self.url_open('/mi-cuenta', allow_redirects=False)
        self.assertIn(sin.status_code, (302, 303))

    def test_apagado(self):
        self.website.qb_diseno_b2b = False
        self.authenticate('compras@alfa.test', 'compras@alfa.test')
        self.assertEqual(self.url_open('/mi-cuenta').status_code, 404)
        self.assertEqual(self.url_open('/my').status_code, 200)
