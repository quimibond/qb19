from unittest.mock import patch

from odoo import Command, http
from odoo.tests import HttpCase, tagged

from odoo.addons.website_quimibond.controllers.calidad import PARAM_EQUIPO, QuimibondCalidad, etapa_reclamo

PDF = b'%PDF-1.4 certificado de prueba'
PNG = (b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89'
       b'\x00\x00\x00\rIDATx\x9cc\xf8\xff\xff?\x00\x05\xfe\x02\xfe\xa75\x81\x84\x00\x00\x00\x00IEND\xaeB`\x82')


@tagged('post_install', '-at_install')
class TestCalidad(HttpCase):
    """Calidad en línea: lotes, certificado y reportes, cada cliente solo lo suyo."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.website = cls.env['website'].search([], limit=1)
        cls.website.qb_diseno_b2b = True
        cls.env.company.country_id = cls.env.ref('base.mx')
        cls.producto = cls.env['product.product'].create({
            'name': 'Scrim de prueba', 'is_storable': True, 'tracking': 'lot', 'list_price': 10.0,
        })
        Socio = cls.env['res.partner']
        cls.empresa_a = Socio.create({'name': 'Cliente Alfa SA', 'is_company': True, 'qb_mi_cuenta': True})
        cls.empresa_b = Socio.create({'name': 'Cliente Beta SA', 'is_company': True, 'qb_mi_cuenta': True})
        cls.contacto_a = Socio.create({'name': 'Calidad Alfa', 'parent_id': cls.empresa_a.id, 'email': 'calidad@alfa.test'})
        cls.contacto_b = Socio.create({'name': 'Calidad Beta', 'parent_id': cls.empresa_b.id, 'email': 'calidad@beta.test'})
        cls.usuario_a = cls.env['res.users'].with_context(no_reset_password=True).create({
            'name': 'Calidad Alfa', 'login': 'calidad@alfa.test', 'password': 'calidad@alfa.test',
            'partner_id': cls.contacto_a.id, 'group_ids': [Command.set([cls.env.ref('base.group_portal').id])],
        })
        cls.entrega_a = cls._entrega(cls.empresa_a, 'OC-ALFA-1', 'LOTE-ALFA-1')
        cls.entrega_b = cls._entrega(cls.empresa_b, 'OC-BETA-9', 'LOTE-BETA-9')
        Adjunto = cls.env['ir.attachment']
        cls.coa_a = Adjunto.create({'name': 'Certificado Alfa.pdf', 'raw': PDF,
                                    'res_model': 'stock.picking', 'res_id': cls.entrega_a.id})
        cls.coa_b = Adjunto.create({'name': 'Certificado Beta.pdf', 'raw': PDF,
                                    'res_model': 'stock.picking', 'res_id': cls.entrega_b.id})

    @classmethod
    def _entrega(cls, socio, referencia, lote):
        """Pedido confirmado y embarcado completo con un lote."""
        almacen = cls.env['stock.warehouse'].search([('company_id', '=', cls.env.company.id)], limit=1)
        lote = cls.env['stock.lot'].create({'name': lote, 'product_id': cls.producto.id})
        cls.env['stock.quant']._update_available_quantity(cls.producto, almacen.lot_stock_id, 5, lot_id=lote)
        pedido = cls.env['sale.order'].create({
            'partner_id': socio.id, 'client_order_ref': referencia,
            'order_line': [Command.create({'product_id': cls.producto.id, 'product_uom_qty': 5})],
        })
        pedido.action_confirm()
        salida = pedido.picking_ids
        salida.action_assign()
        salida.move_ids.picked = True
        salida.button_validate()
        assert salida.state == 'done' and salida.move_line_ids.lot_id == lote
        return salida

    def _coa(self, requerido=False):
        """Imita los campos del SGI: el certificado es el adjunto de la entrega."""
        def adjuntos(_self, picking):
            return picking.env['ir.attachment'].sudo().search([
                ('res_model', '=', 'stock.picking'), ('res_id', 'in', picking.ids), ('name', 'like', 'Certificado')])
        return (patch.object(QuimibondCalidad, '_coa_adjuntos', adjuntos),
                patch.object(QuimibondCalidad, '_coa_requerido', lambda _self, picking: requerido))

    def test_lotes_solo_los_suyos(self):
        self.authenticate('calidad@alfa.test', 'calidad@alfa.test')
        pagina = self.url_open('/mi-cuenta/calidad')
        self.assertEqual(pagina.status_code, 200)
        self.assertIn(self.entrega_a.name, pagina.text)
        self.assertIn('LOTE-ALFA-1', pagina.text)
        self.assertNotIn('LOTE-BETA-9', pagina.text)
        self.assertNotIn(self.entrega_b.name, pagina.text)
        ajena = self.url_open('/mi-cuenta/calidad?cliente=%s' % self.empresa_b.id)
        self.assertNotIn('LOTE-BETA-9', ajena.text)
        # Las demás páginas de Mi cuenta siguen abriendo con la columna del certificado.
        for ruta in ('/mi-cuenta', '/mi-cuenta/pedido/%s' % self.entrega_a.sale_id.id):
            self.assertEqual(self.url_open(ruta).status_code, 200, ruta)

    def test_certificado(self):
        adjuntos, requerido = self._coa()
        with adjuntos, requerido:
            self.authenticate('calidad@alfa.test', 'calidad@alfa.test')
            pagina = self.url_open('/mi-cuenta/calidad')
            liga = '/mi-cuenta/certificado/%s/%s' % (self.entrega_a.id, self.coa_a.id)
            self.assertIn(liga, pagina.text)
            self.assertIn(liga, self.url_open('/mi-cuenta').text)
            descarga = self.url_open(liga)
            self.assertEqual(descarga.status_code, 200)
            self.assertEqual(descarga.content, PDF)
            # El certificado de otra empresa no existe para este usuario, por ningún camino.
            for entrega, adjunto in ((self.entrega_b, self.coa_b), (self.entrega_a, self.coa_b),
                                     (self.entrega_b, self.coa_a)):
                ruta = '/mi-cuenta/certificado/%s/%s' % (entrega.id, adjunto.id)
                self.assertEqual(self.url_open(ruta).status_code, 404, ruta)
                self.assertEqual(self.url_open(ruta + '?cliente=%s' % self.empresa_b.id).status_code, 404, ruta)
            # Un adjunto cualquiera de la entrega que no es certificado tampoco sale.
            otro = self.env['ir.attachment'].create({'name': 'Costos internos.pdf', 'raw': PDF,
                                                     'res_model': 'stock.picking', 'res_id': self.entrega_a.id})
            self.assertEqual(self.url_open('/mi-cuenta/certificado/%s/%s' % (self.entrega_a.id, otro.id)).status_code, 404)

    def test_certificado_en_preparacion(self):
        self.coa_a.unlink()
        adjuntos, requerido = self._coa(requerido=True)
        with adjuntos, requerido:
            self.authenticate('calidad@alfa.test', 'calidad@alfa.test')
            self.assertIn('En preparación', self.url_open('/mi-cuenta/calidad').text)

    def test_publico_va_a_iniciar_sesion(self):
        for ruta in ('/mi-cuenta/calidad', '/mi-cuenta/certificado/%s/%s' % (self.entrega_a.id, self.coa_a.id)):
            respuesta = self.url_open(ruta, allow_redirects=False)
            self.assertIn(respuesta.status_code, (302, 303), ruta)
            self.assertIn('/web/login', respuesta.headers['Location'])

    # -- reportes de calidad (requieren la mesa de ayuda) --------------------

    def _equipo(self):
        if 'helpdesk.ticket' not in self.env:
            self.skipTest('Sin la mesa de ayuda no hay reportes de calidad.')
        Etapa = self.env['helpdesk.stage']
        equipo = self.env['helpdesk.team'].create({'name': 'Reclamaciones de prueba'})
        self.etapas = Etapa.browse()
        for secuencia, nombre, cerrada in ((1, 'Nueva', False), (2, 'Análisis', False),
                                           (3, 'Respuesta al cliente', False), (4, 'Cerrada', True)):
            self.etapas |= Etapa.create({'name': nombre, 'sequence': secuencia, 'fold': cerrada,
                                         'team_ids': [Command.set(equipo.ids)]})
        self.env['ir.config_parameter'].sudo().set_param(PARAM_EQUIPO, str(equipo.id))
        return equipo

    def test_sin_equipo_no_hay_reportes(self):
        self.env['ir.config_parameter'].sudo().set_param(PARAM_EQUIPO, '')
        if self.env['ir.model.data']._xmlid_to_res_id('quimibond_sgi.sgi_helpdesk_team_complaints', raise_if_not_found=False):
            self.skipTest('El equipo de reclamaciones del SGI existe.')
        self.authenticate('calidad@alfa.test', 'calidad@alfa.test')
        self.assertNotIn('Levantar un reporte', self.url_open('/mi-cuenta/calidad').text)
        self.assertEqual(self.url_open('/mi-cuenta/calidad/nueva').status_code, 404)

    def test_reporte_con_seguimiento(self):
        equipo = self._equipo()
        Ticket = self.env['helpdesk.ticket']
        ajeno = Ticket.create({'name': 'Reporte de Beta', 'partner_id': self.contacto_b.id, 'team_id': equipo.id})
        self.authenticate('calidad@alfa.test', 'calidad@alfa.test')

        # Paso 1: elegir la entrega. Solo salen las suyas.
        elegir = self.url_open('/mi-cuenta/calidad/nueva')
        self.assertIn(self.entrega_a.name, elegir.text)
        self.assertNotIn(self.entrega_b.name, elegir.text)
        self.assertEqual(self.url_open('/mi-cuenta/calidad/nueva?entrega=%s' % self.entrega_b.id).status_code, 404)

        # Paso 2: el reporte, con su lote y una foto.
        lote = self.entrega_a.move_line_ids.lot_id
        datos = {
            'csrf_token': http.Request.csrf_token(self), 'entrega': self.entrega_a.id,
            'lote': '%s-%s' % (self.producto.id, lote.id), 'motivo': 'calidad', 'cantidad': '350 m',
            'descripcion': 'Manchas de aceite a lo largo del rollo.',
        }
        corto = self.url_open('/mi-cuenta/calidad/nueva', data=dict(datos, descripcion='Mal'))
        self.assertIn('Cuéntenos qué encontró', corto.text)
        self.assertFalse(Ticket.search([('partner_id', '=', self.contacto_a.id)]))
        malo = self.url_open('/mi-cuenta/calidad/nueva', data=datos, files={'fotos': ('virus.exe', b'MZ', 'application/octet-stream')})
        self.assertIn('Solo se aceptan fotos', malo.text)
        ajena = self.url_open('/mi-cuenta/calidad/nueva', data=dict(datos, entrega=self.entrega_b.id))
        self.assertEqual(ajena.status_code, 404)
        self.assertFalse(Ticket.search([('partner_id', '=', self.contacto_a.id)]))

        creado = self.url_open('/mi-cuenta/calidad/nueva', data=datos, files={'fotos': ('rollo.png', PNG, 'image/png')})
        self.assertEqual(creado.status_code, 200)
        ticket = Ticket.search([('partner_id', '=', self.contacto_a.id)])
        self.assertEqual(len(ticket), 1)
        self.assertEqual(ticket.team_id, equipo)
        self.assertIn('LOTE-ALFA-1', ticket.name)
        self.assertIn('LOTE-ALFA-1', ticket.description)
        self.assertIn('Manchas de aceite', ticket.description)
        if 'sgi_lot_id' in ticket._fields:
            self.assertEqual(ticket.sgi_lot_id, lote)
            self.assertEqual(ticket.sgi_sale_order_id, self.entrega_a.sale_id)
        foto = ticket.message_ids.attachment_ids
        self.assertEqual(foto.mapped('name'), ['rollo.png'])
        self.assertIn('Recibimos su reporte', creado.text)
        self.assertIn('Recibida', creado.text)
        self.assertEqual(etapa_reclamo(ticket)['clave'], 'recibida')

        # Seguimiento: la nota interna no sale; la respuesta pública sí.
        interno = self.env.ref('base.user_admin').partner_id
        nota = ticket.message_post(body='Causa raíz: fuga en la máquina 7', author_id=interno.id,
                                   message_type='comment', subtype_xmlid='mail.mt_note',
                                   attachments=[('analisis interno.pdf', PDF)])
        ticket.stage_id = self.etapas[1]
        base = '/mi-cuenta/calidad/reclamacion/%s' % ticket.id
        pagina = self.url_open(base)
        self.assertIn('En análisis', pagina.text)
        self.assertNotIn('Causa raíz', pagina.text)
        self.assertNotIn('analisis interno', pagina.text)
        self.assertEqual(self.url_open('%s/archivo/%s' % (base, nota.attachment_ids.id)).status_code, 404)
        self.assertEqual(self.url_open('%s/archivo/%s' % (base, foto.id)).status_code, 200)

        ticket.message_post(body='Reponemos los 350 m el lunes.', author_id=interno.id,
                            message_type='comment', subtype_xmlid='mail.mt_comment')
        ticket.stage_id = self.etapas[2]
        pagina = self.url_open(base)
        self.assertIn('Respuesta de Quimibond', pagina.text)
        self.assertIn('Reponemos los 350 m', pagina.text)
        self.assertNotIn(interno.name, pagina.text)

        # El cliente contesta.
        self.url_open(base + '/mensaje', data={'csrf_token': http.Request.csrf_token(self), 'mensaje': 'De acuerdo, gracias.'})
        self.assertIn('De acuerdo, gracias.', ticket.message_ids[0].body)
        self.assertEqual(ticket.message_ids[0].author_id, self.contacto_a)
        self.assertIn('De acuerdo, gracias.', self.url_open(base).text)

        # El reporte de otra empresa no existe para este usuario.
        ruta_ajena = '/mi-cuenta/calidad/reclamacion/%s' % ajeno.id
        self.assertEqual(self.url_open(ruta_ajena).status_code, 404)
        self.assertEqual(self.url_open(ruta_ajena + '?cliente=%s' % self.empresa_b.id).status_code, 404)
        self.assertEqual(self.url_open(ruta_ajena + '/mensaje', data={
            'csrf_token': http.Request.csrf_token(self), 'mensaje': 'Intruso'}).status_code, 404)
        self.assertEqual(self.url_open('%s/archivo/%s' % (ruta_ajena, foto.id)).status_code, 404)
        self.assertNotIn('Reporte de Beta', self.url_open('/mi-cuenta/calidad').text)

        # Cerrado: ya no se escribe.
        ticket.stage_id = self.etapas[3]
        self.assertEqual(self.url_open(base + '/mensaje', data={
            'csrf_token': http.Request.csrf_token(self), 'mensaje': 'Otra cosa'}).status_code, 403)
        self.assertIn('Este reporte está cerrado', self.url_open(base).text)

    def test_vista_previa_no_escribe(self):
        equipo = self._equipo()
        self.authenticate('admin', 'admin')
        previa = self.url_open('/mi-cuenta/calidad?cliente=%s' % self.empresa_a.id)
        self.assertEqual(previa.status_code, 200)
        self.assertIn('LOTE-ALFA-1', previa.text)
        respuesta = self.url_open('/mi-cuenta/calidad/nueva?cliente=%s' % self.empresa_a.id, data={
            'csrf_token': http.Request.csrf_token(self), 'entrega': self.entrega_a.id, 'motivo': 'calidad',
            'descripcion': 'Esto no debe crear nada.',
        })
        self.assertEqual(respuesta.status_code, 403)
        self.assertFalse(self.env['helpdesk.ticket'].search([('team_id', '=', equipo.id)]))
