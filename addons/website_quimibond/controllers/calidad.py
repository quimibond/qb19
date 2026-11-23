"""Calidad en línea: certificado por lote y reclamaciones dentro de «Mi cuenta».

Dos cosas, las dos sobre lo que ya existe en la base:

* **Certificado de calidad** junto a cada entrega. El certificado es el PDF que
  Laboratorio adjunta a la salida (`stock.picking.sgi_coa_attachment_ids`, del
  SGI). Aquí no se genera nada: si el PDF está, se descarga; si el cliente lo
  tiene acordado y todavía no está, dice «En preparación». También se listan
  los lotes de cada entrega, que son los de la remisión.
* **Reclamaciones con seguimiento.** El cliente levanta el reporte con la
  entrega, los lotes y fotos; queda como ticket del equipo de reclamaciones de
  la mesa de ayuda (el del SGI, o el del parámetro
  `website_quimibond.equipo_reclamaciones`) y ve la etapa y los mensajes.

Este módulo corre también sin la mesa de ayuda y sin el SGI (el CI es
Community): todo se pregunta en el momento (`'helpdesk.ticket' in env`,
`'sgi_coa_attachment_ids' in picking._fields`). Sin ellos, la página existe y
muestra los lotes; el certificado y las reclamaciones no aparecen.

Seguridad, igual que en `cuenta.py`: se lee con `sudo()` y siempre filtrado por
la empresa del usuario. Un archivo se entrega solo si cuelga de una entrega o
de una reclamación de esa empresa; lo de otra empresa responde 404. De una
reclamación solo salen los mensajes públicos, nunca las notas internas, ni la
no conformidad que Calidad abra por dentro.
"""
from collections import OrderedDict
from datetime import timedelta

from werkzeug.exceptions import Forbidden, NotFound
from werkzeug.urls import url_encode

from odoo import fields, http
from odoo.http import request
from odoo.tools import plaintext2html

from .cuenta import MESES_HISTORIAL, QuimibondCuenta

PARAM_EQUIPO = 'website_quimibond.equipo_reclamaciones'
XMLID_EQUIPO_SGI = 'quimibond_sgi.sgi_helpdesk_team_complaints'

LIMITE_ENTREGAS = 100
LIMITE_RECLAMOS = 100

MOTIVOS = OrderedDict([
    ('calidad', 'Calidad de la tela'),
    ('cantidad', 'Cantidad o peso'),
    ('entrega', 'Entrega o empaque'),
    ('documentos', 'Documentos'),
    ('otro', 'Otro'),
])

EXTENSIONES = ('.jpg', '.jpeg', '.png', '.webp', '.heic', '.pdf')
IMAGENES = ('image/jpeg', 'image/png', 'image/webp')
MAX_ARCHIVOS = 8
MAX_BYTES = 10 * 1024 * 1024
MAX_TEXTO = 5000

PASOS_RECLAMO = ['Recibida', 'En análisis', 'Respuesta', 'Cerrada']


def etapa_reclamo(ticket):
    """Etapa de la reclamación para el cliente: {'clave', 'texto', 'paso', 'pasos'}.

    Las etapas internas (contención, análisis de causa…) se resumen en cuatro
    pasos; el nombre interno de la etapa no se muestra.
    """
    etapa = ticket.stage_id
    nombre = (etapa.name or '').strip().lower()
    primera = ticket.team_id.stage_ids.sorted('sequence')[:1]
    if etapa.fold:
        clave, texto, paso = 'cerrada', 'Cerrada', 3
    elif 'respuesta' in nombre:
        clave, texto, paso = 'respuesta', 'Con respuesta', 2
    elif not etapa or etapa == primera or nombre.startswith(('nuev', 'new')):
        clave, texto, paso = 'recibida', 'Recibida', 0
    else:
        clave, texto, paso = 'analisis', 'En análisis', 1
    return {'clave': clave, 'texto': texto, 'paso': paso, 'pasos': PASOS_RECLAMO}


class QuimibondCalidad(QuimibondCuenta):

    # -- ligas ---------------------------------------------------------------

    def _liga(self, ruta, **params):
        """Ruta con sus parámetros, conservando la vista previa (?cliente=)."""
        cliente = request.params.get('cliente')
        if cliente and str(cliente).isdigit():
            params['cliente'] = int(cliente)
        return '%s?%s' % (ruta, url_encode(params)) if params else ruta

    # -- entregas, lotes y certificado ---------------------------------------

    def _dominio_entregas(self, empresa):
        return [
            ('sale_id.partner_id', 'child_of', empresa.id), ('sale_id.company_id', 'in', self._companias()),
            ('picking_type_code', '=', 'outgoing'), ('state', '=', 'done'),
        ]

    def _entrega(self, empresa, entrega_id):
        """La entrega embarcada de esa empresa, o un recordset vacío."""
        return request.env['stock.picking'].sudo().search(
            self._dominio_entregas(empresa) + [('id', '=', entrega_id)], limit=1)

    def _lotes(self, picking):
        """Lotes de la entrega: [{'producto', 'lote', 'cantidad', 'unidad', …}], uno por producto y lote."""
        grupos = OrderedDict()
        metro = request.env.ref('uom.product_uom_meter', raise_if_not_found=False)
        for linea in picking.move_line_ids:
            if not linea.quantity:
                continue
            fila = grupos.setdefault((linea.product_id.id, linea.lot_id.id), {
                'clave': '%s-%s' % (linea.product_id.id, linea.lot_id.id or 0),
                'producto_id': linea.product_id.id,
                'producto': linea.product_id.display_name,
                'lote_id': linea.lot_id.id,
                'lote': linea.lot_id.name or '',
                'cantidad': 0.0,
                'unidad': linea.product_uom_id.name,
                'en_metros': bool(metro) and linea.product_uom_id == metro,
            })
            fila['cantidad'] += linea.quantity
        return list(grupos.values())

    def _coa_adjuntos(self, picking):
        """PDF del certificado ligados a la entrega (campo del SGI); vacío si el SGI no está."""
        if 'sgi_coa_attachment_ids' in picking._fields:
            return picking.sgi_coa_attachment_ids
        return request.env['ir.attachment'].sudo()

    def _coa_requerido(self, picking):
        return bool('sgi_requires_coa' in picking._fields and picking.sgi_requires_coa)

    def _certificado(self, picking):
        """{'estado': 'disponible' | 'pendiente' | '', 'archivos': [{'nombre', 'url'}]}."""
        if picking.state != 'done':
            return {'estado': '', 'archivos': []}
        adjuntos = self._coa_adjuntos(picking)
        if adjuntos:
            return {'estado': 'disponible', 'archivos': [{
                'nombre': adjunto.name or 'Certificado',
                'url': self._liga('/mi-cuenta/certificado/%s/%s' % (picking.id, adjunto.id)),
            } for adjunto in adjuntos]}
        return {'estado': 'pendiente' if self._coa_requerido(picking) else '', 'archivos': []}

    def _entrega_vals(self, picking):
        vals = super()._entrega_vals(picking)
        vals.update({
            'id': picking.id,
            'lotes': self._lotes(picking) if picking.state == 'done' else [],
            'certificado': self._certificado(picking),
            'reclamar': self._liga('/mi-cuenta/calidad/nueva', entrega=picking.id),
        })
        return vals

    # -- reclamaciones -------------------------------------------------------

    def _equipo(self):
        """Equipo de la mesa de ayuda que recibe las reclamaciones, o None si no hay."""
        if 'helpdesk.team' not in request.env:
            return None
        Equipo = request.env['helpdesk.team'].sudo()
        param = (request.env['ir.config_parameter'].sudo().get_param(PARAM_EQUIPO) or '').strip()
        equipo = Equipo.browse(int(param)).exists() if param.isdigit() else Equipo
        if not equipo:
            equipo_id = request.env['ir.model.data'].sudo()._xmlid_to_res_id(XMLID_EQUIPO_SGI, raise_if_not_found=False)
            equipo = Equipo.browse(equipo_id).exists() if equipo_id else Equipo
        if not equipo or (equipo.company_id and equipo.company_id.id not in self._companias()):
            return None
        return equipo

    def _dominio_reclamos(self, empresa, equipo):
        return [('partner_id', 'child_of', empresa.id), ('team_id', '=', equipo.id)]

    def _reclamo(self, empresa, reclamo_id):
        equipo = self._equipo()
        if not equipo:
            return None
        return request.env['helpdesk.ticket'].sudo().search(
            self._dominio_reclamos(empresa, equipo) + [('id', '=', reclamo_id)], limit=1) or None

    def _reclamo_vals(self, ticket):
        folio = ticket.ticket_ref if 'ticket_ref' in ticket._fields and ticket.ticket_ref else str(ticket.id)
        return {
            'registro': ticket,
            'id': ticket.id,
            'folio': folio,
            'asunto': ticket.name,
            'fecha': ticket.create_date,
            'etapa': etapa_reclamo(ticket),
            'url': self._liga('/mi-cuenta/calidad/reclamacion/%s' % ticket.id),
        }

    def _mensajes(self, ticket, empresa):
        """Mensajes públicos de la reclamación, del más viejo al más nuevo. Sin notas internas."""
        mensajes = []
        for mensaje in ticket.message_ids.sorted(lambda m: (m.date, m.id)):
            if mensaje.message_type not in ('comment', 'email', 'email_outgoing'):
                continue
            if not mensaje.subtype_id or mensaje.subtype_id.internal:
                continue
            if 'is_internal' in mensaje._fields and mensaje.is_internal:
                continue
            if not mensaje.body and not mensaje.attachment_ids:
                continue
            del_cliente = bool(mensaje.author_id and mensaje.author_id.commercial_partner_id == empresa)
            mensajes.append({
                'registro': mensaje,
                'del_cliente': del_cliente,
                'autor': mensaje.author_id.name if del_cliente else 'Quimibond',
                'fecha': mensaje.date,
                'cuerpo': mensaje.body,
                'adjuntos': [{
                    'nombre': adjunto.name,
                    'imagen': adjunto.mimetype in IMAGENES,
                    'url': self._liga('/mi-cuenta/calidad/reclamacion/%s/archivo/%s' % (ticket.id, adjunto.id)),
                } for adjunto in mensaje.attachment_ids],
            })
        return mensajes

    def _archivos(self):
        """([(nombre, datos)], error) con las fotos o PDF que vienen en el formulario."""
        archivos = [a for a in request.httprequest.files.getlist('fotos') if a and a.filename]
        if len(archivos) > MAX_ARCHIVOS:
            return [], 'Puede adjuntar hasta %s archivos.' % MAX_ARCHIVOS
        salida = []
        for archivo in archivos:
            nombre = archivo.filename.replace('\\', '/').rsplit('/', 1)[-1][:120]
            if not nombre.lower().endswith(EXTENSIONES):
                return [], 'Solo se aceptan fotos (JPG, PNG, WEBP, HEIC) o PDF.'
            datos = archivo.read(MAX_BYTES + 1)
            if len(datos) > MAX_BYTES:
                return [], 'Cada archivo puede pesar hasta 10 MB.'
            if datos:
                salida.append((nombre, datos))
        return salida, ''

    def _publicar(self, ticket, texto, archivos):
        """Mensaje del cliente en la reclamación, con sus archivos."""
        ticket.message_post(
            body=plaintext2html(texto) if texto else '',
            author_id=request.env.user.partner_id.id,
            message_type='comment', subtype_xmlid='mail.mt_comment',
            attachments=archivos,
        )

    def _reclamo_nuevo(self, equipo, picking, lotes, motivo, cantidad, descripcion):
        """Valores del ticket. Los datos de la entrega se escriben en la descripción y,
        si el SGI está, también en sus campos (pedido, producto, lote, metros)."""
        Ticket = request.env['helpdesk.ticket']
        socio = request.env.user.partner_id
        renglones = ['Motivo: %s' % MOTIVOS[motivo]]
        if picking:
            pedido = picking.sale_id
            renglones.append('Entrega: %s' % picking.name)
            renglones.append('Pedido: %s%s' % (pedido.name, ' (su orden %s)' % pedido.client_order_ref
                                                if pedido.client_order_ref else ''))
        for lote in lotes:
            renglones.append('Producto: %s · Lote: %s · Entregado: %s %s' % (
                lote['producto'], lote['lote'] or 'sin lote', ('%.2f' % lote['cantidad']).rstrip('0').rstrip('.'),
                lote['unidad']))
        if cantidad:
            renglones.append('Cantidad afectada: %s' % cantidad)
        renglones += ['', descripcion]
        asunto = MOTIVOS[motivo]
        if picking:
            asunto += ' · %s' % picking.name
        if lotes and lotes[0]['lote']:
            asunto += ' · lote %s' % lotes[0]['lote']
        vals = {
            'name': asunto[:200],
            'partner_id': socio.id,
            'team_id': equipo.id,
            'description': plaintext2html('\n'.join(renglones)),
        }
        opcionales = {
            'sgi_sale_order_id': picking.sale_id.id if picking else False,
            'sgi_product_id': lotes[0]['producto_id'] if lotes else False,
            'sgi_lot_id': lotes[0]['lote_id'] if lotes else False,
        }
        try:
            metros = float((cantidad or '').replace(',', '.').split()[0])
        except (ValueError, IndexError):
            metros = 0.0
        if metros > 0 and lotes and lotes[0]['en_metros']:
            opcionales['sgi_qty_affected'] = metros
        vals.update({campo: valor for campo, valor in opcionales.items() if valor and campo in Ticket._fields})
        return vals

    # -- páginas -------------------------------------------------------------

    @http.route('/mi-cuenta/calidad', type='http', auth='user', website=True, sitemap=False)
    def calidad(self, cliente=None, **kw):
        empresa, vista_previa, salida = self._entrar(cliente)
        if salida:
            return salida
        desde = fields.Datetime.now() - timedelta(days=30 * MESES_HISTORIAL)
        entregas = request.env['stock.picking'].sudo().search(
            self._dominio_entregas(empresa) + [('date_done', '>=', desde)],
            order='date_done desc', limit=LIMITE_ENTREGAS)
        equipo = self._equipo()
        reclamos = request.env['helpdesk.ticket'].sudo().search(
            self._dominio_reclamos(empresa, equipo), order='create_date desc, id desc',
            limit=LIMITE_RECLAMOS) if equipo else []
        filas = [self._entrega_vals(p) for p in entregas]
        return request.render('website_quimibond.cuenta_calidad', self._valores(
            empresa, vista_previa,
            qb_seccion='calidad',
            entregas=filas,
            con_certificado=any(e['certificado']['estado'] for e in filas),
            reclamos=[self._reclamo_vals(t) for t in reclamos],
            reclamos_activo=bool(equipo),
            nueva_url=self._liga('/mi-cuenta/calidad/nueva'),
            enviada=kw.get('enviada'),
            meses=MESES_HISTORIAL,
            title='Calidad | Quimibond',
        ))

    @http.route('/mi-cuenta/certificado/<int:entrega_id>/<int:adjunto_id>', type='http', auth='user',
                website=True, sitemap=False)
    def certificado(self, entrega_id, adjunto_id, cliente=None, **kw):
        empresa, _vista_previa, salida = self._entrar(cliente)
        if salida:
            return salida
        picking = self._entrega(empresa, entrega_id)
        adjunto = self._coa_adjuntos(picking).filtered(lambda a: a.id == adjunto_id) if picking else None
        if not adjunto:
            raise NotFound()
        return request.env['ir.binary']._get_stream_from(adjunto.sudo()).get_response(as_attachment=True)

    def _formulario(self, empresa, vista_previa, entrega=None, valores=None, error=''):
        """Paso 1 (elegir la entrega) o paso 2 (el reporte) de una reclamación nueva."""
        picking = request.env['stock.picking']
        elegir = []
        general = str(entrega) == '0'
        if entrega and str(entrega).isdigit() and not general:
            picking = self._entrega(empresa, int(entrega))
            if not picking:
                raise NotFound()
        elif not general:
            desde = fields.Datetime.now() - timedelta(days=30 * MESES_HISTORIAL)
            recientes = request.env['stock.picking'].sudo().search(
                self._dominio_entregas(empresa) + [('date_done', '>=', desde)], order='date_done desc', limit=40)
            elegir = [self._entrega_vals(p) for p in recientes]
        return request.render('website_quimibond.cuenta_reclamo_nuevo', self._valores(
            empresa, vista_previa,
            qb_seccion='calidad',
            entrega=self._entrega_vals(picking) if picking else None,
            elegir=elegir,
            general=general or (not picking and not elegir),
            general_url=self._liga('/mi-cuenta/calidad/nueva', entrega=0),
            motivos=MOTIVOS,
            valores=valores or {},
            error=error,
            max_archivos=MAX_ARCHIVOS,
            title='Levantar un reporte | Quimibond',
        ))

    @http.route('/mi-cuenta/calidad/nueva', type='http', auth='user', website=True, sitemap=False,
                methods=['GET'])
    def reclamo_nuevo(self, cliente=None, entrega=None, **kw):
        empresa, vista_previa, salida = self._entrar(cliente)
        if salida:
            return salida
        if not self._equipo():
            raise NotFound()
        return self._formulario(empresa, vista_previa, entrega=entrega)

    @http.route('/mi-cuenta/calidad/nueva', type='http', auth='user', website=True, sitemap=False,
                methods=['POST'])
    def reclamo_crear(self, cliente=None, entrega=None, motivo=None, cantidad=None, descripcion=None, **kw):
        empresa, vista_previa, salida = self._entrar(cliente)
        if salida:
            return salida
        equipo = self._equipo()
        if not equipo:
            raise NotFound()
        if vista_previa:
            raise Forbidden()
        descripcion = (descripcion or '').strip()[:MAX_TEXTO]
        cantidad = (cantidad or '').strip()[:60]
        claves = set(request.httprequest.form.getlist('lote'))
        valores = {'motivo': motivo, 'cantidad': cantidad, 'descripcion': descripcion, 'lotes': claves}
        picking = request.env['stock.picking']
        if entrega and str(entrega).isdigit() and str(entrega) != '0':
            picking = self._entrega(empresa, int(entrega))
            if not picking:
                raise NotFound()
        archivos, error = self._archivos()
        if not error and motivo not in MOTIVOS:
            error = 'Elija el motivo del reporte.'
        if not error and len(descripcion) < 10:
            error = 'Cuéntenos qué encontró, con el mayor detalle que pueda.'
        if error:
            return self._formulario(empresa, vista_previa, entrega=entrega or 0, valores=valores, error=error)
        # Solo cuentan los lotes que de verdad salieron en esa entrega.
        lotes = [lote for lote in self._lotes(picking) if lote['clave'] in claves] if picking else []
        Ticket = request.env['helpdesk.ticket'].sudo()
        ticket = Ticket.create(self._reclamo_nuevo(equipo, picking, lotes, motivo, cantidad, descripcion))
        if archivos:
            self._publicar(ticket, 'Fotos y archivos del reporte.', archivos)
        return request.redirect('/mi-cuenta/calidad/reclamacion/%s?enviada=1' % ticket.id)

    @http.route('/mi-cuenta/calidad/reclamacion/<int:reclamo_id>', type='http', auth='user', website=True,
                sitemap=False)
    def reclamo(self, reclamo_id, cliente=None, **kw):
        empresa, vista_previa, salida = self._entrar(cliente)
        if salida:
            return salida
        ticket = self._reclamo(empresa, reclamo_id)
        if not ticket:
            raise NotFound()
        vals = self._reclamo_vals(ticket)
        mensajes = self._mensajes(ticket, empresa)
        # La respuesta formal es lo último que dijo Quimibond una vez que la reclamación llegó a «Respuesta».
        respuesta = None
        if vals['etapa']['paso'] >= 2:
            respuesta = next((m for m in reversed(mensajes) if not m['del_cliente'] and m['cuerpo']), None)
        return request.render('website_quimibond.cuenta_reclamo', self._valores(
            empresa, vista_previa,
            qb_seccion='calidad',
            reclamo=vals,
            descripcion=ticket.description,
            mensajes=[m for m in mensajes if m is not respuesta],
            respuesta=respuesta,
            puede_escribir=not vista_previa and vals['etapa']['clave'] != 'cerrada',
            enviada=kw.get('enviada'),
            error=kw.get('error') and 'Escriba un mensaje o adjunte un archivo válido (fotos o PDF de hasta 10 MB).',
            max_archivos=MAX_ARCHIVOS,
            title='Reporte %s | Quimibond' % vals['folio'],
        ))

    @http.route('/mi-cuenta/calidad/reclamacion/<int:reclamo_id>/mensaje', type='http', auth='user',
                website=True, sitemap=False, methods=['POST'])
    def reclamo_mensaje(self, reclamo_id, cliente=None, mensaje=None, **kw):
        empresa, vista_previa, salida = self._entrar(cliente)
        if salida:
            return salida
        ticket = self._reclamo(empresa, reclamo_id)
        if not ticket:
            raise NotFound()
        if vista_previa or etapa_reclamo(ticket)['clave'] == 'cerrada':
            raise Forbidden()
        texto = (mensaje or '').strip()[:MAX_TEXTO]
        archivos, error = self._archivos()
        base = '/mi-cuenta/calidad/reclamacion/%s' % ticket.id
        if error or not (texto or archivos):
            return request.redirect(base + '?error=1')
        self._publicar(ticket, texto, archivos)
        return request.redirect(base + '#mensajes')

    @http.route('/mi-cuenta/calidad/reclamacion/<int:reclamo_id>/archivo/<int:adjunto_id>', type='http',
                auth='user', website=True, sitemap=False)
    def reclamo_archivo(self, reclamo_id, adjunto_id, cliente=None, **kw):
        empresa, _vista_previa, salida = self._entrar(cliente)
        if salida:
            return salida
        ticket = self._reclamo(empresa, reclamo_id)
        if not ticket:
            raise NotFound()
        # Solo los archivos de los mensajes que el cliente puede ver.
        visibles = request.env['ir.attachment'].sudo()
        for mensaje in self._mensajes(ticket, empresa):
            visibles |= mensaje['registro'].attachment_ids
        adjunto = visibles.filtered(lambda a: a.id == adjunto_id)
        if not adjunto:
            raise NotFound()
        return request.env['ir.binary']._get_stream_from(adjunto).get_response(
            as_attachment=adjunto.mimetype not in IMAGENES)
