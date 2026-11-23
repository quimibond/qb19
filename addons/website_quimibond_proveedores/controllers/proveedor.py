"""Portal de proveedores: órdenes de compra, subir factura y estado de pago.

Mismo criterio que «Mi cuenta» del cliente (`website_quimibond`): todo se lee
con `sudo()` y **siempre** filtrado por la empresa del usuario
(`partner_id child_of commercial_partner_id`). Una orden o una factura de otro
proveedor responde 404, igual que una que no existe. Subir una factura no crea
asientos: deja una «Factura recibida» para Cuentas por pagar.
"""
from collections import OrderedDict
from datetime import timedelta

from werkzeug.exceptions import NotFound

from odoo import fields, http
from odoo.http import request

from odoo.addons.website_quimibond.controllers.cuenta import LIMITE_LISTA, MESES_HISTORIAL, QuimibondCuenta
from odoo.addons.website_quimibond.controllers.main import _activo, _exigir_activo

from ..models.cfdi import CfdiInvalido

# Grupo interno que puede ver el portal «como» un proveedor (?proveedor=<id>).
GRUPO_VISTA_PREVIA = 'purchase.group_purchase_manager'
MAX_SUBIDA = 8 * 1024 * 1024


def fecha_programada(vence, hoy, dia_pago=None):
    """Fecha en que se espera el pago: el vencimiento, o el primer día de pago en o después de él.

    Si ya pasó y no hay día de pago fijo, devuelve False (no se promete una fecha).
    """
    if not vence:
        return False
    if dia_pago in (None, False, ''):
        return vence if vence >= hoy else False
    base = max(vence, hoy)
    return base + timedelta(days=(int(dia_pago) - base.weekday()) % 7)


def estado_pago(move, hoy, dia_pago=None):
    """Estado de una factura de proveedor para el proveedor: (clave, texto, fecha)."""
    if move.state == 'draft':
        return 'revision', 'En revisión', False
    if move.payment_state in ('paid', 'in_payment', 'reversed'):
        fechas = move._get_reconciled_amls().mapped('date')
        return 'pagada', 'Pagada', max(fechas) if fechas else False
    liberada = 'release_to_pay' not in move._fields or move.release_to_pay == 'yes'
    if move.payment_state == 'blocked' or not liberada:
        return 'revision', 'En revisión', False
    fecha = fecha_programada(move.invoice_date_due, hoy, dia_pago)
    if move.payment_state == 'partial':
        return 'parcial', 'Pago parcial', fecha
    if not fecha:
        return 'proceso', 'En proceso de pago', False
    return 'programada', 'Pago programado', fecha


def etapa_orden(order):
    """Etapa de la orden de compra para el proveedor: {'clave', 'texto'}."""
    recepcion = order.receipt_status if 'receipt_status' in order._fields else False
    # Lo recibido manda: una orden a medio recibir con su parcialidad facturada sigue «parcial».
    if recepcion == 'partial':
        return {'clave': 'parcial', 'texto': 'Recepción parcial'}
    if order.invoice_status == 'invoiced':
        return {'clave': 'facturada', 'texto': 'Facturada'}
    if recepcion == 'full':
        return {'clave': 'recibida', 'texto': 'Recibida'}
    return {'clave': 'abierta', 'texto': 'Por entregar'}


class QuimibondProveedor(QuimibondCuenta):

    # -- quién mira ----------------------------------------------------------

    def _proveedor(self, proveedor=None):
        """(empresa, vista_previa). Empresa del usuario, o la que un gerente de Compras pide ver."""
        usuario = request.env.user
        if usuario._is_internal():
            if proveedor and usuario.has_group(GRUPO_VISTA_PREVIA):
                try:
                    socio = request.env['res.partner'].sudo().browse(int(proveedor)).exists()
                except (TypeError, ValueError):
                    socio = None
                if socio:
                    return socio.commercial_partner_id, True
            return None, False
        return usuario.partner_id.commercial_partner_id, False

    def _entrar_proveedor(self, proveedor):
        _exigir_activo()
        empresa, vista_previa = self._proveedor(proveedor)
        if not empresa:
            return None, False, request.redirect('/odoo')
        if not vista_previa and not self._es_proveedor(empresa):
            return None, False, request.redirect('/mi-cuenta')
        return empresa, vista_previa, None

    def _dominio_compras(self, empresa):
        return self._dominio_empresa(empresa) + [('state', '=', 'purchase')]

    def _es_proveedor(self, empresa):
        if empresa.supplier_rank:
            return True
        return bool(request.env['purchase.order'].sudo().search_count(self._dominio_compras(empresa), limit=1))

    def _es_cliente(self, empresa):
        return bool(request.env['sale.order'].sudo().search_count(
            self._dominio_empresa(empresa) + [('state', '=', 'sale')], limit=1))

    def _sufijo_proveedor(self):
        proveedor = request.params.get('proveedor')
        return '?proveedor=%s' % int(proveedor) if proveedor and str(proveedor).isdigit() else ''

    def _valores(self, empresa, vista_previa, **extra):
        # «Mi cuenta» del cliente también sabe si la empresa es proveedor, para mostrar la pestaña.
        valores = super()._valores(empresa, vista_previa, **extra)
        valores.setdefault('qb_es_proveedor', bool(empresa) and self._es_proveedor(empresa))
        return valores

    def _valores_proveedor(self, empresa, vista_previa, **extra):
        valores = {
            'qb_empresa': empresa,
            'qb_vista_previa': vista_previa,
            'qb_sufijo': self._sufijo_proveedor(),
            'qb_seccion': '',
            'qb_es_cliente': not vista_previa and self._es_cliente(empresa),
            'hoy': fields.Date.context_today(request.env.user),
        }
        valores.update(extra)
        return valores

    # -- datos ---------------------------------------------------------------

    def _orden_vals(self, order, pendientes=None):
        """`pendientes`: ids de órdenes con una factura subida que sigue en revisión."""
        en_revision = order.id in (pendientes or ())
        por_facturar = order.invoice_status == 'to invoice'
        return {
            'registro': order,
            'nombre': order.name,
            'referencia': order.partner_ref or '',
            'fecha': order.date_approve or order.date_order,
            'entrega': order.date_planned,
            'total': order.amount_total,
            'moneda': order.currency_id,
            'etapa': etapa_orden(order),
            'por_facturar': por_facturar and not en_revision,
            'en_revision': en_revision,
            'puede_subir': order.invoice_status != 'invoiced' and not en_revision,
            'url': '/mi-cuenta/proveedor/orden/%s%s' % (order.id, self._sufijo_proveedor()),
        }

    def _ordenes_con_factura_en_revision(self, empresa):
        grupos = request.env['qb.proveedor.factura'].sudo().search(
            [('partner_id', '=', empresa.id), ('state', '=', 'recibida')])
        return set(grupos.mapped('purchase_id').ids)

    def _fila_factura(self, move, hoy, dia_pago, subidas):
        """Fila de una factura contable. `subidas`: {move_id: factura del portal}."""
        signo = -1 if move.move_type == 'in_refund' else 1
        clave, estado, fecha = estado_pago(move, hoy, dia_pago)
        subida = subidas.get(move.id)
        orden = move.line_ids.purchase_order_id[:1] if 'purchase_order_id' in move.line_ids._fields else move.browse()
        return {
            'nombre': (subida and subida.name) or move.ref or move.name,
            'nota_credito': move.move_type == 'in_refund',
            'orden': (subida and subida.purchase_id.name) or (orden and orden.name) or move.invoice_origin or '',
            'fecha': move.invoice_date,
            'vence': move.invoice_date_due,
            'total': signo * move.amount_total,
            'saldo': signo * move.amount_residual if clave != 'pagada' else 0.0,
            'moneda': move.currency_id,
            'clave': clave,
            'estado': estado,
            'fecha_pago': fecha,
            'nota': '',
        }

    def _fila_subida(self, factura):
        """Fila de una factura subida en el portal que todavía no es factura contable."""
        rechazada = factura.state == 'rechazada'
        moneda = request.env['res.currency'].sudo().with_context(active_test=False).search(
            [('name', '=', factura.moneda)], limit=1) or factura.purchase_id.currency_id
        return {
            'nombre': factura.name,
            'nota_credito': False,
            'orden': factura.purchase_id.name,
            'fecha': factura.fecha,
            'vence': False,
            'total': factura.total,
            'saldo': 0.0 if rechazada else factura.total,
            'moneda': moneda,
            'clave': 'rechazada' if rechazada else 'revision',
            'estado': 'Rechazada' if rechazada else 'En revisión',
            'fecha_pago': False,
            'nota': (factura.motivo_rechazo if rechazada else factura.avisos) or '',
        }

    def _facturas_proveedor(self, empresa, orden=None, desde=None):
        """Facturas del proveedor (contables y subidas en el portal), las más recientes primero."""
        hoy = fields.Date.context_today(request.env.user)
        dia_pago = request.website.qb_prov_dia_pago
        Subida = request.env['qb.proveedor.factura'].sudo()
        dominio_subidas = [('partner_id', '=', empresa.id), ('company_id', 'in', self._companias())]
        dominio = self._dominio_empresa(empresa) + [
            ('move_type', 'in', ('in_invoice', 'in_refund')), ('state', 'in', ('draft', 'posted'))]
        if orden:
            dominio_subidas.append(('purchase_id', '=', orden.id))
            dominio.append(('id', 'in', orden.invoice_ids.ids))
        if desde:
            dominio += ['|', ('invoice_date', '>=', desde), ('payment_state', 'in', ('not_paid', 'partial'))]
        todas = Subida.search(dominio_subidas, limit=LIMITE_LISTA)
        ligadas = {s.move_id.id: s for s in todas if s.move_id}
        moves = request.env['account.move'].sudo().search(dominio, order='invoice_date desc, id desc', limit=LIMITE_LISTA)
        filas = [self._fila_subida(s) for s in todas if not s.move_id and (
            s.state == 'recibida' or not desde or s.create_date.date() >= desde)]
        filas += [self._fila_factura(m, hoy, dia_pago, ligadas) for m in moves]
        return filas

    def _saldos_proveedor(self, filas):
        """Por cobrar por moneda: [{'moneda', 'saldo', 'programado', 'revision'}]."""
        por_moneda = OrderedDict()
        for f in filas:
            if not f['saldo'] or f['clave'] in ('pagada', 'rechazada'):
                continue
            fila = por_moneda.setdefault(f['moneda'].id, {'moneda': f['moneda'], 'saldo': 0.0, 'revision': 0.0})
            fila['saldo'] += f['saldo']
            if f['clave'] == 'revision':
                fila['revision'] += f['saldo']
        filas_saldo = list(por_moneda.values())
        for fila in filas_saldo:
            fila['programado'] = fila['saldo'] - fila['revision']
        return sorted(filas_saldo, key=lambda fila: -abs(fila['saldo']))

    def _orden(self, empresa, orden_id):
        order = request.env['purchase.order'].sudo().search(
            self._dominio_compras(empresa) + [('id', '=', orden_id)], limit=1)
        if not order:
            raise NotFound()
        return order

    # -- páginas -------------------------------------------------------------

    @http.route()
    def mi_cuenta(self, cliente=None, **kw):
        # Quien solo es proveedor entra directo a su portal.
        usuario = request.env.user
        if _activo() and not usuario._is_internal():
            empresa = usuario.partner_id.commercial_partner_id
            if self._es_proveedor(empresa) and not self._es_cliente(empresa):
                return request.redirect('/mi-cuenta/proveedor')
        return super().mi_cuenta(cliente=cliente, **kw)

    @http.route('/mi-cuenta/proveedor', type='http', auth='user', website=True, sitemap=False)
    def proveedor_resumen(self, proveedor=None, **kw):
        empresa, vista_previa, salida = self._entrar_proveedor(proveedor)
        if salida:
            return salida
        Orden = request.env['purchase.order'].sudo()
        dominio = self._dominio_compras(empresa)
        pendientes = self._ordenes_con_factura_en_revision(empresa)
        por_entregar = Orden.search(dominio + [('receipt_status', 'in', ('pending', 'partial'))],
                                    order='date_planned asc, id asc', limit=LIMITE_LISTA)
        por_facturar = Orden.search(dominio + [('invoice_status', '=', 'to invoice'), ('id', 'not in', list(pendientes))],
                                    order='date_approve asc, id asc', limit=LIMITE_LISTA)
        desde = fields.Date.context_today(request.env.user) - timedelta(days=30 * MESES_HISTORIAL)
        filas = self._facturas_proveedor(empresa, desde=desde)
        abiertas = [f for f in filas if f['clave'] not in ('pagada', 'rechazada')]
        fechas = [f['fecha_pago'] for f in abiertas if f['fecha_pago']]
        return request.render('website_quimibond_proveedores.proveedor', self._valores_proveedor(
            empresa, vista_previa,
            qb_seccion='resumen',
            por_entregar=[self._orden_vals(o, pendientes) for o in por_entregar],
            por_facturar=[self._orden_vals(o, pendientes) for o in por_facturar],
            facturas=sorted(abiertas, key=lambda f: (f['fecha_pago'] or fields.Date.to_date('2999-12-31')))[:8],
            total_abiertas=len(abiertas),
            proximo_pago=min(fechas) if fechas else False,
            saldos=self._saldos_proveedor(abiertas),
            title='Portal de proveedores | Quimibond',
        ))

    @http.route('/mi-cuenta/proveedor/ordenes', type='http', auth='user', website=True, sitemap=False)
    def proveedor_ordenes(self, proveedor=None, **kw):
        empresa, vista_previa, salida = self._entrar_proveedor(proveedor)
        if salida:
            return salida
        desde = fields.Datetime.now() - timedelta(days=30 * MESES_HISTORIAL)
        ordenes = request.env['purchase.order'].sudo().search(
            self._dominio_compras(empresa) + ['|', ('date_approve', '>=', desde), ('invoice_status', '!=', 'invoiced')],
            order='date_approve desc, id desc', limit=LIMITE_LISTA)
        pendientes = self._ordenes_con_factura_en_revision(empresa)
        return request.render('website_quimibond_proveedores.proveedor_ordenes', self._valores_proveedor(
            empresa, vista_previa,
            qb_seccion='ordenes',
            ordenes=[self._orden_vals(o, pendientes) for o in ordenes],
            meses=MESES_HISTORIAL,
            title='Órdenes de compra | Quimibond',
        ))

    def _render_orden(self, empresa, vista_previa, order, **extra):
        lineas = [{
            'producto': linea.product_id.display_name or linea.name,
            'pedido': linea.product_qty,
            'recibido': linea.qty_received,
            'facturado': linea.qty_invoiced,
            'unidad': linea.product_uom_id.name,
            'importe': linea.price_subtotal,
        } for linea in order.order_line if not linea.display_type]
        pendientes = self._ordenes_con_factura_en_revision(empresa)
        Factura = request.env['qb.proveedor.factura']
        return request.render('website_quimibond_proveedores.proveedor_orden', self._valores_proveedor(
            empresa, vista_previa,
            qb_seccion='ordenes',
            orden=self._orden_vals(order, pendientes),
            lineas=lineas,
            facturas=self._facturas_proveedor(empresa, orden=order),
            por_facturar=Factura._por_facturar(order),
            descarga=order.get_portal_url(report_type='pdf', download=True),
            title='Orden %s | Quimibond' % order.name,
            **extra,
        ))

    @http.route('/mi-cuenta/proveedor/orden/<int:orden_id>', type='http', auth='user', website=True, sitemap=False)
    def proveedor_orden(self, orden_id, proveedor=None, subida=None, **kw):
        empresa, vista_previa, salida = self._entrar_proveedor(proveedor)
        if salida:
            return salida
        order = self._orden(empresa, orden_id)
        recien = request.env['qb.proveedor.factura'].sudo().browse()
        if subida and str(subida).isdigit():
            recien = recien.search([('id', '=', int(subida)), ('purchase_id', '=', order.id)], limit=1)
        return self._render_orden(empresa, vista_previa, order, subida=recien)

    @http.route('/mi-cuenta/proveedor/orden/<int:orden_id>/factura', type='http', auth='user', website=True,
                methods=['POST'], sitemap=False, max_content_length=MAX_SUBIDA)
    def proveedor_subir_factura(self, orden_id, **kw):
        # Solo el proveedor sube facturas: la vista previa de Compras es de lectura.
        _exigir_activo()
        usuario = request.env.user
        if usuario._is_internal():
            raise NotFound()
        empresa = usuario.partner_id.commercial_partner_id
        order = self._orden(empresa, orden_id)
        archivos = request.httprequest.files
        xml, pdf = archivos.get('xml'), archivos.get('pdf')
        try:
            if order.invoice_status == 'invoiced':
                raise CfdiInvalido('Esta orden ya está facturada por completo.')
            if not xml or not xml.filename or not pdf or not pdf.filename:
                raise CfdiInvalido('Suba los dos archivos: el XML y el PDF de la factura.')
            factura = request.env['qb.proveedor.factura']._recibir(
                order, xml.read(MAX_SUBIDA), xml.filename, pdf.read(MAX_SUBIDA), pdf.filename, usuario)
        except CfdiInvalido as error:
            return self._render_orden(empresa, False, order, error=str(error), subida=None)
        return request.redirect('/mi-cuenta/proveedor/orden/%s?subida=%s' % (order.id, factura.id))

    @http.route('/mi-cuenta/proveedor/pagos', type='http', auth='user', website=True, sitemap=False)
    def proveedor_pagos(self, proveedor=None, **kw):
        empresa, vista_previa, salida = self._entrar_proveedor(proveedor)
        if salida:
            return salida
        desde = fields.Date.context_today(request.env.user) - timedelta(days=30 * MESES_HISTORIAL)
        filas = self._facturas_proveedor(empresa, desde=desde)
        abiertas = [f for f in filas if f['clave'] not in ('pagada', 'rechazada')]
        return request.render('website_quimibond_proveedores.proveedor_pagos', self._valores_proveedor(
            empresa, vista_previa,
            qb_seccion='pagos',
            abiertas=abiertas,
            rechazadas=[f for f in filas if f['clave'] == 'rechazada'],
            pagadas=[f for f in filas if f['clave'] == 'pagada'],
            saldos=self._saldos_proveedor(abiertas),
            dia_pago=dict(request.website._fields['qb_prov_dia_pago'].selection).get(request.website.qb_prov_dia_pago),
            meses=MESES_HISTORIAL,
            title='Facturas y pagos | Quimibond',
        ))
