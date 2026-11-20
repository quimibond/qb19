"""Mi cuenta: el portal del cliente con el diseño del sitio B2B.

Qué ve un cliente al iniciar sesión (`/mi-cuenta`): sus pedidos en curso con
etapa y fecha compromiso, sus entregas, sus facturas con los documentos
fiscales y su saldo. Las páginas nativas del portal (`/my/...`) siguen
existiendo; `/my` manda aquí a los usuarios de portal cuando el sitio tiene la
casilla «Diseño B2B de Quimibond».

Seguridad: todo se lee con `sudo()` y **siempre** filtrado por la empresa del
usuario (`partner_id child_of commercial_partner_id`). Un contacto ve los
pedidos y las facturas de su empresa completa, que es lo que necesita un
comprador o un contador de un cliente; nunca los de otra. Un pedido o una
factura de otra empresa responde 404, igual que uno que no existe.

No se muestra nada de la operación interna (órdenes de fabricación, centros de
trabajo, recetas): solo etapa, fechas, cantidades y documentos.
"""
from collections import OrderedDict
from datetime import timedelta

from werkzeug.exceptions import NotFound

from odoo import fields, http
from odoo.http import request

from odoo.addons.portal.controllers.portal import CustomerPortal

from .main import _activo, _exigir_activo

# Grupo interno que puede ver el portal «como» un cliente (?cliente=<id>).
GRUPO_VISTA_PREVIA = 'sales_team.group_sale_manager'
MESES_HISTORIAL = 12
LIMITE_LISTA = 200

PASOS = ['Confirmado', 'En proceso', 'Embarcado', 'Facturado']


def etapa_pedido(order):
    """Etapa del pedido para el cliente: {'clave', 'texto', 'paso', 'pasos'}.

    `paso` es el índice del último paso cumplido en PASOS. No expone nada de
    producción más que «En producción» cuando hay una orden de fabricación
    abierta ligada al pedido.
    """
    salidas = order.picking_ids.filtered(lambda p: p.picking_type_code == 'outgoing' and p.state != 'cancel')
    facturado = order.invoice_status == 'invoiced'
    if order.delivery_status == 'full':
        clave, texto, paso = ('facturado', 'Entregado y facturado', 3) if facturado else ('entregado', 'Entregado', 2)
    elif order.delivery_status == 'partial':
        clave, texto, paso = 'parcial', 'Entrega parcial', 2
    else:
        producciones = order.mrp_production_ids if 'mrp_production_ids' in order._fields else order.browse()
        if producciones and any(p.state not in ('done', 'cancel') for p in producciones):
            clave, texto, paso = 'produccion', 'En producción', 1
        elif any(p.state == 'assigned' for p in salidas):
            clave, texto, paso = 'listo', 'Listo para embarque', 1
        elif not salidas:
            # Servicios o pedidos sin entrega: la etapa la da la factura.
            clave, texto, paso = ('facturado', 'Facturado', 3) if facturado else ('confirmado', 'Confirmado', 0)
        else:
            clave, texto, paso = 'preparacion', 'En preparación', 1
    return {'clave': clave, 'texto': texto, 'paso': paso, 'pasos': PASOS}


class QuimibondCuenta(http.Controller):

    # -- quién mira ----------------------------------------------------------

    def _empresa(self, cliente=None):
        """(empresa, vista_previa). Empresa del usuario, o la que un gerente de Ventas pide ver."""
        usuario = request.env.user
        if usuario._is_internal():
            if cliente and usuario.has_group(GRUPO_VISTA_PREVIA):
                try:
                    socio = request.env['res.partner'].sudo().browse(int(cliente)).exists()
                except (TypeError, ValueError):
                    socio = None
                if socio:
                    return socio.commercial_partner_id, True
            return None, False
        return usuario.partner_id.commercial_partner_id, False

    def _dominio_empresa(self, empresa):
        return [('partner_id', 'child_of', empresa.id), ('company_id', 'in', self._companias())]

    def _companias(self):
        return (request.website.company_id or request.env.company).ids

    # -- datos ---------------------------------------------------------------

    def _pedido_vals(self, order):
        fecha = order.commitment_date or order.expected_date
        return {
            'registro': order,
            'nombre': order.name,
            'referencia': order.client_order_ref or '',
            'fecha': order.date_order,
            'compromiso': fecha,
            'total': order.amount_total,
            'moneda': order.currency_id,
            'etapa': etapa_pedido(order),
            'url': '/mi-cuenta/pedido/%s%s' % (order.id, self._sufijo()),
        }

    def _factura_vals(self, move, hoy):
        signo = -1 if move.move_type == 'out_refund' else 1
        vencida = bool(move.amount_residual and move.invoice_date_due and move.invoice_date_due < hoy)
        if move.payment_state in ('paid', 'in_payment', 'reversed'):
            estado, clave = 'Pagada', 'pagada'
        elif vencida:
            estado, clave = 'Vencida', 'vencida'
        elif move.payment_state == 'partial':
            estado, clave = 'Pago parcial', 'parcial'
        else:
            estado, clave = 'Por pagar', 'abierta'
        return {
            'registro': move,
            'nombre': move.name,
            'nota_credito': move.move_type == 'out_refund',
            'referencia': move.ref or '',
            'origen': move.invoice_origin or '',
            'fecha': move.invoice_date,
            'vence': move.invoice_date_due,
            'total': signo * move.amount_total,
            'saldo': signo * move.amount_residual,
            'moneda': move.currency_id,
            'estado': estado,
            'clave': clave,
            'dias_vencida': (hoy - move.invoice_date_due).days if vencida else 0,
            'descarga': move.get_portal_url(report_type='pdf', download=True),
        }

    def _entrega_vals(self, picking):
        pedido = picking.sale_id
        return {
            'nombre': picking.name,
            'pedido': pedido.name,
            'pedido_url': '/mi-cuenta/pedido/%s%s' % (pedido.id, self._sufijo()),
            'referencia': pedido.client_order_ref or '',
            'fecha': picking.date_done or picking.scheduled_date,
            'hecha': picking.state == 'done',
            'guia': picking.carrier_tracking_ref if 'carrier_tracking_ref' in picking._fields else '',
            'descarga': '/my/picking/pdf/%s?access_token=%s' % (picking.id, pedido._portal_ensure_token())
                        if picking.state == 'done' else '',
        }

    def _saldos(self, facturas):
        """Saldo por moneda: [{'moneda', 'saldo', 'vencido', 'por_vencer'}], la de mayor saldo primero."""
        por_moneda = OrderedDict()
        for f in facturas:
            if not f['saldo']:
                continue
            fila = por_moneda.setdefault(f['moneda'].id, {'moneda': f['moneda'], 'saldo': 0.0, 'vencido': 0.0})
            fila['saldo'] += f['saldo']
            if f['clave'] == 'vencida':
                fila['vencido'] += f['saldo']
        filas = list(por_moneda.values())
        for fila in filas:
            fila['por_vencer'] = fila['saldo'] - fila['vencido']
        return sorted(filas, key=lambda fila: -abs(fila['saldo']))

    def _sufijo(self):
        cliente = request.params.get('cliente')
        return '?cliente=%s' % int(cliente) if cliente and str(cliente).isdigit() else ''

    def _valores(self, empresa, vista_previa, **extra):
        valores = {
            'qb_empresa': empresa,
            'qb_vista_previa': vista_previa,
            'qb_sufijo': self._sufijo(),
            'qb_seccion': '',
            'hoy': fields.Date.context_today(request.env.user),
        }
        valores.update(extra)
        return valores

    def _entrar(self, cliente):
        """Empresa que se muestra, o una redirección para el usuario interno sin vista previa."""
        _exigir_activo()
        empresa, vista_previa = self._empresa(cliente)
        if not empresa:
            return None, False, request.redirect('/odoo')
        return empresa, vista_previa, None

    def _facturas(self, empresa, dominio_extra, limite=LIMITE_LISTA):
        hoy = fields.Date.context_today(request.env.user)
        facturas = request.env['account.move'].sudo().search(
            self._dominio_empresa(empresa) + [
                ('move_type', 'in', ('out_invoice', 'out_refund')), ('state', '=', 'posted'),
            ] + dominio_extra, order='invoice_date desc, id desc', limit=limite)
        return [self._factura_vals(f, hoy) for f in facturas]

    # -- páginas -------------------------------------------------------------

    @http.route('/mi-cuenta', type='http', auth='user', website=True, sitemap=False)
    def mi_cuenta(self, cliente=None, **kw):
        empresa, vista_previa, salida = self._entrar(cliente)
        if salida:
            return salida
        Pedido = request.env['sale.order'].sudo()
        dominio = self._dominio_empresa(empresa) + [('state', '=', 'sale')]
        en_curso = Pedido.search(dominio + [
            '|', ('delivery_status', 'in', ('pending', 'started', 'partial')),
            '&', ('delivery_status', '=', False), ('invoice_status', '=', 'to invoice'),
        ], order='commitment_date asc, date_order asc', limit=LIMITE_LISTA)
        pedidos = [self._pedido_vals(o) for o in en_curso]
        proximas = [p['compromiso'] for p in pedidos if p['compromiso']]

        abiertas = self._facturas(empresa, [('payment_state', 'in', ('not_paid', 'partial'))])
        entregas = request.env['stock.picking'].sudo().search([
            ('sale_id.partner_id', 'child_of', empresa.id), ('sale_id.company_id', 'in', self._companias()),
            ('picking_type_code', '=', 'outgoing'), ('state', '=', 'done'),
        ], order='date_done desc', limit=6)
        return request.render('website_quimibond.cuenta', self._valores(
            empresa, vista_previa,
            qb_seccion='resumen',
            pedidos=pedidos,
            proxima=min(proximas) if proximas else False,
            facturas=abiertas[:8],
            total_abiertas=len(abiertas),
            saldos=self._saldos(abiertas),
            entregas=[self._entrega_vals(p) for p in entregas],
            title='Mi cuenta | Quimibond',
        ))

    @http.route('/mi-cuenta/pedidos', type='http', auth='user', website=True, sitemap=False)
    def mis_pedidos(self, cliente=None, **kw):
        empresa, vista_previa, salida = self._entrar(cliente)
        if salida:
            return salida
        desde = fields.Datetime.now() - timedelta(days=30 * MESES_HISTORIAL)
        pedidos = request.env['sale.order'].sudo().search(
            self._dominio_empresa(empresa) + [('state', '=', 'sale'), ('date_order', '>=', desde)],
            order='date_order desc', limit=LIMITE_LISTA)
        return request.render('website_quimibond.cuenta_pedidos', self._valores(
            empresa, vista_previa,
            qb_seccion='pedidos',
            pedidos=[self._pedido_vals(o) for o in pedidos],
            meses=MESES_HISTORIAL,
            title='Mis pedidos | Quimibond',
        ))

    @http.route('/mi-cuenta/pedido/<int:pedido_id>', type='http', auth='user', website=True, sitemap=False)
    def mi_pedido(self, pedido_id, cliente=None, **kw):
        empresa, vista_previa, salida = self._entrar(cliente)
        if salida:
            return salida
        order = request.env['sale.order'].sudo().search(
            self._dominio_empresa(empresa) + [('id', '=', pedido_id), ('state', '=', 'sale')], limit=1)
        if not order:
            raise NotFound()
        hoy = fields.Date.context_today(request.env.user)
        salidas = order.picking_ids.filtered(
            lambda p: p.picking_type_code == 'outgoing' and p.state != 'cancel').sorted(
            lambda p: p.date_done or p.scheduled_date or fields.Datetime.now())
        facturas = order.invoice_ids.filtered(
            lambda m: m.state == 'posted' and m.move_type in ('out_invoice', 'out_refund'))
        lineas = [{
            'producto': linea.product_id.display_name or linea.name,
            'descripcion': linea.name,
            'pedido': linea.product_uom_qty,
            'entregado': linea.qty_delivered,
            'pendiente': max(linea.product_uom_qty - linea.qty_delivered, 0.0),
            'unidad': linea.product_uom_id.name,
        } for linea in order.order_line if not linea.display_type and linea.product_id.type != 'service']
        return request.render('website_quimibond.cuenta_pedido', self._valores(
            empresa, vista_previa,
            qb_seccion='pedidos',
            pedido=self._pedido_vals(order),
            lineas=lineas,
            entregas=[self._entrega_vals(p) for p in salidas],
            facturas=[self._factura_vals(f, hoy) for f in facturas],
            descarga=order.get_portal_url(report_type='pdf', download=True),
            title='Pedido %s | Quimibond' % order.name,
        ))

    @http.route('/mi-cuenta/estado-de-cuenta', type='http', auth='user', website=True, sitemap=False)
    def estado_de_cuenta(self, cliente=None, **kw):
        empresa, vista_previa, salida = self._entrar(cliente)
        if salida:
            return salida
        abiertas = self._facturas(empresa, [('payment_state', 'in', ('not_paid', 'partial'))])
        desde = fields.Date.context_today(request.env.user) - timedelta(days=30 * MESES_HISTORIAL)
        pagadas = self._facturas(empresa, [
            ('payment_state', 'not in', ('not_paid', 'partial')), ('invoice_date', '>=', desde)])
        return request.render('website_quimibond.cuenta_estado', self._valores(
            empresa, vista_previa,
            qb_seccion='estado',
            abiertas=abiertas,
            pagadas=pagadas,
            saldos=self._saldos(abiertas),
            meses=MESES_HISTORIAL,
            title='Estado de cuenta | Quimibond',
        ))


class QuimibondPortalInicio(CustomerPortal):

    @http.route()
    def home(self, **kw):
        # El inicio del portal es «Mi cuenta» para los usuarios de portal. Con
        # ?todo=1 se abre el inicio nativo (reportes, firmas, proyectos…).
        if _activo() and not kw.get('todo') and not request.env.user._is_internal():
            return request.redirect('/mi-cuenta')
        return super().home(**kw)
