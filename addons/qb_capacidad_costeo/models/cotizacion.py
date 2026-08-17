# -*- coding: utf-8 -*-
"""Cotizaciones guardadas: resultado del cotizador con sus supuestos.

Cada cotización queda con los factores usados (FX, ventana de gastos,
denominadores) para trazabilidad y comparación antes/después.
"""
from odoo import api, fields, models

from .glosario import GLOSARIO_HTML


class QbCotizacion(models.Model):
    _name = 'qb.cotizacion'
    _description = 'Cotización de capacidad y costo'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'create_date DESC'

    name = fields.Char(required=True, default='Nueva cotización')
    company_id = fields.Many2one(
        'res.company', default=lambda self: self.env.company, required=True)
    partner_id = fields.Many2one('res.partner', string='Cliente')
    product_id = fields.Many2one(
        'product.product', string='Producto existente')
    spec_descripcion = fields.Char(
        string='Especificación nueva',
        help='Para producto que aún no existe: descripción libre.')
    spec_gramaje = fields.Float(string='Gramaje (g/m²)')
    spec_ancho = fields.Float(string='Ancho (m)')
    spec_galga = fields.Char(string='Galga')
    volumen = fields.Float(string='Volumen (unidades/mes)')
    uom_name = fields.Char(string='Unidad')
    currency_id = fields.Many2one(
        'res.currency', string='Moneda de la cotización',
        help='La moneda en la que se capturó y se presenta el precio al '
             'cliente. Los montos guardados están en MXN; el TC los traduce.')
    fx_rate = fields.Float(
        string='TC usado (MXN por 1 divisa)',
        help='Tipo de cambio de Odoo el día que se cotizó: pesos por 1 '
             'unidad de la divisa (ej. 18.50 = 1 USD costaba $18.50 MXN). '
             '1.0 o vacío = la cotización fue en MXN. Informativo para la '
             'MP: ya viene convertida a MXN al TC de cada compra.')

    # Desglose de costo por capa ($/unidad, siempre MXN)
    mp_unit = fields.Float(
        string='Materia prima $/u MXN', digits=(16, 4),
        help='Receta (BOM) explotada al ÚLTIMO costo de compra de cada '
             'componente, convertido a MXN.')
    energia_unit = fields.Float(
        string='Energía $/u MXN', digits=(16, 4),
        help='Luz/gas/agua variables: $/kg del período × peso de la unidad.')
    fab_unit = fields.Float(
        string='Fabricación $/u MXN', digits=(16, 4),
        help='Parte del gasto FIJO de fábrica (sueldos de planta, renta, '
             'depreciación, arrendamiento de maquinaria) que absorbe cada '
             'unidad, repartida por peso y por metros.')
    op_pct = fields.Float(
        string='Operación % s/venta',
        help='Gastos de administración y ventas (6xx) como % de las ventas. '
             'Se cobra como % del precio.')
    costo_variable = fields.Float(
        string='Costo variable $/u MXN', digits=(16, 4),
        help='MP + energía: lo que sale de la bolsa por producir UNA unidad '
             'más. Piso absoluto de cualquier precio.')
    costo_absorbido_sin_op = fields.Float(
        string='Costo de producción $/u MXN', digits=(16, 4),
        help='Costo variable + fabricación absorbida (aún sin operación).')

    # Precios (guardados SIEMPRE en MXN; el espejo en divisa usa el TC)
    precio_objetivo = fields.Float(
        string='Precio objetivo $/u MXN',
        help='El precio que se propuso o que pidió el cliente. Se capturó '
             'en la moneda de la cotización y aquí está YA CONVERTIDO a MXN '
             'con el TC guardado.')
    precio_mercado = fields.Float(
        string='Precio de mercado $/u MXN',
        help='Promedio REAL facturado de este producto en los 12 meses '
             'previos a cotizar (todos los clientes). El ancla realista: '
             'los pisos dicen debajo de qué no bajar; el mercado dice qué '
             'se está logrando. 0 = sin ventas en la ventana.')
    piso_ocioso = fields.Float(
        string='Piso con capacidad ociosa $/u MXN',
        help='= costo variable. Con capacidad ociosa, todo precio arriba de '
             'esto APORTA a fijos (aunque el absorbido salga negativo). '
             'Nunca vender debajo.')
    piso_lleno = fields.Float(
        string='Piso a planta llena $/u MXN',
        help='= (variable + fab) ÷ (1 − op%): margen cero absorbiendo todo. '
             'Con la planta llena no aceptar debajo de esto.')
    margen_contribucion = fields.Float(
        string='Contribución $/u MXN', digits=(16, 4),
        help='Precio − costo variable: lo que cada unidad aporta para pagar '
             'los costos fijos.')
    margen_contribucion_pct = fields.Float(string='Contribución %')
    margen_bruto_pct = fields.Float(
        string='Margen bruto %',
        help='(precio − costo de producción) ÷ precio, al precio cotizado. '
             'Utilidad después de fabricar, ANTES de admin/ventas.')
    margen_neto_pct = fields.Float(
        string='Margen neto %',
        help='Margen bruto − %operación: lo que queda después de TODO.')

    # Espejo en divisa (desde el TC guardado al cotizar)
    precio_mercado_divisa = fields.Float(
        compute='_compute_divisa', string='Mercado (divisa)', digits=(16, 4))
    piso_ocioso_divisa = fields.Float(
        compute='_compute_divisa', string='Piso ocioso (divisa)', digits=(16, 4))
    piso_lleno_divisa = fields.Float(
        compute='_compute_divisa', string='Piso lleno (divisa)', digits=(16, 4))
    es_divisa = fields.Boolean(compute='_compute_divisa')

    @api.depends('fx_rate', 'precio_mercado', 'piso_ocioso', 'piso_lleno')
    def _compute_divisa(self):
        for rec in self:
            fx = rec.fx_rate if rec.fx_rate and rec.fx_rate != 1.0 else 0.0
            rec.es_divisa = bool(fx)
            rec.precio_mercado_divisa = rec.precio_mercado / fx if fx else 0.0
            rec.piso_ocioso_divisa = rec.piso_ocioso / fx if fx else 0.0
            rec.piso_lleno_divisa = rec.piso_lleno / fx if fx else 0.0

    # El precio EVALUADO (semáforo, márgenes, PDF cliente): el objetivo si
    # se capturó; si no, el de mercado; sin ventas, el piso a planta llena.
    precio_evaluado = fields.Float(
        compute='_compute_precio_evaluado', digits=(16, 2),
        string='Precio evaluado $/u MXN',
        help='Objetivo → mercado → piso lleno. Sobre este precio están '
             'calculados el semáforo y los márgenes.')
    evaluado_fuente = fields.Char(
        compute='_compute_precio_evaluado', string='Fuente del precio')
    precio_cliente_mxn = fields.Float(
        compute='_compute_precio_evaluado', digits=(16, 2),
        string='Precio al cliente $/u MXN')
    precio_cliente_divisa = fields.Float(
        compute='_compute_precio_evaluado', digits=(16, 4),
        string='Precio al cliente (divisa)')

    @api.depends('precio_objetivo', 'precio_mercado', 'piso_lleno', 'fx_rate')
    def _compute_precio_evaluado(self):
        for rec in self:
            if rec.precio_objetivo:
                rec.precio_evaluado = rec.precio_objetivo
                rec.evaluado_fuente = 'precio objetivo'
            elif rec.precio_mercado:
                rec.precio_evaluado = rec.precio_mercado
                rec.evaluado_fuente = 'precio de mercado (prom. 12m)'
            else:
                rec.precio_evaluado = rec.piso_lleno
                rec.evaluado_fuente = 'piso a planta llena'
            rec.precio_cliente_mxn = rec.precio_evaluado
            fx = rec.fx_rate if rec.fx_rate and rec.fx_rate != 1.0 else 0.0
            rec.precio_cliente_divisa = \
                rec.precio_cliente_mxn / fx if fx else 0.0
    contrib_hora_maquina = fields.Float(
        string='Contribución $/hora-máquina',
        help='Para rankear contra otros productos cuando hay cuello de botella.')

    semaforo = fields.Selection([
        ('rojo', 'Debajo del costo variable'),
        ('ambar', 'Aporta a fijos (no absorbe todo)'),
        ('verde', 'Cubre costo total + operación'),
    ], string='Semáforo de precio',
        help='Precio evaluado contra los pisos: rojo = destruye valor; '
             'ámbar = con capacidad ociosa conviene (aporta a fijos); '
             'verde = cubre el costo absorbido completo.')
    sale_order_id = fields.Many2one(
        'sale.order', string='Orden de venta', readonly=True,
        help='Orden desde la que se generó la cotización (si aplica).')

    # Chequeo de capacidad
    capacity_ok = fields.Boolean(string='¿Cabe en capacidad?')
    capacity_detail = fields.Text(
        string='Detalle de capacidad',
        help='Horas-máquina requeridas vs libres por centro de la ruta; '
             'cuántas máquinas/turnos faltan si no cabe.')

    desglose_html = fields.Html(
        string='Desglose explicado', sanitize=False,
        help='Foto del desglose de costos al momento de cotizar: BOM hoja '
             'por hoja con su última compra, peso, factores y fórmulas.')
    tramo_ids = fields.One2many(
        'qb.cotizacion.tramo', 'cotizacion_id',
        string='Escalera de volumen',
        help='Precios estandarizados por tramo de volumen: descuento fijo '
             'por cada duplicación, nunca debajo del piso a planta llena y '
             'con contribución total que nunca baja.')
    comparativa_html = fields.Html(
        string='Comparativa de precios', sanitize=False,
        help='Foto al cotizar: a cuánto se vendía este producto a otros '
             'clientes (últimos 12 meses) y a cuánto sus otras '
             'presentaciones (metros/kilos, nacional/importado), con el '
             'margen de cada una a su precio de venta.')
    glosario_html = fields.Html(
        compute='_compute_glosario', sanitize=False, string='Glosario',
        help='Definición de cada término del cotizador (precio objetivo, '
             'TC, márgenes, pisos, capacidad, ociosidad...).')

    def _compute_glosario(self):
        for rec in self:
            rec.glosario_html = GLOSARIO_HTML

    # Supuestos (trazabilidad)
    factores_id = fields.Many2one('qb.costo.factores', string='Factores usados')
    supuestos = fields.Text(
        string='Supuestos',
        help='Ventana de gastos, denominadores de producción, FX, fuente de '
             'peso — para reproducir el número después.')
    state = fields.Selection([
        ('draft', 'Borrador'),
        ('done', 'Presentada'),
        ('won', 'Ganada'),
        ('lost', 'Perdida'),
    ], default='draft', tracking=True)
    precio_vs_piso_pct = fields.Float(
        compute='_compute_precio_vs_piso', store=True,
        string='% sobre el piso lleno',
        help='(precio evaluado ÷ piso a planta llena) − 1. El insumo del '
             'análisis win/loss: ¿a qué % sobre el piso ganamos y a cuál '
             'perdemos?')

    @api.depends('precio_objetivo', 'precio_mercado', 'piso_lleno')
    def _compute_precio_vs_piso(self):
        for rec in self:
            base = rec.precio_objetivo or rec.precio_mercado
            rec.precio_vs_piso_pct = (
                100.0 * (base / rec.piso_lleno - 1.0)
                if base and rec.piso_lleno else 0.0)
    validez_hasta = fields.Date(
        string='Válida hasta',
        help='Después de esta fecha los supuestos (TC, último costo de MP) '
             'pueden haber cambiado: re-cotizar antes de comprometer.')

    # ------------------------------------------------------------------
    # Post-mortem: qué pasó DE VERDAD después de cotizar
    # (mejor práctica: cerrar el ciclo cotizado → real; sin esto las
    # cotizaciones nunca aprenden)
    # ------------------------------------------------------------------
    real_precio_prom = fields.Float(
        compute='_compute_real', string='Precio real $/u MXN', digits=(16, 2),
        help='Precio promedio al que este producto realmente se vendió en el '
             'último período costeado DESPUÉS de la cotización.')
    real_qty = fields.Float(
        compute='_compute_real', string='Qty real vendida/mes', digits=(16, 0))
    real_margen_pct = fields.Float(
        compute='_compute_real', string='Contribución real %')
    delta_precio_pct = fields.Float(
        compute='_compute_real', string='Δ precio real vs cotizado %',
        help='Positivo = se vendió más caro que lo cotizado; negativo = el '
             'precio real quedó por debajo de lo que se cotizó.')

    def _compute_real(self):
        Costo = self.env['qb.costo.producto']
        for rec in self:
            rec.real_precio_prom = rec.real_qty = 0.0
            rec.real_margen_pct = rec.delta_precio_pct = 0.0
            if not rec.product_id:
                continue
            real = Costo.search([
                ('product_id', '=', rec.product_id.id),
                ('period', '>=', (rec.create_date or fields.Datetime.now())
                 .date().replace(day=1)),
                ('qty_vendida', '>', 0),
            ], order='period DESC', limit=1)
            if not real:
                continue
            rec.real_precio_prom = real.precio_prom
            rec.real_qty = real.qty_vendida
            rec.real_margen_pct = real.margen_contribucion_pct
            base = rec.precio_objetivo or rec.precio_mercado
            if base:
                rec.delta_precio_pct = \
                    100.0 * (real.precio_prom - base) / base

    @api.onchange('product_id')
    def _onchange_product(self):
        if self.product_id:
            self.uom_name = self.product_id.uom_id.name

    # ------------------------------------------------------------------
    # Re-cotizar / duplicar escenario / enviar por correo
    # ------------------------------------------------------------------
    def action_recotizar(self):
        """Abre la calculadora precargada con esta cotización — para
        refrescarla con factores/TC de hoy o probar otro precio como nuevo
        escenario. Guardar crea una cotización NUEVA (la original queda
        intacta como histórico)."""
        self.ensure_one()
        Costo = self.env['qb.costo.producto']
        pricelist = getattr(self.partner_id, 'property_product_pricelist', None)
        currency = (pricelist.currency_id if pricelist and pricelist.currency_id
                    else self.env.company.currency_id)
        rate = Costo.to_mxn_rate(currency)
        ctx = {
            'default_partner_id': self.partner_id.id,
            'default_product_id': self.product_id.id,
            'default_currency_id': currency.id,
            'default_volumen': self.volumen,
            # El objetivo guardado es MXN → a la moneda de la cotización
            'default_precio_objetivo':
                self.precio_objetivo / rate if self.precio_objetivo else 0.0,
        }
        if not self.product_id:
            ctx.update({
                'default_spec_mode': True,
                'default_spec_descripcion': self.spec_descripcion,
                'default_spec_gramaje': self.spec_gramaje,
                'default_spec_ancho': self.spec_ancho,
                'default_spec_galga': self.spec_galga,
            })
        return {
            'type': 'ir.actions.act_window',
            'name': 'Re-cotizar / nuevo escenario',
            'res_model': 'qb.cotizador.wizard',
            'view_mode': 'form',
            'target': 'new',
            'context': ctx,
        }

    def action_enviar_correo(self):
        """Composer de correo al cliente con el PDF COMERCIAL adjunto
        (solo producto, precio y condiciones). La hoja interna de costo
        NUNCA se manda por aquí."""
        self.ensure_one()
        # Sin fallback a la plantilla vieja: aquella adjuntaba la hoja
        # INTERNA con costos y márgenes — jamás debe llegar al cliente.
        template = self.env.ref(
            'qb_capacidad_costeo.mail_template_cotizacion_cliente',
            raise_if_not_found=False)
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'mail.compose.message',
            'view_mode': 'form',
            'target': 'new',
            'context': {
                'default_model': 'qb.cotizacion',
                'default_res_ids': self.ids,
                'default_template_id': template.id if template else False,
                'default_composition_mode': 'comment',
            },
        }

    # ------------------------------------------------------------------
    # Ciclo de vida (botones explícitos; el chatter registra cada paso)
    # ------------------------------------------------------------------
    def action_marcar_presentada(self):
        self.filtered(lambda c: c.state == 'draft').write({'state': 'done'})

    def action_marcar_ganada(self):
        self.write({'state': 'won'})

    def action_marcar_perdida(self):
        self.write({'state': 'lost'})

    def action_reabrir(self):
        self.write({'state': 'draft'})


class QbCotizacionTramo(models.Model):
    _name = 'qb.cotizacion.tramo'
    _description = 'Tramo de la escalera de volumen de una cotización'
    _order = 'volumen'

    cotizacion_id = fields.Many2one(
        'qb.cotizacion', required=True, ondelete='cascade', index=True)
    multiplo = fields.Float(
        string='× volumen cotizado',
        help='0.5 = la mitad del volumen cotizado; 2 = el doble.')
    volumen = fields.Float(string='Volumen/mes', digits=(16, 0))
    es_base = fields.Boolean(
        string='Cotizado',
        help='El tramo del volumen realmente cotizado (múltiplo 1×).')
    precio_mxn = fields.Float(string='Precio $/u MXN', digits=(16, 2))
    precio_divisa = fields.Float(string='Precio (divisa)', digits=(16, 4))
    margen_neto_pct = fields.Float(string='Margen neto %', digits=(16, 1))
    contrib_total_mes = fields.Float(
        string='Contribución $/mes MXN', digits=(16, 0),
        help='(precio − costo variable) × volumen: el cheque total que ese '
             'tramo aporta a los fijos cada mes. La regla de la escalera es '
             'que NUNCA baje al crecer el volumen — si un descuento lo '
             'bajara, el precio del tramo se ajusta hacia arriba.')
    semaforo = fields.Selection([
        ('rojo', 'Debajo del costo variable'),
        ('ambar', 'Aporta a fijos'),
        ('verde', 'Cubre costo total'),
    ], string='Semáforo')
    capacity_ok = fields.Boolean(
        string='¿Cabe?',
        help='Si ese volumen cabe en las horas libres de la planta. Al '
             'cliente solo se le ofrecen tramos que sí caben.')
