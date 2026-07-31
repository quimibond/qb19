# -*- coding: utf-8 -*-
"""Cotizaciones guardadas: resultado del cotizador con sus supuestos.

Cada cotización queda con los factores usados (FX, ventana de gastos,
denominadores) para trazabilidad y comparación antes/después.
"""
from odoo import api, fields, models


class QbCotizacion(models.Model):
    _name = 'qb.cotizacion'
    _description = 'Cotización de capacidad y costo'
    _inherit = ['mail.thread']
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
    fx_rate = fields.Float(
        string='FX (MXN/USD)',
        help='Supuesto de tipo de cambio usado. Informativo: la MP ya viene '
             'convertida a MXN al FX de cada compra.')

    # Desglose de costo por capa ($/unidad)
    mp_unit = fields.Float(string='MP $/u', digits=(16, 4))
    energia_unit = fields.Float(string='Energía $/u', digits=(16, 4))
    fab_unit = fields.Float(string='Fabricación $/u', digits=(16, 4))
    op_pct = fields.Float(string='Operación %')
    costo_variable = fields.Float(string='Costo variable $/u', digits=(16, 4))
    costo_absorbido_sin_op = fields.Float(
        string='Costo variable + fab $/u', digits=(16, 4))

    # Precios
    target_margin = fields.Float(string='Margen meta %')
    precio_objetivo = fields.Float(string='Precio objetivo $/u')
    precio_sugerido = fields.Float(
        string='Precio sugerido $/u',
        help='costo ÷ (1 − margen meta − op%): cubre operación y deja el '
             'margen meta sobre venta.')
    piso_ocioso = fields.Float(
        string='Piso con capacidad ociosa $/u',
        help='= costo variable. Con capacidad ociosa, todo precio arriba de '
             'esto APORTA a fijos (aunque el absorbido salga negativo).')
    piso_lleno = fields.Float(
        string='Piso a planta llena $/u',
        help='= (variable + fab) ÷ (1 − op%): margen cero absorbiendo todo. '
             'Con la planta llena no aceptar debajo de esto.')
    margen_contribucion = fields.Float(string='Contribución $/u', digits=(16, 4))
    margen_contribucion_pct = fields.Float(string='Contribución %')
    margen_bruto_pct = fields.Float(
        string='Margen bruto %',
        help='(precio − costo de producción) ÷ precio, al precio cotizado.')
    margen_neto_pct = fields.Float(
        string='Margen neto %',
        help='(precio − costo de producción − operación) ÷ precio.')

    # Espejo en divisa (desde el TC guardado al cotizar)
    precio_sugerido_divisa = fields.Float(
        compute='_compute_divisa', string='Sugerido (divisa)', digits=(16, 4))
    piso_ocioso_divisa = fields.Float(
        compute='_compute_divisa', string='Piso ocioso (divisa)', digits=(16, 4))
    piso_lleno_divisa = fields.Float(
        compute='_compute_divisa', string='Piso lleno (divisa)', digits=(16, 4))
    es_divisa = fields.Boolean(compute='_compute_divisa')

    @api.depends('fx_rate', 'precio_sugerido', 'piso_ocioso', 'piso_lleno')
    def _compute_divisa(self):
        for rec in self:
            fx = rec.fx_rate if rec.fx_rate and rec.fx_rate != 1.0 else 0.0
            rec.es_divisa = bool(fx)
            rec.precio_sugerido_divisa = rec.precio_sugerido / fx if fx else 0.0
            rec.piso_ocioso_divisa = rec.piso_ocioso / fx if fx else 0.0
            rec.piso_lleno_divisa = rec.piso_lleno / fx if fx else 0.0
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

    @api.depends('precio_objetivo', 'precio_sugerido', 'piso_lleno')
    def _compute_precio_vs_piso(self):
        for rec in self:
            base = rec.precio_objetivo or rec.precio_sugerido
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
            base = rec.precio_objetivo or rec.precio_sugerido
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
            'default_target_margin': self.target_margin,
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
        """Composer de correo con la plantilla y el PDF adjunto."""
        self.ensure_one()
        template = self.env.ref(
            'qb_capacidad_costeo.mail_template_cotizacion',
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
