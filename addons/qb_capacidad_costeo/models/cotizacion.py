# -*- coding: utf-8 -*-
"""Cotizaciones guardadas: resultado del cotizador con sus supuestos.

Cada cotización queda con los factores usados (FX, ventana de gastos,
denominadores) para trazabilidad y comparación antes/después.
"""
from odoo import api, fields, models


class QbCotizacion(models.Model):
    _name = 'qb.cotizacion'
    _description = 'Cotización de capacidad y costo'
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
    ], default='draft')
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
