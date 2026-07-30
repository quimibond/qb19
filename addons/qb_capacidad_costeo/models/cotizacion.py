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

    @api.onchange('product_id')
    def _onchange_product(self):
        if self.product_id:
            self.uom_name = self.product_id.uom_id.name
