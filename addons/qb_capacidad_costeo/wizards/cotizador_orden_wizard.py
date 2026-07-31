# -*- coding: utf-8 -*-
"""Cotizador de ORDEN COMPLETA: todas las líneas de una vez.

Desde una orden con varios productos, muestra cada línea con su costo
variable, pisos, precio actual vs sugerido y semáforo; permite corregir
precios en lote (checkbox por línea) y abrir la calculadora individual
para el detalle de cualquier producto. Las matemáticas son las mismas del
motor (qb.costo.producto.quote_product) — una sola fuente de fórmulas.
"""
from odoo import api, fields, models
from odoo.exceptions import UserError

SEMAFORO = [
    ('rojo', 'Debajo del costo variable'),
    ('ambar', 'Aporta a fijos'),
    ('verde', 'Cubre costo total'),
]


class QbCotizadorOrdenWizard(models.TransientModel):
    _name = 'qb.cotizador.orden.wizard'
    _description = 'Cotizador de orden completa'

    sale_order_id = fields.Many2one('sale.order', required=True, readonly=True)
    partner_id = fields.Many2one(related='sale_order_id.partner_id')
    line_ids = fields.One2many(
        'qb.cotizador.orden.linea', 'wizard_id', string='Líneas')
    factores_id = fields.Many2one('qb.costo.factores', readonly=True)
    resumen = fields.Char(compute='_compute_resumen')

    @api.model
    def default_get(self, fields_list):
        res = super().default_get(fields_list)
        order_id = self.env.context.get('active_id') \
            if self.env.context.get('active_model') == 'sale.order' \
            else self.env.context.get('default_sale_order_id')
        order = self.env['sale.order'].browse(order_id).exists()
        if not order:
            raise UserError('Este cotizador se abre desde una orden de venta.')
        Costo = self.env['qb.costo.producto']
        factores = self.env['qb.costo.factores'].search(
            [], order='period DESC', limit=1)
        if not factores:
            raise UserError(
                'Aún no hay factores calculados: corre "Recalcular costeo '
                '(mes anterior)" en Configuración una primera vez.')
        lines = []
        for line in order.order_line.filtered('product_id'):
            q = Costo.quote_product(line.product_id, factores)
            precio_actual = line.price_unit
            semaforo = Costo.semaforo_for(
                precio_actual, q['piso_ocioso'], q['piso_lleno'])
            lines.append((0, 0, {
                'sale_line_id': line.id,
                'product_id': line.product_id.id,
                'qty': line.product_uom_qty,
                'uom_name': line.product_id.uom_id.name,
                'precio_actual': precio_actual,
                'costo_variable': q['variable'],
                'fab_unit': q['fab'],
                'piso_lleno': q['piso_lleno'],
                'precio_sugerido': q['precio_sugerido'],
                'nuevo_precio': q['precio_sugerido'],
                'semaforo': semaforo,
                'contrib_unit': precio_actual - q['variable'],
                'contrib_hora': ((precio_actual - q['variable'])
                                 / q['hours_per_unit']
                                 if q['hours_per_unit'] else 0.0),
                # Pre-marcar solo lo que destruye valor: decisión obvia
                'aplicar': semaforo == 'rojo',
            }))
        res.update({
            'sale_order_id': order.id,
            'factores_id': factores.id,
            'line_ids': lines,
        })
        return res

    @api.depends('line_ids.semaforo', 'line_ids.contrib_unit',
                 'line_ids.qty', 'line_ids.precio_actual')
    def _compute_resumen(self):
        for wiz in self:
            rojos = len(wiz.line_ids.filtered(lambda l: l.semaforo == 'rojo'))
            ambar = len(wiz.line_ids.filtered(lambda l: l.semaforo == 'ambar'))
            contrib = sum(l.contrib_unit * l.qty for l in wiz.line_ids)
            venta = sum(l.precio_actual * l.qty for l in wiz.line_ids)
            wiz.resumen = (
                '%s líneas · 🔴 %s · 🟡 %s · contribución total $%s '
                '(%.1f%% de $%s de venta)' % (
                    len(wiz.line_ids), rojos, ambar, f'{contrib:,.0f}',
                    100.0 * contrib / venta if venta else 0.0,
                    f'{venta:,.0f}'))

    def action_aplicar_seleccionados(self):
        """Escribe nuevo_precio en las líneas marcadas y documenta en el
        chatter de la orden qué cambió y por qué."""
        self.ensure_one()
        aplicar = self.line_ids.filtered('aplicar')
        if not aplicar:
            raise UserError('Marca al menos una línea en "Aplicar".')
        cambios = []
        for line in aplicar:
            if line.nuevo_precio <= 0:
                raise UserError('El nuevo precio de %s debe ser mayor a 0.'
                                % line.product_id.display_name)
            cambios.append('%s: $%.2f → $%.2f (sugerido $%.2f, variable $%.2f)'
                           % (line.product_id.display_name,
                              line.precio_actual, line.nuevo_precio,
                              line.precio_sugerido, line.costo_variable))
            line.sale_line_id.price_unit = line.nuevo_precio
        self.sale_order_id.message_post(
            body='Cotizador de costos — precios actualizados '
                 '(factores %s):<br/>%s'
                 % (self.factores_id.period, '<br/>'.join(cambios)))
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'sale.order',
            'res_id': self.sale_order_id.id,
            'view_mode': 'form',
            'target': 'current',
        }


class QbCotizadorOrdenLinea(models.TransientModel):
    _name = 'qb.cotizador.orden.linea'
    _description = 'Línea del cotizador de orden'

    wizard_id = fields.Many2one(
        'qb.cotizador.orden.wizard', required=True, ondelete='cascade')
    sale_line_id = fields.Many2one('sale.order.line', readonly=True)
    product_id = fields.Many2one('product.product', readonly=True)
    qty = fields.Float(string='Cantidad', readonly=True)
    uom_name = fields.Char(string='UoM', readonly=True)
    precio_actual = fields.Float(string='Precio actual', readonly=True,
                                 digits=(16, 2))
    costo_variable = fields.Float(string='Costo variable', readonly=True,
                                  digits=(16, 2))
    fab_unit = fields.Float(string='Fabricación', readonly=True, digits=(16, 2))
    piso_lleno = fields.Float(string='Piso lleno', readonly=True, digits=(16, 2))
    precio_sugerido = fields.Float(string='Sugerido', readonly=True,
                                   digits=(16, 2))
    contrib_unit = fields.Float(string='Contribución/u', readonly=True,
                                digits=(16, 2))
    contrib_hora = fields.Float(string='$/hora-máquina', readonly=True,
                                digits=(16, 0))
    semaforo = fields.Selection(SEMAFORO, readonly=True)
    aplicar = fields.Boolean(
        string='Aplicar',
        help='Al confirmar, el "Nuevo precio" se escribe en esta línea.')
    nuevo_precio = fields.Float(string='Nuevo precio', digits=(16, 2))

    def action_detalle(self):
        """Abre la calculadora individual precargada con esta línea (para
        jugar con volumen/margen y ver capacidad al detalle)."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'qb.cotizador.wizard',
            'view_mode': 'form',
            'target': 'new',
            'context': {
                'default_sale_order_id': self.wizard_id.sale_order_id.id,
                'default_sale_line_id': self.sale_line_id.id,
                'default_partner_id': self.wizard_id.partner_id.id,
                'default_product_id': self.product_id.id,
                'default_precio_objetivo': self.precio_actual,
                'default_volumen': self.qty,
            },
        }
