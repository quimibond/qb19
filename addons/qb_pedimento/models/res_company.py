# -*- coding: utf-8 -*-
from odoo import fields, models


class ResCompany(models.Model):
    _inherit = 'res.company'

    qb_pedimento_responsable_id = fields.Many2one(
        'res.users', 'Responsable de pedimentos',
        help='Responsable por omisión de los expedientes y de los avisos de garantías.')
    qb_pedimento_dias_cruce = fields.Integer(
        'Días hábiles para cruzar', default=3,
        help='Aviso si un pedimento despachado no cruza o no llega a planta en estos días hábiles (C2.31).')
    qb_pedimento_dias_cierre = fields.Integer(
        'Días hábiles para cerrar', default=5,
        help='Aviso si un expediente entregado no se cierra en estos días hábiles (C2.32, indicador C2-05).')
    qb_pedimento_dias_garantia = fields.Integer(
        'Días para reclamar garantía', default=60,
        help='Aviso si una garantía sigue sin devolver después de estos días naturales.')
    qb_pedimento_product_igi_id = fields.Many2one(
        'product.product', 'Producto IGI', domain="[('landed_cost_ok', '=', True)]")
    qb_pedimento_product_dta_id = fields.Many2one(
        'product.product', 'Producto DTA', domain="[('landed_cost_ok', '=', True)]")
    qb_pedimento_product_prv_id = fields.Many2one(
        'product.product', 'Producto PRV', domain="[('landed_cost_ok', '=', True)]")
    qb_pedimento_product_gastos_id = fields.Many2one(
        'product.product', 'Producto gastos de importación', domain="[('landed_cost_ok', '=', True)]")
    qb_pedimento_contabilidad_user_id = fields.Many2one(
        'res.users', 'Responsable en contabilidad',
        help='Recibe el aviso de importaciones sin factura de impuestos o que no cuadran con contabilidad. '
             'Vacío: el responsable del expediente.')
