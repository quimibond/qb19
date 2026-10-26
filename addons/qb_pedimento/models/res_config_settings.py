# -*- coding: utf-8 -*-
from odoo import fields, models


class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    qb_pedimento_responsable_id = fields.Many2one(
        related='company_id.qb_pedimento_responsable_id', readonly=False)
    qb_pedimento_dias_cruce = fields.Integer(related='company_id.qb_pedimento_dias_cruce', readonly=False)
    qb_pedimento_dias_cierre = fields.Integer(related='company_id.qb_pedimento_dias_cierre', readonly=False)
    qb_pedimento_dias_garantia = fields.Integer(related='company_id.qb_pedimento_dias_garantia', readonly=False)
    qb_pedimento_product_igi_id = fields.Many2one(
        related='company_id.qb_pedimento_product_igi_id', readonly=False)
    qb_pedimento_product_dta_id = fields.Many2one(
        related='company_id.qb_pedimento_product_dta_id', readonly=False)
    qb_pedimento_product_prv_id = fields.Many2one(
        related='company_id.qb_pedimento_product_prv_id', readonly=False)
    qb_pedimento_product_gastos_id = fields.Many2one(
        related='company_id.qb_pedimento_product_gastos_id', readonly=False)
    qb_pedimento_contabilidad_user_id = fields.Many2one(
        related='company_id.qb_pedimento_contabilidad_user_id', readonly=False)
