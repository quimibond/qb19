# -*- coding: utf-8 -*-
from odoo import models, fields, api

class MrpWorkorder(models.Model):
    _inherit = 'mrp.workorder'

    product_id_category_name = fields.Char(
        related='production_id.product_id.categ_id.display_name',
        readonly=True
    )

    def action_view_recorded_rolls(self):
        self.ensure_one()
        return self.production_id.action_view_recorded_rolls()