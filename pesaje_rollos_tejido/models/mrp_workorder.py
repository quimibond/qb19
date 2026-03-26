# -*- coding: utf-8 -*-
from odoo import models

class MrpWorkorder(models.Model):
    _inherit = 'mrp.workorder'

    def action_view_recorded_rolls(self):
        self.ensure_one()
        return self.production_id.action_view_recorded_rolls()