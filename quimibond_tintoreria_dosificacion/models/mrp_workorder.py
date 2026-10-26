# -*- coding: utf-8 -*-
from odoo import models


class MrpWorkorder(models.Model):
    _inherit = 'mrp.workorder'

    def write(self, vals):
        res = super().write(vals)
        if 'workcenter_id' in vals and not self.env.context.get('skip_dosificacion'):
            # Si cambia la máquina de Tintorería se recalcula con su relación de baño,
            # mientras la requisición no se haya surtido.
            self.production_id._recalcular_dosificacion()
        return res
