# -*- coding: utf-8 -*-
from odoo import models, _
from odoo.exceptions import UserError


class MrpWorkorder(models.Model):
    _inherit = 'mrp.workorder'

    def action_registro_rama(self):
        """REGISTRO RAMA: abre el libro de Rama del artículo de la orden."""
        self.ensure_one()
        if not self.es_rama:
            raise UserError(_('Esta operación no es de Rama.'))
        return self.env['mrp.hoja.calidad']._abrir(self.production_id.product_id, 'rama')
