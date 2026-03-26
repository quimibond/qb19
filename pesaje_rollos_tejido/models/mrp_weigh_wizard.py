from odoo import models, fields, api

class MrpWeighRollWizard(models.TransientModel):
    _name = 'mrp.weigh.roll.wizard'
    _description = 'Asistente de Pesado de Rollos'

    workorder_id = fields.Many2one('mrp.workorder', string="Orden de Trabajo")
    production_id = fields.Many2one('mrp.production', related="workorder_id.production_id", string="Orden de Fabricación")
    workcenter_id = fields.Many2one('mrp.workcenter', related="workorder_id.workcenter_id", string="Centro de Trabajo")
    product_id = fields.Many2one('product.product', related="production_id.product_id", string="Artículo")
    
    qty_to_produce = fields.Float(related="production_id.product_qty", string="Total a Producir")
    qty_produced = fields.Float(related="production_id.qty_producing", string="Producido al Momento")
    roll_count = fields.Integer(related="production_id.roll_count", string="Rollos Registrados")
    
    next_lot_name = fields.Char(string="Número de Lote a Generar", compute="_compute_next_lot_name")
    weight = fields.Float(string="Peso del Rollo Actual (kg)", digits=(12, 3), required=True)

    @api.depends('workorder_id', 'production_id.roll_count')
    def _compute_next_lot_name(self):
        for reg in self:
            if reg.production_id:
                ref = reg.product_id.default_code or "TELA"
                
                # Verificación segura del campo de lote de producción
                prod = reg.production_id
                mo_lot = ""
                
                if hasattr(prod, 'lot_producing_id') and prod.lot_producing_id:
                    mo_lot = prod.lot_producing_id.name
                else:
                    # Si no existe el campo, usamos el final del nombre de la MO
                    mo_lot = prod.name.split('/')[-1]
                
                next_count = prod.roll_count + 1
                reg.next_lot_name = f"{ref}{mo_lot}-{next_count:04d}"
            else:
                reg.next_lot_name = "N/A"

    def confirm_weighing(self):
        self.ensure_one()
        return self.production_id.action_register_roll_with_weight(self.weight)