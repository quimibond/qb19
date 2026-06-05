from odoo import models, fields

class CorregirRolloWizard(models.TransientModel):
    _name = 'corregir.rollo.wizard'

    # Campos que el Administrador verá en la ventanita
    log_id = fields.Many2one('mrp.weighing.log')
    nuevo_valor = fields.Integer(string="Nuevo número de Rollo Circular")

    def confirmar(self):
        # Aquí solo actualizamos el número y cerramos
        self.log_id.rollo_circular = self.nuevo_valor
        return {'type': 'ir.actions.act_window_close'}