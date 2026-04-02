# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError

class ProductTemplate(models.Model):
    _inherit = 'product.template'
    categ_name_static = fields.Char(related='categ_id.complete_name', string="Categoría", store=False)
    porcentaje_revision_standard = fields.Float(string='% Revisión Estándar', default=0.1)

class MrpProduction(models.Model):
    _inherit = 'mrp.production'

    rollos_revisados_count = fields.Integer(
        string="Rollos Revisados", 
        compute="_compute_rollos_revisados_count"
    )

    porcentaje_revision = fields.Float(string='% de Revisión', compute='_compute_revision_perc')
    revision_log_ids = fields.One2many('mrp.revision.log', 'production_id', string="Log Calidad")

    def _compute_revision_perc(self):
        get_param = self.env['ir.config_parameter'].sudo().get_param
        val = float(get_param('pesaje_rollos_tejido.qc_sample_percentage', default=10.0)) / 100.0
        for reg in self:
            reg.porcentaje_revision = val

    def button_mark_done(self):
        """ 
        Mantiene tu validación original: No permite cerrar la OF si hay rollos 
        sorteados pendientes de revisión.
        """
        for production in self:
            if production.product_id.categ_id.complete_name == 'Producto En Proceso / Tac-Producto en proceso-Tejido Circular-kg':
                pendientes = production.move_finished_ids.move_line_ids.lot_id.filtered(
                    lambda l: l.needs_review and not l.is_reviewed
                )
                if pendientes:
                    raise UserError(_("CALIDAD: Revise rollos sorteados antes de finalizar la Orden de Fabricación."))
        return super(MrpProduction, self).button_mark_done()
    
    @api.depends('revision_log_ids') # IMPORTANTE: Verifica que este sea el nombre de tu One2many
    def _compute_rollos_revisados_count(self):
        for reg in self:
            # Contamos cuántos registros hay en el historial de esta OF
            reg.rollos_revisados_count = len(reg.revision_log_ids)

class MrpWorkorder(models.Model):
    _inherit = 'mrp.workorder'

    def button_finish(self):
        """ 
        Mantiene tu validación original en la Orden de Trabajo.
        """
        for wo in self:
            pendientes = wo.production_id.move_finished_ids.move_line_ids.lot_id.filtered(
                lambda l: l.needs_review and not l.is_reviewed
            )
            if pendientes:
                raise UserError(_("CALIDAD: Revise rollos sorteados antes de finalizar la operación."))
        return super(MrpWorkorder, self).button_finish()

class StockMoveLine(models.Model):
    _inherit = 'stock.move.line'
    needs_review = fields.Boolean(related='lot_id.needs_review', string="Sorteado", readonly=True)
    is_reviewed = fields.Boolean(related='lot_id.is_reviewed', string="Revisado", readonly=True)

class MrpRevisionLog(models.Model):
    _name = 'mrp.revision.log'
    _description = 'Log de Calidad'
    _order = 'create_date desc'

    production_id = fields.Many2one('mrp.production', string="Orden de Fabricación")
    lot_id = fields.Many2one('stock.lot', string="Rollo", required=True)
    user_id = fields.Many2one('res.users', string="Revisor", default=lambda self: self.env.user)
    
    # Peso que traía el rollo desde el pesaje (Solo lectura)
    peso_original = fields.Float(string="Peso Inicial", readonly=True)
    
    # Peso que el revisor detecta en su báscula
    peso_final = fields.Float(string="Peso Revisado")
     
    diferencia = fields.Float(compute="_compute_diff", string="Diferencia", store=True)
    create_date = fields.Datetime(string="Fecha/Hora", default=fields.Datetime.now)

    @api.depends('peso_original', 'peso_final')
    def _compute_diff(self):
        for reg in self:
            reg.diferencia = reg.peso_final - reg.peso_original
    
    # Punto 8 y 9: Causa de desviación usando Etiquetas de Calidad filtradas por TEJIDO
    causa_id = fields.Many2one(
        'quality.tag', 
        string="Causa de Desviación",
        domain="[('name', '=like', 'TEJIDO%')]"
    )

    @api.depends('peso_original', 'peso_final')
    def _compute_diff(self):
        for reg in self:
            # Mantenemos el cálculo de diferencia para que el subproducto pueda validarlo
            reg.diferencia = reg.peso_original - reg.peso_final

class StockLot(models.Model):
    _inherit = 'stock.lot'
    
    needs_review = fields.Boolean(string="Necesita Revisión", default=False)
    is_reviewed = fields.Boolean(string="Revisado", default=False)