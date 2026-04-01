# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError

class ProductTemplate(models.Model):
    _inherit = 'product.template'
    categ_name_static = fields.Char(related='categ_id.complete_name', string="Categoría", store=False)
    porcentaje_revision_standard = fields.Float(string='% Revisión Estándar', default=0.1)

class MrpProduction(models.Model):
    _inherit = 'mrp.production'

    porcentaje_revision = fields.Float(string='% de Revisión', compute='_compute_revision_perc')
    revision_log_ids = fields.One2many('mrp.revision.log', 'production_id', string="Log Calidad")

    def _compute_revision_perc(self):
        get_param = self.env['ir.config_parameter'].sudo().get_param
        val = float(get_param('pesaje_rollos_tejido.qc_sample_percentage', default=10.0)) / 100.0
        for reg in self:
            reg.porcentaje_revision = val

    def button_mark_done(self):
        for production in self:
            if production.product_id.categ_id.complete_name == 'Producto En Proceso / Tac-Producto en proceso-Tejido Circular-kg':
                pendientes = production.move_finished_ids.move_line_ids.lot_id.filtered(lambda l: l.needs_review and not l.is_reviewed)
                if pendientes:
                    raise UserError(_("CALIDAD: Faltan revisiones: %s") % ", ".join(pendientes.mapped('name')))
        return super(MrpProduction, self).button_mark_done()

    def _auto_sorteo_revision(self):
        for production in self:
            if production.product_id.categ_id.complete_name != 'Producto En Proceso / Tac-Producto en proceso-Tejido Circular-kg':
                continue
            lotes = production.move_finished_ids.move_line_ids.lot_id
            if not lotes: continue
            cuota = max(1, int(len(lotes) * production.porcentaje_revision))
            if len(lotes.filtered(lambda l: l.needs_review)) < cuota:
                ultimo = lotes.sorted('id', reverse=True)[0]
                if not ultimo.needs_review:
                    ultimo.write({'needs_review': True})

class MrpWorkorder(models.Model):
    _inherit = 'mrp.workorder'
    def button_finish(self):
        for wo in self:
            pendientes = wo.production_id.move_finished_ids.move_line_ids.lot_id.filtered(lambda l: l.needs_review and not l.is_reviewed)
            if pendientes:
                raise UserError(_("CALIDAD: Revise rollos sorteados antes de finalizar."))
        return super(MrpWorkorder, self).button_finish()

# ESTA SECCIÓN SOLUCIONA EL ERROR:
class StockMoveLine(models.Model):
    _inherit = 'stock.move.line'
    needs_review = fields.Boolean(related='lot_id.needs_review', string="Sorteado", readonly=True)
    is_reviewed = fields.Boolean(related='lot_id.is_reviewed', string="Revisado", readonly=True)

class MrpRevisionLog(models.Model):
    _name = 'mrp.revision.log'
    _description = 'Log de Calidad'
    _order = 'create_date desc'
    production_id = fields.Many2one('mrp.production')
    lot_id = fields.Many2one('stock.lot', string="Rollo")
    user_id = fields.Many2one('res.users', string="Revisor", default=lambda self: self.env.user)
    peso_original = fields.Float()
    peso_final = fields.Float()
    diferencia = fields.Float(compute="_compute_diff", store=True)
    create_date = fields.Datetime(default=fields.Datetime.now)
    @api.depends('peso_original', 'peso_final')
    def _compute_diff(self):
        for reg in self: reg.diferencia = reg.peso_final - reg.peso_original

class StockLot(models.Model):
    _inherit = 'stock.lot'
    production_id = fields.Many2one('mrp.production')
    needs_review = fields.Boolean(default=False)
    is_reviewed = fields.Boolean(default=False)