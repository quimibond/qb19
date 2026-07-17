# -*- coding: utf-8 -*-
from odoo import models, fields, api
from odoo.exceptions import UserError


class QualityPoint(models.Model):
    _inherit = 'quality.point'

    sgi_control_plan_id = fields.Many2one('sgi.control.plan', string="Plan de control",
                                          ondelete='set null', index=True)
    sgi_characteristic = fields.Char(string="Característica",
                                     help="Característica de la Master Spec del cliente.")
    sgi_criticality = fields.Selection([
        ('f', "F - Función"),
        ('r', "R - Regulación"),
        ('s', "S - Seguridad"),
    ], string="Criticidad (F/R/S)",
        help="Esquema de criticidad tipo Continental: F=Función, R=Regulación, S=Seguridad.")
    sgi_in_coa = fields.Boolean(string="Aparece en el Certificado de Calidad",
                                help="Si está marcado, esta característica se imprime en el "
                                     "Certificado de Conformidad (CoA) del lote.")
    sgi_cpk_target = fields.Float(string="Cpk objetivo", digits=(4, 2),
                                  help="Cpk objetivo sugerido: 1.33 para F, 1.67 para R/S.")
    sgi_reaction_plan = fields.Text(string="Plan de reacción")


class SgiControlPlan(models.Model):
    _name = 'sgi.control.plan'
    _description = "Plan de control (P-C11)"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'folio desc'

    folio = fields.Char(string="Folio", readonly=True, copy=False, index=True, tracking=True)
    name = fields.Char(string="Nombre", required=True, tracking=True)
    partner_id = fields.Many2one('res.partner', string="Cliente",
                                 domain="[('is_company', '=', True)]")
    product_tmpl_ids = fields.Many2many('product.template', string="Productos")
    phase = fields.Selection([
        ('prototipo', "Prototipo"),
        ('prelanzamiento', "Prelanzamiento"),
        ('produccion', "Producción"),
    ], string="Fase", default='produccion', required=True, tracking=True)
    revision = fields.Char(string="Revisión", default="00", tracking=True)
    state = fields.Selection([
        ('borrador', "Borrador"),
        ('vigente', "Vigente"),
        ('obsoleto', "Obsoleto"),
    ], string="Estado", default='borrador', required=True, tracking=True)
    point_ids = fields.One2many('quality.point', 'sgi_control_plan_id',
                                string="Puntos de control")
    point_count = fields.Integer(string="N° de puntos", compute='_compute_point_count')
    document_id = fields.Many2one('documents.document', string="Especificación del cliente")
    notes = fields.Text(string="Notas")

    @api.model_create_multi
    def create(self, vals_list):
        seq = self.env['ir.sequence']
        for vals in vals_list:
            if not vals.get('folio'):
                vals['folio'] = seq.next_by_code('sgi.control.plan') or '/'
        return super().create(vals_list)

    @api.depends('point_ids')
    def _compute_point_count(self):
        for plan in self:
            plan.point_count = len(plan.point_ids)

    def action_set_vigente(self):
        for plan in self:
            if not plan.point_ids:
                raise UserError(
                    "El plan de control %s no puede pasar a Vigente sin al menos "
                    "un punto de control." % (plan.folio or plan.name))
            plan.state = 'vigente'
        return True

    def action_set_borrador(self):
        self.write({'state': 'borrador'})
        return True

    def action_set_obsoleto(self):
        Cron = self.env['sgi.cron']
        manager_id = Cron._sgi_manager_user_id()
        for plan in self:
            plan.state = 'obsoleto'
            # No se desactivan los quality.point automáticamente (pueden vivir en
            # otro plan): se agenda una revisión al Jefe MAST.
            if plan.point_ids and manager_id:
                Cron._sgi_schedule(
                    plan,
                    "Revisar puntos del plan obsoleto %s" % (plan.folio or plan.name),
                    "El plan de control pasó a obsoleto. Revise si sus %d punto(s) de "
                    "control deben desactivarse o reasignarse a otro plan vigente." % len(plan.point_ids),
                    manager_id)
        return True

    def action_view_points(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Puntos de control",
            'res_model': 'quality.point',
            'view_mode': 'list,form',
            'domain': [('sgi_control_plan_id', '=', self.id)],
            'context': {'default_sgi_control_plan_id': self.id},
        }


class StockLot(models.Model):
    _inherit = 'stock.lot'

    def _sgi_coa_checks(self):
        """Devuelve los quality.check del lote ligados a puntos con sgi_in_coa."""
        self.ensure_one()
        return self.env['quality.check'].search([
            ('lot_ids', 'in', self.ids),
            ('point_id.sgi_in_coa', '=', True),
        ])

    def action_sgi_print_coa(self):
        self.ensure_one()
        if not self._sgi_coa_checks():
            raise UserError(
                "El lote %s no tiene inspecciones de calidad ligadas a puntos "
                "marcados para el Certificado de Calidad. Capture los quality.check "
                "correspondientes antes de emitir el CoA." % self.name)
        return self.env.ref('quimibond_sgi.action_report_coa').report_action(self)
