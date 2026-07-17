# -*- coding: utf-8 -*-
from dateutil.relativedelta import relativedelta

from odoo import models, fields, api


class ResPartner(models.Model):
    _inherit = 'res.partner'

    sgi_supplier_class = fields.Selection([
        ('acreditado', "Acreditado"),
        ('condicionado', "Condicionado"),
        ('baja', "Baja"),
    ], string="Clasificación SGI", tracking=True)
    sgi_supplier_score = fields.Float(string="Calificación SGI")
    sgi_last_eval_date = fields.Date(string="Última evaluación")
    sgi_eval_ids = fields.One2many('sgi.supplier.eval', 'partner_id', string="Evaluaciones SGI")
    sgi_eval_count = fields.Integer(string="# Evaluaciones", compute='_compute_sgi_eval_count')

    def _compute_sgi_eval_count(self):
        data = self.env['sgi.supplier.eval']._read_group(
            [('partner_id', 'in', self.ids)], ['partner_id'], ['__count'])
        mapped = {partner.id: count for partner, count in data}
        for partner in self:
            partner.sgi_eval_count = mapped.get(partner.id, 0)

    def action_sgi_open_evals(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Evaluaciones — %s" % self.display_name,
            'res_model': 'sgi.supplier.eval',
            'view_mode': 'list,form',
            'domain': [('partner_id', '=', self.id)],
            'context': {'default_partner_id': self.id},
        }


class SgiSupplierEval(models.Model):
    _name = 'sgi.supplier.eval'
    _description = "Evaluación de proveedor SGI (8.4)"
    _order = 'date_to desc, partner_id'

    partner_id = fields.Many2one('res.partner', string="Proveedor",
                                 required=True, ondelete='cascade', index=True)
    date_from = fields.Date(string="Desde", required=True)
    date_to = fields.Date(string="Hasta", required=True)
    otd_pct = fields.Float(string="OTD %", compute='_compute_metrics', store=True)
    nc_count = fields.Integer(string="# NC", compute='_compute_metrics', store=True)
    score = fields.Float(string="Calificación", compute='_compute_metrics', store=True)
    supplier_class = fields.Selection([
        ('acreditado', "Acreditado"),
        ('condicionado', "Condicionado"),
        ('baja', "Baja"),
    ], string="Clasificación", compute='_compute_metrics', store=True)
    notes = fields.Text(string="Notas")

    _partner_period_uniq = models.Constraint(
        'unique(partner_id, date_from, date_to)',
        "Ya existe una evaluación de este proveedor para el periodo.",
    )

    @api.depends('partner_id', 'date_from', 'date_to')
    def _compute_metrics(self):
        Param = self.env['ir.config_parameter'].sudo()
        w_otd = float(Param.get_param('quimibond_sgi.supplier_weight_otd', 0.7))
        w_quality = float(Param.get_param('quimibond_sgi.supplier_weight_quality', 0.3))
        for ev in self:
            if not ev.partner_id or not ev.date_from or not ev.date_to:
                ev.otd_pct = ev.score = 0.0
                ev.nc_count = 0
                ev.supplier_class = False
                continue
            ev.otd_pct = ev._sgi_compute_otd()
            ev.nc_count = ev._sgi_count_ncs()
            quality_score = max(0.0, 100.0 - ev.nc_count * 10.0)
            ev.score = round(ev.otd_pct * w_otd + quality_score * w_quality, 2)
            ev.supplier_class = ev._sgi_class_from_score(ev.score)

    def _sgi_class_from_score(self, score):
        if score >= 85:
            return 'acreditado'
        if score >= 70:
            return 'condicionado'
        return 'baja'

    def _sgi_compute_otd(self):
        self.ensure_one()
        dt_from = fields.Datetime.to_datetime(self.date_from)
        dt_to = fields.Datetime.to_datetime(self.date_to) + relativedelta(days=1)
        pickings = self.env['stock.picking'].search([
            ('picking_type_id.code', '=', 'incoming'),
            ('state', '=', 'done'),
            ('partner_id', 'child_of', self.partner_id.commercial_partner_id.id),
            ('date_done', '>=', dt_from), ('date_done', '<', dt_to),
        ])
        if not pickings:
            return 0.0
        on_time = 0
        for pick in pickings:
            po = pick.purchase_id if 'purchase_id' in pick._fields else False
            deadline = (po and po.date_planned) or pick.date_deadline or pick.scheduled_date
            if deadline and pick.date_done and pick.date_done <= deadline:
                on_time += 1
        return round(on_time / len(pickings) * 100.0, 2)

    def _sgi_count_ncs(self):
        self.ensure_one()
        dt_from = fields.Datetime.to_datetime(self.date_from)
        dt_to = fields.Datetime.to_datetime(self.date_to) + relativedelta(days=1)
        return self.env['quality.alert'].search_count([
            ('partner_id', 'child_of', self.partner_id.commercial_partner_id.id),
            ('create_date', '>=', dt_from), ('create_date', '<', dt_to),
        ])

    def action_apply_to_partner(self):
        for ev in self:
            ev.partner_id.write({
                'sgi_supplier_class': ev.supplier_class,
                'sgi_supplier_score': ev.score,
                'sgi_last_eval_date': ev.date_to,
            })
        return True

    @api.depends('partner_id', 'date_to')
    def _compute_display_name(self):
        for ev in self:
            period = ev.date_to and ev.date_to.strftime('%m/%Y') or ''
            ev.display_name = "%s — %s" % (ev.partner_id.display_name or '', period)
