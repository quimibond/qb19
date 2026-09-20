# -*- coding: utf-8 -*-
"""Estudios MSA — análisis de sistemas de medición (IATF 7.1.5.1.1).

Antes solo existía como el elemento 8 del PPAP, sin registro propio. El
estudio se liga al equipo de medición; para Gage R&R de variables el
veredicto sale de los umbrales AIAG (%GRR <10 aceptable, 10-30 marginal,
>30 inaceptable) y un veredicto inaceptable agenda actividad al Jefe MAST
para evaluar el sistema de medición (el bloqueo del equipo es decisión
humana, no automática).
"""
from odoo import models, fields, api


class MaintenanceEquipmentMsa(models.Model):
    _inherit = 'maintenance.equipment'

    sgi_msa_ids = fields.One2many('sgi.msa.study', 'equipment_id',
                                  string="Estudios MSA")
    sgi_msa_count = fields.Integer(string="# MSA", compute='_compute_sgi_msa_count')

    def _compute_sgi_msa_count(self):
        data = self.env['sgi.msa.study']._read_group(
            [('equipment_id', 'in', self.ids)], ['equipment_id'], ['__count'])
        mapped = {eq.id: count for eq, count in data}
        for equipment in self:
            equipment.sgi_msa_count = mapped.get(equipment.id, 0)

    def action_view_sgi_msa(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Estudios MSA — %s" % self.name,
            'res_model': 'sgi.msa.study',
            'view_mode': 'list,form',
            'domain': [('equipment_id', '=', self.id)],
            'context': {'default_equipment_id': self.id},
        }


class SgiMsaStudy(models.Model):
    _name = 'sgi.msa.study'
    _description = "Estudio MSA (IATF 7.1.5.1.1)"
    _inherit = ['sgi.base.mixin']
    _order = 'date desc, folio desc'
    _sgi_sequence_code = 'sgi.msa.study'

    equipment_id = fields.Many2one('maintenance.equipment', string="Equipo de medición",
                                   required=True, tracking=True,
                                   domain=[('sgi_is_measuring', '=', True)])
    study_type = fields.Selection([
        ('grr_variable', "Gage R&R (variables)"),
        ('atributos', "Estudio por atributos"),
        ('sesgo_linealidad', "Sesgo / linealidad"),
        ('estabilidad', "Estabilidad"),
    ], string="Tipo de estudio", default='grr_variable', required=True, tracking=True)
    date = fields.Date(string="Fecha", required=True,
                       default=fields.Date.context_today, tracking=True)
    characteristic = fields.Char(string="Característica medida")
    point_id = fields.Many2one('quality.point', string="Punto de control")
    grr_pct = fields.Float(string="% GRR", digits=(5, 2),
                           help="Porcentaje de variación del sistema de medición "
                                "(solo Gage R&R de variables).")
    ndc = fields.Integer(string="ndc (categorías distintas)",
                         help="Número de categorías distintas; AIAG pide ≥ 5.")
    verdict = fields.Selection([
        ('aceptable', "Aceptable"),
        ('marginal', "Marginal"),
        ('inaceptable', "Inaceptable"),
    ], string="Veredicto", compute='_compute_verdict', store=True,
        readonly=False, tracking=True,
        help="Para Gage R&R se calcula de los umbrales AIAG (%GRR <10 / 10-30 / >30); "
             "para los demás tipos se captura a mano.")
    notes = fields.Text(string="Notas / referencia del reporte")

    _folio_uniq = models.Constraint(
        'unique(folio)', "Ya existe un estudio MSA con ese folio.")

    @api.depends('study_type', 'grr_pct')
    def _compute_verdict(self):
        for study in self:
            if study.study_type != 'grr_variable' or not study.grr_pct:
                study.verdict = study.verdict or False
                continue
            if study.grr_pct < 10.0:
                study.verdict = 'aceptable'
            elif study.grr_pct <= 30.0:
                study.verdict = 'marginal'
            else:
                study.verdict = 'inaceptable'

    @api.depends('folio', 'equipment_id.name')
    def _compute_display_name(self):
        for study in self:
            study.display_name = ("%s - %s" % (study.folio, study.equipment_id.name)
                                  if study.folio else (study.equipment_id.name or ''))

    @api.model_create_multi
    def create(self, vals_list):
        studies = super().create(vals_list)
        studies._sgi_notify_if_unacceptable()
        return studies

    def write(self, vals):
        res = super().write(vals)
        if {'verdict', 'grr_pct', 'study_type'} & set(vals):
            self._sgi_notify_if_unacceptable()
        return res

    def _sgi_notify_if_unacceptable(self):
        Cron = self.env['sgi.cron']
        manager_id = Cron._sgi_manager_user_id()
        if not manager_id:
            return
        for study in self.filtered(lambda s: s.verdict == 'inaceptable'):
            Cron._sgi_schedule(
                study,
                "MSA inaceptable: %s" % study.equipment_id.name,
                "El estudio MSA resultó inaceptable (%%GRR %.1f). Evalúe el "
                "sistema de medición: recalibrar, reentrenar o bloquear el "
                "equipo (No usar)." % (study.grr_pct or 0.0),
                manager_id)
