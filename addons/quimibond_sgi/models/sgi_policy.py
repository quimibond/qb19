# -*- coding: utf-8 -*-
"""Política integral del SGI — cabeza de la Línea Dorada (cascada ISO).

Política → Objetivo integral → Indicador → Proceso. La política es el enunciado
único vigente del que cuelgan los objetivos; el resto de la cascada ya existe
(el indicador tiene objective_id y process_id).
"""
import logging

from odoo import models, fields, api
from odoo.exceptions import UserError

from odoo.addons.quimibond_sgi.models.sgi_objective import sgi_worst_health

_logger = logging.getLogger(__name__)


class SgiPolicy(models.Model):
    _name = 'sgi.policy'
    _description = "Política integral del SGI"
    _inherit = ['sgi.base.mixin']
    _order = 'issue_date desc, folio desc'
    _sgi_sequence_code = 'sgi.policy'

    name = fields.Char(string="Nombre", required=True, tracking=True)
    policy_text = fields.Html(string="Texto de la política")
    issue_date = fields.Date(string="Fecha de emisión",
                             default=fields.Date.context_today)
    state = fields.Selection([
        ('borrador', "Borrador"),
        ('vigente', "Vigente"),
        ('obsoleta', "Obsoleta"),
    ], string="Estado", default='borrador', required=True, tracking=True)
    document_id = fields.Many2one(
        'documents.document', string="Documento publicado (MIID)",
        domain=[('sgi_is_controlled', '=', True)],
        help="Documento controlado donde se publica la política (p. ej. el MIID).")
    objective_ids = fields.One2many('sgi.objective', 'policy_id',
                                    string="Objetivos integrales")
    objective_count = fields.Integer(string="# Objetivos",
                                     compute='_compute_objective_count')
    health = fields.Selection([
        ('verde', "Verde"),
        ('amarillo', "Amarillo"),
        ('rojo', "Rojo"),
    ], string="Salud agregada", compute='_compute_health',
        help="Peor color entre los objetivos de la política (cascada abajo→arriba).")

    @api.depends('objective_ids', 'objective_ids.health')
    def _compute_health(self):
        for policy in self:
            policy.health = sgi_worst_health(policy.objective_ids.mapped('health'))

    def init(self):
        """A lo sumo UNA política vigente, garantizado en BD (la validación
        Python sola permite condición de carrera). Con datos legados (2+
        vigentes) se loggea y se omite el índice en vez de abortar el update
        del módulo (mismo patrón que el índice documental)."""
        super().init()
        cr = self.env.cr
        cr.execute("SELECT array_agg(id ORDER BY id) FROM sgi_policy "
                   "WHERE state = 'vigente'")
        vigentes = cr.fetchone()[0] or []
        if len(vigentes) > 1:
            _logger.error(
                "SGI: NO se creó el índice de política única vigente: hay %d "
                "políticas vigentes (ids %s). Obsoleta las sobrantes y vuelve "
                "a actualizar.", len(vigentes), vigentes)
            return
        cr.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS sgi_policy_one_vigente
            ON sgi_policy ((1)) WHERE state = 'vigente'
        """)

    @api.depends('objective_ids')
    def _compute_objective_count(self):
        for policy in self:
            policy.objective_count = len(policy.objective_ids)

    @api.depends('folio', 'name')
    def _compute_display_name(self):
        for policy in self:
            policy.display_name = "%s - %s" % (policy.folio, policy.name) \
                if policy.folio else policy.name

    def _sgi_obsolete_other_vigentes(self):
        """Obsoleta la política vigente previa ANTES de publicar la nueva
        (evita el candado de unicidad; patrón del ciclo documental)."""
        others = self.search([('state', '=', 'vigente'), ('id', 'not in', self.ids)])
        if others:
            others.write({'state': 'obsoleta'})
            others.flush_recordset(['state'])
            for other in others:
                other.message_post(
                    body="Obsoletada automáticamente: entró en vigor una nueva "
                         "política integral.")

    def action_set_vigente(self):
        for policy in self:
            policy._sgi_obsolete_other_vigentes()
            policy.state = 'vigente'
        return True

    def action_set_borrador(self):
        self.write({'state': 'borrador'})
        return True

    def action_set_obsoleta(self):
        self.write({'state': 'obsoleta'})
        return True

    def action_generate_acks(self):
        """Difusión con firma de la política (deuda D.28): delega en el
        documento controlado ligado. Antes se podía publicar la política sin
        difusión — dependía de ir al documento y acordarse de generar acuses."""
        self.ensure_one()
        if not self.document_id:
            raise UserError(
                "Liga primero el documento controlado donde se publica la "
                "política (campo «Documento publicado (MIID)») y vuelve a "
                "generar los acuses.")
        if not self.document_id.sgi_job_ids:
            raise UserError(
                "El documento %s no tiene puestos asignados: asígnalos en su "
                "pestaña «Puestos que aplican» (define quién debe firmar el "
                "acuse) y vuelve a generar." % (
                    self.document_id.sgi_code or self.document_id.name))
        self.document_id.action_generate_acks()
        return self.document_id.action_open_acks()

    def action_open_objectives(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Objetivos — %s" % self.name,
            'res_model': 'sgi.objective',
            'view_mode': 'list,form',
            'domain': [('policy_id', '=', self.id)],
            'context': {'default_policy_id': self.id},
        }
