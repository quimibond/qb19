# -*- coding: utf-8 -*-
"""57.96.0 (auditoría 2026-10, N-07): traspaso de los riesgos ambientales a la
matriz de aspectos (sgi.env.aspect, ISO 14001 6.1.2).

Antes de 57.96.0 un riesgo se podía capturar con el instrumento «Aspecto
ambiental» y así nacieron 5 (2026-10-02). La evaluación del aspecto vive en la
matriz; el riesgo queda como su tratamiento (``risk_id``). Es un cambio de
datos de negocio: no va en migración; el Jefe MAST lo corre a mano con el
visto bueno de Jose. Al abrir solo cuenta y propone; «Traspasar» crea un
aspecto «En evaluación» por renglón (cada uno en su savepoint), deja nota en
el aspecto y en el riesgo, y registra antes y después en el log. Los riesgos
se conservan salvo que se pida archivarlos. Idempotente; nada se borra."""
import logging

from markupsafe import Markup

from odoo import Command, api, fields, models
from odoo.exceptions import AccessError, UserError, ValidationError

from .sgi_env_aspect import ASPECT_CONDITIONS, ASPECT_TYPES, LIFE_CYCLE_STAGES

_logger = logging.getLogger(__name__)


class SgiEnvAspectTransfer(models.TransientModel):
    """Traspaso de riesgos ambientales a la matriz de aspectos (N-07)."""
    _name = 'sgi.env.aspect.transfer'
    _description = "Traspaso de riesgos ambientales a la matriz de aspectos"

    line_ids = fields.One2many('sgi.env.aspect.transfer.line', 'wizard_id',
                               string="Riesgos ambientales sin aspecto")
    risk_count = fields.Integer(string="Riesgos por traspasar", compute='_compute_risk_count')
    archive_risks = fields.Boolean(
        string="Archivar los riesgos originales",
        help="Apagado: cada riesgo se conserva, ligado al aspecto como su tratamiento. "
             "Encendido: además se archiva (no se borra). Úselo solo si Dirección lo pidió.")
    done_count = fields.Integer(string="Aspectos creados", readonly=True)
    failed_count = fields.Integer(
        string="Riesgos que no se pudieron traspasar", readonly=True,
        help="El motivo de cada uno está en el log del servidor.")

    @api.model
    def _sgi_check_manager(self):
        if not (self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            raise AccessError("Solo el Jefe MAST traspasa los riesgos ambientales a la matriz.")

    @api.model
    def _sgi_candidates(self):
        """Riesgos ambientales (activos o archivados) sin aspecto que los use."""
        risks = self.env['sgi.risk'].sudo().with_context(active_test=False).search(
            [('instrument', '=', 'ambiental')], order='id')
        linked = self.env['sgi.env.aspect'].sudo().with_context(active_test=False).search(
            [('risk_id', 'in', risks.ids)]).risk_id
        return risks - linked

    @api.model
    def default_get(self, fields_list):
        res = super().default_get(fields_list)
        if 'line_ids' in fields_list:
            # _sgi_candidates lee con sudo: nadie más que el Jefe MAST ve la propuesta.
            self._sgi_check_manager()
            res['line_ids'] = [Command.create({
                'risk_id': risk.id, 'activity': risk.name, 'aspect_type': 'otro',
                'condition': 'normal'}) for risk in self._sgi_candidates()]
        return res

    @api.model_create_multi
    def create(self, vals_list):
        self._sgi_check_manager()
        return super().create(vals_list)

    @api.depends('line_ids')
    def _compute_risk_count(self):
        for wizard in self:
            wizard.risk_count = len(wizard.line_ids)

    def _sgi_aspect_vals(self, line):
        risk = line.risk_id.sudo()
        legal = self.env['sgi.legal.requirement'].sudo().with_context(active_test=False).search(
            [('risk_ids', 'in', risk.ids)])
        return {
            'name': risk.name,
            'activity': line.activity or risk.name,
            'aspect_type': line.aspect_type,
            'condition': line.condition,
            'life_cycle_stage': line.life_cycle_stage or False,
            'impact': risk.consequence or risk.name,
            # Mapeo inverso exacto de sgi.env.aspect.action_create_risk.
            'severity': risk.eval_impact or False,
            'frequency': risk.eval_probability or False,
            'process_id': risk.process_id.id,
            'sgi_area_id': risk.sgi_area_id.id,
            'control_description': risk.existing_controls or False,
            'operational_control_id': risk.operational_control_id.id,
            'next_review_date': risk.next_review_date,
            'legal_requirement_ids': [Command.set(legal.ids)],
            'risk_id': risk.id,
            'company_id': self.env['sgi.config']._sgi_company().id,
        }

    def action_apply(self):
        self.ensure_one()
        self._sgi_check_manager()
        candidates = self._sgi_candidates()
        lines = self.line_ids.filtered(lambda l: l.risk_id in candidates)
        who = self.env.user.display_name
        today = fields.Date.context_today(self)
        _logger.info("SGI N-07: antes, %d riesgos ambientales sin aspecto %s; se traspasan %s.",
                     len(candidates), candidates.ids, lines.risk_id.ids)
        Aspect = self.env['sgi.env.aspect']
        done = Aspect.browse()
        failed = self.env['sgi.risk'].browse()
        for line in lines:
            risk = line.risk_id
            try:
                with self.env.cr.savepoint():
                    aspect = Aspect.create(self._sgi_aspect_vals(line))
                    aspect.message_post(body=Markup(
                        "Traspasado desde el riesgo <b>%s</b> el %s por %s (57.96.0, N-07). "
                        "Revise la etapa del ciclo de vida, el tipo y la condición, y registre la "
                        "evaluación.") % (risk.folio or risk.name, today, who))
                    risk.message_post(body=Markup(
                        "La evaluación de este aspecto vive ahora en la matriz: <b>%s</b>. Este "
                        "riesgo queda como su tratamiento.") % aspect.folio)
                    if self.archive_risks:
                        risk.write({'active': False})
                        risk.message_post(body="Archivado al traspasarlo a la matriz (no se borró).")
                done |= aspect
            except Exception as error:
                failed |= risk
                _logger.warning("SGI N-07: el riesgo %s (%s) no se pudo traspasar.",
                                risk.id, risk.folio, exc_info=True)
                # El motivo en el renglón (el mensaje del candado, sin traza).
                line.error = error.args[0] if isinstance(error, (UserError, ValidationError)) \
                    and error.args else "Error inesperado; el detalle está en el log del servidor."

        _logger.info("SGI N-07: después, %d aspectos creados %s, %d riesgos con error %s; "
                     "%d riesgos ambientales siguen sin aspecto.", len(done), done.ids,
                     len(failed), failed.ids, len(self._sgi_candidates()))
        # La ficha que regresa muestra solo lo que falló; los contadores suman
        # si el Jefe MAST vuelve a pulsar «Traspasar» sobre lo que quedó.
        self.line_ids.filtered(lambda l: l.risk_id in done.risk_id).unlink()
        self.write({'done_count': self.done_count + len(done), 'failed_count': len(failed)})
        return {'type': 'ir.actions.act_window', 'res_model': self._name, 'res_id': self.id,
                'view_mode': 'form', 'target': 'new'}

    def action_view_aspects(self):
        self._sgi_check_manager()
        return {
            'type': 'ir.actions.act_window', 'name': "Aspectos ligados a riesgos ambientales",
            'res_model': 'sgi.env.aspect', 'view_mode': 'list,form',
            'domain': [('risk_id.instrument', '=', 'ambiental')],
            'context': {'active_test': False},
        }


class SgiEnvAspectTransferLine(models.TransientModel):
    """Un riesgo ambiental por traspasar y lo que el Jefe MAST decide del aspecto."""
    _name = 'sgi.env.aspect.transfer.line'
    _description = "Riesgo ambiental por traspasar a la matriz"

    wizard_id = fields.Many2one('sgi.env.aspect.transfer', required=True, ondelete='cascade')
    risk_id = fields.Many2one('sgi.risk', string="Riesgo", required=True, readonly=True)
    risk_kind = fields.Selection(related='risk_id.kind', string="Riesgo u oportunidad")
    process_id = fields.Many2one(related='risk_id.process_id', string="Proceso")
    score = fields.Integer(related='risk_id.score', string="Puntaje del riesgo")
    activity = fields.Char(string="Actividad u operación", required=True,
                           help="Qué se hace. Propuesta: el nombre del riesgo; corríjala.")
    aspect_type = fields.Selection(ASPECT_TYPES, string="Tipo de aspecto", required=True)
    condition = fields.Selection(ASPECT_CONDITIONS, string="Condición", required=True)
    life_cycle_stage = fields.Selection(LIFE_CYCLE_STAGES, string="Etapa del ciclo de vida")
    error = fields.Char(string="Por qué no se traspasó", readonly=True,
                        help="Lo llena «Traspasar a la matriz» si este renglón falló.")
