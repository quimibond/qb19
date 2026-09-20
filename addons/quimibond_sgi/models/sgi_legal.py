# -*- coding: utf-8 -*-
"""Requisitos legales y otros requisitos + evaluación del cumplimiento.

ISO 14001 §6.1.3/§9.1.2 e ISO 45001 §6.1.3/§9.1.2 exigen (a) identificar los
requisitos legales y otros requisitos aplicables y (b) EVALUAR periódicamente
su cumplimiento conservando evidencia. Era la brecha normativa más seria del
SGI: no existía matriz legal ni registro de evaluación.

El modelo sigue los patrones del addon: chatter con tracking (la historia de
evaluaciones ES la evidencia), próxima evaluación calculada pero corregible
(patrón calibración), vigilancia por cron con actividades deduplicadas, y el
incumplimiento genera NC por el punto único de entrada (sgi_auto_create) con
fuente propia apagable por MAST.
"""
from dateutil.relativedelta import relativedelta

from odoo import models, fields, api
from odoo.exceptions import UserError


class SgiLegalRequirement(models.Model):
    _name = 'sgi.legal.requirement'
    _description = "Requisito legal / otro requisito (14001·45001 6.1.3)"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'compliance_state desc, next_eval_date, id'

    name = fields.Char(
        string="Requisito", required=True, tracking=True,
        help="La obligación concreta, en lenguaje llano (ej. «Registro como "
             "generador de residuos peligrosos», «Programa interno de "
             "protección civil vigente»).")
    kind = fields.Selection([
        ('ley', "Ley / reglamento"),
        ('nom', "Norma oficial (NOM)"),
        ('permiso', "Permiso / licencia / registro"),
        ('cliente', "Requisito de cliente"),
        ('corporativo', "Requisito corporativo / otro"),
    ], string="Tipo", default='nom', required=True, tracking=True)
    system = fields.Selection([
        ('ambiental', "Ambiental (14001)"),
        ('sst', "Seguridad y salud (45001)"),
        ('calidad', "Calidad (9001 / cliente)"),
        ('varios', "Transversal"),
    ], string="Sistema", default='ambiental', required=True, tracking=True)
    reference = fields.Char(
        string="Referencia", tracking=True,
        help="Instrumento y artículo/numeral (ej. NOM-052-SEMARNAT-2005, "
             "art. 42; NOM-002-STPS-2010).")
    authority = fields.Char(
        string="Autoridad / origen",
        help="Quién lo exige: SEMARNAT, STPS, municipio, cliente, corporativo…")
    description = fields.Text(
        string="Obligación",
        help="Qué obliga a hacer exactamente (la parte aplicable a la planta).")
    evidence = fields.Text(
        string="Cómo se cumple / evidencia",
        help="Con qué se demuestra el cumplimiento: registro, bitácora, "
             "dictamen, constancia, documento controlado…")
    process_ids = fields.Many2many(
        'sgi.process', string="Procesos donde aplica")
    risk_ids = fields.Many2many(
        'sgi.risk', string="Riesgos ligados",
        help="Riesgos (IPER/ambiental) cuyo control responde a este requisito.")
    document_ids = fields.Many2many(
        'documents.document', string="Documentos de evidencia",
        domain=[('sgi_is_controlled', '=', True)])
    responsible_id = fields.Many2one(
        'res.users', string="Responsable de la evaluación", tracking=True)
    expiry_date = fields.Date(
        string="Vencimiento del permiso", tracking=True,
        help="Solo permisos/licencias con vigencia: fecha en que caduca el "
             "instrumento (independiente de la evaluación de cumplimiento).")

    # --- Evaluación del cumplimiento (9.1.2) ---
    eval_frequency_months = fields.Integer(
        string="Frecuencia de evaluación (meses)", default=12)
    last_eval_date = fields.Date(string="Última evaluación", tracking=True)
    next_eval_date = fields.Date(
        string="Próxima evaluación", compute='_compute_next_eval_date',
        store=True, readonly=False,
        help="Última + frecuencia; puede fijarse a mano (prevalece, patrón "
             "de calibraciones).")
    compliance_state = fields.Selection([
        ('pendiente', "Sin evaluar"),
        ('cumple', "Cumple"),
        ('parcial', "Cumple parcialmente"),
        ('no_cumple', "No cumple"),
    ], string="Cumplimiento", default='pendiente', required=True, tracking=True)
    eval_note = fields.Text(
        string="Notas de la última evaluación",
        help="Qué se revisó y qué se encontró (queda también en el chatter "
             "por el tracking del estado).")
    alert_id = fields.Many2one(
        'quality.alert', string="NC de incumplimiento", readonly=True, copy=False)
    active = fields.Boolean(default=True)

    @api.depends('last_eval_date', 'eval_frequency_months')
    def _compute_next_eval_date(self):
        for req in self:
            if req.last_eval_date and req.eval_frequency_months:
                req.next_eval_date = req.last_eval_date + relativedelta(
                    months=req.eval_frequency_months)
            elif not req.last_eval_date:
                # Sin evaluación previa: se debe evaluar ya (el cron lo vigila).
                req.next_eval_date = req.next_eval_date or fields.Date.context_today(req)

    @api.depends('reference', 'name')
    def _compute_display_name(self):
        for req in self:
            req.display_name = ("%s — %s" % (req.reference, req.name)
                                if req.reference else req.name)

    # ------------------------------------------------------------------
    # Evaluación: tres botones explícitos, con sello de fecha y NC en
    # incumplimiento (parcial o total).
    # ------------------------------------------------------------------
    def _sgi_mark(self, state):
        today = fields.Date.context_today(self)
        for req in self:
            req.write({'compliance_state': state, 'last_eval_date': today})
            req.message_post(body="Evaluación de cumplimiento registrada: <b>%s</b>." % dict(
                self._fields['compliance_state'].selection)[state])
        return True

    def action_mark_cumple(self):
        return self._sgi_mark('cumple')

    def action_mark_parcial(self):
        self._sgi_mark('parcial')
        self._sgi_create_alert()
        return True

    def action_mark_no_cumple(self):
        self._sgi_mark('no_cumple')
        self._sgi_create_alert()
        return True

    def _sgi_create_alert(self):
        """NC por incumplimiento legal, vía el punto único de entrada.
        Idempotente mientras la NC previa siga abierta."""
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal',
                            raise_if_not_found=False)
        for req in self:
            nc = req.alert_id
            if nc and not (nc.stage_id.sgi_is_closing_stage
                           or nc.stage_id.sgi_is_cancel_stage):
                continue  # ya hay expediente abierto para este incumplimiento
            label = dict(self._fields['compliance_state'].selection)[req.compliance_state]
            vals = {
                'title': "Incumplimiento legal: %s" % (req.reference or req.name),
                'sgi_origin_type': 'proceso',
                'sgi_classification': 'mayor',
                'sgi_process_id': req.process_ids[:1].id,
                'sgi_deviation':
                    "La evaluación del cumplimiento (14001/45001 9.1.2) del "
                    "requisito «%s» resultó: %s.\nObligación: %s\nEvidencia "
                    "esperada: %s" % (
                        req.display_name, label,
                        req.description or '-', req.evidence or '-'),
            }
            if team:
                vals['team_id'] = team.id
            alert = self.env['quality.alert'].sgi_auto_create(
                'requisito_legal_incumplido', vals)
            if alert:
                req.alert_id = alert.id
                req.message_post(
                    body="Se levantó la NC <b>%s</b> por el incumplimiento."
                         % (alert.sgi_folio or alert.title))
        return True

    def action_view_alert(self):
        self.ensure_one()
        if not self.alert_id:
            raise UserError("Este requisito no tiene NC de incumplimiento ligada.")
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'quality.alert',
            'res_id': self.alert_id.id,
            'view_mode': 'form',
        }
