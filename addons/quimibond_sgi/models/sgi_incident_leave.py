# -*- coding: utf-8 -*-
"""57.96.0 (auditoría 2026-10, N-06 y D7): incidente desde la incapacidad.

Una ausencia aprobada de un tipo de riesgo de trabajo crea el incidente en
«Reportado» con la persona y los días perdidos, y avisa al Jefe MAST para que
lo investigue. Tipos: el parámetro ``quimibond_sgi.work_risk_leave_type_ids``
(ids separados por coma) o, sin él, «Riesgo de trabajo (IMSS)»
(``hr_holidays.l10n_mx_leave_type_work_risk_imss``, que ya existe en
producción).

- La subsecuente (misma persona, hasta N días después de una incapacidad
  ligada a un incidente no cerrado; parámetro
  ``quimibond_sgi.work_risk_followup_days``, 3) se liga al mismo incidente.
- Rechazar o cancelar una incapacidad ligada deja nota y recalcula los días;
  nada se borra.
- El incidente no lleva diagnóstico ni la descripción de la ausencia.
- La aprobación nunca falla por el SGI: cada ausencia en su savepoint; una
  falla queda en el log como WARNING con la traza."""
import logging
from datetime import timedelta

from markupsafe import Markup

from odoo import api, fields, models

_logger = logging.getLogger(__name__)
WORK_RISK_XMLID = 'hr_holidays.l10n_mx_leave_type_work_risk_imss'


class HrLeaveSgiIncident(models.Model):
    _inherit = 'hr.leave'

    sgi_incident_id = fields.Many2one(
        'sgi.incident', string="Incidente SST", readonly=True, copy=False, index='btree_not_null',
        ondelete='set null',
        groups='hr_holidays.group_hr_holidays_user,quimibond_sgi.group_sgi_manager,'
               'quimibond_sgi.group_sgi_health',
        help="Incidente del SGI que se abrió por esta incapacidad por riesgo de trabajo.")

    @api.model
    def _sgi_work_risk_types(self):
        param = self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.work_risk_leave_type_ids', '') or ''
        ids = [int(part) for part in param.split(',') if part.strip().isdigit()]
        if ids:
            return self.env['hr.leave.type'].sudo().browse(ids).exists()
        return self.env.ref(WORK_RISK_XMLID, raise_if_not_found=False) or self.env['hr.leave.type']

    @api.model_create_multi
    def create(self, vals_list):
        leaves = super().create(vals_list)
        leaves.filtered(lambda l: l.state == 'validate')._sgi_sync_work_risk()
        return leaves

    def write(self, vals):
        before = {leave.id: leave.state for leave in self} if 'state' in vals else {}
        res = super().write(vals)
        if before:
            self.filtered(lambda l: l.state == 'validate'
                          and before.get(l.id) != 'validate')._sgi_sync_work_risk()
            self.filtered(lambda l: before.get(l.id) == 'validate'
                          and l.state != 'validate')._sgi_sync_work_risk(dropped=True)
        return res

    def _sgi_sync_work_risk(self, dropped=False):
        types = self._sgi_work_risk_types()
        if not types:
            return
        for leave in self.filtered(lambda l: l.holiday_status_id in types):
            try:
                with self.env.cr.savepoint():
                    if dropped:
                        leave._sgi_unlink_note()
                    else:
                        leave._sgi_link_incident()
            except Exception:
                _logger.warning("SGI: no se pudo registrar el incidente de la incapacidad %s; la "
                                "ausencia sigue su curso.", leave.id, exc_info=True)

    def _sgi_followup_incident(self):
        """Incidente no cerrado de una incapacidad aprobada de la misma persona
        que terminó hasta N días antes de que empiece esta."""
        self.ensure_one()
        Param = self.env['ir.config_parameter'].sudo()
        try:
            days = int(Param.get_param('quimibond_sgi.work_risk_followup_days', 3) or 3)
        except (TypeError, ValueError):
            days = 3
        start = self.request_date_from
        previous = self.sudo().search([
            ('id', '!=', self.id), ('employee_id', '=', self.employee_id.id),
            ('state', '=', 'validate'), ('sgi_incident_id', '!=', False),
            ('sgi_incident_id.state', '!=', 'cerrado'),
            ('request_date_to', '<', start), ('request_date_to', '>=', start - timedelta(days=days)),
        ], order='request_date_to desc', limit=1)
        return previous.sgi_incident_id

    def _sgi_link_incident(self):
        self.ensure_one()
        leave = self.sudo()
        # Ya ligada (p. ej. hr_holidays valida dentro de su create y el create
        # de aquí vuelve a pasar): solo se refrescan los días, sin nota.
        already = leave.sgi_incident_id
        incident = already or leave._sgi_followup_incident()
        created = False
        if not incident:
            user = self.env.user
            incident = self.env['sgi.incident'].sudo().create({
                # Sin diagnóstico ni la descripción de la ausencia (dato de salud).
                'name': "Riesgo de trabajo (incapacidad IMSS) del %s" % leave.request_date_from,
                'incident_type': 'lesion',
                'severity': 'moderado',
                'date': leave.date_from,
                'employee_ids': [(6, 0, leave.employee_id.ids)],
                'description': "Registrado desde una incapacidad por riesgo de trabajo aprobada en "
                               "Ausencias. Complete la fecha y el lugar del evento, el proceso, la "
                               "descripción y la investigación.",
                'reporter_id': user.id,
                'reporter_employee_id': user.employee_id.id or False,
                'sgi_from_leave': True,
            })
            created = True
        if leave.sgi_incident_id != incident:
            leave.sgi_incident_id = incident
        if incident.state != 'cerrado':  # un incidente cerrado es evidencia
            incident._sgi_refresh_leave_days()
        if created:
            Cron = self.env['sgi.cron']
            Cron._sgi_schedule(
                incident, "Investigar riesgo de trabajo %s" % (incident.folio or incident.name),
                "RH aprobó una incapacidad por riesgo de trabajo. Complete el incidente e inicie la "
                "investigación SCAT.", Cron._sgi_manager_user_id(), key='incidente_incapacidad')
        elif not already:
            incident.message_post(body="Se ligó otra incapacidad por riesgo de trabajo aprobada.")

    def _sgi_unlink_note(self):
        self.ensure_one()
        incident = self.sudo().sgi_incident_id
        if not incident:
            return
        incident.message_post(body=Markup(
            "Una incapacidad ligada a este incidente ya no está aprobada (rechazada o cancelada en "
            "Ausencias). No se borró nada."))
        if incident.state != 'cerrado':
            incident._sgi_refresh_leave_days()
