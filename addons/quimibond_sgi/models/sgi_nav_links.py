# -*- coding: utf-8 -*-
"""57.150.0 (auditoría UX 2026-10-13, fase 3): ligas de navegación que
faltaban entre registros que ya se conocían por un solo lado.

- NC (``quality.alert``) → hallazgo de auditoría que la generó y su auditoría
  (inverso de ``sgi.audit.finding.alert_id``).
- Proceso → auditorías que lo auditaron, hallazgos y periodos del registro de
  cumplimiento atrasados.
- Documento controlado → riesgos que tienen ese documento como control
  operacional.
- Indicador → NC que levantaron sus mediciones en rojo.

Va al final de ``models/__init__.py``: extiende modelos de varios archivos.
Los conteos van con sudo y los botones con ``groups``: la ficha de la NC la
abre también quien está en Calidad sin estar en el SGI.
"""
from odoo import fields, models

from .sgi_activity_execution import EXEC_OPEN, sgi_exec_start_date


def _sgi_counts(model, domain, group_field):
    """{id: conteo} con una sola consulta agrupada."""
    return {rec.id: count for rec, count in model._read_group(domain, [group_field], ['__count'])}


def _sgi_list_action(name, res_model, domain, context=None):
    return {
        'type': 'ir.actions.act_window', 'name': name, 'res_model': res_model,
        'view_mode': 'list,form', 'domain': domain, 'context': dict(context or {}, create=False),
    }


class QualityAlertSgiLinks(models.Model):
    _inherit = 'quality.alert'

    sgi_finding_ids = fields.One2many(
        'sgi.audit.finding', 'alert_id', string="Hallazgos de auditoría", readonly=True,
        groups='quimibond_sgi.group_sgi_user',
        help="Hallazgo de auditoría del que nació esta NC.")
    sgi_finding_count = fields.Integer(string="# Hallazgos", compute='_compute_sgi_audit_links')
    sgi_audit_id = fields.Many2one(
        'sgi.audit', string="Auditoría", compute='_compute_sgi_audit_links',
        help="Auditoría del hallazgo que generó esta NC.")
    sgi_indicator_id = fields.Many2one(
        related='sgi_indicator_measure_id.indicator_id', string="Indicador",
        help="Indicador cuya medición en rojo levantó esta NC.")

    def _compute_sgi_audit_links(self):
        findings = self.env['sgi.audit.finding'].sudo().search([('alert_id', 'in', self.ids)]) \
            if self.ids else self.env['sgi.audit.finding']
        for alert in self:
            mine = findings.filtered(lambda f, a=alert: f.alert_id.id == a.id)
            alert.sgi_finding_count = len(mine)
            alert.sgi_audit_id = mine[:1].audit_id

    def action_sgi_open_findings(self):
        self.ensure_one()
        findings = self.env['sgi.audit.finding'].search([('alert_id', '=', self.id)])
        if len(findings) == 1:
            return {'type': 'ir.actions.act_window', 'name': "Hallazgo", 'res_model': 'sgi.audit.finding',
                    'res_id': findings.id, 'view_mode': 'form'}
        return _sgi_list_action("Hallazgos — %s" % self.display_name, 'sgi.audit.finding',
                                [('id', 'in', findings.ids)])

    def action_sgi_open_audit(self):
        self.ensure_one()
        audit = self.sgi_audit_id
        return {'type': 'ir.actions.act_window', 'name': "Auditoría", 'res_model': 'sgi.audit',
                'res_id': audit.id, 'view_mode': 'form'}

    def action_sgi_open_indicator(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': "Indicador", 'res_model': 'sgi.indicator',
                'res_id': self.sgi_indicator_id.id, 'view_mode': 'form'}


class SgiProcessLinks(models.Model):
    _inherit = 'sgi.process'

    audit_count = fields.Integer(string="# Auditorías", compute='_compute_sgi_link_counts')
    finding_count = fields.Integer(string="# Hallazgos", compute='_compute_sgi_link_counts')
    execution_late_count = fields.Integer(
        string="Periodos atrasados", compute='_compute_sgi_link_counts',
        help="Periodos del registro de cumplimiento de las actividades del proceso que vencieron sin "
             "hacerse (desde el arranque del registro).")

    def _sgi_late_execution_domain(self):
        today = fields.Date.context_today(self)
        return [('process_id', 'in', self.ids), ('state', 'in', EXEC_OPEN), ('date_due', '<', today),
                ('date_due', '>=', sgi_exec_start_date(self.env))]

    def _compute_sgi_link_counts(self):
        processes = self.filtered('id')
        for process in self - processes:
            process.audit_count = process.finding_count = process.execution_late_count = 0
        if not processes:
            return
        ids = processes.ids
        audits = self.env['sgi.audit'].sudo().search([('process_ids', 'in', ids)])
        findings = _sgi_counts(self.env['sgi.audit.finding'].sudo(), [('process_id', 'in', ids)], 'process_id')
        late = _sgi_counts(self.env['sgi.activity.execution'].sudo(),
                           processes._sgi_late_execution_domain(), 'process_id')
        for process in processes:
            process.audit_count = len(audits.filtered(lambda a, p=process: p in a.process_ids))
            process.finding_count = findings.get(process.id, 0)
            process.execution_late_count = late.get(process.id, 0)

    def action_sgi_open_audits(self):
        self.ensure_one()
        return _sgi_list_action("Auditorías — %s" % (self.code or self.name), 'sgi.audit',
                                [('process_ids', 'in', self.ids)])

    def action_sgi_open_findings(self):
        self.ensure_one()
        return _sgi_list_action("Hallazgos — %s" % (self.code or self.name), 'sgi.audit.finding',
                                [('process_id', '=', self.id)])

    def action_sgi_open_late_executions(self):
        self.ensure_one()
        action = self.env['ir.actions.act_window']._for_xml_id('quimibond_sgi.sgi_activity_execution_action')
        action.update(name="Periodos atrasados — %s" % (self.code or self.name),
                      domain=self._sgi_late_execution_domain(), context={'create': False})
        return action


class DocumentsDocumentRiskLinks(models.Model):
    _inherit = 'documents.document'

    sgi_controlled_risk_count = fields.Integer(
        string="Riesgos que controla", compute='_compute_sgi_controlled_risk_count',
        help="Riesgos que tienen este documento como control operacional.")

    def _compute_sgi_controlled_risk_count(self):
        ids = self.filtered('id').ids
        counts = _sgi_counts(self.env['sgi.risk'].sudo(), [('operational_control_id', 'in', ids)],
                             'operational_control_id') if ids else {}
        for doc in self:
            doc.sgi_controlled_risk_count = counts.get(doc.id, 0)

    def action_sgi_open_controlled_risks(self):
        self.ensure_one()
        return _sgi_list_action("Riesgos que controla — %s" % self.display_name, 'sgi.risk',
                                [('operational_control_id', '=', self.id)])


class SgiIndicatorNcLinks(models.Model):
    _inherit = 'sgi.indicator'

    nc_generated_count = fields.Integer(
        string="NC generadas", compute='_compute_nc_generated_count',
        help="No conformidades que levantaron las mediciones en rojo de este indicador.")

    def _sgi_nc_domain(self):
        return [('sgi_indicator_measure_id.indicator_id', 'in', self.ids)]

    def _compute_nc_generated_count(self):
        ids = self.filtered('id').ids
        counts = {}
        if ids:
            alerts = self.env['quality.alert'].sudo().search(self.filtered('id')._sgi_nc_domain())
            for alert in alerts:
                key = alert.sgi_indicator_measure_id.indicator_id.id
                counts[key] = counts.get(key, 0) + 1
        for indicator in self:
            indicator.nc_generated_count = counts.get(indicator.id, 0)

    def action_sgi_open_generated_ncs(self):
        self.ensure_one()
        return _sgi_list_action("NC generadas — %s" % (self.code or self.name), 'quality.alert',
                                self._sgi_nc_domain())
