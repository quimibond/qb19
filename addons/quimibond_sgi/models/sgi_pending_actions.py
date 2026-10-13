# -*- coding: utf-8 -*-
"""57.150.0 (auditoría UX 2026-10-13, fase 2): lo que «Mis pendientes»
mandaba a resolver «en su registro» se resuelve desde el renglón.

- ``sgi.nc.step``: el paso que toca de una NC (contención, causa raíz o
  plan de acción), el mismo hecho que da por cumplido cada plazo
  (``quality.alert._compute_sgi_deadline_states``).
- ``sgi.doc.review``: la revisión de un documento controlado que se acerca a
  su próxima revisión: sigue vigente sin cambios (nueva fecha a dos años y
  nota en su historial) o necesita cambio (la solicitud de cambio
  documental de siempre, ya apuntando al documento).

Los dos ponen al día en el momento los renglones de Mis pendientes de todos
los usuarios que apuntan al registro (``sgi.my.pending._sgi_refresh_for``).
"""
from dateutil.relativedelta import relativedelta
from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import AccessError, UserError

NC_STEPS = [
    ('contencion', "Contención"),
    ('causa', "Causa raíz"),
    ('plan', "Plan de acción"),
]
NC_STEP_TYPES = {'contencion': 'contencion', 'plan': 'correctiva'}
DOC_REVIEW_YEARS = 2


class SgiNcStep(models.TransientModel):
    _name = 'sgi.nc.step'
    _description = "Registrar el paso que toca de una NC"

    alert_id = fields.Many2one('quality.alert', string="No conformidad", required=True, ondelete='cascade')
    step = fields.Selection(NC_STEPS, string="Paso", required=True,
                            help="Contención: atiende el efecto ya. Causa raíz: por qué pasó. Plan de acción: la "
                                 "acción correctiva con responsable y compromiso que ataca la causa.")
    name = fields.Char(string="Qué se hace", help="Descripción de la acción de contención o correctiva.")
    responsible_id = fields.Many2one('res.users', string="Responsable", default=lambda self: self.env.user,
                                     help="Quién hace la acción.")
    date_commit = fields.Date(string="Compromiso", default=fields.Date.context_today,
                              help="Fecha en que la acción queda hecha.")
    root_cause = fields.Text(string="Causa raíz", help="Por qué pasó (5 porqués, Ishikawa).")
    plazos = fields.Char(string="Plazos de la NC", compute='_compute_plazos')

    @api.depends('alert_id')
    def _compute_plazos(self):
        for wiz in self:
            alert = wiz.alert_id.sudo()
            labels = dict(alert._fields['sgi_containment_state'].selection) \
                if 'sgi_containment_state' in alert._fields else {}
            parts = []
            for label, state, due in (
                    ("Contención", alert.sgi_containment_state, alert.sgi_due_containment),
                    ("Causa raíz", alert.sgi_root_cause_state, alert.sgi_due_root_cause),
                    ("Plan", alert.sgi_plan_state, alert.sgi_due_plan)):
                parts.append("%s: %s%s" % (label, labels.get(state, state or '—'),
                                           (" (vence %s)" % due.strftime('%d/%m/%Y')) if due else ''))
            wiz.plazos = " · ".join(parts)

    @api.model
    def _sgi_next_step(self, alert):
        """El primer paso sin cumplir de la NC, o False si ya están los tres."""
        alert = alert.sudo()
        if not alert.sgi_containment_done:
            return 'contencion'
        if not (alert.sgi_root_cause or '').strip():
            return 'causa'
        if alert.sgi_plan_state != 'hecha':
            return 'plan'
        return False

    @api.model
    def _sgi_can_register(self, alert):
        env = self.env
        if env.su or any(env.user.has_group(g) for g in (
                'quimibond_sgi.group_sgi_manager', 'quimibond_sgi.group_sgi_director')):
            return True
        alert = alert.sudo()
        if 'sgi_responsible_ids' in alert._fields and env.user in alert.sgi_responsible_ids:
            return True
        owner = alert.sgi_process_id.owner_id if 'sgi_process_id' in alert._fields else False
        return bool(owner and owner.user_id == env.user)

    def action_confirm(self):
        self.ensure_one()
        alert = self.alert_id
        if not self._sgi_can_register(alert):
            raise AccessError("Solo los responsables de la NC, el dueño de su proceso, el Jefe MAST o "
                              "Dirección registran sus pasos.")
        Alert = alert.sudo()
        step_label = dict(NC_STEPS)[self.step]
        if self.step == 'causa':
            cause = (self.root_cause or '').strip()
            if not cause:
                raise UserError("Escriba la causa raíz.")
            Alert.write({'sgi_root_cause': cause})
            detail = cause
        else:
            if not ((self.name or '').strip() and self.responsible_id and self.date_commit):
                raise UserError("La acción necesita qué se hace, responsable y compromiso.")
            line = self.env['sgi.action.line'].sudo().create({
                'alert_id': alert.id, 'action_type': NC_STEP_TYPES[self.step], 'name': self.name.strip(),
                'responsible_id': self.responsible_id.id, 'date_commit': self.date_commit})
            detail = "%s (%s, compromiso %s)" % (line.name, line.responsible_id.name,
                                                 line.date_commit.strftime('%d/%m/%Y'))
        Alert.message_post(body=Markup("%s registrada desde Mis pendientes: %s") % (step_label, detail))
        Pending = self.env['sgi.my.pending']
        Pending._sgi_refresh_for('quality.alert', alert.ids)
        following = self._sgi_next_step(alert)
        return Pending._sgi_feedback("%s registrada. %s" % (
            step_label, ("Sigue: %s." % dict(NC_STEPS)[following]) if following
            else "Los tres plazos están cumplidos; la verificación y el cierre siguen en la ficha de la NC."))


class SgiDocReview(models.TransientModel):
    _name = 'sgi.doc.review'
    _description = "Revisar un documento controlado"

    document_id = fields.Many2one('documents.document', string="Documento", required=True, ondelete='cascade')
    decision = fields.Selection([
        ('vigente', "Sigue vigente sin cambios"),
        ('cambio', "Necesita cambio"),
    ], string="Resultado de la revisión", required=True, default='vigente')
    note = fields.Text(string="Qué se revisó",
                       help="Con quién y contra qué se revisó (práctica actual, norma, requisito). Queda en el "
                            "historial del documento.")
    next_review_date = fields.Date(string="Próxima revisión", compute='_compute_next_review_date')

    @api.depends('decision')
    def _compute_next_review_date(self):
        today = fields.Date.context_today(self)
        for wiz in self:
            wiz.next_review_date = today + relativedelta(years=DOC_REVIEW_YEARS) \
                if wiz.decision == 'vigente' else False

    @api.model
    def _sgi_can_review(self, doc):
        env = self.env
        if env.su or env.user.has_group('quimibond_sgi.group_sgi_manager'):
            return True
        return doc.sudo().sgi_owner_id == env.user

    def action_confirm(self):
        self.ensure_one()
        doc = self.document_id
        if not self._sgi_can_review(doc):
            raise AccessError("Solo el responsable del documento o el Jefe MAST registran su revisión.")
        Pending = self.env['sgi.my.pending']
        if self.decision == 'vigente':
            note = (self.note or '').strip()
            if not note:
                raise UserError("Diga qué se revisó: con quién y contra qué.")
            Doc = doc.sudo()
            Doc.write({'sgi_next_review_date': self.next_review_date})
            Doc.message_post(body=Markup("Revisado sin cambios: %s. Próxima revisión: %s.") % (
                note, self.next_review_date.strftime('%d/%m/%Y')))
            Pending._sgi_refresh_for('documents.document', doc.ids)
            return Pending._sgi_feedback("Revisado sin cambios: %s" % (doc.sudo().display_name or ''))
        category = self.env.ref('quimibond_sgi.sgi_approval_category_doc_change', raise_if_not_found=False)
        if not category:
            raise UserError("No está configurada la categoría de cambio documental del SGI.")
        Doc = doc.sudo()
        context = {
            'default_category_id': category.id,
            'default_name': "Cambio a %s %s" % (Doc.sgi_code or '', Doc.sgi_title or Doc.name or ''),
            'default_sgi_change_kind': 'modificacion',
            'default_sgi_what_changes': 'contenido',
            'default_sgi_document_id': doc.id,
            'default_sgi_reason': (self.note or '').strip() or "Revisión del documento.",
            'default_request_owner_id': self.env.user.id,
        }
        if Doc.sgi_process_id:
            context['default_sgi_affected_process_ids'] = [(6, 0, Doc.sgi_process_id.ids)]
        return {'type': 'ir.actions.act_window', 'name': "Solicitar cambio — %s" % (Doc.display_name or ''),
                'res_model': 'approval.request', 'view_mode': 'form', 'target': 'current',
                'context': context}
