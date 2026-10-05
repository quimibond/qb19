# -*- coding: utf-8 -*-
"""57.108.0: proponer una actividad en lenguaje normal.

Hasta 57.107.0 la propuesta (``sgi.activity.change``) pedía los ~20 campos
de la ficha técnica en cinco pestañas: en producción había tres propuestas,
las tres en borrador y dos vacías. Ahora quien propone contesta seis
preguntas en una sola pantalla:

1. ¿Qué se hace? (``name``)
2. ¿Cada cuándo? (``q_when`` y su día; o qué la dispara, ``trigger_note``)
3. ¿Quién la hace? (``q_who``: yo / otro puesto / como está hoy)
4. ¿Dónde se hace? (``q_where``: en Odoo, papel, otro sistema, correo o
   teléfono, trabajo físico)
5. ¿Cómo sabe que quedó bien? (``done_criteria``)
6. ¿Por qué? (``reason``)

Las preguntas 2 a 4 son campos calculados con inverso sobre los campos
técnicos de siempre (cadencia, vencimiento, canal, rol «Ejecuta»): la ficha
técnica y la pantalla sencilla leen y escriben lo mismo. Abajo, en vivo, la
frase de cómo quedará en su procedimiento y avisos en lenguaje normal.

El Jefe MAST completa lo técnico (instructivo, formatos, aprueba y escala,
contra qué se compara, si no se puede) desde la solicitud con «Completar
antes de aprobar»; al guardar, la solicitud lleva el cambio actualizado.
"""
from markupsafe import Markup, escape

from odoo import Command, api, fields, models
from odoo.exceptions import AccessError, UserError

from .sgi_activity_spec import (
    DEFAULT_VAGUE_VERBS, SGI_MONTHS, SGI_WEEKDAYS, VAGUE_VERBS_PARAM, sgi_plain)

WHEN_CHOICES = [
    ('evento', "Cada vez que pasa algo"),
    ('diaria', "Todos los días"),
    ('semanal', "Cada semana"),
    ('mensual', "Cada mes"),
    ('anual', "Cada año"),
    ('otra', "Otra (cada quincena, trimestre o semestre)"),
]
OTHER_CADENCES = [
    ('quincenal', "Cada quincena"),
    ('trimestral', "Cada trimestre"),
    ('semestral', "Cada semestre"),
]
WHO_CHOICES = [
    ('yo', "Yo (mi puesto)"),
    ('otro', "Otro puesto"),
    ('igual', "Como está hoy"),
]
WHERE_CHOICES = [
    ('odoo', "En Odoo"),
    ('papel', "En papel"),
    ('otro_sistema', "En otro sistema o portal"),
    ('correo', "Por correo o teléfono"),
    ('fisico', "Trabajo físico (no se registra)"),
]
# Canal técnico (exec_channel) → respuesta sencilla.
_CHANNEL_TO_WHERE = {
    'odoo': 'odoo', 'papel': 'papel', 'fisico': 'fisico',
    'sistema_externo': 'otro_sistema', 'portal_cliente': 'otro_sistema', 'edi': 'otro_sistema',
    'correo': 'correo', 'telefono': 'correo',
}
_WHERE_TO_CHANNEL = {'odoo': 'odoo', 'papel': 'papel', 'fisico': 'fisico',
                     'otro_sistema': 'sistema_externo', 'correo': 'correo'}
# Campos de vencimiento que valen para cada cadencia; los demás se limpian.
_DUE_FIELDS = ('due_weekday', 'due_business_day', 'due_month', 'due_day')
_DUE_FOR = {
    'semanal': ('due_weekday',),
    'mensual': ('due_business_day',),
    'anual': ('due_month', 'due_day'),
    'trimestral': ('due_month', 'due_day'),
    'semestral': ('due_month', 'due_day'),
}


class SgiActivityChangeSimple(models.Model):
    _inherit = 'sgi.activity.change'

    trigger_note = fields.Char(
        string="Qué la dispara",
        help="Para las que se hacen cada vez que pasa algo: qué pasa («llega un pedido nuevo», "
             "«se rechaza un lote»). Al aprobarse queda al inicio de la descripción.")
    q_when = fields.Selection(
        WHEN_CHOICES, string="¿Cada cuándo?", compute='_compute_q_answers',
        inverse='_inverse_q_when', readonly=False)
    q_other_cadence = fields.Selection(
        OTHER_CADENCES, string="¿Cada cuánto?", compute='_compute_q_answers',
        inverse='_inverse_q_other_cadence', readonly=False)
    q_who = fields.Selection(
        WHO_CHOICES, string="¿Quién la hace?", compute='_compute_q_answers',
        inverse='_inverse_q_who', readonly=False)
    q_who_job_id = fields.Many2one(
        'hr.job', string="¿Qué puesto?", compute='_compute_q_answers',
        inverse='_inverse_q_who', readonly=False)
    q_where = fields.Selection(
        WHERE_CHOICES, string="¿Dónde se hace?", compute='_compute_q_answers',
        inverse='_inverse_q_where', readonly=False)
    preview_html = fields.Html(
        string="Así quedará en su procedimiento", compute='_compute_preview_html', sanitize=False)
    hints_html = fields.Html(string="Avisos", compute='_compute_preview_html', sanitize=False)
    missing_html = fields.Html(
        string="Lo que falta para publicarla", compute='_compute_missing_html', sanitize=False,
        help="Lo que el Jefe MAST completa antes de aprobar (lo mismo que pide «Faltantes de "
             "especificación» a una actividad).")
    is_sgi_manager = fields.Boolean(compute='_compute_is_sgi_manager')

    @api.depends_context('uid')
    def _compute_is_sgi_manager(self):
        manager = self.env.user.has_group('quimibond_sgi.group_sgi_manager')
        for rec in self:
            rec.is_sgi_manager = manager

    # ------------------------------------------------------------------
    # Preguntas ↔ campos técnicos
    # ------------------------------------------------------------------
    def _sgi_executor_line(self):
        """El renglón «Ejecuta» sin condición (el que dice quién la hace)."""
        self.ensure_one()
        return self.role_line_ids.filtered(
            lambda line: line.role == 'ejecuta' and not (line.condition or '').strip())[:1]

    @api.depends('measure_cadence', 'exec_channel', 'job_id', 'change_type',
                 'role_line_ids.role', 'role_line_ids.target_type', 'role_line_ids.job_id',
                 'role_line_ids.condition')
    def _compute_q_answers(self):
        for rec in self:
            cadence = rec.measure_cadence
            other = dict(OTHER_CADENCES)
            rec.q_when = 'otra' if cadence in other else (cadence or False)
            rec.q_other_cadence = cadence if cadence in other else False
            line = rec._sgi_executor_line()
            if line and line.target_type == 'job' and line.job_id:
                if rec.job_id and line.job_id == rec.job_id:
                    rec.q_who, rec.q_who_job_id = 'yo', False
                else:
                    rec.q_who, rec.q_who_job_id = 'otro', line.job_id
            elif line:
                # Familia o rol relativo: solo MAST lo cambia en la ficha completa.
                rec.q_who, rec.q_who_job_id = 'igual', False
            else:
                rec.q_who = 'yo' if rec.job_id else 'otro'
                rec.q_who_job_id = False
            rec.q_where = _CHANNEL_TO_WHERE.get(rec.exec_channel or '', False)

    def _sgi_clear_due(self, cadence):
        keep = _DUE_FOR.get(cadence, ())
        return {f: (0 if self._fields[f].type == 'integer' else False)
                for f in _DUE_FIELDS if f not in keep and self[f]}

    def _inverse_q_when(self):
        for rec in self:
            when = rec.q_when
            if not when:
                continue
            if when == 'otra':
                cadence = rec.measure_cadence if rec.measure_cadence in dict(OTHER_CADENCES) \
                    else (rec.q_other_cadence or 'trimestral')
            else:
                cadence = when
            vals = rec._sgi_clear_due(cadence)
            if rec.measure_cadence != cadence:
                vals['measure_cadence'] = cadence
            if when != 'evento' and rec.trigger_note:
                vals['trigger_note'] = False
            if vals:
                rec.write(vals)

    def _inverse_q_other_cadence(self):
        for rec in self:
            if rec.q_when == 'otra' and rec.q_other_cadence \
                    and rec.measure_cadence != rec.q_other_cadence:
                rec.write(dict(rec._sgi_clear_due(rec.q_other_cadence),
                               measure_cadence=rec.q_other_cadence))

    def _inverse_q_who(self):
        for rec in self:
            if rec.q_who == 'yo':
                job = rec.job_id
            elif rec.q_who == 'otro':
                job = rec.q_who_job_id
            else:
                continue
            if not job:
                continue
            line = rec._sgi_executor_line()
            if line and line.target_type == 'job' and line.job_id == job:
                continue
            if line:
                command = Command.update(line.id, {'target_type': 'job', 'job_id': job.id,
                                                   'family_id': False, 'relative_role': False})
            else:
                command = Command.create({'role': 'ejecuta', 'target_type': 'job', 'job_id': job.id})
            rec.write({'role_line_ids': [command]})

    def _inverse_q_where(self):
        for rec in self:
            where = rec.q_where
            if not where:
                continue
            channel = rec.exec_channel if _CHANNEL_TO_WHERE.get(rec.exec_channel or '') == where \
                else _WHERE_TO_CHANNEL[where]
            vals = {}
            if rec.exec_channel != channel:
                vals['exec_channel'] = channel
            if where != 'odoo' and rec.odoo_menu_id:
                vals['odoo_menu_id'] = False
            if where != 'otro_sistema' and rec.external_system:
                vals['external_system'] = False
            if vals:
                rec.write(vals)

    # ------------------------------------------------------------------
    # Vista previa y avisos
    # ------------------------------------------------------------------
    def _sgi_when_text(self):
        """«Cada lunes», «Cada mes, a más tardar el día hábil 5», «Cada vez
        que llega un pedido»…"""
        self.ensure_one()
        cadence = self.measure_cadence
        if cadence == 'evento' or not cadence:
            trigger = (self.trigger_note or '').strip()
            return ("Cada vez que %s" % (trigger[0].lower() + trigger[1:]).rstrip('.')) if trigger \
                else "Cada vez que hace falta"
        if cadence == 'diaria':
            return "Todos los días"
        if cadence == 'semanal':
            day = dict(SGI_WEEKDAYS).get(self.due_weekday or '')
            return "Cada %s" % day.lower() if day else "Cada semana"
        if cadence == 'mensual':
            return ("Cada mes, a más tardar el día hábil %d" % self.due_business_day) \
                if self.due_business_day else "Cada mes"
        labels = {'quincenal': "Cada quincena", 'trimestral': "Cada trimestre",
                  'semestral': "Cada semestre", 'anual': "Cada año"}
        text = labels.get(cadence, "")
        month = dict(SGI_MONTHS).get(self.due_month or '')
        if month and self.due_day:
            text += ", el %d de %s" % (self.due_day, month.lower())
        return text

    def _sgi_who_text(self):
        self.ensure_one()
        line = self._sgi_executor_line()
        if not line:
            return "alguien"
        if line.target_type == 'job':
            return (line.job_id.name or "alguien").capitalize() if line.job_id else "alguien"
        if line.target_type == 'family':
            return line.family_id.name or "alguien"
        selection = dict(line._fields['relative_role']._description_selection(self.env))
        return selection.get(line.relative_role, "alguien")

    def _sgi_where_text(self):
        self.ensure_one()
        where = _CHANNEL_TO_WHERE.get(self.exec_channel or '')
        if where == 'odoo':
            menu = self.sudo().odoo_menu_id.complete_name
            return " en Odoo (%s)" % menu if menu else " en Odoo"
        if where == 'otro_sistema':
            return " en %s" % self.external_system if self.external_system else " en otro sistema"
        return {'papel': " en papel", 'correo': " por correo o teléfono", 'fisico': ""}.get(where, "")

    @api.depends('name', 'trigger_note', 'measure_cadence', 'due_weekday', 'due_business_day',
                 'due_month', 'due_day', 'exec_channel', 'odoo_menu_id', 'external_system',
                 'done_criteria', 'how_steps', 'reason', 'change_type',
                 'role_line_ids.role', 'role_line_ids.target_type', 'role_line_ids.job_id',
                 'role_line_ids.family_id', 'role_line_ids.relative_role', 'role_line_ids.condition')
    def _compute_preview_html(self):
        Activity = self.env['sgi.process.activity']
        vague = Activity._sgi_verb_list(VAGUE_VERBS_PARAM, DEFAULT_VAGUE_VERBS)
        for rec in self:
            what = (rec.name or '').strip()
            if rec.change_type == 'quitar' or not what:
                rec.preview_html = False
            else:
                sentence = "%s, %s: %s%s." % (
                    rec._sgi_when_text(), rec._sgi_who_text(),
                    what[0].lower() + what[1:], rec._sgi_where_text())
                parts = [Markup("<p class='mb-1'>%s</p>") % sentence]
                if rec.how_steps:
                    parts.append(Markup("<p class='mb-1'><b>Cómo:</b> %s</p>") % rec.how_steps)
                if rec.done_criteria:
                    parts.append(Markup("<p class='mb-0'><b>Terminada cuando:</b> %s</p>")
                                 % rec.done_criteria.strip())
                rec.preview_html = Markup('').join(parts)
            hints = []
            if rec.change_type != 'quitar':
                words = sgi_plain(what).split()
                first, first_two = (words[0] if words else ''), " ".join(words[:2])
                if first in vague or first_two in vague:
                    hints.append("«%s» no se puede ver ni contar: diga qué se entrega. Por ejemplo, "
                                 "en lugar de «Dar seguimiento a los pedidos», «Avisar a Ventas los "
                                 "pedidos atrasados»." % (first_two if first_two in vague else first))
                if what and not (rec.done_criteria or '').strip():
                    hints.append("Falta cómo sabe que quedó bien: sin eso nadie puede decir si "
                                 "está terminada.")
                if rec.measure_cadence == 'evento' and what and not (rec.trigger_note or '').strip():
                    hints.append("Diga qué pasa que la dispara (por ejemplo, «llega un pedido "
                                 "nuevo»).")
            if not (rec.reason or '').strip():
                hints.append("Escriba por qué la propone: qué problema resuelve o qué pasa hoy.")
            rec.hints_html = Markup('').join(
                Markup("<div>• %s</div>") % hint for hint in hints) if hints else False

    @api.depends('done_criteria', 'on_fail', 'measure_cadence', 'due_weekday', 'due_business_day',
                 'due_month', 'due_day', 'exec_channel', 'odoo_menu_id', 'external_system',
                 'instruction_id', 'how_steps', 'trigger_note', 'role_line_ids.role')
    def _compute_missing_html(self):
        for rec in self:
            missing = []
            if not (rec.done_criteria or '').strip():
                missing.append("Criterio de terminado.")
            roles = rec.role_line_ids.mapped('role')
            if not (rec.on_fail or '').strip() and 'escala' not in roles:
                missing.append("Qué hacer si no se puede cumplir, o un rol «Escala».")
            if 'escala' not in roles:
                missing.append("Rol «Escala» (a quién se le avisa si se atrasa).")
            cadence = rec.measure_cadence
            if cadence == 'semanal' and not rec.due_weekday:
                missing.append("Día de la semana en que vence.")
            if cadence == 'mensual' and not rec.due_business_day:
                missing.append("Día hábil del mes en que vence.")
            if cadence in ('trimestral', 'semestral', 'anual') and not (rec.due_month and rec.due_day):
                missing.append("Mes y día en que vence.")
            if not rec.exec_channel:
                missing.append("Dónde se hace.")
            if rec.exec_channel == 'odoo' and not rec.odoo_menu_id:
                missing.append("La pantalla de Odoo donde se hace.")
            if rec.exec_channel in ('sistema_externo', 'portal_cliente', 'edi') \
                    and not (rec.external_system or '').strip():
                missing.append("El nombre del sistema externo.")
            if not rec.instruction_id and not (rec.how_steps or '').strip():
                missing.append("Cómo se hace: instructivo o pasos.")
            rec.missing_html = Markup('').join(
                Markup("<div>• %s</div>") % escape(item) for item in missing) if missing else False

    # ------------------------------------------------------------------
    # Lo que dispara la actividad entra a la descripción y al antes → después
    # ------------------------------------------------------------------
    def _sgi_values_for_activity(self):
        vals = super()._sgi_values_for_activity()
        trigger = (self.trigger_note or '').strip()
        if trigger and self.measure_cadence == 'evento':
            lead = "Se hace cuando %s." % (trigger[0].lower() + trigger[1:]).rstrip('.')
            description = (vals.get('description') or '').strip()
            if lead not in description:
                vals['description'] = "%s\n%s" % (lead, description) if description else lead
        return vals

    def _sgi_changes(self):
        rows = super()._sgi_changes()
        trigger = (self.trigger_note or '').strip()
        if trigger and self.measure_cadence == 'evento':
            rows.append(("Qué la dispara", "", trigger))
        return rows

    def action_submit(self):
        # El antes → después congelado debe incluir «Qué la dispara».
        self.invalidate_recordset(['diff_html'])
        return super().action_submit()

    # ------------------------------------------------------------------
    # Botones
    # ------------------------------------------------------------------
    def _sgi_full_form_action(self, name):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'name': name, 'res_model': self._name,
            'res_id': self.id, 'view_mode': 'form', 'target': 'new',
            'views': [(self.env.ref('quimibond_sgi.sgi_activity_change_view_form_full').id, 'form')],
        }

    def action_open_full_form(self):
        """«Ver todos los campos» (Jefe MAST): la ficha técnica completa."""
        if not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise AccessError("La ficha completa de la propuesta es del Jefe MAST.")
        return self._sgi_full_form_action("Propuesta: todos los campos")

    def action_sgi_mast_complete(self):
        """«Guardar y actualizar la solicitud»: lo que el Jefe MAST completó
        pasa a la solicitud (antes → después y motivo) antes de aprobar."""
        self.ensure_one()
        if not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise AccessError("Solo el Jefe MAST completa la propuesta.")
        if self.state != 'enviada' or not self.request_id:
            raise UserError("Solo se completa una propuesta enviada que espera aprobación.")
        self.invalidate_recordset(['diff_html'])
        snapshot = self.diff_html
        self.sudo().write({'diff_snapshot': snapshot})
        request = self.request_id.sudo()
        request.write({'reason': snapshot})
        request.message_post(body="El Jefe MAST completó la propuesta antes de aprobarla; el "
                                  "cambio de arriba ya lo incluye.")
        return {'type': 'ir.actions.client', 'tag': 'soft_reload'}


class ApprovalRequestMpComplete(models.Model):
    _inherit = 'approval.request'

    def action_sgi_mp_complete(self):
        """«Completar antes de aprobar» (Jefe MAST): abre la propuesta con
        todos los campos y lo que le falta."""
        self.ensure_one()
        if not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise AccessError("Solo el Jefe MAST completa la propuesta.")
        proposal = self.sgi_mp_proposal_id
        if not proposal:
            raise UserError("Esta solicitud no tiene propuesta de actividad.")
        if proposal.sudo().state != 'enviada':
            raise UserError("La propuesta ya se aplicó; los cambios se hacen en la actividad.")
        return proposal._sgi_full_form_action("Completar antes de aprobar")
