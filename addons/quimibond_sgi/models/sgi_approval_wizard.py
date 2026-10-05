# -*- coding: utf-8 -*-
"""57.109.0: configurar una aprobación del procedimiento en lenguaje normal.

En producción (2026-10-05), de 82 roles «Aprueba» de actividades vigentes, 61
estaban «Sin configurar»: el procedimiento decía que algo se aprueba y Odoo
no lo pedía (p. ej. S6.03, alta de usuarios). La ficha arrancaba en «Botón de
Odoo» y pedía documento, método y condición en términos técnicos.

El asistente (``sgi.approval.wizard``) pregunta:

1. ¿Qué se aprueba? Un documento de Odoo (y qué acción: Confirmar, Validar,
   Publicar…), una decisión o petición sin documento (solicitud en
   Aprobaciones) o algo que hoy se firma en papel (Firma electrónica).
2. ¿Siempre o solo a veces? (campo, operador y valor, con su etiqueta).
3. Vista previa: «Cuando el Planeador pulse Confirmar en una orden de
   compra con total mayor que 50,000, Odoo le pedirá la aprobación a …».

Además: una sugerencia por rol (documento con botón conocido → botón; si no
→ solicitud), «Activar las sugeridas como solicitud» en lote (solo las que no
bloquean botones) y el faltante «Aprobación sin activar» en la actividad.
"""
from lxml import etree
from markupsafe import Markup

from odoo import api, fields, models
from odoo.exceptions import AccessError, UserError

from .sgi_approval_native import APPROVAL_METHOD_BY_MODEL, CONDITION_FIELD_TYPES, CONDITION_OPERATORS

WHAT_CHOICES = [
    ('documento', "Que se confirme, valide o publique un documento de Odoo"),
    ('decision', "Una decisión o petición sin documento (se pide en Aprobaciones)"),
    ('papel', "Algo que hoy se firma en papel (Firma electrónica)"),
]
_KIND_TO_WHAT = {'boton': 'documento', 'solicitud': 'decision', 'firma': 'papel'}
_WHAT_TO_KIND = {v: k for k, v in _KIND_TO_WHAT.items()}
# Estados de una aprobación que no funciona en Odoo.
APPROVAL_NOT_ACTIVE = ('sin_configurar', 'sin_aprobadores', 'por_sincronizar', 'conflicto')


def _sgi_manager_only(env):
    if not (env.su or env.user.has_group('quimibond_sgi.group_sgi_manager')):
        raise AccessError("Solo el Jefe MAST configura las aprobaciones del procedimiento.")


class SgiActivityRoleApprovalWizard(models.Model):
    """Sugerencia, activación en lote y el faltante de la actividad."""
    _inherit = 'sgi.activity.role'

    approval_suggestion = fields.Char(
        string="Sugerencia", compute='_compute_approval_suggestion',
        help="Cómo se sugiere aprobar: con el botón del documento que materializa la actividad, si "
             "tiene uno conocido; si no, con una solicitud en Aprobaciones.")

    def _sgi_suggested_model(self):
        self.ensure_one()
        activity = self.activity_id.sudo()
        model = self.approval_model_id or activity.measure_model_id
        if not model and hasattr(activity, '_sgi_output_deliverable'):
            model = activity._sgi_output_deliverable().odoo_model_id
        return model

    def _sgi_approval_suggestion_vals(self):
        """(qué, modelo, método): ``documento`` con su botón si el documento
        tiene uno conocido; si no, ``decision`` (solicitud en Aprobaciones)."""
        self.ensure_one()
        model = self._sgi_suggested_model()
        method = self.approval_method or APPROVAL_METHOD_BY_MODEL.get(model.model if model else '')
        if model and method and model.model != 'approval.request':
            return 'documento', model, method
        return 'decision', self.env['ir.model'], False

    def _compute_approval_suggestion(self):
        for role in self:
            if role.role != 'aprueba':
                role.approval_suggestion = False
                continue
            what, model, method = role._sgi_approval_suggestion_vals()
            role.approval_suggestion = ("Botón «%s» de %s" % (method, model.name)) \
                if what == 'documento' else "Solicitud en Aprobaciones"

    def action_sgi_approval_wizard(self):
        """«Configurar»: el asistente de tres preguntas."""
        self.ensure_one()
        _sgi_manager_only(self.env)
        wizard = self.env['sgi.approval.wizard'].create({'role_id': self.id})
        return wizard._sgi_reopen()

    def action_sgi_activate_suggested(self):
        """«Activar las sugeridas como solicitud»: las aprobaciones sin
        configurar cuya sugerencia es una solicitud en Aprobaciones (no
        bloquean ningún botón) se activan de una vez. Las de botón quedan para
        el asistente, una por una."""
        _sgi_manager_only(self.env)
        roles = self if self else self.search([
            ('role', '=', 'aprueba'), ('activity_id.active', '=', True),
            ('activity_id.process_id.active', '=', True)])
        todo = roles.filtered(lambda r: r.role == 'aprueba' and r.approval_state == 'sin_configurar'
                              and r._sgi_approval_suggestion_vals()[0] == 'decision'
                              and not r._sgi_record_dependent())
        if todo:
            todo.write({'approval_kind': 'solicitud'})
            todo._sgi_sync_approval_rule()
            todo.activity_id._sgi_refresh_spec_gaps()
        active = todo.filtered(lambda r: r.approval_state == 'activa')
        return {
            'type': 'ir.actions.client', 'tag': 'display_notification',
            'params': {
                'type': 'success' if active else 'warning',
                'message': "Se activaron %d aprobaciones como solicitud en Aprobaciones; %d quedaron sin "
                           "personas en el puesto. Las de botón se configuran una por una." % (
                               len(active), len(todo) - len(active)),
                'next': {'type': 'ir.actions.client', 'tag': 'soft_reload'},
            },
        }

    @api.model
    def cron_sgi_sync_approvals(self):
        res = super().cron_sgi_sync_approvals()
        # 57.109.0: el faltante «Aprobación sin activar» cambia cuando cambian
        # las personas de los puestos.
        roles = self.sudo().search([('role', '=', 'aprueba'), ('activity_id.active', '=', True),
                                    ('activity_id.process_id.active', '=', True)])
        roles.activity_id._sgi_refresh_spec_gaps()
        self._sgi_notice_not_active(roles)
        return res

    @api.model
    def _sgi_notice_not_active(self, roles):
        """57.109.0: un aviso al Jefe MAST por proceso con aprobaciones del
        procedimiento sin activar en Odoo (Mis pendientes); se cierra solo
        cuando ya no queda ninguna en ese proceso."""
        Cron = self.env['sgi.cron']
        manager_id = Cron._sgi_manager_user_id()
        by_process = {}
        for role in roles:
            if role.approval_state in APPROVAL_NOT_ACTIVE:
                by_process.setdefault(role.activity_id.process_id, []).append(role)
        for process, pending in by_process.items():
            Cron._sgi_schedule(
                process, "Aprobaciones sin activar en Odoo: %d" % len(pending),
                "El procedimiento dice que se aprueban y Odoo no lo pide: %s. Configúrelas en "
                "Administración SGI → Aprobaciones del SGI (botón «Configurar»)." % ", ".join(
                    r.activity_id.number or r.activity_id.name or '' for r in pending),
                manager_id, key='aprobaciones_sin_activar:%d' % process.id)
        stale = self.env['mail.activity'].sudo().search([
            ('sgi_cron_kind', '=', 'aprobaciones_sin_activar'), ('sgi_episode_closed', '=', False),
            ('res_model', '=', 'sgi.process'),
            ('res_id', 'not in', [p.id for p in by_process])])
        if stale:
            Cron._sgi_close_activities(stale, "ya no quedan aprobaciones sin activar en el proceso")
        return by_process


class SgiActivityApprovalGap(models.Model):
    _inherit = 'sgi.process.activity'

    def _sgi_approval_problems(self):
        """57.109.0: mensajes de las aprobaciones de la actividad que no
        funcionan en Odoo (el procedimiento dice que se aprueba y Odoo no lo
        pide)."""
        self.ensure_one()
        out = []
        labels = dict(self.env['sgi.activity.role']._fields['approval_state']._description_selection(
            self.env))
        for role in self.role_ids.filtered(lambda r: r.role == 'aprueba'):
            if role.approval_state in APPROVAL_NOT_ACTIVE:
                who = role.job_id.name or role.family_id.name or "quien aprueba"
                out.append("La aprobación de %s no está activa en Odoo (%s): configúrela en "
                           "Administración SGI → Aprobaciones del SGI." % (
                               who, labels.get(role.approval_state, role.approval_state).lower()))
        return out


class SgiApprovalWizard(models.TransientModel):
    """Configurar la aprobación de un rol «Aprueba» con tres preguntas."""
    _name = 'sgi.approval.wizard'
    _description = "Configurar una aprobación del procedimiento"

    role_id = fields.Many2one('sgi.activity.role', string="Aprobación", required=True,
                              ondelete='cascade')
    activity_id = fields.Many2one(related='role_id.activity_id', string="Actividad")
    approver_user_ids = fields.Many2many(related='role_id.approval_user_ids', string="Quién aprueba")
    step = fields.Selection([('what', "Qué se aprueba"), ('how', "Cómo")], default='what',
                            required=True)
    q_what = fields.Selection(WHAT_CHOICES, string="¿Qué se aprueba?", required=True)
    model_id = fields.Many2one('ir.model', string="¿Qué documento?",
                               domain=[('transient', '=', False)],
                               help="El documento de Odoo que se aprueba, por su nombre (Orden de compra, "
                                    "Factura, Entrega…).")
    button_line_id = fields.Many2one('sgi.approval.wizard.button', string="¿Al pulsar qué?",
                                     domain="[('wizard_id', '=', id)]")
    button_line_ids = fields.One2many('sgi.approval.wizard.button', 'wizard_id')
    buttons_model = fields.Char(help="Modelo del que se cargaron las acciones.")
    condition_on = fields.Boolean(string="Solo a veces",
                                  help="Márquelo si solo se aprueba bajo una condición (p. ej. cuando el "
                                       "total pasa de cierta cantidad).")
    condition_field_id = fields.Many2one(
        'ir.model.fields', string="Cuando el campo",
        domain="[('model_id', '=', model_id), ('ttype', 'in', %s), ('store', '=', True)]"
               % (list(CONDITION_FIELD_TYPES),))
    condition_operator = fields.Selection(CONDITION_OPERATORS, string="sea", default='>')
    condition_value = fields.Char(string="el valor")
    category_name = fields.Char(string="¿Cómo se llama la solicitud?",
                                help="El nombre con el que se pide en Aprobaciones (p. ej. «Alta de usuario»).")
    sign_template_id = fields.Many2one('sign.template', string="¿Qué documento se firma?")
    button_supported = fields.Boolean(compute='_compute_button_supported')
    suggestion = fields.Char(related='role_id.approval_suggestion', string="Sugerencia")
    preview_html = fields.Html(string="Así funcionará", compute='_compute_preview_html',
                               sanitize=False)

    @api.model_create_multi
    def create(self, vals_list):
        Role = self.env['sgi.activity.role']
        for vals in vals_list:
            role = Role.browse(vals.get('role_id')).sudo()
            if role and 'q_what' not in vals:
                if role.approval_state not in ('sin_configurar', False):
                    what = _KIND_TO_WHAT.get(role.approval_kind, 'decision')
                    model, method = role.approval_model_id, role.approval_method
                else:
                    what, model, method = role._sgi_approval_suggestion_vals()
                vals.setdefault('q_what', what)
                if model:
                    vals.setdefault('model_id', model.id)
                vals.setdefault('category_name', (role.approval_category_id.name if role.approval_category_id
                                                  else role.activity_id.name) or False)
                vals.setdefault('sign_template_id', role.approval_sign_template_id.id or False)
                if role.condition_field_id:
                    vals.setdefault('condition_on', True)
                    vals.setdefault('condition_field_id', role.condition_field_id.id)
                    vals.setdefault('condition_operator', role.condition_operator)
                    vals.setdefault('condition_value', role.condition_value)
                vals['_sgi_method'] = method
        methods = [vals.pop('_sgi_method', False) for vals in vals_list]
        wizards = super().create(vals_list)
        for wizard, method in zip(wizards, methods):
            if wizard.q_what == 'documento' and wizard.model_id:
                wizard._sgi_load_buttons(method)
        return wizards

    @api.depends('role_id')
    def _compute_button_supported(self):
        for wiz in self:
            wiz.button_supported = bool(wiz.role_id.sudo()._sgi_button_supported())

    # ------------------------------------------------------------------
    def _sgi_reopen(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'name': "Configurar aprobación",
            'res_model': self._name, 'res_id': self.id, 'view_mode': 'form', 'target': 'new',
            'views': [(self.env.ref('quimibond_sgi.sgi_approval_wizard_view_form').id, 'form')],
        }

    def _sgi_load_buttons(self, selected_method=None):
        """Las acciones (botones) del formulario del documento, con su
        etiqueta: Confirmar, Validar, Publicar…"""
        self.ensure_one()
        self.button_line_ids.unlink()
        model = self.model_id.model
        self.buttons_model = model or False
        if not model or model not in self.env:
            return
        try:
            arch = self.env[model].sudo().get_view(view_type='form')['arch']
            tree = etree.fromstring(arch.encode() if isinstance(arch, str) else arch)
        except Exception:  # noqa: BLE001 - un formulario raro no detiene el asistente
            tree = None
        seen, lines = set(), []
        suggested = APPROVAL_METHOD_BY_MODEL.get(model)
        if suggested:
            lines.append((suggested, suggested))
            seen.add(suggested)
        if tree is not None:
            for node in tree.iter('button'):
                name = node.get('name') or ''
                if node.get('type') != 'object' or not name or name in seen or name.startswith('_'):
                    continue
                seen.add(name)
                lines.append((name, node.get('string') or name))
        if selected_method and selected_method not in seen:
            lines.append((selected_method, selected_method))
        labels = {}
        if tree is not None:
            for node in tree.iter('button'):
                if node.get('name') and node.get('string'):
                    labels.setdefault(node.get('name'), node.get('string'))
        created = self.env['sgi.approval.wizard.button'].create([{
            'wizard_id': self.id, 'method': method, 'name': labels.get(method, label),
            'sequence': i} for i, (method, label) in enumerate(lines)])
        pick = created.filtered(lambda b: b.method == (selected_method or suggested))[:1] or created[:1]
        self.button_line_id = pick

    def action_next(self):
        self.ensure_one()
        if self.q_what == 'documento':
            if not self.model_id:
                raise UserError("Elija el documento de Odoo que se aprueba.")
            if self.buttons_model != self.model_id.model:
                self._sgi_load_buttons()
            if not self.button_line_ids:
                raise UserError("No se encontraron acciones en el formulario de %s: elija «Una decisión o "
                                "petición sin documento»." % self.model_id.name)
        self.step = 'how'
        return self._sgi_reopen()

    def action_back(self):
        self.step = 'what'
        return self._sgi_reopen()

    # ------------------------------------------------------------------
    def _sgi_executor_text(self):
        roles = self.role_id.activity_id.sudo().role_ids.filtered(lambda r: r.role == 'ejecuta')
        names = [r.job_id.name or r.family_id.name for r in roles if r.job_id or r.family_id]
        return (", ".join(n.capitalize() for n in names if n)) or "quien la ejecuta"

    def _sgi_condition_text(self):
        if not (self.condition_on and self.condition_field_id):
            return ""
        op = dict(CONDITION_OPERATORS).get(self.condition_operator, self.condition_operator or '')
        return " con %s %s %s" % (self.condition_field_id.field_description.lower(), op,
                                  self.condition_value or '')

    @api.depends('q_what', 'model_id', 'button_line_id', 'condition_on', 'condition_field_id',
                 'condition_operator', 'condition_value', 'category_name', 'sign_template_id',
                 'role_id')
    def _compute_preview_html(self):
        for wiz in self:
            approvers = ", ".join(wiz.role_id.sudo().approval_user_ids.mapped('name'))
            approvers = approvers or "(nadie: el puesto que aprueba no tiene personas con usuario)"
            who = wiz._sgi_executor_text()
            if wiz.q_what == 'documento':
                if not (wiz.model_id and wiz.button_line_id):
                    wiz.preview_html = False
                    continue
                text = Markup("Cuando %s pulse <b>«%s»</b> en <b>%s</b>%s, Odoo le pedirá la aprobación a "
                              "<b>%s</b> antes de continuar.") % (
                    who, wiz.button_line_id.name, wiz.model_id.name, wiz._sgi_condition_text(), approvers)
            elif wiz.q_what == 'decision':
                text = Markup("%s pedirá en <b>Aprobaciones</b> «%s» y <b>%s</b> la aprueba. Queda "
                              "registrado quién aprobó y cuándo.") % (
                    who.capitalize(), wiz.category_name or wiz.activity_id.name or '', approvers)
            elif wiz.q_what == 'papel':
                text = Markup("Se firmará en <b>Firma electrónica</b>%s; firma <b>%s</b>.") % (
                    Markup(" con la plantilla «%s»") % wiz.sign_template_id.name
                    if wiz.sign_template_id else '', approvers)
            else:
                text = False
            wiz.preview_html = text

    def action_activate(self):
        """«Activar»: escribe la aprobación en el rol y la sincroniza."""
        self.ensure_one()
        _sgi_manager_only(self.env)
        role = self.role_id
        if self.q_what == 'documento':
            if not (self.model_id and self.button_line_id):
                raise UserError("Elija el documento y la acción que se aprueba.")
            if not self.button_supported:
                raise UserError("Bloquear un botón lo mantiene el módulo «Quimibond SGI - Aprobaciones de "
                                "Studio», que no está instalado. Elija «Una decisión o petición sin "
                                "documento»: se pide en Aprobaciones.")
            vals = {'approval_kind': 'boton', 'approval_model_id': self.model_id.id,
                    'approval_method': self.button_line_id.method}
            if self.condition_on and self.condition_field_id:
                vals.update(condition_field_id=self.condition_field_id.id,
                            condition_operator=self.condition_operator,
                            condition_value=self.condition_value)
            else:
                vals.update(condition_field_id=False, condition_value=False)
        elif self.q_what == 'decision':
            vals = {'approval_kind': 'solicitud'}
        else:
            if not self.sign_template_id:
                raise UserError("Elija la plantilla de Firma electrónica que se firma.")
            vals = {'approval_kind': 'firma', 'approval_sign_template_id': self.sign_template_id.id}
        role.write(vals)
        role.action_sgi_sync_approval()
        category = role.approval_category_id.sudo()
        if self.q_what == 'decision' and category.sgi_role_id == role and self.category_name \
                and category.name != self.category_name:
            category.name = self.category_name
        role.activity_id._sgi_refresh_spec_gaps()
        labels = dict(role._fields['approval_state']._description_selection(self.env))
        return {
            'type': 'ir.actions.client', 'tag': 'display_notification',
            'params': {
                'type': 'success' if role.approval_state == 'activa' else 'warning',
                'message': "Aprobación de %s: %s." % (role.activity_id.display_name,
                                                       labels.get(role.approval_state, '').lower()),
                'next': {'type': 'ir.actions.client', 'tag': 'soft_reload'},
            },
        }


class SgiApprovalWizardButton(models.TransientModel):
    """Una acción del formulario del documento (para elegirla por su etiqueta)."""
    _name = 'sgi.approval.wizard.button'
    _description = "Acción que se aprueba"
    _order = 'sequence, id'

    wizard_id = fields.Many2one('sgi.approval.wizard', required=True, ondelete='cascade')
    sequence = fields.Integer()
    method = fields.Char(required=True)
    name = fields.Char(string="Acción", required=True)

    @api.depends('name', 'method')
    def _compute_display_name(self):
        for line in self:
            line.display_name = "%s (%s)" % (line.name, line.method) if line.name != line.method \
                else line.name
