# -*- coding: utf-8 -*-
"""Cimiento común de los registros del SGI.

Centraliza el patrón que hasta ahora se repetía en ~7 modelos: herencia de
``mail.thread`` + ``mail.activity.mixin``, folio con secuencia propia y candado
de inmutabilidad por estado. Los modelos concretos sólo declaran su código de
secuencia (``_sgi_sequence_code``) y, si aplica, sus estados bloqueados
(``_sgi_locked_states``).
"""
from odoo import models, fields, api
from odoo.exceptions import UserError


class SgiBaseMixin(models.AbstractModel):
    _name = 'sgi.base.mixin'
    _description = "Cimiento de registros del SGI"
    _inherit = ['mail.thread', 'mail.activity.mixin']

    # --- Configuración por modelo -------------------------------------------
    # Código de la secuencia (ir.sequence) del folio. Debe ser uno de los
    # códigos YA existentes; esta ola NO renumera ni crea secuencias nuevas.
    _sgi_sequence_code = None
    # Estados en los que el registro queda cerrado: sólo MAST puede editarlo.
    _sgi_locked_states = ()
    folio = fields.Char(string="Folio", readonly=True, copy=False,
                        index=True, tracking=True)

    # ------------------------------------------------------------------------
    # Folio con secuencia (patrón centralizado)
    # ------------------------------------------------------------------------
    @api.model_create_multi
    def create(self, vals_list):
        code = self._sgi_sequence_code
        if code:
            seq = self.env['ir.sequence']
            for vals in vals_list:
                if not vals.get('folio'):
                    vals['folio'] = seq.next_by_code(code) or '/'
        return super().create(vals_list)

    # ------------------------------------------------------------------------
    # Helpers de actividad (envolturas de activity_schedule / activity_feedback)
    # ------------------------------------------------------------------------
    def _sgi_schedule_activity(self, user, summary, note=False, date_deadline=False):
        """Agenda una actividad genérica «Por hacer» en el registro."""
        self.ensure_one()
        user_id = user.id if hasattr(user, 'ids') else user
        return self.activity_schedule(
            'mail.mail_activity_data_todo',
            summary=summary,
            note=note or False,
            user_id=user_id or self.env.uid,
            date_deadline=date_deadline or fields.Date.context_today(self))

    def _sgi_done_activities(self, feedback=False):
        """Marca como hechas las actividades «Por hacer» del/los registro(s)."""
        return self.activity_feedback(
            ['mail.mail_activity_data_todo'],
            feedback=feedback or "Hecho.")

    # ------------------------------------------------------------------------
    # Inmutabilidad de registros cerrados (evidencia del SGI)
    # ------------------------------------------------------------------------
    def _sgi_locked_records(self):
        """Subconjunto de self que está en un estado bloqueado."""
        states = self._sgi_locked_states
        if not states or 'state' not in self._fields:
            return self.browse()
        return self.filtered(lambda r: r.state in states)

    def _sgi_vals_touch_locked(self, vals):
        """Campos de vals que cuentan como edición real.

        Se exceptúan sólo los del chatter/actividades (que se mueven solos al
        publicar mensajes o agendar tareas). El estado NO se exceptúa: reabrir
        un registro cerrado es justo lo que se reserva a MAST. Como el candado
        evalúa el estado ANTERIOR del registro, las transiciones que ENTRAN al
        estado cerrado no se bloquean.
        """
        return {k for k in vals
                if not k.startswith('message_')
                and not k.startswith('activity_')
                and not k.startswith('website_message')
                and not k.startswith('rating_')}

    def write(self, vals):
        if (self._sgi_locked_states and not self.env.su
                and not self.env.context.get('sgi_bypass_lock')
                and self._sgi_vals_touch_locked(vals)):
            locked = self._sgi_locked_records()
            if locked and not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
                raise UserError(
                    "Este registro del SGI está cerrado y es evidencia: no puede "
                    "modificarse ni reabrirse. Pide al Jefe de MAST reabrirlo "
                    "(cambiar su estado) si hay un error real.\n\n"
                    "Registros bloqueados: %s"
                    % ", ".join(locked.mapped('display_name')))
        return super().write(vals)


def sgi_find_menu(env, path):
    """Encuentra el ir.ui.menu (con acción) cuya ruta coincide con `path`
    («App/Sub/Menú», separadores ya normalizados a «/»).

    ir.ui.menu.complete_name NO es un campo almacenado en Odoo 19: ponerlo
    en un dominio de search truena con «Cannot convert ... to SQL». Por eso
    los candidatos se buscan por su nombre (almacenado) y la ruta completa
    se compara en Python. Lo usan la actividad del procedimiento
    (_sgi_resolve_menu) y el documento «Formulario de Odoo»."""
    Menu = env['ir.ui.menu'].sudo()
    parts = [p for p in (path or '').split('/') if p]
    if not parts:
        return Menu
    candidates = Menu.search([
        ('name', '=ilike', parts[-1]), ('action', '!=', False)])
    low = '/'.join(parts).lower()
    for menu in candidates:
        if (menu.complete_name or '').lower() == low:
            return menu
    if len(parts) > 1:
        first = parts[0].lower() + '/'
        for menu in candidates:
            if (menu.complete_name or '').lower().startswith(first):
                return menu
        return Menu
    return candidates[:1]
