# -*- coding: utf-8 -*-
"""Cimiento común de los registros del SGI.

Centraliza el patrón que hasta ahora se repetía en ~7 modelos: herencia de
``mail.thread`` + ``mail.activity.mixin``, folio con secuencia propia y candado
de inmutabilidad por estado. Los modelos concretos sólo declaran su código de
secuencia (``_sgi_sequence_code``) y, si aplica, sus estados bloqueados
(``_sgi_locked_states``).
"""
from odoo import models, fields, api
from odoo.exceptions import AccessError, UserError


def sgi_bypass_allowed(env):
    """Un contexto de bypass de candado (sgi_bypass_lock, sgi_force_close,
    sgi_bypass_dirty) solo cuenta si viene de código de sistema (superusuario)
    o de un Jefe MAST. El contexto lo controla el cliente: sin esta guarda,
    cualquier usuario podía brincarse los candados de evidencia pasando el
    contexto por RPC."""
    return env.su or env.user.has_group('quimibond_sgi.group_sgi_manager')


class SgiBaseMixin(models.AbstractModel):
    """Cimiento de los registros del SGI: chatter y actividades, folio con secuencia propia
    (``_sgi_sequence_code``) y agenda de actividades con ``_sgi_schedule_activity``."""
    _name = 'sgi.base.mixin'
    _description = "Cimiento de registros del SGI"
    _inherit = ['mail.thread', 'mail.activity.mixin']

    # --- Configuración por modelo -------------------------------------------
    # Código de la secuencia (ir.sequence) del folio. Debe ser uno de los
    # códigos YA existentes; esta ola NO renumera ni crea secuencias nuevas.
    _sgi_sequence_code = None
    # Estados en los que el registro queda cerrado: sólo MAST puede editarlo.
    _sgi_locked_states = ()
    # D-009 (57.41.0, decisión de Jose): transiciones reservadas al Jefe MAST
    # y al dueño del proceso del registro. Entrar a uno de estos estados, o
    # salir de él, es la decisión (aprobar/rechazar, cerrar/reabrir, marcar
    # obsoleto/regresarlo). Ver _sgi_check_decision().
    _sgi_decision_states = ()
    _sgi_decision_label = "Esta decisión"
    folio = fields.Char(string="Folio", readonly=True, copy=False,
                        index=True, tracking=True)
    # V-A03 (57.41.0): la ficha pone sus campos en solo lectura cuando el
    # servidor rechazaría el cambio (candado de evidencia). Sin guardar.
    sgi_is_locked = fields.Boolean(
        string="Cerrado (solo lectura)", compute='_compute_sgi_is_locked',
        help="El registro está cerrado y es evidencia: solo el Jefe MAST lo edita.")

    @api.depends(lambda self: ('state',) if 'state' in self._fields else ())
    @api.depends_context('uid')
    def _compute_sgi_is_locked(self):
        locked = self._sgi_readonly_records()
        for rec in self:
            rec.sgi_is_locked = rec in locked

    def _sgi_readonly_records(self):
        """Subconjunto de self que el usuario actual ve en solo lectura. Por
        omisión, los que el candado de write() rechazaría."""
        if (not self._sgi_locked_states or self.env.su
                or self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            return self.browse()
        return self._sgi_locked_records()

    # ------------------------------------------------------------------------
    # D-009: decisiones del Jefe MAST y del dueño del proceso
    # ------------------------------------------------------------------------
    def _sgi_decision_processes(self):
        """Procesos cuyo dueño puede decidir sobre este registro (hook por
        modelo). Por omisión, el campo process_id si el modelo lo tiene."""
        self.ensure_one()
        if 'process_id' in self._fields:
            return self.sudo().process_id
        return self.env['sgi.process']

    def _sgi_can_decide(self):
        """¿El usuario actual es Jefe MAST o dueño del proceso de TODOS los
        registros de self?"""
        if self.env.su or self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            return True
        user = self.env.user
        return all(user in rec._sgi_decision_processes().sudo().owner_id.user_id
                   for rec in self)

    def _sgi_decision_moving(self, vals):
        """Registros de self cuyo cambio de estado en vals es una decisión."""
        states = self._sgi_decision_states
        if not states or 'state' not in vals:
            return self.browse()
        new = vals['state']
        return self.filtered(
            lambda r: r.state != new and (new in states or r.state in states))

    def _sgi_check_decision(self, vals):
        if self.env.su:
            return
        moving = self._sgi_decision_moving(vals)
        if moving and not moving._sgi_can_decide():
            raise AccessError(
                "%s la toman solo el Jefe MAST y el dueño del proceso. "
                "Pida a alguno de ellos que la registre.\n\nRegistros: %s"
                % (self._sgi_decision_label, ", ".join(moving.mapped('display_name'))))

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

    def _sgi_is_decision_reopen(self, vals):
        """Reabrir por decisión (D-009): solo cambia el estado, sale de un
        estado cerrado y quien lo hace es dueño del proceso. El candado no lo
        detiene; el resto de los campos sigue cerrado para el dueño."""
        return (self._sgi_decision_states
                and set(self._sgi_vals_touch_locked(vals)) == {'state'}
                and vals['state'] not in self._sgi_locked_states
                and all(r.state in self._sgi_decision_states for r in self)
                and self._sgi_can_decide())

    def write(self, vals):
        self._sgi_check_decision(vals)
        if (self._sgi_locked_states and not self.env.su
                and not (self.env.context.get('sgi_bypass_lock')
                         and sgi_bypass_allowed(self.env))
                and self._sgi_vals_touch_locked(vals)
                and not self._sgi_is_decision_reopen(vals)):
            locked = self._sgi_locked_records()
            if locked and not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
                raise UserError(
                    "Este registro del SGI está cerrado y es evidencia: no puede "
                    "modificarse ni reabrirse. Pida al Jefe de MAST reabrirlo "
                    "(cambiar su estado) si hay un error real.\n\n"
                    "Registros bloqueados: %s"
                    % ", ".join(locked.mapped('display_name')))
        return super().write(vals)

    def unlink(self):
        """Cinturón del candado: un registro cerrado tampoco se borra. Hoy los
        ACL ya niegan unlink a los usuarios en todos los modelos con estados
        bloqueados, pero la regla no debe depender de que ningún ACL futuro lo
        abra."""
        if (self._sgi_locked_states and not self.env.su
                and not (self.env.context.get('sgi_bypass_lock')
                         and sgi_bypass_allowed(self.env))
                and not self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
            locked = self._sgi_locked_records()
            if locked:
                raise UserError(
                    "Este registro del SGI está cerrado y es evidencia: no puede "
                    "borrarse. Pida al Jefe de MAST reabrirlo si hay un error "
                    "real.\n\nRegistros bloqueados: %s"
                    % ", ".join(locked.mapped('display_name')))
        return super().unlink()


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
    # El texto viene de una persona: un «%», «_» o «\» literal son comodines
    # de LIKE y harían matchear menús que no son (o no matchear el correcto).
    pattern = (parts[-1].replace('\\', '\\\\')
               .replace('%', '\\%').replace('_', '\\_'))
    candidates = Menu.search([
        ('name', '=ilike', pattern), ('action', '!=', False)])
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
