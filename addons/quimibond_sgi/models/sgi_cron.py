# -*- coding: utf-8 -*-
import base64
import logging
import uuid
from collections import defaultdict
from datetime import date
from dateutil.relativedelta import relativedelta

from odoo import models, fields, api
from odoo.tools import html2plaintext

from .sgi_guard import sgi_require_system
from .sgi_menu_paths import sgi_menu_path

_logger = logging.getLogger(__name__)


# 56.37.0 (auditoría G-004/G-005): login del Jefe MAST y SGI cuando el
# parámetro quimibond_sgi.mast_user_id no está puesto. Se resuelve por login y
# no por id fijo (en producción es el usuario 128, Blanca Areli Ballesteros).
SGI_MAST_LOGIN = 'mas@quimibond.com'


def _sgi_plain(text):
    """Texto comparable de una nota (Html guardado contra texto nuevo)."""
    return ' '.join(html2plaintext(text or '').split())


class MailActivitySgiCron(models.Model):
    """56.37.0 (G-004, G-005, G-025): los avisos de los crons del SGI llevan
    una clave estable por registro y un episodio.

    - La clave (``sgi_cron_key``) no lleva usuario ni números: si cambia
      quien recibe (otro Jefe MAST) o el dato del resumen, la misma actividad
      se reasigna y se actualiza en vez de duplicarse.
    - Una actividad marcada como hecha sigue en el episodio (archivada): no
      se vuelve a crear mientras dure la misma causa (decisión 5 de la tanda
      2). El episodio se cierra (``sgi_episode_closed``) cuando la causa ya no
      aplica; si reaparece después, nace un aviso nuevo.
    - ``sgi_cron_run`` guarda la última corrida que vio el aviso: lo que una
      corrida completa ya no ve, se cierra solo (``sgi.cron._sgi_sweep``)."""
    _inherit = 'mail.activity'

    sgi_cron_key = fields.Char(
        string="Clave del aviso (SGI)", index=True, copy=False, readonly=True,
        help="Clave estable del aviso automático del SGI sobre este registro.")
    sgi_episode_closed = fields.Boolean(
        string="Episodio cerrado (SGI)", copy=False, readonly=True,
        help="La causa del aviso ya se resolvió; si vuelve, nace otro aviso.")
    sgi_cron_run = fields.Char(
        string="Última corrida que lo vio (SGI)", copy=False, readonly=True)


class SgiCron(models.AbstractModel):
    _name = 'sgi.cron'
    _description = "Tareas programadas SGI"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @api.model
    def _sgi_activity_exists(self, record, summary, user_id):
        """Evita duplicar actividades (idempotencia)."""
        activity_type = self.env.ref('mail.mail_activity_data_todo')
        return bool(self.env['mail.activity'].search_count([
            ('res_model', '=', record._name),
            ('res_id', '=', record.id),
            ('summary', '=', summary),
            ('user_id', '=', user_id),
            ('activity_type_id', '=', activity_type.id),
        ]))

    @api.model
    def _sgi_schedule(self, record, summary, note, user_id, date_deadline=None,
                      key=None, anywhere=False):
        """Agenda un aviso del SGI sobre ``record``.

        Sin ``key`` se conserva el comportamiento anterior (una actividad por
        resumen y usuario): lo usan los avisos de eventos fuera de los crons.

        Con ``key`` (56.37.0, G-005) el aviso es uno por registro y clave:
        - si hay uno abierto del episodio, se actualiza (usuario, resumen,
          nota y fecha límite) y se cierran los repetidos;
        - si el del episodio ya se marcó como hecho, no se vuelve a crear
          (decisión 5 de la tanda 2);
        - las actividades de antes de 56.37.0 (sin clave, mismo resumen, de
          cualquier usuario) se adoptan en lugar de duplicarlas.
        ``anywhere=True`` busca la clave en todo el modelo, para los avisos
        cuyo registro ancla puede cambiar (NEWS, Mi procedimiento).
        ``date_deadline`` es la fecha real del compromiso (G-011); sin ella
        Odoo pone hoy."""
        if not user_id or not record:
            return False
        if not key:
            if self._sgi_activity_exists(record, summary, user_id):
                return False
            return record.activity_schedule(
                'mail.mail_activity_data_todo', date_deadline=date_deadline,
                summary=summary, note=note or '', user_id=user_id)
        todo = self.env.ref('mail.mail_activity_data_todo')
        Activity = self.env['mail.activity'].sudo().with_context(active_test=False)
        domain = [('res_model', '=', record._name), ('sgi_cron_key', '=', key),
                  ('sgi_episode_closed', '=', False)]
        if not anywhere:
            domain.append(('res_id', '=', record.id))
        episode = Activity.search(domain, order='id')
        if not episode:
            episode = Activity.search([
                ('res_model', '=', record._name), ('res_id', '=', record.id),
                ('sgi_cron_key', '=', False), ('summary', '=', summary),
                ('activity_type_id', '=', todo.id), ('active', '=', True)], order='id')
            # Si el mismo resumen le llegó a varios (empleado, RH, MAST), cada
            # papel adopta la suya; si no hay del mismo usuario (cambió el
            # Jefe MAST), se adoptan todas y se reasigna una.
            own = episode.filtered(lambda a: a.user_id.id == user_id)
            episode = own or episode
            if episode:
                episode.write({'sgi_cron_key': key})
        run = self.env.context.get('sgi_cron_run')
        if episode:
            if run:
                episode.filtered(lambda a: a.sgi_cron_run != run).write({'sgi_cron_run': run})
            current = episode.filtered('active')
            if not current:
                return False
            keep = current[:1]
            self._sgi_close_activities(current[1:], "aviso repetido")
            self._sgi_refresh_activity(keep, summary, note, user_id, date_deadline)
            return keep
        activity = record.activity_schedule(
            'mail.mail_activity_data_todo', date_deadline=date_deadline,
            summary=summary, note=note or '', user_id=user_id)
        activity.sudo().write({'sgi_cron_key': key, 'sgi_cron_run': run or False})
        return activity

    @api.model
    def _sgi_refresh_activity(self, activity, summary, note, user_id, date_deadline=None):
        """G-005 (a) y G-025: el aviso abierto sigue al destinatario y al dato
        de hoy. Solo escribe lo que cambió. La reasignación no manda correo
        (mail_activity_quick_update): el aviso aparece en la bandeja del
        nuevo responsable."""
        vals = {}
        if user_id and activity.user_id.id != user_id:
            vals['user_id'] = user_id
        if summary and activity.summary != summary:
            vals['summary'] = summary
        if _sgi_plain(activity.note) != _sgi_plain(note):
            vals['note'] = note or ''
        if date_deadline and activity.date_deadline != date_deadline:
            vals['date_deadline'] = date_deadline
        if vals:
            activity.sudo().with_context(mail_activity_quick_update=True).write(vals)
        return bool(vals)

    @api.model
    def _sgi_close_activities(self, activities, reason):
        """Cierra avisos con nota (``action_feedback``, que los archiva; nunca
        unlink) y cierra su episodio. Cada uno en su savepoint: un aviso que
        truena no revierte los demás (G-004: el paso completo fallaba en
        silencio). Devuelve cuántos estaban abiertos y se cerraron."""
        closed = 0
        for activity in activities.sudo():
            try:
                with self.env.cr.savepoint():
                    was_open = activity.active
                    if was_open:
                        activity.action_feedback(feedback="Cerrada automáticamente: %s." % reason)
                    if activity.exists() and not activity.sgi_episode_closed:
                        activity.sgi_episode_closed = True
                    closed += int(was_open)
            except Exception:
                _logger.exception("SGI: no se pudo cerrar la actividad %s (%s); continúo.",
                                  activity.id, reason)
        return closed

    @api.model
    def _sgi_new_run(self):
        """Corrida con ficha propia: los avisos que toque quedan marcados con
        ella y ``_sgi_sweep`` cierra los que la corrida ya no vio."""
        return self.with_context(sgi_cron_run=uuid.uuid4().hex)

    @api.model
    def _sgi_sweep(self, kinds, reason, failures=0):
        """Cierre por episodio (G-004): los avisos de estas clases que la
        corrida actual no volvió a ver ya no aplican; se cierran con nota y
        se cierra su episodio. Si algún registro de la corrida falló, no se
        barre (no se sabe si su aviso sigue vigente)."""
        run = self.env.context.get('sgi_cron_run')
        if not run or failures:
            if failures:
                _logger.warning("SGI: %d registro(s) fallaron; no cierro avisos de %s en esta corrida.",
                                failures, ", ".join(kinds))
            return 0
        key_domain = []
        for kind in kinds:
            key_domain = (['|'] if key_domain else []) + key_domain + [
                '|', ('sgi_cron_key', '=', kind), ('sgi_cron_key', '=like', kind + ':%')]
        stale = self.env['mail.activity'].sudo().with_context(active_test=False).search(
            key_domain + [('sgi_episode_closed', '=', False),
                          '|', ('sgi_cron_run', '=', False), ('sgi_cron_run', '!=', run)])
        closed = self._sgi_close_activities(stale, reason)
        if closed:
            _logger.info("SGI: %d aviso(s) cerrados porque ya no aplican (%s).", closed, ", ".join(kinds))
        return closed

    def _sgi_first_user_id(self, group):
        """Primer usuario ACTIVO del grupo, por id (determinista). all_user_ids
        no garantiza orden: con 2+ usuarios en el grupo, el destinatario de los
        escalamientos cambiaba entre corridas y burlaba la deduplicación de
        actividades por usuario.

        56.7.0: primero los miembros DIRECTOS del grupo. Administrador y
        Dirección implican Jefe MAST, y por id el primero de all_user_ids era
        el CEO: todas las escalaciones sin dueño le llegaban a él y no a MAST."""
        if not group:
            return False
        for users in (group.user_ids, group.all_user_ids):
            users = users.filtered('active').sorted('id')
            if users:
                return users[:1].id
        return False

    def _sgi_manager_user_id(self):
        """Jefe MAST y SGI que recibe lo que no tiene dueño. Parámetro
        quimibond_sgi.mast_user_id (id de usuario) si está puesto; si no, el
        usuario activo con login ``mas@quimibond.com`` (56.37.0, I-003); si
        no existe, el primer miembro directo del grupo."""
        param = self.env['ir.config_parameter'].sudo().get_param('quimibond_sgi.mast_user_id')
        if param and param.isdigit():
            user = self.env['res.users'].sudo().browse(int(param)).exists()
            if user.active:
                return user.id
        by_login = self._sgi_mast_user_by_login()
        if by_login:
            return by_login.id
        group = self.env.ref('quimibond_sgi.group_sgi_manager', raise_if_not_found=False)
        return self._sgi_first_user_id(group)

    def _sgi_mast_user_by_login(self):
        return self.env['res.users'].sudo().search([('login', '=', SGI_MAST_LOGIN)], limit=1)

    def _sgi_director_user_id(self):
        group = self.env.ref('quimibond_sgi.group_sgi_director', raise_if_not_found=False)
        return self._sgi_first_user_id(group) or self._sgi_manager_user_id()

    def _sgi_sales_admin_user_id(self):
        """Administrador de ventas (P-A28): primer usuario del grupo Admin de ventas
        de Odoo; fallback al Jefe MAST/SGI."""
        group = self.env.ref('sales_team.group_sale_manager', raise_if_not_found=False)
        return self._sgi_first_user_id(group) or self._sgi_manager_user_id()

    # ------------------------------------------------------------------
    # Aislamiento de errores: un registro/paso envenenado no debe tumbar
    # (ni revertir) la corrida completa del cron. Es el patrón que ya
    # protege cron_measure_activities — puesto ahí después de que un solo
    # tropiezo tumbó la medición completa en producción.
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # Correo inmediato para eventos críticos: incidente grave/fatal, NC
    # mayor y equipo bloqueado por calibración. Todo lo demás sigue siendo
    # actividad — el correo es solo para quien no vive dentro de Odoo.
    # ------------------------------------------------------------------
    def _sgi_critical_mail_emails(self):
        """Correos de Jefe MAST + Dirección (deduplicados)."""
        users = self.env['res.users']
        for xmlid in ('quimibond_sgi.group_sgi_manager',
                      'quimibond_sgi.group_sgi_director'):
            group = self.env.ref(xmlid, raise_if_not_found=False)
            if group:
                users |= group.all_user_ids
        return ','.join(sorted({e for e in users.mapped('email') if e}))

    def _sgi_send_critical_mail(self, template_xmlid, record):
        """Envía el correo crítico SIN romper el flujo que lo dispara: un
        fallo de plantilla/SMTP se loggea y el negocio continúa."""
        template = self.env.ref(template_xmlid, raise_if_not_found=False)
        emails = self._sgi_critical_mail_emails()
        if not template or not emails or not record:
            return
        try:
            template.sudo().with_context(sgi_email_to=emails).send_mail(
                record.id)
        except Exception:
            _logger.exception(
                "SGI: falló el correo crítico %s para %s; continúo.",
                template_xmlid, record)

    def _sgi_step(self, label, func):
        """Ejecuta un paso independiente del cron en su propio savepoint:
        si truena, se registra y se continúa con el siguiente paso."""
        try:
            with self.env.cr.savepoint():
                func()
        except Exception:
            _logger.exception("SGI cron: falló el paso «%s»; continúo.", label)
            return False
        return True

    def _sgi_for_each(self, records, func, label):
        """Aplica func(record) con savepoint por registro: el registro que
        truena se revierte y se loggea, sin abortar el resto de la corrida.
        Devuelve cuántos registros fallaron (56.37.0: sin fallos se pueden
        cerrar los avisos que ya no aplican)."""
        failures = 0
        for record in records:
            try:
                with self.env.cr.savepoint():
                    func(record)
            except Exception:
                failures += 1
                _logger.exception(
                    "SGI cron (%s): falló el registro %s (id %s); continúo.",
                    label, record.display_name, record.id)
        return failures

    # ------------------------------------------------------------------
    # 56.37.0 (I-003): avisos de MAST que quedaron en otras bandejas.
    # Lo llama la migración 19.0.56.37.0; vive aquí para poder probarlo.
    # ------------------------------------------------------------------
    # (modelo, prefijo del resumen, clave que llevará el aviso que se queda)
    _SGI_MAST_NOTICES = (
        ('documents.document', "Procedimiento vivo cambió: ", 'procedimiento_vivo'),
        ('sgi.interested.party', "Revisar parte interesada: ", 'revisar_parte_interesada'),
        ('sgi.risk', "Riesgo alto sin acción: ", 'riesgo_alto_sin_accion'),
        ('sgi.risk', "Revisar riesgo ", 'revisar_riesgo'),
        ('sgi.process', "Eslabón atorado: ", False),
        ('survey.survey', "Revisar DNC y plan de capacitación", False),
    )

    @api.model
    def _sgi_backup_activities(self, table, activities):
        """Copia las filas de ``mail_activity`` a ``table`` antes de tocarlas
        (patrón de 56.31.0). Idempotente: no repite filas ya respaldadas."""
        if not activities:
            return 0
        self.env.flush_all()
        cr = self.env.cr
        cr.execute("CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM mail_activity WITH NO DATA" % table)
        cr.execute("INSERT INTO %s SELECT * FROM mail_activity WHERE id IN %%s "
                   "AND id NOT IN (SELECT id FROM %s)" % (table, table), (tuple(activities.ids),))
        return cr.rowcount

    @api.model
    def _sgi_notice_route_user(self, activity):
        """A quién le toca hoy el aviso, con las mismas reglas de los crons."""
        manager_id = self._sgi_manager_user_id()
        record = self.env[activity.res_model].sudo().with_context(
            active_test=False).browse(activity.res_id).exists()
        if not record:
            return manager_id
        if activity.res_model == 'documents.document':
            return record.sgi_owner_id.id or manager_id
        if activity.res_model == 'sgi.risk':
            owner = record.process_id.owner_id.user_id if record.sgi_process_active else False
            return owner.id if owner else manager_id
        if activity.res_model == 'sgi.process':
            # Con el proceso archivado el aviso va a MAST (misma regla que
            # la revisión de riesgos: el dueño del proceso sustituido ya no
            # responde por él).
            owner = record.owner_id.user_id if record.active else False
            return owner.id if owner else manager_id
        if activity.res_model == 'survey.survey':
            return self._sgi_rh_user_id()
        return manager_id

    @api.model
    def _sgi_migrate_mast_notices(self, from_user_ids, backup_table):
        """Reasigna a quien le tocan hoy (MAST, salvo la DNC que es de RH)
        los avisos de los crons que están con ``from_user_ids`` y deja uno
        por registro y resumen: los repetidos (de cualquier usuario) se
        cierran con nota. Respaldo previo en ``backup_table``. Devuelve
        {'closed': [ids], 'moved': {user_id: [ids]}}."""
        from_user_ids = list(from_user_ids)
        todo = self.env.ref('mail.mail_activity_data_todo')
        Activity = self.env['mail.activity'].sudo()
        result = {'closed': [], 'moved': defaultdict(list)}
        for model, prefix, key in self._SGI_MAST_NOTICES:
            seeds = Activity.search([
                ('user_id', 'in', from_user_ids), ('res_model', '=', model),
                ('summary', '=like', prefix + '%'), ('activity_type_id', '=', todo.id)])
            if not seeds:
                continue
            siblings = Activity.search([
                ('res_model', '=', model), ('res_id', 'in', seeds.mapped('res_id')),
                ('summary', 'in', list(set(seeds.mapped('summary')))),
                ('activity_type_id', '=', todo.id)], order='id')
            groups = defaultdict(lambda: Activity.browse())
            for activity in siblings:
                groups[(activity.res_id, activity.summary)] |= activity
            to_close, moves = Activity.browse(), []
            for acts in groups.values():
                dest = self._sgi_notice_route_user(acts[:1])
                # Solo se tocan las de los usuarios de origen y las del
                # destinatario: las de terceros (p. ej. el dueño de un proceso
                # archivado) se quedan como están.
                acts = acts.filtered(lambda a: a.user_id.id in from_user_ids or a.user_id.id == dest)
                keep = acts.filtered(lambda a: a.user_id.id == dest)[:1] or acts[:1]
                to_close |= acts - keep
                if dest and keep.user_id.id != dest:
                    moves.append((keep, dest))
                elif key and keep.sgi_cron_key != key:
                    moves.append((keep, False))
            self._sgi_backup_activities(backup_table, to_close | Activity.browse(
                [keep.id for keep, _dest in moves]))
            for keep, dest in moves:
                vals = {'sgi_cron_key': key} if key else {}
                if dest:
                    vals['user_id'] = dest
                    result['moved'][dest].append(keep.id)
                keep.with_context(mail_activity_quick_update=True).write(vals)
            result['closed'] += to_close.ids
            self._sgi_close_activities(to_close, "aviso repetido (migración 56.37.0)")
        result['moved'] = dict(result['moved'])
        return result

    # ------------------------------------------------------------------
    # 1. Cron diario — No Conformidades
    # ------------------------------------------------------------------
    @api.model
    def cron_nonconformities(self):
        sgi_require_system(self.env)  # F-008
        self = self._sgi_new_run()  # 56.37.0: ficha de la corrida para el cierre por episodio
        today = fields.Date.context_today(self)
        Param = self.env['ir.config_parameter'].sudo()
        default_days = int(Param.get_param('quimibond_sgi.nc_escalation_days', 5))
        external_days = int(Param.get_param('quimibond_sgi.nc_escalation_days_external', 3))
        try:
            effectiveness_days = int(Param.get_param('quimibond_sgi.nc_effectiveness_days', 90) or 90)
        except (TypeError, ValueError):
            effectiveness_days = 90

        # 56.7.0: actividades que agendaron los crons y cuya causa ya se
        # resolvió (captura, eslabón que fluye, riesgo con acción…). Va en
        # este cron porque corre TODOS los días; el de indicadores solo mide
        # el tercer día hábil.
        self._sgi_step("cerrar actividades ya resueltas", self._sgi_close_resolved_activities)
        # Marca acciones vencidas (recomputo del store)
        self._sgi_step(
            "recomputar estado de acciones",
            lambda: self._sgi_refresh_action_states(
                self.env['sgi.action.line'].search([('date_done', '=', False)])))

        open_alerts = self.env['quality.alert'].search([
            ('team_id.sgi_sequence_id', '!=', False),
            ('stage_id.sgi_is_closing_stage', '=', False),
            ('stage_id.sgi_is_cancel_stage', '=', False),
        ])

        def _process(alert):
            # NC-1 (49.0.0): plazos por etapa (contención / causa raíz / plan)
            # con aviso el día que vencen y escalamiento al dueño del proceso
            # y a MAST. Las NC viejas sin plazos los reciben aquí.
            if not alert.sgi_due_plan:
                alert._sgi_set_deadlines()
            alert._sgi_deadline_escalation(today)
            alert._sgi_supplier_escalation(today)  # NC-6
            days = external_days if alert.sgi_origin_type in (
                'auditoria_externa', 'reclamacion', 'scorecard') else default_days
            deadline = fields.Datetime.to_datetime(alert.create_date).date() + relativedelta(days=days)
            no_action = not alert.sgi_action_line_ids.filtered(lambda l: l.progress != '0')

            # Escalamiento por inacción
            if no_action and today >= deadline:
                user_id = alert.user_id.id or self._sgi_manager_user_id()
                self._sgi_schedule(
                    alert,
                    "NC sin acción: %s" % (alert.sgi_folio or alert.name),
                    "La NC lleva más de %d días abierta sin acción registrada." % days,
                    user_id, date_deadline=deadline, key='nc_sin_accion')

            # Verificación de eficacia pendiente. G-010 (56.37.0): al terminar
            # la última correctiva, la NC ya agenda «Verificar eficacia de la
            # NC X (a N días)» con su fecha. El cron solo avisa cuando esa
            # fecha ya llegó (o si la NC no la tiene), con el MISMO resumen:
            # antes agendaba otro «Verificar eficacia: X» que vencía hoy.
            all_done = alert.sgi_action_line_ids and all(l.date_done for l in alert.sgi_action_line_ids)
            due = alert.sgi_effectiveness_due
            if all_done and not alert.sgi_effectiveness_date and (not due or due <= today):
                user_id = alert.sgi_effectiveness_by.id or alert.user_id.id or self._sgi_manager_user_id()
                folio = alert.sgi_folio or alert.name
                summary = ("Verificar eficacia de la NC %s (a %d días)" % (folio, effectiveness_days)
                           if due else "Verificar eficacia: %s" % folio)
                self._sgi_schedule(
                    alert, summary,
                    "Todas las acciones terminaron; falta registrar la verificación de eficacia.",
                    user_id, date_deadline=due or today, key='nc_verificar_eficacia')

        failures = self._sgi_for_each(open_alerts, _process, "seguimiento de NC")
        self._sgi_sweep(['nc_sin_accion', 'nc_verificar_eficacia'],
                        "la NC ya tiene acción, ya se verificó o cerró", failures)
        return True

    # ------------------------------------------------------------------
    # 1b. Cron diario — Escalamiento de acciones vencidas (H12)
    # ------------------------------------------------------------------
    @api.model
    def cron_overdue_actions(self):
        """Escalamiento en 3 niveles de acciones vencidas:
        - nivel 1 (responsable): ya lo recuerda la actividad espejo (Ola 0);
        - > N días: además su jefe directo (employee_id.parent_id.user_id,
          fallback Jefe MAST);
        - > M días: además Dirección (group_sgi_director).
        Idempotente por nivel: el resumen difiere por nivel, así que no duplica
        actividades ya agendadas del mismo nivel."""
        sgi_require_system(self.env)  # F-008
        self = self._sgi_new_run()  # 56.37.0: cierre por episodio
        today = fields.Date.context_today(self)
        Param = self.env['ir.config_parameter'].sudo()
        d_mgr = int(Param.get_param('quimibond_sgi.action_escalation_manager_days', 7))
        d_dir = int(Param.get_param('quimibond_sgi.action_escalation_director_days', 15))
        Line = self.env['sgi.action.line']
        overdue = Line.search([('date_done', '=', False), ('date_commit', '<', today)])
        self._sgi_step("recomputar estado de acciones vencidas",
                       lambda: self._sgi_refresh_action_states(overdue))  # asegura 'vencida'
        manager_fallback = self._sgi_manager_user_id()
        director_id = self._sgi_director_user_id()

        def _process(line):
            origin = line._sgi_origin()
            if not origin:
                return
            days = (today - line.date_commit).days
            who = line.responsible_id.display_name or '-'
            if days > d_mgr:
                boss = line.responsible_id.employee_id.parent_id.user_id
                self._sgi_schedule(
                    origin,
                    "Acción vencida (+%dd) escalada al jefe: %s" % (d_mgr, line.name),
                    "La acción de %s lleva %d días vencida (compromiso %s); se "
                    "escala a su jefe directo." % (who, days, line.date_commit),
                    boss.id or manager_fallback, date_deadline=today,
                    key='accion_vencida_jefe:%d' % line.id)
            if days > d_dir:
                self._sgi_schedule(
                    origin,
                    "Acción vencida (+%dd) escalada a Dirección: %s" % (d_dir, line.name),
                    "La acción de %s lleva %d días vencida (compromiso %s); se "
                    "escala a Dirección." % (who, days, line.date_commit),
                    director_id, date_deadline=today,
                    key='accion_vencida_direccion:%d' % line.id)

        failures = self._sgi_for_each(overdue, _process, "escalamiento de acciones")
        self._sgi_sweep(['accion_vencida_jefe', 'accion_vencida_direccion'],
                        "la acción ya se terminó o se reprogramó", failures)
        return True

    # ------------------------------------------------------------------
    # 2. Cron diario — Documentos
    # ------------------------------------------------------------------
    @api.model
    def cron_documents(self):
        sgi_require_system(self.env)  # F-008
        self = self._sgi_new_run()  # 56.37.0: cierre por episodio
        today = fields.Date.context_today(self)
        Doc = self.env['documents.document']
        Param = self.env['ir.config_parameter'].sudo()
        notice_days = int(Param.get_param('quimibond_sgi.doc_review_notice_days', 60))
        notice_final = int(Param.get_param('quimibond_sgi.doc_review_notice_days_final', 30))
        pilot_days = int(Param.get_param('quimibond_sgi.doc_pilot_notice_days', 7))
        ack_days = int(Param.get_param('quimibond_sgi.doc_ack_pending_days', 7))
        failures = 0

        # Revisión bienal: dos avisos configurables (por defecto 60 y 30 días).
        # Por RANGO (<=), no por igualdad exacta: con igualdad, un día sin
        # corrida del cron (mantenimiento, error previo) perdía el aviso de esa
        # cohorte para siempre. 56.37.0 (G-005): un solo aviso por documento
        # (clave «revision_bienal»); al entrar al segundo nivel el mismo aviso
        # cambia de resumen, y vence el día de la revisión (G-011).
        offsets = sorted({notice_days, notice_final})
        docs = Doc.search([
            ('sgi_state', '=', 'vigente'),
            ('sgi_next_review_date', '!=', False),
            ('sgi_next_review_date', '<=', today + relativedelta(days=offsets[-1])),
        ])

        def _review_notice(doc):
            offset = next(o for o in offsets
                          if doc.sgi_next_review_date <= today + relativedelta(days=o))
            self._sgi_schedule(
                doc,
                "Revisión bienal (aviso %d días): %s" % (offset, doc.sgi_code or doc.name),
                "El documento requiere revisión antes del %s." % doc.sgi_next_review_date,
                doc.sgi_owner_id.id or self._sgi_manager_user_id(),
                date_deadline=doc.sgi_next_review_date, key='revision_bienal')

        failures += self._sgi_for_each(docs, _review_notice, "aviso de revisión bienal")

        # Pilotos que vencen (aviso configurable, por defecto 7 días). También
        # por rango: un piloto ya vencido sin cerrar sigue mereciendo su aviso.
        pilot_target = today + relativedelta(days=pilot_days)
        pilots = Doc.search([
            ('sgi_state', '=', 'piloto'),
            ('sgi_pilot_end_date', '!=', False),
            ('sgi_pilot_end_date', '<=', pilot_target),
        ])

        def _pilot_notice(doc):
            self._sgi_schedule(
                doc,
                "Piloto por vencer: %s" % (doc.sgi_code or doc.name),
                "La prueba piloto vence el %s." % doc.sgi_pilot_end_date,
                doc.sgi_owner_id.id or self._sgi_manager_user_id(),
                date_deadline=doc.sgi_pilot_end_date, key='piloto_por_vencer')

        failures += self._sgi_for_each(pilots, _pilot_notice, "aviso de piloto")

        # Acuses pendientes (umbral configurable, por defecto 7 días).
        limit_date = fields.Datetime.now() - relativedelta(days=ack_days)
        acks = self.env['sgi.document.ack'].search([
            ('state', '=', 'pendiente'),
            ('create_date', '<=', limit_date),
        ])
        manager_id = self._sgi_manager_user_id()

        def _ack_notice(ack):
            user_id = ack.user_id.id or manager_id
            self._sgi_schedule(
                ack.document_id,
                "Acuse pendiente: %s" % (ack.employee_id.name),
                "El acuse de lectura lleva más de %d días pendiente." % ack_days,
                user_id, date_deadline=today, key='acuse_pendiente:%d' % ack.id)

        failures += self._sgi_for_each(acks, _ack_notice, "acuses pendientes")
        self._sgi_sweep(['revision_bienal', 'piloto_por_vencer', 'acuse_pendiente'],
                        "el documento ya se revisó, el piloto cerró o el acuse se dio", failures)
        return True

    # ------------------------------------------------------------------
    # 3. Cron mensual — NEWS (F-P-G01-16)
    # ------------------------------------------------------------------
    @api.model
    def cron_news(self):
        sgi_require_system(self.env)  # F-008
        today = fields.Date.context_today(self)
        first_this_month = today.replace(day=1)
        first_prev_month = first_this_month - relativedelta(months=1)

        requests = self.env['approval.request'].search([
            ('sgi_is_doc_change', '=', True),
            ('request_status', '=', 'approved'),
            ('sgi_applied', '=', True),
            ('write_date', '>=', fields.Datetime.to_datetime(first_prev_month)),
            ('write_date', '<', fields.Datetime.to_datetime(first_this_month)),
        ])
        if not requests:
            return True

        report = self.env.ref('quimibond_sgi.action_report_news', raise_if_not_found=False)
        manager_id = self._sgi_manager_user_id()
        # La actividad se agenda sobre una approval.request (tiene mail.activity.mixin);
        # res.company NO hereda el mixin.
        anchor = requests[:1]
        if not (report and anchor):
            return True
        # G-012 (56.37.0): idempotente. Un boletín por mes: si su aviso ya
        # existe (abierto o hecho) no se vuelve a generar; el PDF del mes se
        # reutiliza si ya está adjunto. El render va en su savepoint.
        key = 'news:%s' % first_prev_month.strftime('%Y-%m')
        if self.env['mail.activity'].sudo().with_context(active_test=False).search_count(
                [('res_model', '=', 'approval.request'), ('sgi_cron_key', '=', key)]):
            return True
        filename = "NEWS_%s.pdf" % first_prev_month.strftime('%Y_%m')
        Attachment = self.env['ir.attachment'].sudo()
        attachment = Attachment.search([('res_model', '=', 'approval.request'),
                                        ('name', '=', filename)], limit=1)

        def _render():
            pdf_content, _ = self.env['ir.actions.report']._render_qweb_pdf(
                report.report_name, requests.ids)
            return Attachment.create({
                'name': filename,
                'type': 'binary',
                'datas': base64.b64encode(pdf_content),
                'res_model': 'approval.request',
                'res_id': anchor.id,
                'mimetype': 'application/pdf',
            })
        if not attachment:
            rendered = []
            self._sgi_step("render del boletín NEWS", lambda: rendered.append(_render()))
            attachment = rendered and rendered[0] or Attachment
        if manager_id and attachment:
            self._sgi_schedule(
                anchor, "Revisar y difundir NEWS %s" % first_prev_month.strftime('%m/%Y'),
                "Boletín de cambios documentales aprobados del mes (adjunto ID %d)." % attachment.id,
                manager_id, date_deadline=today + relativedelta(days=7), key=key, anywhere=True)
        return True

    # ------------------------------------------------------------------
    # 4. Cron mensual — Indicadores (F-P-A10-03)
    # ------------------------------------------------------------------
    @api.model
    def cron_indicators(self):
        """Cron mensual: mide los indicadores de frecuencia mensual del mes anterior.

        Los tres bloques (mediciones, cierre de mes de presupuestos, refresco de
        la foto) son independientes y van cada uno en su savepoint: antes, una
        excepción en el refresco de UN presupuesto revertía TODAS las mediciones
        del mes y sus NCs — y al ser mensual, el mes quedaba sin medir hasta una
        corrida manual."""
        sgi_require_system(self.env)  # F-008
        today = fields.Date.context_today(self)
        first_this = today.replace(day=1)
        first_prev = first_this - relativedelta(months=1)
        last_prev = first_this - relativedelta(days=1)
        deadline = first_this + relativedelta(days=4)
        indicators = self.env['sgi.indicator'].search([('frequency', '=', 'monthly')])
        self._sgi_step(
            "foto del valor del inventario (S3-04)",
            lambda: self.env['sgi.inventory.value']._sgi_snapshot(last_prev))
        self._sgi_step(
            "trayectorias faltantes",
            lambda: self.env['sgi.indicator'].cron_missing_trajectories())
        self._sgi_step(
            "mediciones mensuales",
            lambda: self._sgi_generate_measures(
                indicators, first_prev, first_prev, last_prev, deadline,
                "%s" % first_prev.strftime('%m/%Y')))
        self._sgi_step(
            "cierre de mes de presupuestos",
            lambda: self._sgi_sales_budget_month_close(first_prev, last_prev))
        # Refresca la foto de facturado/pedido de los presupuestos vigentes.
        self._sgi_step(
            "refresco de foto de presupuestos",
            lambda: self.env['sgi.sales.budget'].search(
                [('state', '!=', 'obsoleto')]).action_refresh_actuals())
        return True

    @api.model
    def _sgi_team_net_invoiced(self, team, date_from, date_to):
        """Facturación neta (out_invoice − out_refund, sin impuestos, moneda
        compañía) de un equipo en el rango de fechas de factura."""
        moves = self.env['account.move'].search([
            ('move_type', 'in', ('out_invoice', 'out_refund')),
            ('state', '=', 'posted'),
            ('company_id', '=', self.env['sgi.indicator']._sgi_kpi_company().id),
            ('team_id', '=', team.id),
            ('invoice_date', '>=', date_from), ('invoice_date', '<=', date_to),
        ])
        return sum(moves.mapped('amount_untaxed_signed'))

    @api.model
    def cron_forecast_coverage(self):
        """Cron semanal (lunes): por cada pronóstico vigente/revisado, agrupa las
        líneas descubiertas EN HORIZONTE y los pedidos fuera de pronóstico, y crea
        UNA actividad al coordinador con el resumen. Idempotente (dedup por
        resumen). No aplica a presupuestos (P-A28 4.2.2.7)."""
        sgi_require_system(self.env)  # F-008
        self = self._sgi_new_run()  # 56.37.0: cierre por episodio

        def _fmt(value):
            return '{:,.0f}'.format(value or 0)
        Budget = self.env['sgi.sales.budget']
        Line = self.env['sgi.sales.budget.line']

        def _process(budget):
            budget.action_refresh_actuals()  # cobertura con el horizonte de hoy
            uncovered = budget.line_ids.filtered(
                lambda l: l.coverage_state in ('sin_pedido', 'parcial'))
            orphans = budget._sgi_orders_without_forecast()
            if not uncovered and not orphans:
                return
            user_id = budget.team_id.user_id.id or self._sgi_sales_admin_user_id()
            if not user_id:
                return
            parts = []
            if uncovered:
                rows = ["%s · %s: pronosticado %s, comprometido %s, faltante %s %s" % (
                    l.product_id.default_code or l.product_id.name, l.date,
                    _fmt(l.qty_budget), _fmt(l.qty_real),
                    _fmt(l.qty_budget - l.qty_real), l.uom_id.name or '')
                    for l in uncovered.sorted(
                        lambda x: (x.date, x.product_id.default_code or ''))]
                parts.append("Semanas descubiertas:\n- " + "\n- ".join(rows))
            if orphans:
                rows = ["Pedido sin pronóstico: %s · %s (%s) — agrégalo al pronóstico" % (
                    sol.product_id.default_code or sol.product_id.name,
                    Line._sgi_effective_monday(sol.order_id), sol.order_id.name)
                    for sol in orphans.sorted(
                        lambda s: s.product_id.default_code or '')]
                parts.append("Pedidos fuera de pronóstico:\n- " + "\n- ".join(rows))
            self._sgi_schedule(
                budget,
                "Cobertura del pronóstico %s (P-A28 4.2.2.7)" % (
                    budget.partner_id.name or budget.name),
                "\n\n".join(parts), user_id,
                date_deadline=fields.Date.context_today(self) + relativedelta(days=7),
                key='cobertura_pronostico')

        budgets = Budget.search([('kind', '=', 'pronostico'),
                                 ('state', '!=', 'obsoleto')])
        failures = self._sgi_for_each(budgets, _process, "cobertura del pronóstico")
        self._sgi_sweep(['cobertura_pronostico'], "el pronóstico ya está cubierto", failures)
        return True

    @api.model
    def _sgi_sales_budget_month_close(self, first_prev, last_prev):
        """Aviso de cierre de mes: por cada equipo con presupuesto aprobado del
        año, si el acumulado facturado del año va por debajo del umbral del
        presupuesto acumulado (sales_budget_alert_pct), agenda una actividad al
        responsable del equipo. Idempotente (dedup por resumen)."""
        Param = self.env['ir.config_parameter'].sudo()
        pct = float(Param.get_param('quimibond_sgi.sales_budget_alert_pct', 80) or 0)
        # Umbral de justificación (P-A28 4.3.6.1): puede diferir del de aviso.
        min_pct = float(Param.get_param('quimibond_sgi.budget_fulfillment_min', 80) or 0)
        sales_admin_id = self._sgi_sales_admin_user_id()
        year = first_prev.year
        year_start = first_prev.replace(month=1, day=1)
        budgets = self.env['sgi.sales.budget'].search([
            ('kind', '=', 'presupuesto'),
            ('state', '=', 'aprobado'), ('year', '=', year)])

        def _process(budget):
            budgeted = sum(budget.line_ids.filtered(
                lambda l: l.date and l.date <= last_prev).mapped('amount_budget'))
            if not budgeted:
                return
            real = self._sgi_team_net_invoiced(budget.team_id, year_start, last_prev)
            achieved = real / budgeted * 100.0
            # Justificación del incumplimiento (P-A28 4.3.6.1): si va por debajo del
            # mínimo y AÚN no hay justificación capturada, se pide al Admin de ventas.
            # No bloquea nada: es evidencia del análisis.
            if achieved < min_pct and not (budget.nonfulfillment_note or '').strip() \
                    and sales_admin_id:
                self._sgi_schedule(
                    budget,
                    "Justificar incumplimiento del presupuesto %s (%.0f%%) — "
                    "P-A28 4.3.6.1" % (budget.team_id.name, achieved),
                    "El presupuesto va en %.1f%% (mínimo %.0f%%) y no tiene "
                    "justificación. Captura el análisis del incumplimiento en el "
                    "campo «Justificación de incumplimiento»." % (achieved, min_pct),
                    sales_admin_id, date_deadline=last_prev + relativedelta(days=15),
                    key='presupuesto_justificar:%d' % year)
            if achieved >= pct:
                return
            user = budget.team_id.user_id
            if not user:
                return
            top = self._sgi_budget_top_gaps(budget, last_prev)
            note = ("El acumulado facturado del equipo va en %.1f%% del presupuesto "
                    "aprobado del año. Revisa el pipeline y las acciones "
                    "comerciales." % achieved)
            if top:
                note += "\nProductos con mayor brecha (ppto − facturado):\n- " + \
                    "\n- ".join(top)
            self._sgi_schedule(
                budget,
                "Presupuesto %s por debajo del %.0f%% al cierre de %s" % (
                    budget.team_id.name, pct, first_prev.strftime('%m/%Y')),
                note, user.id, date_deadline=last_prev + relativedelta(days=15),
                key='presupuesto_bajo:%s' % first_prev.strftime('%Y-%m'))

        self._sgi_for_each(budgets, _process, "cierre de mes de presupuesto")
        return True

    @api.model
    def _sgi_budget_top_gaps(self, budget, last_prev, limit=5):
        """Los productos con mayor brecha (amount_budget − amount_real) del
        presupuesto hasta el mes (accionable: dónde se está quedando corto)."""
        gaps = defaultdict(lambda: [0.0, 0.0])  # product -> [ppto, real]
        for line in budget.line_ids.filtered(
                lambda l: l.date and l.date <= last_prev):
            gaps[line.product_id][0] += line.amount_budget
            gaps[line.product_id][1] += line.amount_real
        ranked = sorted(
            ((product, vals[0] - vals[1]) for product, vals in gaps.items()),
            key=lambda kv: kv[1], reverse=True)
        currency = budget.currency_id
        out = []
        for product, gap in ranked[:limit]:
            if gap <= 0:
                break
            out.append("%s: %s %s" % (
                product.default_code or product.name,
                '{:,.0f}'.format(gap), currency.name or ''))
        return out

    # ------------------------------------------------------------------
    # 4b. Cron anual (junio) — Revaluación del S2 (P-A28 Nota 1)
    # ------------------------------------------------------------------
    @api.model
    def cron_budget_revaluation(self):
        """P-A28 Nota 1: en JUNIO, pide al Admin de ventas revaluar las cantidades
        del segundo semestre de cada presupuesto aprobado del año en curso. Corre
        anual pero se protege con la guarda de mes (idempotente el resto del año)."""
        sgi_require_system(self.env)  # F-008
        if fields.Date.context_today(self).month != 6:
            return True
        return self._sgi_sales_budget_revaluation(
            fields.Date.context_today(self).year)

    @api.model
    def _sgi_sales_budget_revaluation(self, year):
        """Agenda al Admin de ventas una actividad de revaluación del S2 por cada
        presupuesto aprobado del año (enlaza a la acción «Revisar (nueva Rev.)»).
        Idempotente (dedup por resumen)."""
        sales_admin_id = self._sgi_sales_admin_user_id()
        if not sales_admin_id:
            return True
        budgets = self.env['sgi.sales.budget'].search([
            ('kind', '=', 'presupuesto'),
            ('state', '=', 'aprobado'), ('year', '=', year)])

        def _process(budget):
            self._sgi_schedule(
                budget,
                "Revaluar cantidades del S2 — P-A28 Nota 1",
                "Revaluación de mitad de año (P-A28 Nota 1): revisa y ajusta las "
                "cantidades del segundo semestre del presupuesto %s. Si cambian, "
                "genera la siguiente revisión con «Revisar (nueva Rev.)»." % (
                    budget.folio or budget.name),
                sales_admin_id, date_deadline=date(year, 6, 30),
                key='revaluar_s2:%d' % year)

        self._sgi_for_each(budgets, _process, "revaluación del S2")
        return True

    @api.model
    def cron_indicators_weekly(self):
        """Cron semanal: mide los indicadores de frecuencia semanal de la semana previa."""
        sgi_require_system(self.env)  # F-008
        today = fields.Date.context_today(self)
        this_monday = today - relativedelta(days=today.weekday())
        prev_monday = this_monday - relativedelta(days=7)
        prev_sunday = this_monday - relativedelta(days=1)
        deadline = this_monday + relativedelta(days=2)
        indicators = self.env['sgi.indicator'].search([('frequency', '=', 'weekly')])
        self._sgi_generate_measures(
            indicators, prev_monday, prev_monday, prev_sunday, deadline,
            "semana del %s" % prev_monday.strftime('%d/%m/%Y'))
        return True

    @api.model
    def _sgi_close_resolved_activities(self):
        """56.7.0: las actividades que agendan los crons se marcan hechas
        solas cuando su causa se resolvió. Antes solo las NC se cerraban y se
        acumulaban vencidas (59 al 28-sep-2026): capturar la medición, que el
        eslabón vuelva a fluir o registrar la acción del riesgo no las cerraba.

        56.37.0 (G-004): cada tipo va en su propio paso y cada aviso en su
        savepoint (antes una sola falla revertía todo el cierre y en
        producción no se veía efecto). También cierra el episodio de los
        avisos con clave que ya se habían marcado como hechos, para que el
        aviso vuelva a nacer si la causa reaparece."""
        todo = self.env.ref('mail.mail_activity_data_todo')
        Activity = self.env['mail.activity'].sudo().with_context(active_test=False)
        today = fields.Date.context_today(self)
        closed = []

        def _open(model, prefix):
            return Activity.search([
                ('res_model', '=', model), ('summary', '=like', prefix + '%'),
                ('activity_type_id', '=', todo.id),
                '|', ('active', '=', True),
                '&', ('sgi_cron_key', '!=', False), ('sgi_episode_closed', '=', False)])

        def _close(activities, reason):
            closed.append(self._sgi_close_activities(activities, reason))

        def _capture():
            # Capturar indicador X (periodo). G-021/I-008 (56.37.0): con clave,
            # se cierra por SU medición; también si el indicador se archivó.
            # Mientras siga pendiente, sigue al responsable actual.
            acts = _open('sgi.indicator', "Capturar indicador ")
            Indicator = self.env['sgi.indicator'].sudo().with_context(active_test=False)
            Measure = self.env['sgi.indicator.measure'].sudo()
            keyed = acts.filtered(lambda a: (a.sgi_cron_key or '').startswith('capturar_indicador:'))
            legacy = acts - keyed
            pending = set(Measure.search([
                ('indicator_id', 'in', legacy.mapped('res_id')),
                ('state', '=', 'pendiente')]).indicator_id.ids)
            done = self.env['mail.activity']
            manager_id = self._sgi_manager_user_id()
            for act in keyed:
                indicator = Indicator.browse(act.res_id).exists()
                measure = Measure.browse(int(act.sgi_cron_key.split(':')[1])).exists()
                if not indicator.active or not measure or measure.state != 'pendiente':
                    done |= act
                elif act.active:
                    self._sgi_refresh_activity(
                        act, act.summary, act.note,
                        indicator.responsible_id.id or manager_id)
            for act in legacy:
                indicator = Indicator.browse(act.res_id).exists()
                if not indicator.active or act.res_id not in pending:
                    done |= act
            _close(done, "la medición ya se capturó o el indicador se archivó")

        def _nc():
            # NC sin acción: la NC ya tiene acción con avance, o ya cerró o se canceló.
            acts = _open('quality.alert', "NC sin acción: ")
            alerts = self.env['quality.alert'].sudo().browse(acts.mapped('res_id')).exists()
            still = set(alerts.filtered(
                lambda a: not (a.stage_id.sgi_is_closing_stage or a.stage_id.sgi_is_cancel_stage)
                and not a.sgi_action_line_ids.filtered(lambda line: line.progress != '0')).ids)
            _close(acts.filtered(lambda a: a.res_id not in still), "la NC ya tiene acción o cerró")

        def _procedure():
            # Procedimiento vivo cambió: ya se revisó (sin divergencia) o el
            # documento ya no está vigente. Duplicados: queda uno por
            # documento, sin importar a quién le llegó (56.37.0).
            acts = _open('documents.document', "Procedimiento vivo cambió: ")
            docs = self.env['documents.document'].sudo().browse(acts.mapped('res_id')).exists()
            dirty = set(docs.filtered(lambda d: d.sgi_procedure_dirty and d.sgi_state == 'vigente').ids)
            _close(acts.filtered(lambda a: a.res_id not in dirty), "el procedimiento ya se revisó")
            seen = set()
            duplicates = self.env['mail.activity']
            for activity in _open('documents.document', "Procedimiento vivo cambió: ").filtered(
                    'active').sorted('id'):
                key = (activity.res_id, activity.summary)
                if key in seen:
                    duplicates |= activity
                seen.add(key)
            _close(duplicates, "aviso repetido")

        def _calc():
            # Indicador X no calculó: ya calcula (o ya es manual, o se archivó).
            acts = _open('sgi.indicator', "Indicador ")
            acts = acts.filtered(lambda a: (a.summary or '').endswith(" no calculó"))
            indicators = self.env['sgi.indicator'].sudo().with_context(
                active_test=False).browse(acts.mapped('res_id')).exists()
            failing = set(indicators.filtered(
                lambda i: i.active and i.calc_status in ('error', 'sin_formula', 'sin_datos')).ids)
            _close(acts.filtered(lambda a: a.res_id not in failing), "el indicador ya calcula")

        def _chain():
            # Eslabón atorado: <eslabón>: ya ningún eslabón con ese nombre está atorado.
            prefix = "Eslabón atorado: "
            acts = _open('sgi.process', prefix)
            stuck = {(link.to_activity_id.process_id.id, link.name or '')
                     for link in self.env['sgi.activity.link'].sudo().search([('chain_state', '=', 'atorado')])}
            _close(acts.filtered(lambda a: (a.res_id, (a.summary or '')[len(prefix):]) not in stuck),
                   "el eslabón volvió a fluir")

        def _risks():
            # Riesgos: alto sin acción que ya tiene acción; revisión ya registrada.
            acts = _open('sgi.risk', "Riesgo alto sin acción: ")
            risks = self.env['sgi.risk'].sudo().browse(acts.mapped('res_id')).exists()
            still = set(risks.filtered('high_without_action').ids)
            _close(acts.filtered(lambda a: a.res_id not in still), "el riesgo ya tiene acción")
            acts = _open('sgi.risk', "Revisar riesgo ")
            risks = self.env['sgi.risk'].sudo().browse(acts.mapped('res_id')).exists()
            due = set(risks.filtered(lambda r: r.state != 'cerrado' and r.next_review_date
                                     and r.next_review_date <= today).ids)
            _close(acts.filtered(lambda a: a.res_id not in due), "la revisión ya se registró")

        def _parties():
            # Partes interesadas con revisión ya registrada.
            acts = _open('sgi.interested.party', "Revisar parte interesada: ")
            parties = self.env['sgi.interested.party'].sudo().browse(acts.mapped('res_id')).exists()
            due = set(parties.filtered(lambda p: p.next_review_date and p.next_review_date <= today).ids)
            _close(acts.filtered(lambda a: a.res_id not in due), "la revisión ya se registró")

        for label, func in (("captura de indicador", _capture), ("NC sin acción", _nc),
                            ("procedimiento vivo", _procedure), ("indicador no calculó", _calc),
                            ("eslabón atorado", _chain), ("riesgos", _risks),
                            ("partes interesadas", _parties)):
            self._sgi_step("cerrar avisos resueltos: %s" % label, func)
        total = sum(closed)
        if total:
            _logger.info("SGI: %s actividades cerradas porque su causa ya se resolvió.", total)
        return total

    # ------------------------------------------------------------------
    # G-024 (56.37.0): los estados guardados que dependen de «hoy» se
    # refrescan escribiendo SOLO los registros que cambian. Llamar el
    # compute a mano escribía todos cada día (write_date de los 148 equipos
    # de medición = hora del cron).
    # ------------------------------------------------------------------
    @api.model
    def _sgi_write_changed(self, records, fname, new_value):
        by_value = defaultdict(lambda: records.browse())
        for record in records:
            value = new_value(record)
            if record[fname] != value:
                by_value[value] |= record
        for value, changed in by_value.items():
            changed.write({fname: value})
        return sum(len(changed) for changed in by_value.values())

    @api.model
    def _sgi_refresh_action_states(self, lines):
        today = fields.Date.context_today(self)
        return self._sgi_write_changed(lines, 'state', lambda line: line._sgi_state_on(today))

    @api.model
    def _sgi_refresh_calibration_states(self, equipments):
        today = fields.Date.context_today(self)
        return self._sgi_write_changed(
            equipments, 'sgi_calibration_state', lambda eq: eq._sgi_calibration_state_on(today))

    @api.model
    def _sgi_generate_measures(self, indicators, period_date, date_from, date_to,
                               deadline, period_label):
        """Genera (idempotente) las mediciones del periodo y evalúa la NC
        automática. Cada indicador va en su savepoint: un cálculo que truene
        (fuente de datos rota) no deja sin medir a los demás."""
        Measure = self.env['sgi.indicator.measure']
        manager_id = self._sgi_manager_user_id()

        def _process(indicator):
            measure = Measure.search([
                ('indicator_id', '=', indicator.id),
                ('period_date', '=', period_date),
            ], limit=1)
            if not measure:
                # Antes de «medir desde» no hay dato confiable: no se crea.
                if not indicator._sgi_measurable_on(date_to):
                    indicator._sgi_set_calc('antes', "Mide desde el %s." % indicator.measure_from)
                    return
                # 6.1: el motivo queda en el indicador; si truena, el error
                # también (y avisa al responsable) en vez de perderse en el log.
                try:
                    with self.env.cr.savepoint():
                        measure_vals = indicator._sgi_measure_vals(date_from, date_to)
                except Exception as error:
                    indicator._sgi_set_calc('error', str(error)[:250])
                    self._sgi_calc_notice(indicator, 'error', str(error)[:250], manager_id)
                    return
                status, reason = indicator._sgi_calc_diagnose(measure_vals)
                indicator._sgi_set_calc(status, reason)
                if status in ('sin_formula', 'sin_datos'):
                    self._sgi_calc_notice(indicator, status, reason, manager_id)
                vals = dict(measure_vals, indicator_id=indicator.id, period_date=period_date)
                measure = Measure.create(vals)
                if measure.state == 'pendiente':
                    user_id = indicator.responsible_id.id or manager_id
                    if user_id:
                        # G-021 (56.37.0): la clave lleva la medición, así
                        # se cierra por medición y no por indicador.
                        self._sgi_schedule(
                            indicator,
                            "Capturar indicador %s (%s)" % (indicator.code, period_label),
                            "Registre el valor del indicador del periodo (%s) antes del %s." % (
                                period_label, deadline),
                            user_id, date_deadline=deadline,
                            key='capturar_indicador:%d' % measure.id)
            # NC automática (solo mediciones rojas validadas con nc_on_red)
            measure._sgi_maybe_create_nc()

        self._sgi_for_each(indicators, _process, "medición de indicadores")
        return True

    @api.model
    def _sgi_calc_notice(self, indicator, status, reason, manager_id):
        """Aviso al responsable (o a MAST) de que el indicador no calculó.
        Uno por indicador: se cierra solo cuando vuelve a calcular."""
        label = dict(indicator._fields['calc_status'].selection).get(status, status)
        self._sgi_schedule(
            indicator, "Indicador %s no calculó" % (indicator.code or indicator.name),
            "%s: %s" % (label, reason or ''), indicator.responsible_id.id or manager_id,
            date_deadline=fields.Date.context_today(self), key='indicador_no_calcula')

    @api.model
    def _sgi_schedule_deadline(self, anchor, measure, summary, note, user_id, deadline):
        """Agenda una actividad con fecha límite, evitando duplicados por medición."""
        if self._sgi_activity_exists(anchor, summary, user_id):
            return
        anchor.activity_schedule(
            'mail.mail_activity_data_todo',
            date_deadline=deadline,
            summary=summary, note=note or '', user_id=user_id)

    # ------------------------------------------------------------------
    # 5. Cron diario — Programa de auditorías
    # ------------------------------------------------------------------
    @api.model
    def cron_audit_program(self):
        sgi_require_system(self.env)  # F-008
        today = fields.Date.context_today(self)
        lines = self.env['sgi.audit.program.line'].search([
            ('state', '=', 'pendiente'),
            ('program_id.state', '=', 'aprobado'),
        ])
        manager_id = self._sgi_manager_user_id()

        def _process(line):
            month_start = fields.Date.to_date(
                '%s-%02d-01' % (line.program_id.year, int(line.planned_month)))
            notice_date = month_start - relativedelta(days=15)
            if notice_date <= today < month_start:
                user_id = line.lead_auditor_id.id or manager_id
                self._sgi_schedule(
                    line.program_id,
                    "Preparar auditoría de %s (%s/%s)" % (
                        line.process_id.name or 'proceso',
                        line.planned_month, line.program_id.year),
                    "La auditoría planificada inicia el mes próximo; cree la auditoría.",
                    user_id, date_deadline=month_start,
                    key='preparar_auditoria:%d' % line.id)

        self._sgi_for_each(lines, _process, "programa de auditorías")
        return True

    # ------------------------------------------------------------------
    # 6. Cron diario — Revisión de riesgos vencidos
    # ------------------------------------------------------------------
    @api.model
    def cron_risk_review(self):
        sgi_require_system(self.env)  # F-008
        self = self._sgi_new_run()  # 56.37.0: cierre por episodio
        today = fields.Date.context_today(self)
        risks = self.env['sgi.risk'].search([
            ('next_review_date', '!=', False),
            ('next_review_date', '<=', today),
            ('state', '!=', 'cerrado'),
        ])
        manager_id = self._sgi_manager_user_id()

        def _process(risk):
            # Con el proceso archivado, la revisión va al Jefe MAST (el dueño
            # del proceso sustituido ya no responde por él).
            owner = risk.process_id.owner_id.user_id if risk.sgi_process_active else False
            user_id = owner.id if owner else manager_id
            self._sgi_schedule(
                risk,
                "Revisar riesgo %s" % (risk.folio or risk.name),
                "Reevaluación periódica (enero / julio): la revisión del riesgo/oportunidad "
                "venció el %s. Actualiza probabilidad e impacto y pulsa «Registrar "
                "evaluación»." % risk.next_review_date,
                user_id, date_deadline=risk.next_review_date, key='revisar_riesgo')

        failures = self._sgi_for_each(risks, _process, "revisión de riesgos")
        # DIR-2: riesgo alto sin acción abierta → actividad al dueño del proceso.
        flagged = self.env['sgi.risk'].search([('high_without_action', '=', True)])

        def _flagged(risk):
            owner = risk.process_id.owner_id.user_id if risk.sgi_process_active else False
            self._sgi_schedule(
                risk, "Riesgo alto sin acción: %s" % (risk.folio or risk.name),
                "El riesgo está en atención alta o inmediata y no tiene ninguna acción de "
                "tratamiento abierta. Registra una acción con responsable y compromiso.",
                owner.id if owner else manager_id, date_deadline=today,
                key='riesgo_alto_sin_accion')

        failures += self._sgi_for_each(flagged, _flagged, "riesgos altos sin acción")
        self._sgi_sweep(['revisar_riesgo', 'riesgo_alto_sin_accion'],
                        "la revisión ya se registró o el riesgo ya tiene acción", failures)
        return True

    # ------------------------------------------------------------------
    # 7. Cron trimestral — Evaluación de proveedores
    # ------------------------------------------------------------------
    @api.model
    def cron_supplier_eval(self):
        sgi_require_system(self.env)  # F-008
        today = fields.Date.context_today(self)
        # Trimestre anterior
        current_q_start_month = ((today.month - 1) // 3) * 3 + 1
        current_q_start = today.replace(month=current_q_start_month, day=1)
        prev_q_end = current_q_start - relativedelta(days=1)
        prev_q_start = prev_q_end.replace(day=1) - relativedelta(months=2)
        dt_from = fields.Datetime.to_datetime(prev_q_start)
        dt_to = fields.Datetime.to_datetime(prev_q_end) + relativedelta(days=1)

        Eval = self.env['sgi.supplier.eval']
        pickings = self.env['stock.picking'].search([
            ('picking_type_id.code', '=', 'incoming'),
            ('state', '=', 'done'),
            ('date_done', '>=', dt_from), ('date_done', '<', dt_to),
            ('partner_id', '!=', False),
        ])
        # Solo proveedores críticos (materia prima y maquila): los marcados en
        # el contacto o los que en el periodo entregaron productos de las
        # categorías críticas (Ajustes → SGI → Categorías de proveedores críticos).
        critical_categs = self.env['sgi.supplier.eval']._sgi_critical_categ_ids()
        if critical_categs:
            critical_pickings = pickings.filtered(
                lambda p: p.partner_id.commercial_partner_id.sgi_supplier_critical
                or any(m.product_id.categ_id.id in critical_categs for m in p.move_ids))
        else:
            critical_pickings = pickings.filtered(lambda p: p.partner_id.commercial_partner_id.sgi_supplier_critical)
        partners = critical_pickings.mapped('partner_id.commercial_partner_id')
        purchase_user_id = self._sgi_purchase_user_id()

        def _process(partner):
            existing = Eval.search([
                ('partner_id', '=', partner.id),
                ('date_from', '=', prev_q_start),
                ('date_to', '=', prev_q_end),
            ], limit=1)
            if not existing:
                existing = Eval.create({
                    'partner_id': partner.id,
                    'date_from': prev_q_start,
                    'date_to': prev_q_end,
                })
            # Refresca las métricas antes de aplicar: pudieron llegar
            # recepciones o NCs después de creada la evaluación.
            existing.action_recompute()
            existing.action_apply_to_partner()
            if existing.supplier_class in ('condicionado', 'baja') and purchase_user_id:
                self._sgi_schedule(
                    existing.partner_id,
                    "Proveedor %s: %s" % (partner.name, existing.supplier_class),
                    "La evaluación trimestral dejó al proveedor como %s (calif. %s)." % (
                        existing.supplier_class, existing.score),
                    purchase_user_id, date_deadline=today + relativedelta(days=15),
                    key='proveedor_clase:%s' % prev_q_start.strftime('%Y-%m'))

        self._sgi_for_each(partners, _process, "evaluación de proveedores")
        return True

    def _sgi_purchase_user_id(self):
        group = self.env.ref('purchase.group_purchase_manager', raise_if_not_found=False)
        return self._sgi_first_user_id(group) or self._sgi_manager_user_id()

    def _sgi_rh_user_id(self):
        """Coordinador de RH (parametrizable); fallback al Jefe MAST."""
        user_id = int(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.rh_user_id', 0))
        if user_id and self.env['res.users'].browse(user_id).exists():
            return user_id
        return self._sgi_manager_user_id()

    # ------------------------------------------------------------------
    # 8. Cron diario — Calibraciones (P-C03)
    # ------------------------------------------------------------------
    @api.model
    def cron_calibrations(self):
        sgi_require_system(self.env)  # F-008
        self = self._sgi_new_run()  # 56.37.0: cierre por episodio
        today = fields.Date.context_today(self)
        Equipment = self.env['maintenance.equipment']
        manager_id = self._sgi_manager_user_id()

        # Recomputa el estado de calibración (store) antes de evaluar. G-024
        # (56.37.0): solo escribe los equipos cuyo estado cambió.
        measuring = Equipment.search([('sgi_is_measuring', '=', True)])
        self._sgi_step("recomputar estado de calibración",
                       lambda: self._sgi_refresh_calibration_states(measuring))

        # Por vencer (<= 30 días) y vencidos.
        def _calibration(eq):
            if not eq.sgi_next_calibration_date:
                return
            owner = eq.technician_user_id or eq.owner_user_id
            user_id = owner.id or manager_id
            if eq.sgi_calibration_state == 'por_vencer':
                self._sgi_schedule(
                    eq,
                    "Calibración por vencer: %s" % eq.name,
                    "El equipo vence su calibración el %s. Programe la calibración." % (
                        eq.sgi_next_calibration_date),
                    user_id, date_deadline=eq.sgi_next_calibration_date,
                    key='calibracion_por_vencer')
            elif eq.sgi_calibration_state == 'vencido':
                # Vencido: bloquear y avisar al Jefe MAST. El correo crítico
                # sale solo en la transición al bloqueo (no cada corrida).
                if not eq.sgi_do_not_use:
                    eq.sgi_do_not_use = True
                    self._sgi_send_critical_mail(
                        'quimibond_sgi.mail_template_sgi_calibration_blocked',
                        eq)
                self._sgi_schedule(
                    eq,
                    "Calibración VENCIDA: %s" % eq.name,
                    "El equipo tiene la calibración vencida desde el %s y quedó "
                    "bloqueado (No usar)." % eq.sgi_next_calibration_date,
                    manager_id or user_id, date_deadline=today, key='calibracion_vencida')

        failures = self._sgi_for_each(measuring, _calibration, "calibraciones")

        # EPP por vencer (P-S03).
        ppe = Equipment.search([
            ('sgi_is_ppe', '=', True),
            ('sgi_ppe_expiry_date', '!=', False),
        ])

        def _ppe(eq):
            if eq.sgi_ppe_expiry_date <= today + relativedelta(days=30):
                owner = eq.technician_user_id or eq.owner_user_id
                user_id = owner.id or manager_id
                self._sgi_schedule(
                    eq,
                    "EPP por vencer/vencido: %s" % eq.name,
                    "El EPP vence (o venció) el %s. Gestione su reposición." % (
                        eq.sgi_ppe_expiry_date),
                    user_id, date_deadline=eq.sgi_ppe_expiry_date, key='epp_por_vencer')

        failures += self._sgi_for_each(ppe, _ppe, "EPP")
        self._sgi_sweep(['calibracion_por_vencer', 'calibracion_vencida', 'epp_por_vencer'],
                        "el equipo ya se calibró o el EPP ya se repuso", failures)
        return True

    # ------------------------------------------------------------------
    # 9. Cron diario — Competencias, certificaciones y currículos (P-A01)
    # ------------------------------------------------------------------
    @api.model
    def cron_competences(self):
        sgi_require_system(self.env)  # F-008
        self = self._sgi_new_run()  # 56.37.0: cierre por episodio
        today = fields.Date.context_today(self)
        soon = today + relativedelta(days=30)
        manager_id = self._sgi_manager_user_id()
        rh_id = self._sgi_rh_user_id()

        # Certificaciones (hr.employee.skill de tipo certificación) con vigencia.
        certs = self.env['hr.employee.skill'].search([
            ('is_certification', '=', True),
            ('valid_to', '!=', False),
            ('valid_to', '<=', soon),
        ])

        def _cert(cert):
            employee = cert.employee_id
            emp_user_id = employee.user_id.id
            label = "%s — %s" % (employee.name, cert.skill_id.name or '')
            # 56.37.0: una clave por certificación y destinatario (empleado,
            # RH, MAST); si dos papeles son la misma persona, un solo aviso.
            if cert.valid_to < today:
                summary = "Certificación VENCIDA: %s" % label
                note = "La certificación venció el %s. Reprograme la recertificación." % cert.valid_to
                roles = (('empleado', emp_user_id), ('rh', rh_id), ('mast', manager_id))
                kind, deadline = 'certificacion_vencida', today
            else:
                summary = "Certificación por vencer: %s" % label
                note = "La certificación vence el %s (≤30 días)." % cert.valid_to
                roles = (('empleado', emp_user_id), ('rh', rh_id))
                kind, deadline = 'certificacion_por_vencer', cert.valid_to
            seen = set()
            for role, user_id in roles:
                if not user_id or user_id in seen:
                    continue
                seen.add(user_id)
                self._sgi_schedule(employee, summary, note, user_id, date_deadline=deadline,
                                   key='%s:%d:%s' % (kind, cert.id, role))

        failures = self._sgi_for_each(certs, _cert, "certificaciones")

        # Currículos / cursos con fecha de fin próxima (hr.resume.line).
        resume_lines = self.env['hr.resume.line'].search([
            ('date_end', '!=', False),
            ('date_end', '<=', soon),
            ('date_end', '>=', today),
        ])

        def _resume(line):
            employee = line.employee_id
            self._sgi_schedule(
                employee,
                "Formación por concluir: %s (%s)" % (line.name, employee.name),
                "La formación registrada concluye el %s." % line.date_end,
                employee.user_id.id or rh_id, date_deadline=line.date_end,
                key='formacion_por_concluir:%d' % line.id)

        failures += self._sgi_for_each(resume_lines, _resume, "formación por concluir")
        self._sgi_sweep(['certificacion_vencida', 'certificacion_por_vencer', 'formacion_por_concluir'],
                        "la certificación se renovó o la formación concluyó", failures)
        return True

    # ------------------------------------------------------------------
    # Fase 7 — Voz del cliente, DNC y emergencias
    # ------------------------------------------------------------------
    @api.model
    def _sgi_quarter_label(self, day):
        return "T%d %d" % ((day.month - 1) // 3 + 1, day.year)

    @api.model
    def _sgi_quarter_end(self, day):
        """Último día del trimestre de ``day`` (fecha límite de los avisos
        trimestrales, G-011)."""
        first = day.replace(month=((day.month - 1) // 3) * 3 + 1, day=1)
        return first + relativedelta(months=3, days=-1)

    @api.model
    def cron_satisfaction_survey(self):
        """Cron trimestral: recuerda al Admin de Ventas distribuir la Encuesta
        de Satisfacción del Cliente (9001 9.1.2). Las respuestas alimentan el
        KPI CA-02 automáticamente. No envía correos a clientes por sí solo:
        el envío es una acción humana desde la app Encuestas."""
        sgi_require_system(self.env)  # F-008
        survey = self.env['sgi.indicator']._sgi_satisfaction_survey()
        user_id = self._sgi_sales_admin_user_id()
        if not survey or not user_id:
            return True
        label = self._sgi_quarter_label(fields.Date.context_today(self))
        self._sgi_schedule(
            survey,
            "Enviar encuesta de satisfacción del cliente (%s)" % label,
            "Comparta la encuesta con los clientes activos desde la app "
            "Encuestas (botón Compartir). Las respuestas del periodo alimentan "
            "el KPI CA-02 (Satisfacción del cliente) automáticamente.",
            user_id, date_deadline=self._sgi_quarter_end(fields.Date.context_today(self)),
            key='encuesta_satisfaccion:%s' % label)
        return True

    @api.model
    def cron_dnc(self):
        """Cron trimestral: cierra el ciclo de la DNC (P-A01). Cuenta las
        brechas de competencia abiertas y agenda al coordinador de RH la
        distribución de la encuesta DNC (F-P-A01-17) y el plan de
        capacitación. Idempotente por trimestre."""
        sgi_require_system(self.env)  # F-008
        survey = self.env.ref('quimibond_sgi.sgi_survey_dnc',
                              raise_if_not_found=False)
        rh_id = self._sgi_rh_user_id()
        if not survey or not rh_id:
            return True
        gaps = self.env['sgi.competence.gap'].search_count([])
        label = self._sgi_quarter_label(fields.Date.context_today(self))
        self._sgi_schedule(
            survey,
            "Revisar DNC y plan de capacitación (%s)" % label,
            "Hay %d brecha(s) de competencia abiertas (%s). Distribuya la "
            "encuesta DNC (F-P-A01-17) desde la app Encuestas y arme el plan de "
            "capacitación del periodo." % (gaps, sgi_menu_path('brechas_competencia')),
            rh_id, date_deadline=self._sgi_quarter_end(fields.Date.context_today(self)),
            key='dnc:%s' % label)
        return True

    @api.model
    def cron_emergency_drills(self):
        """Cron diario: vigila los simulacros de los planes de emergencia
        vigentes (14001/45001 8.2). Idempotente por resumen."""
        sgi_require_system(self.env)  # F-008
        self = self._sgi_new_run()  # 56.37.0: cierre por episodio
        today = fields.Date.context_today(self)
        soon = today + relativedelta(days=30)
        manager_id = self._sgi_manager_user_id()
        plans = self.env['sgi.emergency.plan'].search([('state', '=', 'vigente')])

        def _plan(plan):
            user_id = plan.responsible_id.id or manager_id
            if not user_id:
                return
            if not plan.next_drill_date:
                self._sgi_schedule(
                    plan,
                    "Programar el primer simulacro: %s" % (plan.folio or plan.name),
                    "El plan de emergencia está vigente y no tiene ningún "
                    "simulacro realizado. Programe y ejecute el primero.",
                    user_id, date_deadline=today + relativedelta(days=30),
                    key='simulacro_programar')
            elif plan.next_drill_date < today:
                self._sgi_schedule(
                    plan,
                    "Simulacro VENCIDO: %s" % (plan.folio or plan.name),
                    "El simulacro venció el %s (frecuencia: cada %d meses)."
                    % (plan.next_drill_date, plan.drill_frequency_months or 12),
                    manager_id or user_id, date_deadline=plan.next_drill_date,
                    key='simulacro_vencido')
            elif plan.next_drill_date <= soon:
                self._sgi_schedule(
                    plan,
                    "Simulacro por vencer: %s" % (plan.folio or plan.name),
                    "El próximo simulacro vence el %s. Prográmelo."
                    % plan.next_drill_date,
                    user_id, date_deadline=plan.next_drill_date, key='simulacro_por_vencer')

        failures = self._sgi_for_each(plans, _plan, "planes de emergencia")

        overdue_drills = self.env['sgi.emergency.drill'].search([
            ('state', '=', 'programado'),
            ('date_planned', '<', today),
        ])

        def _drill(drill):
            user_id = drill.plan_id.responsible_id.id or manager_id
            if user_id:
                self._sgi_schedule(
                    drill,
                    "Simulacro no realizado: %s" % (drill.folio or ''),
                    "El simulacro estaba programado para el %s y sigue sin "
                    "realizarse." % drill.date_planned,
                    user_id, date_deadline=drill.date_planned, key='simulacro_no_realizado')

        failures += self._sgi_for_each(overdue_drills, _drill, "simulacros vencidos")
        self._sgi_sweep(['simulacro_programar', 'simulacro_vencido', 'simulacro_por_vencer',
                         'simulacro_no_realizado'],
                        "el simulacro ya se programó o se realizó", failures)
        return True

    # ------------------------------------------------------------------
    # Fase 8 — Señales operativas: lo que Odoo ya registra alimenta la
    # mejora continua sin captura adicional.
    # ------------------------------------------------------------------
    @api.model
    def cron_operational_signals(self):
        """Cron diario. (a) Falla repetitiva: ≥3 correctivas del mismo equipo
        en 90 días → actividad al Jefe MAST sugiriendo levantar NC y revisar
        el plan de mantenimiento. (b) Reclamación abierta con SLA vencido →
        actividad al Jefe MAST. Idempotente por resumen."""
        sgi_require_system(self.env)  # F-008
        self = self._sgi_new_run()  # 56.37.0: cierre por episodio
        now = fields.Datetime.now()
        today = fields.Date.context_today(self)
        manager_id = self._sgi_manager_user_id()
        if not manager_id:
            return True
        # (a) Mantenimiento repetitivo (los datos ya están en la app nativa).
        since = now - relativedelta(days=90)
        requests = self.env['maintenance.request'].search([
            ('maintenance_type', '=', 'corrective'),
            ('create_date', '>=', since),
            ('equipment_id', '!=', False),
        ])
        by_equipment = {}
        for request in requests:
            by_equipment.setdefault(request.equipment_id, 0)
            by_equipment[request.equipment_id] += 1

        def _repetitive(equipment):
            count = by_equipment[equipment]
            if count < 3:
                return
            self._sgi_schedule(
                equipment,
                "Falla repetitiva: %s (%d correctivas en 90 días)"
                % (equipment.name, count),
                "El equipo acumula %d solicitudes correctivas en 90 días. "
                "Evalúe levantar una NC (botón «Levantar NC» en la solicitud) "
                "y revisar su plan de mantenimiento preventivo." % count,
                manager_id, date_deadline=today + relativedelta(days=7), key='falla_repetitiva')

        failures = self._sgi_for_each(list(by_equipment), _repetitive, "falla repetitiva")
        # (b) Reclamaciones con SLA vencido que siguen abiertas.
        team = self.env.ref('quimibond_sgi.sgi_helpdesk_team_complaints',
                            raise_if_not_found=False)
        Ticket = self.env['helpdesk.ticket']
        if team and 'sla_deadline' in Ticket._fields:
            tickets = Ticket.search([
                ('team_id', '=', team.id),
                ('stage_id.fold', '=', False),
                ('sla_deadline', '!=', False),
                ('sla_deadline', '<', now),
            ])

            def _sla(ticket):
                self._sgi_schedule(
                    ticket,
                    "SLA vencido: reclamación %s" % (ticket.name or ticket.id),
                    "La reclamación superó su SLA de respuesta y sigue "
                    "abierta. Escale la respuesta al cliente.",
                    manager_id, date_deadline=today, key='sla_vencido')

            failures += self._sgi_for_each(tickets, _sla, "SLA de reclamaciones")
        self._sgi_sweep(['falla_repetitiva', 'sla_vencido'],
                        "el equipo dejó de fallar o la reclamación ya se atendió", failures)
        return True

    # ------------------------------------------------------------------
    # Cumplimiento legal (14001/45001 6.1.3 / 9.1.2)
    # ------------------------------------------------------------------
    def cron_legal_requirements(self):
        """Cron diario: evaluaciones de cumplimiento vencidas y permisos por
        vencer (≤60 días) o vencidos. Idempotente por resumen."""
        sgi_require_system(self.env)  # F-008
        self = self._sgi_new_run()  # 56.37.0: cierre por episodio
        today = fields.Date.context_today(self)
        soon = today + relativedelta(days=60)
        manager_id = self._sgi_manager_user_id()
        Requirement = self.env['sgi.legal.requirement']
        overdue = Requirement.search([
            ('next_eval_date', '!=', False),
            ('next_eval_date', '<=', today),
        ])

        def _overdue(req):
            self._sgi_schedule(
                req,
                "Evaluar cumplimiento legal: %s" % req.display_name,
                "La evaluación periódica del cumplimiento (9.1.2) venció el "
                "%s. Evalúe y registre el resultado (Cumple / Parcial / No "
                "cumple)." % req.next_eval_date,
                req.responsible_id.id or manager_id,
                date_deadline=req.next_eval_date, key='evaluar_cumplimiento')

        failures = self._sgi_for_each(overdue, _overdue, "evaluaciones legales vencidas")

        # DIR-1 (51.0.0): aviso 60 días antes de la próxima evaluación, al
        # responsable del requisito (aparece en sus actividades y en Mis
        # pendientes). Idempotente por resumen.
        upcoming = Requirement.search([
            ('next_eval_date', '>', today), ('next_eval_date', '<=', soon)])

        def _upcoming(req):
            self._sgi_schedule(
                req,
                "Evaluación de cumplimiento vence el %s: %s" % (req.next_eval_date, req.display_name),
                "Evalúe el cumplimiento del requisito antes del %s y registre resultado, "
                "evidencia y siguiente fecha («Registrar evaluación»)." % req.next_eval_date,
                req.responsible_id.id or manager_id,
                date_deadline=req.next_eval_date, key='evaluacion_proxima')

        failures += self._sgi_for_each(upcoming, _upcoming, "evaluaciones legales próximas")

        expiring = Requirement.search([
            ('expiry_date', '!=', False), ('expiry_date', '<=', soon),
        ])

        def _expiring(req):
            summary = ("Permiso VENCIDO: %s" if req.expiry_date < today
                       else "Permiso por vencer: %s") % req.display_name
            self._sgi_schedule(
                req, summary,
                "El permiso/licencia vence el %s. Gestione la renovación."
                % req.expiry_date,
                req.responsible_id.id or manager_id,
                date_deadline=req.expiry_date, key='permiso')

        failures += self._sgi_for_each(expiring, _expiring, "permisos por vencer")
        self._sgi_sweep(['evaluar_cumplimiento', 'evaluacion_proxima', 'permiso'],
                        "la evaluación ya se registró o el permiso se renovó", failures)
        return True

    @api.model
    def _sgi_participation_survey(self):
        """Encuesta de consulta y participación de los trabajadores (45001
        §5.4, F-P-A10-05). Vive en producción (creada por MCP, MAST la puede
        editar), no como semilla del módulo: se localiza por la clave en el
        título — mismo patrón que la evaluación del auditor."""
        return self.env['survey.survey'].sudo().search(
            [('title', 'like', 'F-P-A10-05')], limit=1)

    @api.model
    def cron_worker_participation(self):
        """Cron semestral: recuerda distribuir la encuesta de consulta y
        participación de los trabajadores (45001 §5.4). Las respuestas y las
        quejas del canal interno alimentan la entrada 12 de la RxD.
        Idempotente por semestre (el resumen lleva el semestre)."""
        sgi_require_system(self.env)  # F-008
        survey = self._sgi_participation_survey()
        rh_id = self._sgi_rh_user_id()
        if not survey or not rh_id:
            return True
        today = fields.Date.context_today(self)
        label = "S%d %d" % (1 if today.month <= 6 else 2, today.year)
        self._sgi_schedule(
            survey,
            "Distribuir consulta y participación de trabajadores (%s)" % label,
            "45001 §5.4: comparta la encuesta F-P-A10-05 con el personal desde "
            "la app Encuestas (botón Compartir). Las respuestas del periodo "
            "alimentan la entrada 12 de la Revisión por la Dirección.",
            rh_id, date_deadline=date(today.year, 6 if today.month <= 6 else 12,
                                      30 if today.month <= 6 else 31),
            key='participacion:%s' % label)
        return True

    @api.model
    def cron_context_review(self):
        """Cron semanal: partes interesadas (4.1/4.2) con revisión vencida.
        Idempotente por resumen."""
        sgi_require_system(self.env)  # F-008
        self = self._sgi_new_run()  # 56.37.0: cierre por episodio
        today = fields.Date.context_today(self)
        manager_id = self._sgi_manager_user_id()
        if not manager_id:
            return True
        parties = self.env['sgi.interested.party'].search([
            ('next_review_date', '!=', False),
            ('next_review_date', '<=', today),
        ])

        def _process(party):
            self._sgi_schedule(
                party,
                "Revisar parte interesada: %s" % party.name,
                "La revisión periódica del contexto (4.1/4.2) venció el %s. "
                "Confirme o actualice sus necesidades/expectativas y marque "
                "la revisión." % party.next_review_date,
                manager_id, date_deadline=party.next_review_date,
                key='revisar_parte_interesada')

        failures = self._sgi_for_each(parties, _process, "revisión del contexto")
        self._sgi_sweep(['revisar_parte_interesada'], "la revisión ya se registró", failures)
        return True

    # ------------------------------------------------------------------
    # Integraciones Sign / eLearning — sincronización diaria
    # ------------------------------------------------------------------
    @api.model
    def cron_sign_elearning_sync(self):
        """Cron diario: sella acuses cuya firma electrónica ya se completó y
        otorga competencias de cursos eLearning terminados."""
        sgi_require_system(self.env)  # F-008
        self._sgi_step(
            "acuses firmados vía Sign",
            lambda: self.env['sgi.document.ack']._sgi_sync_from_sign())
        self._sgi_step(
            "competencias por cursos eLearning",
            lambda: self.env['slide.channel']._sgi_sync_completions())
        self._sgi_step(
            "responsivas de EPP firmadas vía Sign",
            lambda: self.env['sgi.epp.delivery']._sgi_sync_from_sign())
        self._sgi_step(
            "cambios documentales firmados vía Sign",
            lambda: self.env['approval.request']._sgi_sync_all_sign())
        self._sgi_step(
            "«Mi procedimiento» firmado vía Sign",
            lambda: self.env['documents.document']._sgi_sync_publish_sign())
        return True

    # ------------------------------------------------------------------
    # Digest semanal — el pulso del SGI para quien no vive dentro de Odoo
    # ------------------------------------------------------------------
    @api.model
    def cron_weekly_digest(self):
        """Cron semanal: correo-resumen de pendientes del SGI al Jefe de
        MAST y Dirección. Cada métrica va en su propio savepoint: una
        consulta rota no tumba el resumen."""
        sgi_require_system(self.env)  # F-008
        emails = self._sgi_critical_mail_emails()
        if not emails:
            return True
        today = fields.Date.context_today(self)
        rows = []

        def metric(label, func):
            def _run():
                count = func()
                if count:
                    rows.append((label, count))
            self._sgi_step("digest: %s" % label, _run)

        metric("No Conformidades abiertas", lambda: self.env['quality.alert'].search_count([
            ('team_id.sgi_sequence_id', '!=', False),
            ('stage_id.sgi_is_closing_stage', '=', False),
            ('stage_id.sgi_is_cancel_stage', '=', False)]))
        metric("Acciones CAPA vencidas", lambda: self.env['sgi.action.line'].search_count([
            ('date_done', '=', False), ('state', '=', 'vencida')]))
        # Indicadores y riesgos de procesos archivados siguen midiéndose, pero
        # el resumen solo cuenta los de procesos vigentes.
        metric("Indicadores en rojo (último semáforo)", lambda: self.env['sgi.indicator'].search_count([
            ('last_semaphore', '=', 'rojo'), ('sgi_process_active', '=', True)]))
        metric("Evaluaciones legales vencidas", lambda: self.env['sgi.legal.requirement'].search_count([
            ('next_eval_date', '!=', False), ('next_eval_date', '<=', today)]))
        metric("Requisitos legales en incumplimiento (total o parcial)",
               lambda: self.env['sgi.legal.requirement'].search_count([
                   ('compliance_state', 'in', ('no_cumple', 'parcial'))]))
        metric("Riesgos en atención alta sin tratamiento", lambda: self.env['sgi.risk'].search_count([
            ('attention_level', 'in', ('inmediata', 'alto')),
            ('state', '=', 'identificado'), ('sgi_process_active', '=', True)]))
        metric("Acuses de lectura pendientes", lambda: self.env['sgi.document.ack'].search_count([
            ('state', '=', 'pendiente')]))
        metric("Partes interesadas con revisión vencida",
               lambda: self.env['sgi.interested.party'].search_count([
                   ('next_review_date', '!=', False),
                   ('next_review_date', '<=', today)]))

        if rows:
            lines = "".join(
                "<tr><td style='padding:4px 12px 4px 0;'>%s</td>"
                "<td style='text-align:right;font-weight:bold;'>%d</td></tr>"
                % (label, count) for label, count in rows)
            body = (
                "<p>Resumen semanal del SGI al %s:</p>"
                "<table style='border-collapse:collapse;'>%s</table>"
                "<p>El detalle vive en Odoo → SGI (Panel y Diagnóstico del "
                "SGI).</p>" % (today.strftime('%d/%m/%Y'), lines))
        else:
            body = ("<p>Resumen semanal del SGI al %s: sin pendientes "
                    "abiertos en los indicadores vigilados.</p>"
                    % today.strftime('%d/%m/%Y'))

        def _send():
            self.env['mail.mail'].sudo().create({
                'subject': "SGI: resumen semanal (%s)" % today.strftime('%d/%m/%Y'),
                'email_to': emails,
                'body_html': body,
                'auto_delete': True,
            }).send()
        self._sgi_step("digest: envío del correo", _send)
        return True
