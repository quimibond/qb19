# -*- coding: utf-8 -*-
"""57.99.0 «Salud del SGI» (auditoría 2026-10: sección 8 y hallazgo D-01).

Diez indicadores de nivel Dirección (proceso E2), semanales, con modo propio:
procesos en vigor, personas que usan el SGI, planta identificable, acuses al
día, mediciones validadas a tiempo, rojos con respuesta, NC eficaces a la
primera, avisos vencidos, programa de auditoría y formatos migrados
utilizables. Página «Salud del SGI» en el Tablero con la tabla por dueño de
proceso (hallazgo D-01) y un correo semanal a Dirección. Las mediciones de
salud no piden validación, ni causa y plan, ni avisan «no calculó»: su
respuesta es la revisión semanal.

Va al final de ``models/__init__.py``: hereda ``sgi.indicator``,
``sgi.indicator.measure``, ``sgi.process``, ``sgi.direction.board`` y
``sgi.cron``. Todo se lee con ``sudo()`` y se filtra por la empresa del SGI
(D-03). Solo conteos: ningún nombre sale en las notas."""
import logging
from collections import Counter
from datetime import timedelta

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

from .sgi_calendar import sgi_local_date, sgi_local_datetime_utc, sgi_today
from .sgi_guard import sgi_require_system
from .sgi_health_const import (
    EXCLUDED_USERS_PARAM, FORMAT_USE_DAYS, HEALTH_MODES, HEALTH_PRIVATE_MODELS,
    HEALTH_TOUCH_MODELS, HEALTH_XMLIDS, IDLE_DAYS, LATE_WINDOW_DAYS, MAIL_USERS_PARAM, NC_OPEN_DAYS,
    NC_WINDOW_DAYS, PEOPLE_DAYS, RED_WINDOW_MONTHS, VALIDATION_PREFILTER_DAYS,
    VALIDATION_WINDOW_DAYS, param_ids)

_logger = logging.getLogger(__name__)

_SEMAPHORE = [('verde', "Verde"), ('amarillo', "Amarillo"), ('rojo', "Rojo")]
_SEM_COLOR = {'verde': '#1e7e34', 'amarillo': '#b58105', 'rojo': '#c82333'}
_NO_COLOR = '#6c757d'
_DATA_STATES = ('capturado', 'validado')

_SELECTION = [
    ('salud_procesos', "Salud del SGI: procesos en vigor"),
    ('salud_personas', "Salud del SGI: personas que usan el SGI (30 días)"),
    ('salud_planta', "Salud del SGI: planta identificable"),
    ('salud_acuses', "Salud del SGI: acuses al día"),
    ('salud_validacion', "Salud del SGI: mediciones validadas a tiempo"),
    ('salud_rojos', "Salud del SGI: rojos con respuesta"),
    ('salud_nc', "Salud del SGI: NC eficaces a la primera"),
    ('salud_avisos', "Salud del SGI: avisos vencidos"),
    ('salud_auditoria', "Salud del SGI: programa de auditoría cumplido"),
    ('salud_formatos', "Salud del SGI: formatos migrados utilizables"),
]


class SgiIndicatorHealth(models.Model):
    _inherit = 'sgi.indicator'

    calc_mode = fields.Selection(
        selection_add=_SELECTION,
        ondelete={mode: 'set default' for mode, _label in _SELECTION})
    # Correo y Tablero leen la medición de la SEMANA PASADA (no la última con
    # dato): un «sin dato» de esta semana se ve como tal.
    sgi_health_week_measure_id = fields.Many2one(
        'sgi.indicator.measure', string="Medición de la semana pasada",
        compute='_compute_sgi_health_week')
    sgi_health_week_value = fields.Char(
        string="Semana pasada", compute='_compute_sgi_health_week',
        help="Valor de la medición de la semana pasada (lunes a domingo). «Sin dato» si no "
             "hubo casos o todavía no se mide.")
    sgi_health_week_semaphore = fields.Selection(
        _SEMAPHORE, string="Semáforo de la semana", compute='_compute_sgi_health_week',
        help="Semáforo de la medición de la semana pasada.")
    sgi_health_week_previous = fields.Char(
        string="Semana anterior", compute='_compute_sgi_health_week',
        help="Valor de la semana antepasada, para comparar.")

    @api.model
    def _sgi_health_week(self):
        """Lunes de la semana pasada (el periodo que mide el cron)."""
        today = sgi_today(self.env)
        return today - timedelta(days=today.weekday() + 7)

    def _compute_sgi_health_week(self):
        week = self._sgi_health_week()
        before = week - timedelta(days=7)
        ids = [i for i in self.ids if isinstance(i, int)]
        measures = self.env['sgi.indicator.measure'].sudo().search(
            [('indicator_id', 'in', ids), ('period_date', 'in', (week, before))]) if ids else []
        by_key = {(m.indicator_id.id, m.period_date): m for m in measures}
        for indicator in self:
            current = by_key.get((indicator.id, week))
            previous = by_key.get((indicator.id, before))
            with_data = current and current.state in _DATA_STATES
            indicator.sgi_health_week_measure_id = current.id if current else False
            indicator.sgi_health_week_value = indicator._sgi_health_fmt(current.value) \
                if with_data else "Sin dato"
            indicator.sgi_health_week_semaphore = current.semaphore if with_data else False
            indicator.sgi_health_week_previous = indicator._sgi_health_fmt(previous.value) \
                if previous and previous.state in _DATA_STATES else "—"

    def _sgi_health_fmt(self, value):
        """«75 %», «12 personas»: el valor sin ceros de más, con su unidad."""
        text = ('%.1f' % (value or 0.0)).rstrip('0').rstrip('.') or '0'
        return ("%s %s" % (text, self.uom or '')).strip()

    # ---- comunes ---------------------------------------------------------
    def _sgi_health_company(self):
        return self.env['sgi.config']._sgi_company()

    def _sgi_health_bounds(self, date_to, days):
        """(inicio, fin) en UTC de los ``days`` días locales que terminan en date_to."""
        return (sgi_local_datetime_utc(self.env, date_to - timedelta(days=days - 1), 0),
                sgi_local_datetime_utc(self.env, date_to + timedelta(days=1), 0))

    def _sgi_health_count(self, records, note=None):
        """Indicador de conteo: valor = número de registros (0 es un valor)."""
        out = {'value': float(len(records)), 'numerator': len(records), 'denominator': None,
               'model': records._name, 'ids': records.ids}
        if note:
            out['note'] = note
        return out

    @api.model
    def _sgi_health_concentration(self, counts):
        """(mayor, total, %) de un {usuario: avisos}: qué tanto los tiene una sola persona."""
        total = sum(counts.values())
        top = max(counts.values()) if counts else 0
        return top, total, round(top * 100.0 / total, 1) if total else 0.0

    @api.model
    def _sgi_health_touches(self, start, end):
        """{user_id: último movimiento (datetime UTC)} en el SGI entre start y
        end: altas y escrituras de HEALTH_TOUCH_MODELS y mensajes en los
        modelos ``sgi.*``, en las NC con folio y en los documentos
        controlados. Fuente de SG-02 y de los días sin movimiento del dueño
        de proceso. Nunca lee salud ocupacional (HEALTH_PRIVATE_MODELS)."""
        env = self.env
        last = {}

        def _keep(user_id, moment):
            if user_id and moment and (user_id not in last or moment > last[user_id]):
                last[user_id] = moment

        for model in HEALTH_TOUCH_MODELS:
            if model not in env or model in HEALTH_PRIVATE_MODELS:
                continue
            Model = env[model].sudo().with_context(active_test=False)
            if Model._transient or Model._abstract or not Model._log_access:
                continue
            extra = []
            if model == 'quality.alert':
                extra = [('sgi_folio', '!=', False)]
            elif model == 'documents.document':
                extra = [('sgi_is_controlled', '=', True)]
            for uid_field, date_field in (('create_uid', 'create_date'), ('write_uid', 'write_date')):
                for user, moment in Model._read_group(
                        [(date_field, '>=', start), (date_field, '<', end)] + extra,
                        [uid_field], ['%s:max' % date_field]):
                    _keep(user.id, moment)
        # Mensajes: una lectura por rama (modelos sgi.*, NC con folio,
        # documentos controlados) con la lista explícita de modelos, no con
        # «=like 'sgi.%'».
        sgi_models = [name for name in env.registry
                      if name.startswith('sgi.') and name not in HEALTH_PRIVATE_MODELS]
        branches = [[('model', 'in', sgi_models)]]
        alerts = env['quality.alert'].sudo().with_context(active_test=False).search(
            [('sgi_folio', '!=', False)]).ids
        if alerts:
            branches.append([('model', '=', 'quality.alert'), ('res_id', 'in', alerts)])
        documents = env['documents.document'].sudo().with_context(active_test=False).search(
            [('sgi_is_controlled', '=', True)]).ids
        if documents:
            branches.append([('model', '=', 'documents.document'), ('res_id', 'in', documents)])
        by_partner = {}
        Message = env['mail.message'].sudo()
        for branch in branches:
            for partner, moment in Message._read_group(
                    [('date', '>=', start), ('date', '<', end), ('author_id', '!=', False)] + branch,
                    ['author_id'], ['date:max']):
                if partner and moment and (partner.id not in by_partner
                                           or moment > by_partner[partner.id]):
                    by_partner[partner.id] = moment
        if by_partner:
            users = env['res.users'].sudo().with_context(active_test=False).search(
                [('partner_id', 'in', list(by_partner))])
            for user in users:
                _keep(user.id, by_partner.get(user.partner_id.id))
        return last

    @api.model
    def _sgi_health_excluded_ids(self):
        """OdooBot, el administrador técnico y los del parámetro de excluidos."""
        excluded = set(param_ids(self.env, EXCLUDED_USERS_PARAM))
        for xmlid in ('base.user_root', 'base.user_admin'):
            ref = self.env.ref(xmlid, raise_if_not_found=False)
            if ref:
                excluded.add(ref.id)
        return excluded

    @api.model
    def _sgi_health_people(self, last):
        """Personas: usuarios internos activos con empleado activo de la
        empresa del SGI (las cuentas genéricas no tienen), sin OdooBot, sin el
        administrador técnico y sin los del parámetro de excluidos."""
        if not last:
            return self.env['res.users']
        employees = self.env['hr.employee'].sudo().search([
            ('company_id', '=', self._sgi_health_company().id),
            ('user_id', 'in', list(last))])
        excluded = self._sgi_health_excluded_ids()
        return employees.mapped('user_id').filtered(
            lambda u: u.active and not u.share and u.id not in excluded)

    # ---- 1. procesos en vigor ---------------------------------------------
    def _detail_salud_procesos(self, date_from, date_to):
        processes = self.env['sgi.process'].sudo().search(
            [('company_id', '=', self._sgi_health_company().id)])
        vigentes = processes.filtered(lambda p: p.state == 'vigente')
        return self._ratio(len(vigentes), len(processes), processes)

    # ---- 2. personas que usan el SGI ---------------------------------------
    def _detail_salud_personas(self, date_from, date_to):
        start, end = self._sgi_health_bounds(date_to, PEOPLE_DAYS)
        people = self._sgi_health_people(self._sgi_health_touches(start, end))
        return self._sgi_health_count(people)

    # ---- 3. planta identificable --------------------------------------------
    def _detail_salud_planta(self, date_from, date_to):
        # Sin dominio sobre job_id: en Odoo 19 pasa por la versión y la
        # búsqueda no encuentra a los empleados (_sgi_mp_ids_where); se
        # filtra en Python.
        employees = self.env['hr.employee'].sudo().search([
            ('company_id', '=', self._sgi_health_company().id)]).filtered('job_id')
        ok = employees.filtered(lambda e: e.user_id and e.user_id.active and not e.user_id.share)
        return self._ratio(len(ok), len(employees), employees, model='hr.employee.public')

    # ---- 4. acuses al día -------------------------------------------------
    def _detail_salud_acuses(self, date_from, date_to):
        _days, limit = self.env['sgi.cron']._sgi_ack_limit()
        acks = self.env['sgi.document.ack'].sudo().search([
            ('document_id.sgi_is_controlled', '=', True),
            ('document_id.sgi_state', '=', 'vigente'), ('document_id.active', '=', True),
            ('employee_id.active', '=', True),
            ('employee_id.company_id', '=', self._sgi_health_company().id)])
        read = acks.filtered(lambda a: a.state == 'leido')
        late = acks.filtered(lambda a: a.state != 'leido' and a.create_date < limit)
        out = self._ratio(len(acks) - len(late), len(acks), acks)
        out['note'] = "Leídos: %d · En plazo: %d · Vencidos: %d" % (
            len(read), len(acks) - len(read) - len(late), len(late))
        return out

    # ---- 5. mediciones validadas a tiempo -----------------------------------
    def _detail_salud_validacion(self, date_from, date_to):
        first_day = date_to - timedelta(days=VALIDATION_WINDOW_DAYS - 1)
        since = first_day - timedelta(days=VALIDATION_PREFILTER_DAYS)
        Measure = self.env['sgi.indicator.measure'].sudo()
        measures = Measure.search([
            ('state', 'in', ('capturado', 'validado')),
            ('indicator_id.active', '=', True),
            ('indicator_id.calc_mode', 'not in', HEALTH_MODES),
            '|', ('captured_date', '>=', since),
            '&', ('captured_date', '=', False),
            ('create_date', '>=', sgi_local_datetime_utc(self.env, since, 0))])
        universe = Measure
        on_time = 0
        for measure in measures:
            due = measure._sgi_validate_due()
            if not first_day <= due <= date_to:
                continue
            universe |= measure
            # Las validadas antes de 57.99.0 no tienen fecha: cuentan a tiempo.
            if measure.state == 'validado' and (measure.sgi_validated_date or due) <= due:
                on_time += 1
        return self._ratio(on_time, len(universe), universe)

    # ---- 6. rojos con respuesta ---------------------------------------------
    def _detail_salud_rojos(self, date_from, date_to):
        start = date_to.replace(day=1) - relativedelta(months=RED_WINDOW_MONTHS - 1)
        reds = self.env['sgi.indicator.measure'].sudo().search([
            ('semaphore', '=', 'rojo'), ('state', 'in', ('capturado', 'validado')),
            ('small_sample', '=', False), ('indicator_id.active', '=', True),
            ('indicator_id.calc_mode', 'not in', HEALTH_MODES),
            ('period_date', '>=', start), ('period_date', '<=', date_to)])
        answered = reds.filtered(lambda m: m.alert_id or (m.cause and m.action_line_ids))
        return self._ratio(len(answered), len(reds), reds)

    # ---- 7. NC eficaces a la primera ----------------------------------------
    def _detail_salud_nc(self, date_from, date_to):
        """Desde 57.93.0 toda NC cierra «Eficaz»: se cuenta la que cerró sin
        ninguna verificación «No eficaz» (eficaz a la primera)."""
        Alert = self.env['quality.alert'].sudo()
        base = [('company_id', '=', self._sgi_health_company().id), ('sgi_folio', '!=', False)]
        start, end = self._sgi_health_bounds(date_to, NC_WINDOW_DAYS)
        closed = Alert.search(base + [('sgi_stage_is_closing', '=', True),
                                      ('date_close', '>=', start), ('date_close', '<', end)])
        first_time = closed.filtered(lambda a: not a.sgi_ineffective_count)
        limit = sgi_local_datetime_utc(self.env, date_to - timedelta(days=NC_OPEN_DAYS), 0)
        old_open = Alert.search_count(base + [
            ('sgi_stage_is_closing', '=', False), ('sgi_stage_is_cancel', '=', False),
            ('create_date', '<', limit)])
        out = self._ratio(len(first_time), len(closed), closed)
        out['note'] = "NC abiertas con más de %d días: %d (meta: 0)." % (NC_OPEN_DAYS, old_open)
        return out

    # ---- 8. avisos vencidos ---------------------------------------------------
    @api.model
    def _sgi_health_overdue_activities(self, date_to, users=None):
        """Avisos del SGI vencidos al cierre de ``date_to`` (vencimiento ese
        día o antes), de la empresa del SGI (D-03). Una sola regla para SG-08
        y para la tabla por dueño de proceso."""
        domain = [('date_deadline', '<=', date_to),
                  '|', ('sgi_cron_key', '!=', False), ('res_model', '=like', 'sgi.%')]
        if users is not None:
            domain = [('user_id', 'in', users.ids)] + domain
        activities = self.env['mail.activity'].sudo().search(domain)
        # D-03: la actividad es de la empresa del registro sobre el que está.
        return self.env['sgi.my.pending']._sgi_notices_in_company(
            activities, self._sgi_health_company())

    def _detail_salud_avisos(self, date_from, date_to):
        activities = self._sgi_health_overdue_activities(date_to)
        counts = Counter(a.user_id.id for a in activities if a.user_id)
        top, total, pct = self._sgi_health_concentration(counts)
        note = ("Concentración: el %s %% (%d de %d) los tiene una sola persona "
                "(meta: menos de 30 %%)." % (pct, top, total)) if total else \
            "Concentración: sin avisos vencidos con responsable."
        unassigned = len(activities) - total
        if unassigned:
            note += " Sin responsable: %d." % unassigned
        return self._sgi_health_count(activities, note)

    # ---- 9. programa de auditoría -----------------------------------------------
    def _detail_salud_auditoria(self, date_from, date_to):
        year = date_to.year
        programs = self.env['sgi.audit.program'].sudo().search([('year', '=', year)])
        if not programs:
            return {'value': None, 'model': 'sgi.audit.program.line', 'ids': [],
                    'note': "Sin programa de auditorías %d." % year}
        lines = programs.line_ids.filtered(
            lambda line: line.audit_type == 'interna' and int(line.planned_month) <= date_to.month)
        done = lines.filtered(lambda line: line.state == 'cerrada'
                              or line.audit_id.state in ('informe', 'cerrada'))
        out = self._ratio(len(done), len(lines), lines)
        notes = ["Auditorías internas del programa %d programadas hasta el mes en curso." % year]
        drafts = programs.filtered(lambda p: p.state == 'borrador')
        if drafts:
            notes.append("El programa %d sigue en borrador." % year)
        out['note'] = " ".join(notes)
        return out

    # ---- 10. formatos migrados utilizables -------------------------------------
    def _sgi_health_model_used(self, model, start, company, cache):
        """True si ``model`` tuvo al menos un alta desde ``start`` (de la
        empresa del SGI o sin empresa); None si no es un modelo con registros
        que contar (pantalla calculada, transitorio)."""
        if model in cache:
            return cache[model]
        result = None
        if model and model in self.env:
            Model = self.env[model].sudo().with_context(active_test=False)
            if not (Model._transient or Model._abstract) and Model._log_access:
                domain = [('create_date', '>=', start)]
                field = Model._fields.get('company_id')
                if field is not None and field.store and field.type == 'many2one':
                    domain.append(('company_id', 'in', (company.id, False)))
                result = bool(Model.search_count(domain, limit=1))
        cache[model] = result
        return result

    def _detail_salud_formatos(self, date_from, date_to):
        company = self._sgi_health_company()
        Doc = self.env['documents.document'].sudo()
        domain = [('sgi_is_controlled', '=', True), ('sgi_migration_state', '=', 'migrado')]
        if 'company_id' in Doc._fields and Doc._fields['company_id'].store:
            domain.append(('company_id', 'in', (company.id, False)))
        docs = Doc.search(domain)
        start, _end = self._sgi_health_bounds(date_to, FORMAT_USE_DAYS)
        Check = self.env['quality.check'].sudo()
        cache = {}
        points = {}
        causes = Counter()
        used = 0
        for doc in docs:
            menu = doc.sgi_odoo_menu_id.with_context(active_test=False)
            point = doc.sgi_migration_point_id.with_context(active_test=False)
            uses = []
            if menu and menu.active:
                action = menu.action
                action = action.sudo().exists() if action else action
                if not action:
                    # Menú sin acción: no lleva a ningún formulario.
                    status = False
                elif action._name == 'ir.actions.act_window':
                    model = action.res_model
                    # Un modelo que ya no existe no es utilizable; un
                    # transitorio o una pantalla calculada no tiene qué contar.
                    status = self._sgi_health_model_used(model, start, company, cache) \
                        if model in self.env else False
                else:
                    status = None
                if status is None:
                    causes['screen'] += 1
                    uses.append(True)
                else:
                    uses.append(status)
            if point and point.active:
                if point.id not in points:
                    points[point.id] = bool(Check.search_count(
                        [('point_id', '=', point.id), ('create_date', '>=', start)], limit=1))
                uses.append(points[point.id])
            if any(uses):
                used += 1
            elif uses:
                causes['idle'] += 1
            elif menu:
                causes['menu_off'] += 1
            elif point:
                causes['point_off'] += 1
            else:
                causes['text'] += 1
        out = self._ratio(used, len(docs), docs)
        out['note'] = (
            "Worksheet archivado: %(point_off)d · Solo texto: %(text)d · Menú archivado: "
            "%(menu_off)d · Sin registros en %(days)d días: %(idle)d · Pantalla sin registros "
            "que contar (cuenta como utilizable): %(screen)d" % dict(
                {key: causes.get(key, 0) for key in ('point_off', 'text', 'menu_off', 'idle', 'screen')},
                days=FORMAT_USE_DAYS))
        return out

    # ---- diagnóstico y liga al proceso ---------------------------------------
    def _sgi_calc_diagnose(self, vals):
        """Salud: «sin dato» es un resultado (no hubo NC cerradas, no hay
        programa), no una falla: no agenda «Indicador no calculó»."""
        self.ensure_one()
        if self.calc_mode in HEALTH_MODES and vals.get('state') == 'sin_dato':
            return 'ok', vals.get('note') or "Sin casos en el periodo."
        return super()._sgi_calc_diagnose(vals)

    @api.model
    def _sgi_health_link_process(self):
        """Liga los indicadores de salud sin proceso al E2 de la empresa del
        SGI y les pone responsable (usuario del dueño de E2 o el Jefe MAST)
        si no tienen. No toca lo que MAST haya puesto. Idempotente: devuelve
        los ids cambiados."""
        company = self.env['sgi.config']._sgi_company()
        process = self.env['sgi.process'].sudo().search(
            [('code', '=', 'E2'), ('company_id', '=', company.id)], limit=1)
        owner_user = process.owner_id.user_id.filtered(lambda u: u.active and not u.share)
        fallback = owner_user.id or self.env['sgi.cron']._sgi_manager_user_id()
        changed = []
        for xmlid in HEALTH_XMLIDS:
            indicator = self.env.ref(xmlid, raise_if_not_found=False)
            if not indicator:
                continue
            indicator = indicator.sudo()
            vals = {}
            if process and not indicator.process_id:
                vals['process_id'] = process.id
            if fallback and not indicator.responsible_id:
                vals['responsible_id'] = fallback
            if vals:
                indicator.write(vals)
                indicator.message_post(body=(
                    "57.99.0 (Salud del SGI): se liga al proceso E2 y se le asigna "
                    "responsable donde faltaban. Se puede cambiar en la ficha."))
                changed.append(indicator.id)
        return changed


class SgiIndicatorMeasureHealth(models.Model):
    _inherit = 'sgi.indicator.measure'

    sgi_validated_date = fields.Date(
        string="Validada el", readonly=True, copy=False, index=True,
        help="Día en que la medición pasó a «Validado». Con él se mide si se validó a "
             "tiempo (3 días hábiles desde la captura).")

    @api.model_create_multi
    def create(self, vals_list):
        """Una medición que nace validada también lleva su fecha."""
        today = sgi_today(self.env)
        for vals in vals_list:
            if vals.get('state') == 'validado' and not vals.get('sgi_validated_date'):
                vals['sgi_validated_date'] = today
        return super().create(vals_list)

    def write(self, vals):
        """57.99.0: guarda el día en que la medición PASA a validada (SG-05).
        Re-validar una ya validada no lo mueve."""
        newly = self.browse()
        if vals.get('state') == 'validado':
            newly = self.filtered(lambda m: m.state != 'validado')
        res = super().write(vals)
        if newly:
            # sudo: el campo no está en el candado de la validada, pero así
            # queda explícito que lo escribe el sistema.
            newly.sudo().write({'sgi_validated_date': sgi_today(self.env)})
        return res

    @api.depends('semaphore', 'state', 'small_sample', 'period_date', 'cause',
                 'action_line_ids', 'indicator_id.frequency', 'indicator_id.calc_mode')
    def _compute_plan(self):
        """Salud: un rojo no pide causa y plan ni escala (su respuesta es la
        revisión semanal de Dirección)."""
        super()._compute_plan()
        for measure in self.filtered(lambda m: m.indicator_id.calc_mode in HEALTH_MODES):
            measure.plan_required = False

    def action_view_evidence(self):
        """Los de salud guardan sus registros en la medición: «Ver evidencia»
        abre esos registros (patrón de los modos de «indicadores 2»)."""
        self.ensure_one()
        if self.indicator_id.calc_mode not in HEALTH_MODES:
            return super().action_view_evidence()
        if self.detail_model and self.detail_count:
            return self.action_view_records()
        raise UserError(
            "Esta medición de salud del SGI no guardó registros: no hubo casos en el "
            "periodo. Si cree que falta alguno, recalcule la medición.")


class SgiProcessHealth(models.Model):
    """Hallazgo D-01 (auditoría 2026-10): por dueño de proceso, avisos
    vencidos, validaciones atrasadas y días sin movimiento en el SGI. Un
    solo cálculo para los cuatro campos y todos los procesos leídos."""
    _inherit = 'sgi.process'

    sgi_health_owner_user_id = fields.Many2one(
        'res.users', string="Usuario del dueño", compute='_compute_sgi_health',
        help="Usuario activo del dueño del proceso. Vacío si el dueño no tiene usuario.")
    sgi_health_overdue_count = fields.Integer(
        string="Avisos vencidos", compute='_compute_sgi_health',
        help="Avisos del SGI vencidos que tiene el dueño del proceso.")
    sgi_health_late_validation_count = fields.Integer(
        string="Validaciones atrasadas", compute='_compute_sgi_health',
        help="Mediciones que el dueño debía validar y cuyo plazo ya pasó.")
    sgi_health_idle_days = fields.Integer(
        string="Días sin movimiento", compute='_compute_sgi_health',
        help="Días desde la última vez que el dueño creó, modificó o comentó algo del SGI. "
             "91 significa más de 90. Vacío si el dueño no tiene usuario.")

    def _compute_sgi_health(self):
        today = sgi_today(self.env)
        users = self.sudo().mapped('owner_id.user_id').filtered(lambda u: u.active and not u.share)
        overdue = Counter()
        late = Counter()
        last = {}
        if users:
            Indicator = self.env['sgi.indicator']
            # Misma regla que SG-08: vencidos al cierre de ayer.
            overdue.update(activity.user_id.id for activity in
                           Indicator._sgi_health_overdue_activities(today - timedelta(days=1), users))
            # Ventana: solo periodos de los últimos LATE_WINDOW_DAYS días; lo
            # más viejo sin validar ya lo cuenta el Diagnóstico.
            measures = self.env['sgi.indicator.measure'].sudo().search([
                ('state', '=', 'capturado'), ('indicator_id.active', '=', True),
                ('period_date', '>=', today - timedelta(days=LATE_WINDOW_DAYS)),
                ('indicator_id.responsible_id', 'in', users.ids),
                ('indicator_id.calc_mode', 'not in', HEALTH_MODES)])
            for measure in measures:
                if measure._sgi_validate_due() < today:
                    late[measure.indicator_id.responsible_id.id] += 1
            start, end = Indicator._sgi_health_bounds(today, IDLE_DAYS)
            last = Indicator._sgi_health_touches(start, end)
        for process in self:
            user = process.sudo().owner_id.user_id
            user = user if user in users else self.env['res.users']
            process.sgi_health_owner_user_id = user
            process.sgi_health_overdue_count = overdue.get(user.id, 0)
            process.sgi_health_late_validation_count = late.get(user.id, 0)
            if not user:
                process.sgi_health_idle_days = 0
            elif user.id in last:
                moment = sgi_local_date(self.env, last[user.id])
                process.sgi_health_idle_days = max((today - moment).days, 0)
            else:
                process.sgi_health_idle_days = IDLE_DAYS + 1


class SgiDirectionBoardHealth(models.TransientModel):
    """Página «Salud del SGI» del Tablero: los diez indicadores (sección 8)
    y la tabla por dueño de proceso (hallazgo D-01)."""
    _inherit = 'sgi.direction.board'

    health_indicator_ids = fields.Many2many(
        'sgi.indicator', string="Salud del SGI", compute='_compute_health',
        help="Los diez indicadores de salud del SGI (auditoría 2026-10, sección 8).")
    health_process_ids = fields.Many2many(
        'sgi.process', string="Por dueño de proceso", compute='_compute_health',
        help="Avisos vencidos, validaciones atrasadas y días sin movimiento del dueño de cada "
             "proceso.")
    health_note = fields.Char(string="Aviso de salud del SGI", compute='_compute_health')

    @api.depends('date')
    def _compute_health(self):
        company = self.env['sgi.config']._sgi_company()
        indicators = self.env['sgi.indicator'].sudo().search(
            [('calc_mode', 'in', HEALTH_MODES)], order='code')
        processes = self.env['sgi.process'].sudo().search(
            [('company_id', '=', company.id), ('owner_id', '!=', False)], order='code')
        for board in self:
            board.health_indicator_ids = indicators.ids
            board.health_process_ids = processes.ids
            board.health_note = False if indicators else (
                "No hay indicadores de salud del SGI activos. Revise Administración SGI → "
                "Indicadores (claves SG-01 a SG-10).")


class SgiCronHealth(models.AbstractModel):
    """Correo semanal «Salud del SGI» a Dirección (patrón de D-14)."""
    _inherit = 'sgi.cron'

    @api.model
    def _sgi_health_mail_users(self):
        """Dirección de Operaciones (SGI) y los de
        quimibond_sgi.health_mail_user_ids: activos, internos, con correo y
        con acceso a la empresa del SGI (D-03, como D-14)."""
        company = self.env['sgi.config']._sgi_company()
        users = self.env['res.users'].sudo()
        group = self.env.ref('quimibond_sgi.group_sgi_director', raise_if_not_found=False)
        if group:
            users |= group.sudo().all_user_ids
        users |= self.env['res.users'].sudo().browse(
            param_ids(self.env, MAIL_USERS_PARAM)).exists()
        return users.filtered(lambda u: u.active and not u.share and u.email
                              and company in u.company_ids)

    @api.model
    def _sgi_health_rows(self, indicators):
        """Un renglón por indicador con la medición de la semana pasada."""
        semaphores = dict(_SEMAPHORE)
        rows = []
        for indicator in indicators:
            measure = indicator.sgi_health_week_measure_id
            semaphore = indicator.sgi_health_week_semaphore
            rows.append({
                'code': indicator.code or '',
                'name': indicator.name or '',
                'value': indicator.sgi_health_week_value or "Sin dato",
                # La meta se lee como tope (≤) si más bajo es mejor y como
                # piso (≥) si más alto es mejor.
                'target': "%s %s" % ("≤" if indicator.direction == 'lower_better' else "≥",
                                     indicator._sgi_health_fmt(indicator.target_objective)),
                'semaphore': semaphores.get(semaphore, "—"),
                'red': semaphore == 'rojo',
                'color': _SEM_COLOR.get(semaphore, _NO_COLOR),
                'previous': indicator.sgi_health_week_previous or "—",
                'note': " ".join((measure.note or '').split()) if measure else '',
            })
        return rows

    @api.model
    def _sgi_health_owner_rows(self):
        """La tabla por dueño de proceso (hallazgo D-01): solo conteos y el
        nombre del dueño, que Dirección ya ve en el mapa de procesos."""
        company = self.env['sgi.config']._sgi_company()
        processes = self.env['sgi.process'].sudo().search(
            [('company_id', '=', company.id), ('owner_id', '!=', False)], order='code')
        rows = []
        for process in processes:
            if not process.sgi_health_owner_user_id:
                idle = "—"
            elif process.sgi_health_idle_days > IDLE_DAYS:
                idle = "más de %d" % IDLE_DAYS
            else:
                idle = str(process.sgi_health_idle_days)
            rows.append({
                'process': "%s %s" % (process.code or '', process.name or ''),
                'owner': process.owner_id.name or '',
                'overdue': process.sgi_health_overdue_count,
                'late': process.sgi_health_late_validation_count,
                'idle': idle,
            })
        return rows

    @api.model
    def cron_health_weekly_mail(self):
        """57.99.0, cada lunes: mide (si falta) la semana pasada de los
        indicadores de salud y manda a Dirección el correo con su valor, meta,
        semáforo, la semana anterior y la tabla por dueño de proceso (hallazgo
        D-01). Un correo por persona, cada uno en su savepoint. Solo conteos.
        Devuelve los ids de usuario a los que se mandó."""
        sgi_require_system(self.env)  # F-008
        Indicator = self.env['sgi.indicator'].sudo()
        indicators = Indicator.search([('calc_mode', 'in', HEALTH_MODES)], order='code')
        if not indicators:
            return []
        prev = Indicator._sgi_health_week()
        monday = prev + timedelta(days=7)
        label = "semana del %s" % prev.strftime('%d/%m/%Y')
        weekly = indicators.filtered(lambda i: i.frequency == 'weekly')
        if weekly:
            # Idempotente: el cron semanal de indicadores pudo correr más tarde.
            self._sgi_step("mediciones de salud del SGI", lambda: self._sgi_generate_measures(
                weekly, prev, prev, prev + timedelta(days=6), monday, label))
        template = self.env.ref('quimibond_sgi.mail_template_sgi_health_weekly',
                                raise_if_not_found=False)
        users = self._sgi_health_mail_users()
        if not template or not users:
            return []
        indicators.invalidate_recordset()
        rows = self._sgi_health_rows(indicators)
        owners = self._sgi_health_owner_rows()
        base_url = (self.env['ir.config_parameter'].sudo().get_param('web.base.url') or '').rstrip('/')
        ctx = {
            'sgi_week': prev.strftime('%d/%m/%Y'),
            'sgi_rows': rows,
            'sgi_owners': owners,
            'sgi_red': len([row for row in rows if row['red']]),
            'sgi_link': "%s/odoo/action-quimibond_sgi.sgi_direction_board_action_open" % base_url,
        }
        sent = []
        for user in users:
            def _send(user=user):
                template.sudo().with_context(**ctx).send_mail(
                    user.id, email_layout_xmlid='mail.mail_notification_light')
            if self._sgi_step("correo de salud del SGI a %s" % user.login, _send):
                sent.append(user.id)
        _logger.info("SGI 57.99.0: correo semanal de salud del SGI a %d persona(s).", len(sent))
        return sent
