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

from .sgi_calendar import sgi_local_datetime_utc
from .sgi_health_const import (
    EXCLUDED_USERS_PARAM, FORMAT_USE_DAYS, HEALTH_MODES, HEALTH_PRIVATE_MODELS,
    HEALTH_TOUCH_MODELS, HEALTH_XMLIDS, NC_OPEN_DAYS, NC_WINDOW_DAYS, PEOPLE_DAYS,
    RED_WINDOW_MONTHS, VALIDATION_PREFILTER_DAYS, VALIDATION_WINDOW_DAYS, param_ids)

_logger = logging.getLogger(__name__)

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
        employees = self.env['hr.employee'].sudo().search([
            ('company_id', '=', self._sgi_health_company().id), ('job_id', '!=', False)])
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
    def _detail_salud_avisos(self, date_from, date_to):
        activities = self.env['mail.activity'].sudo().search([
            ('date_deadline', '<=', date_to),
            '|', ('sgi_cron_key', '!=', False), ('res_model', '=like', 'sgi.%')])
        # D-03: la actividad es de la empresa del registro sobre el que está.
        activities = self.env['sgi.my.pending']._sgi_notices_in_company(
            activities, self._sgi_health_company())
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
                if action and action._name == 'ir.actions.act_window':
                    status = self._sgi_health_model_used(action.sudo().res_model, start, company, cache)
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
