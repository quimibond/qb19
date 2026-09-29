# -*- coding: utf-8 -*-
"""Indicadores 2 (57.14.0, aprobado por Jose 2026-09-29): siete fichas que
eran de captura manual pasan a medirse con código porque cruzan modelos,
ventanas o reglas de plazo (regla de Jose: cuenta o suma A/B en el periodo →
fórmula configurable; rankings, ventanas, lógica contable o cruces → código).

Cada modo aporta ``_detail_<modo>`` (valor, numerador, denominador y los
registros que se guardan en la medición, como I-1/I-3) y su ``_calc_<modo>``.

- S2-01 ``complementos_pago``: pagos de clientes del periodo aplicados a
  facturas PPD (``account.payment.invoice_ids``) cuyo complemento de pago
  (``l10n_mx_edi.document`` en ``payment_sent`` del asiento del pago) se
  timbró a más tardar el día 5 del mes siguiente al pago, hora de México.
  Pago sin complemento timbrado = fuera de plazo.
- S1-05 ``desviacion_precio_compra``: Σ |precio pagado − precio de la OC| ×
  cantidad ÷ importe de esas líneas, en líneas de factura de proveedor
  publicadas que vienen de una OC (decisión de Jose: valor absoluto). La nota
  separa lo pagado de más, lo pagado de menos y la cobertura (líneas e
  importe con OC contra el total).
- C4-01 ``ordenes_vencidas_48h`` (definición nueva de Jose, 2026-09-29):
  foto al cierre de la semana. Órdenes de fabricación abiertas (ni hechas ni
  canceladas) cuya fecha de fin programada (``mrp.production.date_finished``:
  en Odoo 19 es la fecha esperada mientras la orden no está hecha, y la real
  al cerrarla) venció hace más de 48 h ÷ órdenes abiertas. Como el estado de
  una orden en el pasado no se puede reconstruir (al cerrarla,
  ``date_finished`` pasa a ser la fecha real), solo se mide la semana recién
  cerrada, dentro de los 7 días siguientes a su cierre; después, sin dato.
- C1-04 ``desarrollos_vendidos``: artículos (``product.template`` de las
  categorías de ``quimibond_sgi.finished_product_categ_ids``) dados de alta
  en el mes de hace 6 meses con al menos un pedido de venta confirmado en los
  6 meses siguientes a su alta ÷ artículos dados de alta ese mes.
- RH-01 ``cobertura_plantilla``: por puesto con «Plantilla autorizada», los
  empleados que lo ocupan al cierre del periodo (hasta la plantilla: un
  puesto con gente de más no cubre a otro) ÷ plantilla autorizada.
- S4-01 ``bajas_registradas``: bajas del periodo (``departure_date``) con
  motivo, registradas (seguimiento nativo de ``departure_date`` y
  ``departure_reason_id``) a más tardar el día hábil siguiente a la salida.
- S6-02 ``bajas_accesos_equipo``: bajas del periodo con las actividades
  «Retirar accesos» y «Recoger equipo» (tipos de
  ``data/sgi_offboarding_plan_data.xml``, usados en el plan de producción
  «Baja de personal»; ver ``_sgi_adopt_offboarding_plan``) hechas a más tardar el día hábil
  siguiente a la salida. Fecha de hecho: el mensaje que Odoo publica al marcar
  la actividad como hecha (``mail_activity_type_id``, subtipo Actividades);
  si no está, ``mail.activity.date_done`` de la actividad archivada.

Los modos con plazo posterior al cierre del periodo (S2-01, S4-01, S6-02)
y la foto de C4-01 no se miden antes de que el plazo venza: la medición queda
«pendiente» y el cron diario la re-mide (``recompute_pending_measures``).
"""
import logging
from datetime import datetime, time, timedelta

import pytz
from dateutil.relativedelta import relativedelta

from odoo import api, fields, models

from .sgi_calendar import sgi_add_business_days
from .sgi_indicator_i3 import _param_ids

_logger = logging.getLogger(__name__)

DEFAULT_TZ = 'America/Mexico_City'
COMPLEMENT_DAY = 5          # día del mes siguiente (Art. 29 CFF, regla 2.7.1.32 RMF)
OVERDUE_HOURS = 48          # C4-01
SNAPSHOT_DAYS = 7           # C4-01: días después del cierre en que la foto aún vale
COHORT_MONTHS = 6           # C1-04
FINISHED_CATEGS_PARAM = 'quimibond_sgi.finished_product_categ_ids'
OFFBOARDING_TYPES = ('quimibond_sgi.sgi_activity_type_retirar_accesos',
                     'quimibond_sgi.sgi_activity_type_recoger_equipo')
# Plan de producción «Baja de personal» (id 5): qué renglón recibe qué tipo.
OFFBOARDING_PLAN_NAME = 'Baja de personal'
OFFBOARDING_RETYPE = (
    ('Desactivar usuario de Odoo, correo y accesos',
     'quimibond_sgi.sgi_activity_type_retirar_accesos'),
    ('Recuperar EPP', 'quimibond_sgi.sgi_activity_type_recuperar_epp'),
)
OFFBOARDING_COMPUTER_SUMMARY = "Recoger equipo de cómputo"

IND2_MODES = ('complementos_pago', 'desviacion_precio_compra', 'ordenes_vencidas_48h',
              'desarrollos_vendidos', 'cobertura_plantilla', 'bajas_registradas',
              'bajas_accesos_equipo')

# Clave de la ficha → modo.
IND2_ACTIVATE = {
    'S2-01': 'complementos_pago',
    'S1-05': 'desviacion_precio_compra',
    'C4-01': 'ordenes_vencidas_48h',
    'C1-04': 'desarrollos_vendidos',
    'RH-01': 'cobertura_plantilla',
    'S4-01': 'bajas_registradas',
    'S6-02': 'bajas_accesos_equipo',
}
# Sin historia en Odoo: se miden desde el mes siguiente a la activación.
IND2_FROM_NEXT_MONTH = ('S6-02',)

# Textos de ficha que cambian con las decisiones de Jose (2026-09-29).
S1_05_OLD_PHRASE = "lista de precios del proveedor"
S1_05_NEW_PHRASE = "precio de la orden de compra"
C4_01_FICHA = {
    'name': "Órdenes vencidas más de 48 horas",
    'formula': ("Órdenes de fabricación abiertas (ni hechas ni canceladas) con su fecha de "
                "fin programada vencida hace más de 48 horas al cierre de la semana ÷ "
                "órdenes abiertas al cierre de la semana × 100"),
    'source': ("Fabricación: órdenes abiertas y su fecha de fin programada, foto al cierre "
               "de la semana"),
    # El número ahora es de órdenes vencidas: más bajo es mejor. Metas espejo
    # de las anteriores (90 / 80 % a tiempo → 10 / 20 % vencidas).
    'direction': 'lower_better',
    'target_objective': 10.0,
    'target_acceptable': 20.0,
}


class SgiIndicatorInd2(models.Model):
    _inherit = 'sgi.indicator'

    # ------------------------------------------------------------------
    # Comunes
    # ------------------------------------------------------------------
    def _sgi_local_tz(self):
        return pytz.timezone(self._sgi_kpi_company().partner_id.tz or DEFAULT_TZ)

    def _sgi_local_date(self, value):
        """Fecha local (México) de un datetime de Odoo (UTC sin zona)."""
        return pytz.utc.localize(value).astimezone(self._sgi_local_tz()).date()

    def _sgi_local_midnight_utc(self, day):
        """00:00 hora local de ``day``, en UTC sin zona (como guarda Odoo)."""
        local = self._sgi_local_tz().localize(datetime.combine(day, time.min))
        return local.astimezone(pytz.utc).replace(tzinfo=None)

    def _sgi_ind2_ready_on(self, date_to):
        """Primer día en que el periodo que cierra en ``date_to`` ya se puede
        medir (su plazo venció), o False si el modo no espera nada."""
        self.ensure_one()
        mode = self.calc_mode
        if mode == 'complementos_pago':
            return self._sgi_complement_deadline(date_to) + timedelta(days=1)
        if mode in ('bajas_registradas', 'bajas_accesos_equipo'):
            return sgi_add_business_days(self.env, date_to, 1) + timedelta(days=1)
        if mode == 'ordenes_vencidas_48h':
            return date_to + timedelta(days=1)
        return False

    def _sgi_measure_vals(self, date_from, date_to):
        """Un periodo cuyo plazo aún no vence queda «pendiente» (sin valor ni
        semáforo) y el cron diario lo re-mide cuando ya vence."""
        self.ensure_one()
        if self.calc_mode in IND2_MODES:
            ready = self._sgi_ind2_ready_on(date_to)
            if ready and fields.Date.context_today(self) < ready:
                return {'state': 'pendiente', 'note': (
                    "Se calcula solo a partir del %s, cuando vence el plazo del "
                    "periodo; no hay que capturar nada." % ready.strftime('%d/%m/%Y'))}
        return super()._sgi_measure_vals(date_from, date_to)

    def _sgi_departures(self, date_from, date_to):
        """Empleados (archivados incluidos) de la compañía del KPI con fecha
        de salida dentro del periodo."""
        return self.env['hr.employee'].sudo().with_context(active_test=False).search([
            ('company_id', '=', self._sgi_kpi_company().id),
            ('departure_date', '>=', date_from), ('departure_date', '<=', date_to)])

    # ------------------------------------------------------------------
    # S2-01 — complementos de pago en plazo
    # ------------------------------------------------------------------
    @staticmethod
    def _sgi_complement_deadline(pay_date):
        """Último día (inclusive) para timbrar el complemento de un pago."""
        return pay_date.replace(day=1) + relativedelta(months=1, day=COMPLEMENT_DAY)

    def _sgi_complement_payments(self, date_from, date_to):
        """Pagos de clientes del periodo aplicados a facturas PPD, o None sin
        la localización mexicana."""
        env = self.env
        if ('l10n_mx_edi.document' not in env
                or 'l10n_mx_edi_payment_policy' not in env['account.move']._fields):
            return None
        return env['account.payment'].sudo().search([
            ('company_id', '=', self._sgi_kpi_company().id),
            ('payment_type', '=', 'inbound'), ('partner_type', '=', 'customer'),
            ('state', 'in', ('in_process', 'paid')),
            ('date', '>=', date_from), ('date', '<=', date_to),
            ('invoice_ids.l10n_mx_edi_payment_policy', '=', 'PPD')])

    def _detail_complementos_pago(self, date_from, date_to):
        payments = self._sgi_complement_payments(date_from, date_to)
        if payments is None:
            return {'value': None, 'note': "Requiere la localización mexicana (l10n_mx_edi)."}
        stamped = {}
        if payments:
            docs = self.env['l10n_mx_edi.document'].sudo().search([
                ('move_id', 'in', payments.move_id.ids), ('state', '=', 'payment_sent')])
            for doc in docs:
                prev = stamped.get(doc.move_id.id)
                stamped[doc.move_id.id] = min(prev, doc.datetime) if prev else doc.datetime
        on_time = missing = 0
        for payment in payments:
            when = stamped.get(payment.move_id.id)
            if not when:
                missing += 1
                continue
            limit = self._sgi_local_midnight_utc(
                self._sgi_complement_deadline(payment.date) + timedelta(days=1))
            if when < limit:
                on_time += 1
        out = self._ratio(on_time, len(payments), payments)
        if missing:
            out['note'] = "%d pago(s) a facturas PPD sin complemento timbrado." % missing
        return out

    def _calc_complementos_pago(self, date_from, date_to):
        return self._detail_complementos_pago(date_from, date_to)['value']

    # ------------------------------------------------------------------
    # S1-05 — desviación de precio de compra contra la OC
    # ------------------------------------------------------------------
    def _sgi_bill_lines_domain(self, date_from, date_to):
        return [('company_id', '=', self._sgi_kpi_company().id),
                ('parent_state', '=', 'posted'),
                ('move_id.move_type', '=', 'in_invoice'),
                ('display_type', '=', 'product'),
                ('move_id.invoice_date', '>=', date_from),
                ('move_id.invoice_date', '<=', date_to)]

    def _sgi_agreed_price(self, line):
        """Precio neto de la OC expresado en la moneda y la unidad de la línea
        de factura, o None si la unidad no se puede convertir."""
        po_line = line.purchase_line_id
        price = po_line.price_unit * (1.0 - (po_line.discount or 0.0) / 100.0)
        po_uom, bill_uom = po_line.product_uom_id, line.product_uom_id
        if po_uom and bill_uom and po_uom != bill_uom:
            if not po_uom._has_common_reference(bill_uom):
                return None
            price = po_uom._compute_price(price, bill_uom)
        po_currency = po_line.currency_id or po_line.order_id.currency_id
        move = line.move_id
        if po_currency and move.currency_id and po_currency != move.currency_id:
            price = po_currency._convert(price, move.currency_id, move.company_id,
                                         move.invoice_date or move.date)
        return price

    def _detail_desviacion_precio_compra(self, date_from, date_to):
        Line = self.env['account.move.line'].sudo()
        domain = self._sgi_bill_lines_domain(date_from, date_to)
        lines = Line.search(domain + [('purchase_line_id', '!=', False)])
        without_po = Line.search_count(domain + [('purchase_line_id', '=', False)])
        deviation = amount = over = under = 0.0
        compared = Line
        no_uom = 0
        for line in lines:
            if not line.price_subtotal:
                continue
            agreed = self._sgi_agreed_price(line)
            if agreed is None:
                no_uom += 1
                continue
            paid = line.price_unit * (1.0 - (line.discount or 0.0) / 100.0)
            # A moneda de la compañía con el tipo de cambio de la propia línea.
            rate = line.balance / line.price_subtotal
            diff = (paid - agreed) * line.quantity * rate
            if diff > 0:
                over += diff
            else:
                under -= diff
            deviation += abs(diff)
            amount += line.balance
            compared |= line
        out = self._ratio(round(deviation, 2), round(amount, 2), compared)
        total = Line._read_group(domain, [], ['balance:sum'])
        total_amount = (total[0][0] if total else 0.0) or 0.0
        money = self._sgi_money
        notes = [
            "Pagado de más: %s; pagado de menos: %s (moneda de la compañía)."
            % (money(over), money(under)),
            "Cobertura: %d de %d línea(s) con OC; %s de %s del importe."
            % (len(lines), len(lines) + without_po, money(amount), money(total_amount)),
        ]
        if without_po:
            notes.append("%d línea(s) sin OC quedaron fuera." % without_po)
        if no_uom:
            notes.append("%d línea(s) con una unidad que no se convierte a la de la OC "
                         "quedaron fuera." % no_uom)
        if notes:
            out['note'] = " ".join(notes)
        return out

    @staticmethod
    def _sgi_money(amount):
        return "{:,.2f}".format(amount or 0.0)

    def _calc_desviacion_precio_compra(self, date_from, date_to):
        return self._detail_desviacion_precio_compra(date_from, date_to)['value']

    # ------------------------------------------------------------------
    # C4-01 — órdenes abiertas vencidas más de 48 h (foto al cierre)
    # ------------------------------------------------------------------
    def _sgi_snapshot_valid(self, date_to):
        """La foto de C4-01 solo vale en los días siguientes al cierre del
        periodo: lo abierto HOY se toma como lo abierto al cierre."""
        today = fields.Date.context_today(self)
        return date_to < today <= date_to + timedelta(days=SNAPSHOT_DAYS)

    def _detail_ordenes_vencidas_48h(self, date_from, date_to):
        if not self._sgi_snapshot_valid(date_to):
            return {'value': None, 'note': (
                "Es una foto de las órdenes abiertas: solo se mide en los %d días "
                "siguientes al cierre del periodo; el pasado no se reconstruye."
                % SNAPSHOT_DAYS)}
        cut = self._sgi_local_midnight_utc(date_to + timedelta(days=1))
        orders = self.env['mrp.production'].sudo().search([
            ('company_id', '=', self._sgi_kpi_company().id),
            ('state', 'not in', ('done', 'cancel')),
            ('create_date', '<', cut)])
        limit = cut - timedelta(hours=OVERDUE_HOURS)
        overdue = orders.filtered(lambda o: o.date_finished and o.date_finished < limit)
        out = self._ratio(len(overdue), len(orders), orders)
        drafts = len(orders.filtered(lambda o: o.state == 'draft'))
        out['note'] = ("Foto del %s: %d orden(es) abiertas (%d en borrador), %d vencidas "
                       "hace más de %d h." % (fields.Date.context_today(self).strftime('%d/%m/%Y'),
                                              len(orders), drafts, len(overdue),
                                              OVERDUE_HOURS))
        return out

    def _calc_ordenes_vencidas_48h(self, date_from, date_to):
        return self._detail_ordenes_vencidas_48h(date_from, date_to)['value']

    # ------------------------------------------------------------------
    # C1-04 — desarrollos que se venden en sus primeros 6 meses
    # ------------------------------------------------------------------
    @staticmethod
    def _sgi_cohort_bounds(date_from, date_to):
        """El mismo periodo, 6 meses antes (fin de mes respetado: el periodo
        de septiembre mide la cohorte del 1 al 31 de marzo)."""
        return (date_from - relativedelta(months=COHORT_MONTHS),
                date_to + timedelta(days=1) - relativedelta(months=COHORT_MONTHS)
                - timedelta(days=1))

    def _detail_desarrollos_vendidos(self, date_from, date_to):
        env = self.env
        categs = env['product.category'].browse(_param_ids(env, FINISHED_CATEGS_PARAM)).exists()
        if not categs:
            return {'value': None, 'note': (
                "Configure las categorías de producto terminado (parámetro %s)."
                % FINISHED_CATEGS_PARAM)}
        company = self._sgi_kpi_company()
        cohort_from, cohort_to = self._sgi_cohort_bounds(date_from, date_to)
        c_from, c_to = self._sgi_dt_bounds(cohort_from, cohort_to)
        templates = env['product.template'].sudo().with_context(active_test=False).search([
            ('categ_id', 'child_of', categs.ids),
            ('company_id', 'in', (company.id, False)),
            ('create_date', '>=', c_from), ('create_date', '<', c_to)])
        sold = set()
        if templates:
            lines = env['sale.order.line'].sudo().with_context(active_test=False).search([
                ('company_id', '=', company.id),
                ('order_id.state', '=', 'sale'),
                ('product_id.product_tmpl_id', 'in', templates.ids),
                ('order_id.date_order', '>=', c_from)])
            for line in lines:
                template = line.product_id.product_tmpl_id
                if line.order_id.date_order <= template.create_date + relativedelta(
                        months=COHORT_MONTHS):
                    sold.add(template.id)
        out = self._ratio(len(sold), len(templates), templates)
        out['note'] = "Cohorte: artículos dados de alta del %s al %s." % (
            cohort_from.strftime('%d/%m/%Y'), cohort_to.strftime('%d/%m/%Y'))
        return out

    def _calc_desarrollos_vendidos(self, date_from, date_to):
        return self._detail_desarrollos_vendidos(date_from, date_to)['value']

    # ------------------------------------------------------------------
    # RH-01 — cobertura de plantilla
    # ------------------------------------------------------------------
    def _detail_cobertura_plantilla(self, date_from, date_to):
        env = self.env
        company = self._sgi_kpi_company()
        jobs = env['hr.job'].sudo().search([
            ('sgi_authorized_headcount', '>', 0), ('company_id', 'in', (company.id, False))])
        if not jobs:
            return {'value': None, 'note': "Ningún puesto tiene «Plantilla autorizada»."}
        Employee = env['hr.employee'].sudo().with_context(active_test=False)
        employees = Employee.search([
            ('company_id', '=', company.id), ('job_id', 'in', jobs.ids),
            '|', ('departure_date', '=', False), ('departure_date', '>', date_to)])
        # Archivado sin fecha de salida: no se sabe desde cuándo no está.
        employees = employees.filtered(lambda e: e.active or e.departure_date)
        if 'contract_date_start' in Employee._fields:
            employees = employees.filtered(
                lambda e: not e.contract_date_start or e.contract_date_start <= date_to)
        by_job = {}
        for employee in employees:
            by_job[employee.job_id.id] = by_job.get(employee.job_id.id, 0) + 1
        covered = sum(min(by_job.get(job.id, 0), job.sgi_authorized_headcount) for job in jobs)
        authorized = sum(jobs.mapped('sgi_authorized_headcount'))
        return self._ratio(covered, authorized, employees)

    def _calc_cobertura_plantilla(self, date_from, date_to):
        return self._detail_cobertura_plantilla(date_from, date_to)['value']

    # ------------------------------------------------------------------
    # S4-01 — bajas registradas con motivo al día hábil siguiente
    # ------------------------------------------------------------------
    def _sgi_departure_registered(self, employees):
        """{empleado: datetime UTC en que quedó registrada su baja}: el primer
        seguimiento que puso su fecha de salida actual y, si hay seguimiento
        del motivo, el primero que puso su motivo actual (el más tardío de los
        dos). Lee el seguimiento nativo del empleado y de su contrato
        (``hr.version``)."""
        env = self.env
        if not employees:
            return {}
        versions = env['hr.version'].sudo().with_context(active_test=False).search(
            [('employee_id', 'in', employees.ids)]) if 'hr.version' in env else None
        version_owner = {v.id: v.employee_id.id for v in versions} if versions else {}
        tracked = env['ir.model.fields'].sudo().search([
            ('model', 'in', ('hr.employee', 'hr.version')),
            ('name', 'in', ('departure_date', 'departure_reason_id'))])
        domain = [('field_id', 'in', tracked.ids)]
        owners = ['&', ('mail_message_id.model', '=', 'hr.employee'),
                  ('mail_message_id.res_id', 'in', employees.ids)]
        if version_owner:
            owners = ['|'] + owners + ['&', ('mail_message_id.model', '=', 'hr.version'),
                                       ('mail_message_id.res_id', 'in', list(version_owner))]
        values = env['mail.tracking.value'].sudo().search(domain + owners)
        by_id = {e.id: e for e in employees}
        first_date, first_reason = {}, {}
        for value in values:
            message = value.mail_message_id
            owner = message.res_id if message.model == 'hr.employee' \
                else version_owner.get(message.res_id)
            employee = by_id.get(owner)
            if not employee:
                continue
            when = message.date
            if value.field_id.name == 'departure_date':
                new = value.new_value_datetime
                if new and new.date() == employee.departure_date:
                    prev = first_date.get(owner)
                    first_date[owner] = min(prev, when) if prev else when
            elif employee.departure_reason_id \
                    and value.new_value_integer == employee.departure_reason_id.id:
                prev = first_reason.get(owner)
                first_reason[owner] = min(prev, when) if prev else when
        return {emp_id: max(when, first_reason.get(emp_id, when))
                for emp_id, when in first_date.items()}

    def _detail_bajas_registradas(self, date_from, date_to):
        employees = self._sgi_departures(date_from, date_to)
        registered = self._sgi_departure_registered(employees)
        on_time = no_reason = untracked = 0
        for employee in employees:
            when = registered.get(employee.id)
            if not employee.departure_reason_id:
                no_reason += 1
            elif not when:
                untracked += 1
            elif self._sgi_local_date(when) <= sgi_add_business_days(
                    self.env, employee.departure_date, 1):
                on_time += 1
        out = self._ratio(on_time, len(employees), employees)
        notes = []
        if no_reason:
            notes.append("%d baja(s) sin motivo de salida." % no_reason)
        if untracked:
            notes.append("%d baja(s) sin registro de cuándo se capturó la fecha de salida."
                         % untracked)
        if notes:
            out['note'] = " ".join(notes)
        return out

    def _calc_bajas_registradas(self, date_from, date_to):
        return self._detail_bajas_registradas(date_from, date_to)['value']

    # ------------------------------------------------------------------
    # S6-02 — bajas con accesos y equipo retirados al día hábil siguiente
    # ------------------------------------------------------------------
    def _sgi_offboarding_types(self):
        types = self.env['mail.activity.type']
        for xmlid in OFFBOARDING_TYPES:
            types |= self.env.ref(xmlid, raise_if_not_found=False) or types.browse()
        return types

    def _sgi_activity_done_dates(self, employees, types):
        """{(empleado, tipo): fecha local en que se marcó hecha} — la primera.
        Fuente: el mensaje «hecha» que publica ``mail.activity._action_done``
        (lleva ``mail_activity_type_id``); de respaldo, la actividad archivada
        con ``date_done``."""
        env = self.env
        done = {}
        if not employees or not types:
            return done
        subtype = env.ref('mail.mt_activities', raise_if_not_found=False)
        domain = [('model', '=', 'hr.employee'), ('res_id', 'in', employees.ids),
                  ('mail_activity_type_id', 'in', types.ids)]
        if subtype:
            domain.append(('subtype_id', '=', subtype.id))
        for message in env['mail.message'].sudo().search(domain):
            key = (message.res_id, message.mail_activity_type_id.id)
            day = self._sgi_local_date(message.date)
            done[key] = min(done[key], day) if key in done else day
        activities = env['mail.activity'].sudo().with_context(active_test=False).search([
            ('res_model', '=', 'hr.employee'), ('res_id', 'in', employees.ids),
            ('activity_type_id', 'in', types.ids),
            ('active', '=', False), ('date_done', '!=', False)])
        for activity in activities:
            done.setdefault((activity.res_id, activity.activity_type_id.id), activity.date_done)
        return done

    def _detail_bajas_accesos_equipo(self, date_from, date_to):
        types = self._sgi_offboarding_types()
        if len(types) < len(OFFBOARDING_TYPES):
            return {'value': None, 'note': "Faltan los tipos de actividad del plan de salida."}
        employees = self._sgi_departures(date_from, date_to)
        done = self._sgi_activity_done_dates(employees, types)
        on_time = pending = 0
        for employee in employees:
            limit = sgi_add_business_days(self.env, employee.departure_date, 1)
            days = [done.get((employee.id, t.id)) for t in types]
            if not all(days):
                pending += 1
            elif max(days) <= limit:
                on_time += 1
        out = self._ratio(on_time, len(employees), employees)
        if pending:
            out['note'] = ("%d baja(s) sin «Retirar accesos» o «Recoger equipo» marcadas como "
                           "hechas (¿se lanzó el plan de salida?)." % pending)
        return out

    def _calc_bajas_accesos_equipo(self, date_from, date_to):
        return self._detail_bajas_accesos_equipo(date_from, date_to)['value']

    # ------------------------------------------------------------------
    # Activación (migración 19.0.57.14.0)
    # ------------------------------------------------------------------
    @api.model
    def _sgi_activate_ind2(self):
        """57.14.0: las fichas de «indicadores 2» dejan de ser manuales. Mismo
        criterio que E1-02 en 57.6.0: solo el indicador ACTIVO con esa clave
        que siga en «manual» y sin términos de fórmula. S6-02 no tiene
        historia (el plan de salida se ajusta hoy): si no tiene «Medir desde»,
        se mide desde el mes siguiente. El modo anterior queda en el chatter.
        Idempotente. Devuelve {clave: [ids]}."""
        today = fields.Date.context_today(self)
        next_month = today.replace(day=1) + relativedelta(months=1)
        labels = dict(self._fields['calc_mode'].selection)
        done = {}
        for code, mode in IND2_ACTIVATE.items():
            indicators = self.search([('code', '=', code), ('calc_mode', '=', 'manual')])
            for indicator in indicators.filtered(lambda i: not i.term_ids):
                vals = {'calc_mode': mode}
                if code in IND2_FROM_NEXT_MONTH and not indicator.measure_from:
                    vals['measure_from'] = next_month
                indicator.write(vals)
                indicator.message_post(body=(
                    "57.14.0 (indicadores 2): el indicador pasa de «Captura manual» a «%s». "
                    "Para regresar: modo «Captura manual»." % labels.get(mode, mode)))
                done.setdefault(code, []).append(indicator.id)
        return done

    @api.model
    def _sgi_update_ind2_fichas(self):
        """57.14.0 (decisiones de Jose 2026-09-29), textos de ficha:

        - S1-05: en «De dónde sale el dato» (``source``) la frase «lista de
          precios del proveedor» pasa a «precio de la orden de compra». Solo
          esa frase.
        - C4-01: nombre, fórmula y fuente con la definición nueva (órdenes
          abiertas vencidas más de 48 h, foto al cierre de la semana). El
          sentido pasa a «más bajo es mejor» con metas espejo (10 / 20) solo si
          sigue en «más alto es mejor».

        Idempotente; el valor anterior de cada campo queda en el log y en el
        chatter. Devuelve {clave: [ids]} de lo que cambió."""
        changed = {}

        def _write(indicator, vals):
            vals = {k: v for k, v in vals.items() if indicator[k] != v}
            if not vals:
                return
            before = {k: indicator[k] for k in vals}
            _logger.info("SGI 57.14.0: ficha %s (id %s) antes: %s", indicator.code,
                         indicator.id, before)
            indicator.write(vals)
            indicator.message_post(body="57.14.0: ficha actualizada. Antes: %s" % "; ".join(
                "%s = %s" % (k, v) for k, v in before.items()))
            changed.setdefault(indicator.code, []).append(indicator.id)

        for indicator in self.search([('code', '=', 'S1-05')]):
            source = indicator.source or ''
            if S1_05_OLD_PHRASE in source:
                _write(indicator, {'source': source.replace(S1_05_OLD_PHRASE, S1_05_NEW_PHRASE)})
        for indicator in self.search([('code', '=', 'C4-01')]):
            vals = {k: C4_01_FICHA[k] for k in ('name', 'formula', 'source')}
            if indicator.direction == 'higher_better':
                vals.update({k: C4_01_FICHA[k] for k in
                             ('direction', 'target_objective', 'target_acceptable')})
            _write(indicator, vals)
        return changed

    @api.model
    def _sgi_adopt_offboarding_plan(self):
        """57.14.0 (S6-02): el plan de salida es el que ya existe en
        producción («Baja de personal», id 5), no uno nuevo. Sus renglones
        «Desactivar usuario de Odoo, correo y accesos» y «Recuperar EPP…»
        toman los tipos propios «Retirar accesos» y «Recuperar EPP» (EPP no es
        equipo de cómputo: S6-02 no lo cuenta), y se agrega «Recoger equipo de
        cómputo» con el tipo «Recoger equipo» (la ficha de S6-02 pide el
        equipo de cómputo), con el responsable y el plazo del renglón de
        accesos. Busca por plan (nombre, modelo empleado) y resumen; lo que no
        encuentra lo avisa en el log y sigue. Idempotente; nada se borra y el
        tipo anterior de cada renglón queda en el log. Devuelve la lista de
        cambios."""
        env = self.env
        report = []
        plans = env['mail.activity.plan'].sudo().with_context(active_test=False).search([
            ('res_model', '=', 'hr.employee'), ('name', '=like', OFFBOARDING_PLAN_NAME + '%')])
        if not plans:
            _logger.warning("SGI 57.14.0: no hay plan «%s…» de empleados; S6-02 no tiene plan "
                            "de salida.", OFFBOARDING_PLAN_NAME)
            return report
        for plan in plans:
            templates = plan.template_ids
            access_tpl = templates.browse()
            for prefix, xmlid in OFFBOARDING_RETYPE:
                act_type = env.ref(xmlid, raise_if_not_found=False)
                matches = templates.filtered(lambda t: (t.summary or '').startswith(prefix))
                if not matches or not act_type:
                    _logger.warning("SGI 57.14.0: el plan %s (id %s) no tiene el renglón «%s…».",
                                    plan.name, plan.id, prefix)
                    continue
                if xmlid == OFFBOARDING_TYPES[0]:
                    access_tpl = matches[:1]
                for template in matches.filtered(lambda t: t.activity_type_id != act_type):
                    _logger.info("SGI 57.14.0: plan %s, renglón %s «%s»: tipo %s (id %s) → %s.",
                                 plan.id, template.id, template.summary,
                                 template.activity_type_id.name, template.activity_type_id.id,
                                 act_type.name)
                    template.activity_type_id = act_type
                    report.append(('retype', template.id))
            computer = env.ref(OFFBOARDING_TYPES[1], raise_if_not_found=False)
            if computer and not templates.filtered(lambda t: t.activity_type_id == computer):
                vals = {'plan_id': plan.id, 'activity_type_id': computer.id,
                        'summary': OFFBOARDING_COMPUTER_SUMMARY,
                        'responsible_type': 'on_demand'}
                if access_tpl:
                    vals.update({
                        'sequence': access_tpl.sequence,
                        'responsible_type': access_tpl.responsible_type,
                        'responsible_id': access_tpl.responsible_id.id,
                        'delay_count': access_tpl.delay_count,
                        'delay_unit': access_tpl.delay_unit,
                        'delay_from': access_tpl.delay_from})
                template = env['mail.activity.plan.template'].sudo().create(vals)
                _logger.info("SGI 57.14.0: plan %s: renglón nuevo %s «%s».", plan.id,
                             template.id, OFFBOARDING_COMPUTER_SUMMARY)
                report.append(('create', template.id))
        return report


class SgiIndicatorMeasureInd2(models.Model):
    _inherit = 'sgi.indicator.measure'

    def action_view_evidence(self):
        """Los modos de «indicadores 2» no caben en un dominio de fecha: su
        evidencia son los registros guardados en la medición (o los del
        periodo, calculados ahora, si la medición no los guardó)."""
        self.ensure_one()
        indicator = self.indicator_id
        if indicator.calc_mode not in IND2_MODES:
            return super().action_view_evidence()
        if self.detail_model and self.detail_count:
            return self.action_view_records()
        date_from, date_to = indicator._sgi_period_bounds(self.period_date)
        detail = indicator._sgi_compute_detail(date_from, date_to) or {}
        return {
            'type': 'ir.actions.act_window',
            'name': "%s — evidencia de %s" % (indicator.name, self.period_date),
            'res_model': detail.get('model') or 'sgi.indicator.measure',
            'view_mode': 'list,form',
            'domain': [('id', 'in', detail.get('ids') or [])],
            'context': {'active_test': False},
        }


class HrJobHeadcount(models.Model):
    _inherit = 'hr.job'

    sgi_authorized_headcount = fields.Integer(
        string="Plantilla autorizada", tracking=True,
        help="Personas que Dirección autoriza para este puesto. RH-01 (cobertura "
             "de plantilla) compara contra ella a los empleados que lo ocupan.")
