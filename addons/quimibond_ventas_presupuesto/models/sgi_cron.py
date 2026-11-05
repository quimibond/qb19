# -*- coding: utf-8 -*-
"""Avisos del presupuesto y del pronóstico de ventas en el motor de crons del
SGI (``sgi.cron``): cierre de mes (aviso por debajo del umbral y
justificación del incumplimiento, P-A28 4.3.6.1), refresco de la foto de
facturado, cobertura semanal del pronóstico (P-A28 4.2.2.7) y revaluación
del S2 en junio (P-A28 Nota 1).

Vivía en quimibond_sgi/models/sgi_cron.py hasta 57.10.0; el núcleo deja el
gancho ``_sgi_monthly_close_steps`` (A-016).
"""
from collections import defaultdict
from datetime import date

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models

from odoo.addons.quimibond_sgi.models.sgi_guard import sgi_require_system


class SgiCronSalesBudget(models.AbstractModel):
    _inherit = 'sgi.cron'

    @api.model
    def _sgi_monthly_close_steps(self, first_prev, last_prev):
        res = super()._sgi_monthly_close_steps(first_prev, last_prev)
        self._sgi_step(
            "cierre de mes de presupuestos",
            lambda: self._sgi_sales_budget_month_close(first_prev, last_prev))
        # Refresca la foto de facturado/pedido de los presupuestos vigentes.
        self._sgi_step(
            "refresco de foto de presupuestos",
            lambda: self.env['sgi.sales.budget'].search(
                [('state', '!=', 'obsoleto')]).action_refresh_actuals())
        return res

    @api.model
    def cron_forecast_coverage(self):
        """Cron semanal (P-A28 4.2.2.7), 3.0.0 sobre el pronóstico vigente: por cliente,
        las semanas FIRMES con pronóstico sin pedido (qb.forecast.line.qty_neta) y los
        pedidos de las semanas firmes que no tienen pronóstico (qb.demand.line), en UNA
        actividad al Administrador de ventas sobre el cliente. Idempotente por clave."""
        sgi_require_system(self.env)  # F-008
        self = self._sgi_new_run()  # 56.37.0: cierre por episodio
        Forecast = self.env['qb.forecast.line']
        Forecast.action_qb_refresh()
        self.env['qb.demand.line'].action_qb_rebuild()
        user_id = self._sgi_sales_admin_user_id()

        def _fmt(value):
            return '{:,.0f}'.format(value or 0)
        firm = Forecast.search([('zona', '=', 'firme'), ('qty_neta', '>', 0)])
        firm_keys = {(l.partner_id.id, l.articulo_id.id, l.week) for l in Forecast.search([('zona', '=', 'firme')])}
        orphans = self.env['qb.demand.line'].search([('tipo', '=', 'pedido'), ('zona', '=', 'firme')]).filtered(
            lambda d: (d.partner_id.id, d.articulo_id.id, d.week) not in firm_keys)
        partners = firm.partner_id | orphans.partner_id

        def _process(partner):
            if not user_id:
                return
            parts = []
            mine = firm.filtered(lambda l: l.partner_id == partner)
            if mine:
                rows = ["%s · semana %s: pronóstico %s, pedido %s, sin pedido %s %s" % (
                    l.product_id.default_code or l.product_id.name, l.week, _fmt(l.qty_venta),
                    _fmt(l.qty_pedido), _fmt(l.qty_neta), l.uom_venta or '')
                    for l in mine.sorted(lambda x: (x.week, x.product_id.default_code or ''))]
                parts.append("Semanas firmes sin pedido:\n- " + "\n- ".join(rows))
            extra = orphans.filtered(lambda d: d.partner_id == partner)
            if extra:
                rows = ["Pedido sin pronóstico: %s · semana %s (%s)" % (
                    d.product_id.default_code or d.product_id.name, d.week, d.order_id.name or '')
                    for d in extra.sorted(lambda x: (x.week, x.product_id.default_code or ''))]
                parts.append("Pedidos fuera de pronóstico:\n- " + "\n- ".join(rows))
            if parts:
                self._sgi_schedule(
                    partner, "Cobertura del pronóstico %s (P-A28 4.2.2.7)" % partner.name,
                    "\n\n".join(parts), user_id,
                    date_deadline=fields.Date.context_today(self) + relativedelta(days=7),
                    key='cobertura_pronostico')
        failures = self._sgi_for_each(partners, _process, "cobertura del pronóstico")
        self._sgi_sweep(['cobertura_pronostico'], "el pronóstico ya está cubierto", failures)
        return True

    @api.model
    def _sgi_sales_budget_month_close(self, first_prev, last_prev):
        """Aviso de cierre de mes: si el acumulado facturado del año (la venta neta del
        VE-02) va por debajo del umbral del presupuesto aprobado acumulado
        (sales_budget_alert_pct), agenda una actividad al Administrador de ventas.
        3.5.0: un presupuesto por año con los cinco mercados, ya no uno por equipo.
        Idempotente (dedup por resumen)."""
        Param = self.env['ir.config_parameter'].sudo()
        pct = float(Param.get_param('quimibond_sgi.sales_budget_alert_pct', 80) or 0)
        # Umbral de justificación (P-A28 4.3.6.1): puede diferir del de aviso.
        min_pct = float(Param.get_param('quimibond_sgi.budget_fulfillment_min', 80) or 0)
        sales_admin_id = self._sgi_sales_admin_user_id()
        year = first_prev.year
        budgets = self.env['sgi.sales.budget'].search([
            ('kind', '=', 'presupuesto'),
            ('state', '=', 'aprobado'), ('year', '=', year)])

        def _process(budget):
            budgeted = sum(budget.line_ids.filtered(
                lambda l: l.date and l.date <= last_prev).mapped('amount_budget'))
            if not budgeted:
                return
            # 3.5.4: la venta de los equipos de los mercados, la misma del cumplimiento de la ficha.
            real = -sum(budget._qb_sales_lines(date_to=last_prev).mapped('balance'))
            achieved = real / budgeted * 100.0
            # Justificación del incumplimiento (P-A28 4.3.6.1): si va por debajo del
            # mínimo y AÚN no hay justificación capturada, se pide al Admin de ventas.
            # No bloquea nada: es evidencia del análisis.
            if achieved < min_pct and not (budget.nonfulfillment_note or '').strip() \
                    and sales_admin_id:
                self._sgi_schedule(
                    budget,
                    "Justificar incumplimiento del presupuesto %s (%.0f%%) — "
                    "P-A28 4.3.6.1" % (budget.year, achieved),
                    "El presupuesto va en %.1f%% (mínimo %.0f%%) y no tiene "
                    "justificación. Captura el análisis del incumplimiento en el "
                    "campo «Justificación de incumplimiento»." % (achieved, min_pct),
                    sales_admin_id, date_deadline=last_prev + relativedelta(days=15),
                    key='presupuesto_justificar:%d' % year)
            if achieved >= pct or not sales_admin_id:
                return
            top = self._sgi_budget_top_gaps(budget, last_prev)
            note = ("El acumulado facturado del año va en %.1f%% del presupuesto "
                    "aprobado. Por mercado: %s. Revise el pipeline y las acciones "
                    "comerciales." % (achieved, budget.amount_by_mercado_text or '—'))
            if top:
                note += "\nProductos con mayor brecha (ppto − facturado):\n- " + \
                    "\n- ".join(top)
            self._sgi_schedule(
                budget,
                "Presupuesto %s por debajo del %.0f%% al cierre de %s" % (
                    budget.year, pct, first_prev.strftime('%m/%Y')),
                note, sales_admin_id, date_deadline=last_prev + relativedelta(days=15),
                key='presupuesto_bajo:%s' % first_prev.strftime('%Y-%m'))

        self._sgi_for_each(budgets, _process, "cierre de mes de presupuesto")
        return True

    @api.model
    def _sgi_budget_top_gaps(self, budget, last_prev, limit=5):
        """Los productos con mayor brecha (amount_budget − amount_real) del
        presupuesto hasta el mes (accionable: dónde se está quedando corto)."""
        gaps = defaultdict(lambda: [0.0, 0.0])  # producto o proyecto -> [ppto, real]
        for line in budget.line_ids.filtered(
                lambda l: l.date and l.date <= last_prev):
            gaps[line._qb_label()][0] += line.amount_budget
            gaps[line._qb_label()][1] += line.amount_real
        ranked = sorted(
            ((label, vals[0] - vals[1]) for label, vals in gaps.items()),
            key=lambda kv: kv[1], reverse=True)
        currency = budget.currency_id
        out = []
        for label, gap in ranked[:limit]:
            if gap <= 0:
                break
            out.append("%s: %s %s" % (label, '{:,.0f}'.format(gap), currency.name or ''))
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
        presupuesto aprobado del año (enlaza a la acción «Nueva revisión»).
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
                "genera la siguiente revisión con «Nueva revisión»." % (
                    budget.folio or budget.name),
                sales_admin_id, date_deadline=date(year, 6, 30),
                key='revaluar_s2:%d' % year)

        self._sgi_for_each(budgets, _process, "revaluación del S2")
        return True
