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
