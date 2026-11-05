# -*- coding: utf-8 -*-
"""3.5.0 (diseño v3, 2.6): presupuesto anual por año y revisión con los cinco mercados.

- Equipos de los mercados en Ajustes: Industrial, Confección y Especiales por nombre (solo si
  no están puestos).
- Los renglones de presupuesto toman el mercado del equipo de su documento.
- El precio presupuestal de los renglones existentes es el que ya tenían (precio de lista en
  pesos): el importe no cambia, y el VE-02 del aprobado (Industrial 2026) da lo mismo.
- Los presupuestos por equipo abiertos (borrador o revisado) pasan a obsoletos: ahora hay una
  sola revisión abierta por año. El aprobado se queda aprobado.
"""
import logging
import unicodedata

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

NAMES = {'industrial': 'industrial', 'confeccion': 'confeccion', 'especiales': 'especiales'}


def _norm(text):
    text = (text or '').strip().lower()
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Param = env['ir.config_parameter']
    team_by_base = {}
    for team in env['crm.team'].with_context(active_test=False).search([], order='id'):
        base = NAMES.get(_norm(team.name))
        if base and base not in team_by_base:
            team_by_base[base] = team
    for base, team in team_by_base.items():
        key = 'quimibond_ventas_presupuesto.team_%s' % base
        if not Param.get_param(key):
            Param.set_param(key, team.id)
    mercado_by_team = {team.id: base for base, team in team_by_base.items()}

    cr.execute("""SELECT l.id, b.team_id FROM sgi_sales_budget_line l
                    JOIN sgi_sales_budget b ON b.id = l.budget_id
                   WHERE b.kind = 'presupuesto' AND l.mercado IS NULL""")
    by_mercado = {}
    for line_id, team_id in cr.fetchall():
        by_mercado.setdefault(mercado_by_team.get(team_id, 'industrial'), []).append(line_id)
    for mercado, ids in by_mercado.items():
        cr.execute("UPDATE sgi_sales_budget_line SET mercado = %s WHERE id = ANY(%s)", (mercado, ids))

    cr.execute("""UPDATE sgi_sales_budget_line l
                     SET price_input = l.price_unit_budget, price_currency_id = l.currency_id,
                         price_basis = 'venta', price_origen = 'lista'
                    FROM sgi_sales_budget b
                   WHERE b.id = l.budget_id AND b.kind = 'presupuesto'
                     AND COALESCE(l.price_input, 0) = 0 AND l.price_unit_budget > 0""")
    priced = cr.rowcount
    cr.execute("""UPDATE sgi_sales_budget SET state = 'obsoleto'
                   WHERE kind = 'presupuesto' AND team_id IS NOT NULL
                     AND state IN ('borrador', 'revisado')""")
    closed = cr.rowcount

    env.invalidate_all()
    lines = env['sgi.sales.budget.line'].search([('kind', '=', 'presupuesto')])
    lines.modified(['mercado', 'price_input', 'price_currency_id', 'price_basis'])
    budgets = env['sgi.sales.budget'].with_context(active_test=False).search([])
    budgets.modified(['revision'])
    env.flush_all()
    _logger.info("3.5.0: equipos %s; %d renglón(es) con mercado, %d con precio; %d presupuesto(s) "
                 "abiertos por equipo pasaron a obsoletos.",
                 {b: t.name for b, t in team_by_base.items()},
                 sum(len(v) for v in by_mercado.values()), priced, closed)
