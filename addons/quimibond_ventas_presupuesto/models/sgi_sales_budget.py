# -*- coding: utf-8 -*-
"""Presupuesto maestro de ventas (F-P-A28-18 / F-P-A28-17).

Matriz tipo MPS: por mercado (equipo de ventas) y año, filas = producto y
columnas = enero…diciembre en cantidad y pesos. El REAL se calcula solo desde lo
facturado (la cifra dura) y, complementariamente, desde lo pedido.

Dos invariantes del negocio, tratadas con cuidado:
  · UNIDADES — vendemos en metros, kg, rollos y piezas. Cada línea lleva su
    unidad; las cantidades NUNCA se suman entre unidades distintas (los totales
    de cantidad son POR unidad). El único total global es el de dinero.
  · DIVISAS — facturamos MXN y USD. Todos los montos van en moneda de la
    compañía; el real en dinero sale de account.move.line.balance (contabilidad
    ya convirtió cada factura a su tipo de cambio de la fecha — no reconvertimos).

Este archivo tiene la CABECERA (sgi.sales.budget) y los helpers de módulo
compartidos (_REAL_MOVE_TYPES, _convert_qty). La LÍNEA (sgi.sales.budget.line)
vive en sgi_sales_budget_line.py, solo separada por tamaño.
"""
from collections import defaultdict
from datetime import date

from markupsafe import Markup

from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError

_REAL_MOVE_TYPES = ('out_invoice', 'out_refund')

# 3.5.0 (diseño v3, 2.6): el presupuesto es por año y revisión, con los cinco
# mercados dentro. El mercado va en la línea; el equipo de ventas de cada
# mercado base es un parámetro (Ajustes), no se adivina por nombre.
MERCADOS = [
    ('industrial', "Industrial"),
    ('proyectos_industrial', "Proyectos industrial"),
    ('confeccion', "Confección"),
    ('proyectos_confeccion', "Proyectos confección"),
    ('especiales', "Especiales"),
]
MERCADO_BASE = {
    'industrial': 'industrial', 'proyectos_industrial': 'industrial',
    'confeccion': 'confeccion', 'proyectos_confeccion': 'confeccion',
    'especiales': 'especiales',
}
PROYECTO_MERCADOS = ('proyectos_industrial', 'proyectos_confeccion')
# Mercados donde el renglón lleva cliente (confección se presupuesta sin cliente).
MERCADOS_CON_CLIENTE = ('industrial', 'especiales')
TEAM_PARAM = 'quimibond_ventas_presupuesto.team_%s'
OBJETIVO_PARAM = 'quimibond_ventas_presupuesto.objetivo_pct'
MINIMO_PARAM = 'quimibond_ventas_presupuesto.minimo_pct'


def mercado_team(env, mercado):
    """Equipo de ventas del mercado base de ``mercado`` (parámetro de Ajustes)."""
    base = MERCADO_BASE.get(mercado)
    if not base:
        return env['crm.team']
    team_id = int(env['ir.config_parameter'].sudo().get_param(TEAM_PARAM % base, 0) or 0)
    return env['crm.team'].sudo().browse(team_id).exists() if team_id else env['crm.team']


def team_mercado(env, team):
    """Mercado base del equipo ``team`` según los parámetros; None si no es de ninguno."""
    if not team:
        return None
    param = env['ir.config_parameter'].sudo()
    for base in ('industrial', 'confeccion', 'especiales'):
        if int(param.get_param(TEAM_PARAM % base, 0) or 0) == team.id:
            return base
    return None


def default_mercado(env, partner):
    """Mercado de un renglón nuevo sin mercado: el del equipo del cliente; sin cliente,
    confección (es el único mercado que se presupuesta sin cliente); si no, industrial."""
    if not partner:
        return 'confeccion'
    return team_mercado(env, partner.commercial_partner_id.team_id or partner.team_id) or 'industrial'


def _convert_qty(qty, from_uom, to_uom):
    """Convierte `qty` de `from_uom` a `to_uom` si son de la misma categoría de
    unidad; devuelve None si son incompatibles (no se pueden sumar entre sí).

    Centraliza el patrón _has_common_reference + _compute_quantity repetido en
    todos los agregados de cantidad (facturado, comprometido, pedido, demanda)."""
    if from_uom and to_uom and from_uom._has_common_reference(to_uom):
        return from_uom._compute_quantity(qty, to_uom, round=False)
    return None


class SgiSalesBudget(models.Model):
    _name = 'sgi.sales.budget'
    _description = "Presupuesto de ventas (F-P-A28-17/18)"
    _inherit = ['sgi.base.mixin', 'sgi.format.mixin']
    _order = 'year desc, revision desc'
    _sgi_sequence_code = 'sgi.sales.budget'
    _sgi_locked_states = ('aprobado', 'reemplazado')

    _folio_uniq = models.Constraint(
        'unique(folio)', "Ya existe un presupuesto con ese folio.")

    year = fields.Integer(string="Año", required=True, tracking=True,
                          default=lambda self: fields.Date.context_today(self).year)
    # 3.5.0: un documento por año y revisión con los cinco mercados (diseño v3, 2.6).
    # REV0 es la propuesta del año; REV1, la revaluación de junio. Solo una aprobada
    # por año: al aprobar la nueva, la anterior queda «reemplazada» (congelada).
    revision = fields.Integer(string="Revisión", default=0, required=True,
                              tracking=True)
    revision_label = fields.Char(string="Revisión (etiqueta)", compute='_compute_revision_label',
                                 store=True)
    revision_date = fields.Date(string="Fecha de la revisión", copy=False, tracking=True,
                                default=fields.Date.context_today)
    # 3.0.0: el pronóstico ya no es un documento; vive en qb.forecast.line. Los
    # pronósticos viejos (kind='pronostico') quedan obsoletos, en solo lectura,
    # en Presupuesto y pronóstico → Histórico. No se crean nuevos.
    kind = fields.Selection([
        ('presupuesto', "Presupuesto anual"),
        ('pronostico', "Pronóstico semanal (histórico)"),
    ], string="Tipo", default='presupuesto', required=True, tracking=True,
        help="Presupuesto: el anual por mes con los cinco mercados (F-P-A28-18). "
             "Pronóstico: documento por cliente y año de antes de 3.0.0, solo "
             "histórico; el pronóstico vigente está en Pronóstico vigente.")
    partner_id = fields.Many2one(
        'res.partner', string="Cliente del pronóstico",
        domain="[('is_company', '=', True)]",
        help="Solo en los pronósticos históricos (un documento por cliente y año).")
    rate_ids = fields.One2many('qb.sales.budget.rate', 'budget_id', string="Tipo de cambio por mes",
                               copy=True)
    rate_default = fields.Float(
        string="Tipo de cambio para todos los meses", digits=(12, 4), copy=False,
        help="Escriba el tipo de cambio y use «Aplicar a los 12 meses».")
    objetivo_pct = fields.Float(
        string="Objetivo de crecimiento (%)", tracking=True,
        default=lambda self: float(self.env['ir.config_parameter'].sudo().get_param(
            OBJETIVO_PARAM, 20) or 0),
        help="Crecimiento contra la venta del año anterior que se busca (20 % por omisión).")
    minimo_pct = fields.Float(
        string="Crecimiento mínimo (%)", tracking=True,
        default=lambda self: float(self.env['ir.config_parameter'].sudo().get_param(
            MINIMO_PARAM, 15) or 0),
        help="Crecimiento contra el año anterior por debajo del cual la propuesta no alcanza (15 %).")
    base_anio_anterior = fields.Monetary(
        string="Venta del año anterior", copy=False, readonly=True,
        help="Venta neta del año anterior (la del VE-02). Se calcula al enviar a revisión y se "
             "guarda al aprobar; «Actualizar base» la recalcula en borrador.")
    objetivo_amount = fields.Monetary(string="Objetivo", compute='_compute_objetivo')
    minimo_amount = fields.Monetary(string="Mínimo", compute='_compute_objetivo')
    growth_pct = fields.Float(string="Crecimiento presupuestado (%)", compute='_compute_objetivo')
    amount_by_mercado_text = fields.Char(string="Por mercado", compute='_compute_amount_by_mercado')
    name = fields.Char(string="Nombre", compute='_compute_name', store=True)
    company_id = fields.Many2one('res.company', string="Compañía",
                                 default=lambda self: self.env.company, required=True)
    currency_id = fields.Many2one('res.currency', related='company_id.currency_id',
                                  string="Moneda", readonly=True)
    # Ciclo (P-A28 Rev.15, 4.2.2): borrador → revisado (Admin de ventas, 4.2.2.3) →
    # aprobado (alta dirección, 4.2.2.4), congelado. 3.5.0: al aprobar una revisión
    # nueva, la aprobada anterior pasa a «reemplazado» (congelada y comparable).
    state = fields.Selection([
        ('borrador', "Borrador"),
        ('revisado', "Revisado"),
        ('aprobado', "Aprobado"),
        ('reemplazado', "Reemplazado"),
        ('obsoleto', "Obsoleto"),
    ], string="Estado", default='borrador', required=True, tracking=True)
    line_ids = fields.One2many('sgi.sales.budget.line', 'budget_id',
                               string="Líneas del presupuesto", copy=True)
    line_count = fields.Integer(string="# Líneas", compute='_compute_line_count')

    # Totales — SOLO de dinero (moneda compañía). Las cantidades no se totalizan
    # globalmente porque mezclarían unidades distintas.
    amount_budget_total = fields.Monetary(string="Presupuesto (total)",
                                          compute='_compute_amount_totals')
    amount_real_total = fields.Monetary(string="Facturado (total)",
                                        compute='_compute_amount_totals')
    amount_ordered_total = fields.Monetary(string="Pedido (total)",
                                           compute='_compute_amount_totals')
    fulfillment_pct = fields.Float(string="% Cumplimiento (importe)",
                                   compute='_compute_amount_totals')
    amount_real_unbudgeted = fields.Monetary(
        string="Facturado NO presupuestado",
        compute='_compute_unbudgeted',
        help="Facturación neta del equipo en el año que no matchea ninguna línea "
             "(ni global ni por cliente): total real del equipo menos el real "
             "capturado por las líneas. Producto/cliente vendido sin presupuestar.")
    qty_budget_text = fields.Char(string="Cantidad presupuestada (por unidad)",
                                  compute='_compute_qty_texts')
    qty_real_text = fields.Char(string="Cantidad facturada (por unidad)",
                                compute='_compute_qty_texts')
    unconverted_count = fields.Integer(
        string="Facturas sin convertir (otra unidad)",
        compute='_compute_unconverted_count',
        help="Líneas de factura del periodo cuya unidad es de otra categoría que "
             "la presupuestada: se cuentan en importe pero NO en cantidad. El "
             "hueco queda visible aquí para corregir la unidad.")
    amount_currency_text = fields.Char(
        string="Totales por divisa", compute='_compute_amount_currency_text',
        help="Totales POR divisa de las líneas cuya lista no es la moneda de la "
             "compañía. Nunca se suman entre sí: el único total global es en pesos.")
    no_price_count = fields.Integer(
        string="Renglones sin precio", compute='_compute_problem_counts',
        help="Renglones con cantidad y sin precio presupuestal. Cuentan 0 en el importe (nunca el "
             "precio de venta del producto). Capture el precio o corrija la lista y use "
             "«Sugerir precios de la lista».")
    no_rate_count = fields.Integer(
        string="Renglones sin tipo de cambio", compute='_compute_problem_counts',
        help="Renglones con precio en dólares en un mes sin tipo de cambio: cuentan 0.")
    no_conversion_count = fields.Integer(
        string="Renglones sin conversión", compute='_compute_problem_counts',
        help="El precio es por metro o por kilo y la cantidad no se pudo convertir (falta el "
             "rendimiento del producto, o el gramaje y el ancho del proyecto): cuentan 0.")
    no_client_count = fields.Integer(
        string="Renglones sin cliente", compute='_compute_problem_counts',
        help="Renglones de Industrial o Especiales sin cliente: no se puede aprobar así.")
    price_deviation_count = fields.Integer(
        string="Desviaciones de precio", compute='_compute_price_deviation_count',
        help="Líneas con desviación de precio leve o grave (facturado vs lista).")
    # KPI y justificación (P-A28 4.3.6.1): no bloquea nada; es evidencia del
    # análisis del incumplimiento, no un candado.
    nonfulfillment_note = fields.Text(
        string="Justificación de incumplimiento (F-P-A28 4.3.6.1)",
        help="Análisis del porqué del incumplimiento del presupuesto (bajo el "
             "umbral de cumplimiento). No bloquea: es justificación, no candado.")
    needs_justification = fields.Boolean(
        string="Requiere justificación", compute='_compute_needs_justification',
        help="El presupuesto aprobado va por debajo del umbral de cumplimiento "
             "(Ajustes → 'Cumplimiento mínimo del presupuesto') y aún no tiene "
             "justificación capturada.")

    @api.depends('fulfillment_pct', 'state', 'kind', 'amount_budget_total',
                 'nonfulfillment_note')
    def _compute_needs_justification(self):
        threshold = float(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.budget_fulfillment_min', 80) or 0)
        for budget in self:
            budget.needs_justification = bool(
                budget.kind == 'presupuesto' and budget.state == 'aprobado'
                and budget.amount_budget_total > 0
                and budget.fulfillment_pct < threshold
                and not (budget.nonfulfillment_note or '').strip())

    @api.depends('year', 'revision', 'kind', 'partner_id')
    def _compute_name(self):
        for budget in self:
            if budget.kind == 'pronostico':
                budget.name = "Pronóstico %s %s Rev.%s" % (
                    budget.partner_id.name or "?", budget.year or "?", budget.revision or 1)
            else:
                budget.name = "Presupuesto %s %s" % (budget.year or "?", budget.revision_label)

    @api.depends('revision')
    def _compute_revision_label(self):
        for budget in self:
            budget.revision_label = "REV%d" % (budget.revision or 0)

    @api.depends('base_anio_anterior', 'objetivo_pct', 'minimo_pct', 'amount_budget_total')
    def _compute_objetivo(self):
        for budget in self:
            base = budget.base_anio_anterior
            budget.objetivo_amount = base * (1 + budget.objetivo_pct / 100.0)
            budget.minimo_amount = base * (1 + budget.minimo_pct / 100.0)
            budget.growth_pct = ((budget.amount_budget_total / base - 1) * 100.0) if base else 0.0

    @api.depends('line_ids.amount_budget', 'line_ids.mercado')
    def _compute_amount_by_mercado(self):
        labels = dict(MERCADOS)
        for budget in self:
            by_mercado = defaultdict(float)
            for line in budget.line_ids:
                by_mercado[line.mercado or ''] += line.amount_budget
            parts = ["%s %s" % (labels.get(key, "Sin mercado"), '{:,.0f}'.format(amount))
                     for key, amount in sorted(by_mercado.items(),
                                               key=lambda kv: [k for k, _l in MERCADOS].index(kv[0])
                                               if kv[0] in labels else 99)
                     if amount]
            budget.amount_by_mercado_text = " · ".join(parts)

    def _qb_company_net_invoiced(self, date_from, date_to):
        """Venta neta de la compañía en el rango, con la misma regla que el VE-02."""
        self.ensure_one()
        indicator = self.env['sgi.indicator'].sudo().with_company(self.company_id)
        return indicator._sgi_net_invoiced(date_from, date_to)

    def action_qb_update_base(self):
        """Venta del año anterior (la del VE-02) para comparar contra el objetivo y el mínimo."""
        for budget in self:
            if budget.state not in ('borrador', 'revisado'):
                raise UserError("La base del año anterior se actualiza en borrador o revisado.")
            budget.base_anio_anterior = budget._qb_company_net_invoiced(
                date(budget.year - 1, 1, 1), date(budget.year - 1, 12, 31))
        return True

    def action_qb_apply_rate_all(self):
        """El mismo tipo de cambio en los 12 meses."""
        for budget in self:
            if budget.rate_default <= 0:
                raise UserError("Escriba un tipo de cambio mayor a cero.")
            existing = {r.month: r for r in budget.rate_ids}
            for month in range(1, 13):
                if month in existing:
                    existing[month].rate = budget.rate_default
                else:
                    self.env['qb.sales.budget.rate'].create(
                        {'budget_id': budget.id, 'month': month, 'rate': budget.rate_default})
        return True

    def _qb_month_rate(self, month):
        """Tipo de cambio USD/MXN del mes: el del documento; si no hay, el presupuestal de
        Ajustes; 0 si tampoco (el renglón en dólares cuenta 0 y queda en «sin tipo de cambio»)."""
        self.ensure_one()
        for rate in self.rate_ids:
            if rate.month == month and rate.rate > 0:
                return rate.rate
        return float(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.budget_planning_rate', 0) or 0)

    def _qb_freeze_rates(self):
        """Al aprobar: los meses sin tipo de cambio propio se guardan con el que se usó (el de
        Ajustes), para que el aprobado no cambie si cambia el parámetro."""
        fallback = float(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sgi.budget_planning_rate', 0) or 0)
        if fallback <= 0:
            return
        for budget in self:
            have = {r.month for r in budget.rate_ids if r.rate > 0}
            months = {l.date.month for l in budget.line_ids if l.date} - have
            for month in sorted(months):
                budget.rate_ids.filtered(lambda r, m=month: r.month == m).sudo().unlink()
                self.env['qb.sales.budget.rate'].sudo().create(
                    {'budget_id': budget.id, 'month': month, 'rate': fallback})

    @api.depends('line_ids')
    def _compute_line_count(self):
        for budget in self:
            budget.line_count = len(budget.line_ids)

    @api.depends('line_ids.amount_budget', 'line_ids.amount_real',
                 'line_ids.amount_ordered')
    def _compute_amount_totals(self):
        for budget in self:
            lines = budget.line_ids
            budget.amount_budget_total = sum(lines.mapped('amount_budget'))
            budget.amount_real_total = sum(lines.mapped('amount_real'))
            budget.amount_ordered_total = sum(lines.mapped('amount_ordered'))
            budget.fulfillment_pct = (
                round(budget.amount_real_total / budget.amount_budget_total * 100.0, 2)
                if budget.amount_budget_total else 0.0)

    @api.depends('line_ids.qty_budget', 'line_ids.qty_real', 'line_ids.uom_id')
    def _compute_qty_texts(self):
        for budget in self:
            budget.qty_budget_text = budget._sgi_qty_text('qty_budget')
            budget.qty_real_text = budget._sgi_qty_text('qty_real')

    def _sgi_qty_text(self, field_name):
        """Totaliza una cantidad POR unidad (nunca mezcla): '12,500 m · 3,200 kg'."""
        self.ensure_one()
        by_uom = defaultdict(float)
        for line in self.line_ids:
            if line.uom_id:
                by_uom[line.uom_id] += line[field_name]
        parts = []
        for uom, qty in sorted(by_uom.items(), key=lambda kv: kv[0].name or ''):
            parts.append("%s %s" % ('{:,.2f}'.format(qty).rstrip('0').rstrip('.'),
                                    uom.name))
        return " · ".join(parts)

    @api.depends('line_ids.unconverted_count')
    def _compute_unconverted_count(self):
        for budget in self:
            budget.unconverted_count = sum(budget.line_ids.mapped('unconverted_count'))

    @api.depends('line_ids.amount_currency', 'line_ids.price_currency_id',
                 'amount_budget_total', 'currency_id')
    def _compute_amount_currency_text(self):
        for budget in self:
            company_ccy = budget.currency_id
            by_ccy = defaultdict(float)
            for line in budget.line_ids:
                if line.price_currency_id and line.price_currency_id != company_ccy:
                    by_ccy[line.price_currency_id] += line.amount_currency
            parts = ["%s %s" % (ccy.name, '{:,.0f}'.format(amt))
                     for ccy, amt in sorted(by_ccy.items(), key=lambda kv: kv[0].name)]
            parts.append("Total compañía %s %s" % (
                company_ccy.name or '', '{:,.0f}'.format(budget.amount_budget_total)))
            budget.amount_currency_text = " · ".join(parts)

    @api.depends('line_ids.price_problem')
    def _compute_problem_counts(self):
        for budget in self:
            problems = budget.line_ids.mapped('price_problem')
            budget.no_price_count = problems.count('sin_precio')
            budget.no_rate_count = problems.count('sin_tc')
            budget.no_conversion_count = problems.count('sin_conversion')
            budget.no_client_count = len(budget.line_ids.filtered(
                lambda l: l.mercado in MERCADOS_CON_CLIENTE and not l.partner_id))

    def action_view_no_price(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Renglones con problema de precio — %s" % self.name,
            'res_model': 'sgi.sales.budget.line',
            'view_mode': 'list,form',
            'domain': [('budget_id', '=', self.id), ('price_problem', '!=', False)],
        }

    @api.depends('line_ids.price_gap_alert')
    def _compute_price_deviation_count(self):
        for budget in self:
            budget.price_deviation_count = len(budget.line_ids.filtered(
                lambda l: l.price_gap_alert in ('leve', 'grave')))

    def action_view_price_deviations(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Desviaciones de precio — %s" % self.name,
            'res_model': 'sgi.sales.budget.line',
            'view_mode': 'list,form',
            'domain': [('budget_id', '=', self.id),
                       ('price_gap_alert', 'in', ('leve', 'grave'))],
        }

    def action_price_coverage_report(self):
        """Auditoría de cobertura (en cualquier estado): las líneas sin precio de
        lista, para revisar ANTES de intentar aprobar."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Cobertura de precios — %s" % self.name,
            'res_model': 'sgi.sales.budget.line',
            'view_mode': 'list,form',
            'domain': [('budget_id', '=', self.id), ('has_list_price', '=', False)],
        }

    def _qb_year_moves(self):
        """Facturas y notas de crédito publicadas del año en los equipos de los mercados
        (Ajustes): el real del presupuesto mide esos equipos, como las líneas."""
        self.ensure_one()
        teams = self.env['crm.team']
        for base in ('industrial', 'confeccion', 'especiales'):
            teams |= mercado_team(self.env, base)
        if not self.year or not teams:
            return self.env['account.move']
        return self.env['account.move'].sudo().search([
            ('move_type', 'in', _REAL_MOVE_TYPES),
            ('state', '=', 'posted'),
            ('team_id', 'in', teams.ids),
            ('company_id', '=', self.company_id.id),  # solo la compañía del ppto
            ('invoice_date', '>=', date(self.year, 1, 1)),
            ('invoice_date', '<=', date(self.year, 12, 31)),
        ])

    def _sgi_team_year_real(self):
        """Facturación neta del año en los equipos de los mercados (moneda compañía).
        Base para detectar lo vendido sin presupuestar."""
        self.ensure_one()
        return sum(self._qb_year_moves().mapped('amount_untaxed_signed'))

    @api.depends('line_ids.amount_real', 'year')
    def _compute_unbudgeted(self):
        for budget in self:
            if budget.kind != 'presupuesto':
                # N5 (diseño v3): en un pronóstico daba el facturado de todo el equipo.
                budget.amount_real_unbudgeted = 0.0
                continue
            captured = sum(budget.line_ids.mapped('amount_real'))
            budget.amount_real_unbudgeted = budget._sgi_team_year_real() - captured

    # --- Conciliación facturado vs contabilidad (cuadre del presupuesto) ------
    def _sgi_reconcile_data(self):
        """Descompone el facturado contable del equipo en el año contra lo que
        capturaron las líneas del presupuesto, para explicar la brecha:

          A  = facturado contable del equipo (out_invoice/out_refund posted del
               año, amount_untaxed_signed);
          B  = suma de amount_real de las líneas del presupuesto (lo capturado);
          partidas que explican A − B (cada una con monto):
            · unbudgeted        productos facturados del equipo NO presupuestados;
            · no_product        líneas de factura sin producto (fletes/servicios);
            · by_client_others  facturado de productos presupuestados POR CLIENTE a
                                OTROS clientes distintos del presupuestado;
          residual = A − (B + partidas) → 0 si la foto real está fresca y cuadra;
          no_team  = informativo (FUERA de A): facturas del año SIN equipo de
                     ventas (la fuga principal del cuadre).

        Objetivo verificable: A = B + unbudgeted + no_product + by_client_others +
        residual. Trabaja sobre la foto real vigente (amount_real); refresca antes
        (action_refresh_actuals) para que el residuo sea solo redondeo."""
        self.ensure_one()
        result = {
            'A': 0.0, 'B': 0.0, 'unbudgeted': 0.0, 'no_product': 0.0,
            'by_client_others': 0.0, 'residual': 0.0, 'captured_aml': 0.0,
            'no_team': 0.0, 'currency': self.currency_id,
        }
        if self.kind != 'presupuesto':
            return result
        result['B'] = sum(self.line_ids.mapped('amount_real'))
        # Esquema presupuestado por producto (global vs por cliente).
        global_products = set()
        by_client = defaultdict(set)
        for line in self.line_ids:
            if line.partner_id:
                by_client[line.product_id.id].add(
                    line.partner_id.commercial_partner_id.id)
            else:
                global_products.add(line.product_id.id)
        moves = self._qb_year_moves()
        captured = 0.0
        for move in moves:
            result['A'] += move.amount_untaxed_signed
            cp = move.commercial_partner_id
            for aml in move.invoice_line_ids.filtered(
                    lambda l: l.display_type == 'product'):
                contrib = -aml.balance  # base sin impuestos, moneda compañía, signada
                product = aml.product_id
                if not product:
                    result['no_product'] += contrib
                elif product.id in global_products:
                    captured += contrib
                elif product.id in by_client:
                    if cp.id in by_client[product.id]:
                        captured += contrib
                    else:
                        result['by_client_others'] += contrib
                else:
                    result['unbudgeted'] += contrib
        result['captured_aml'] = captured
        result['residual'] = (
            result['A'] - result['B'] - result['unbudgeted']
            - result['no_product'] - result['by_client_others'])
        no_team_moves = self.env['account.move'].sudo().search([
            ('move_type', 'in', _REAL_MOVE_TYPES),
            ('state', '=', 'posted'),
            ('team_id', '=', False),
            ('company_id', '=', self.company_id.id),  # solo la compañía del ppto
            ('invoice_date', '>=', date(self.year, 1, 1)),
            ('invoice_date', '<=', date(self.year, 12, 31)),
        ]) if self.year else self.env['account.move']
        result['no_team'] = sum(no_team_moves.mapped('amount_untaxed_signed'))
        return result

    def action_reconcile_invoiced(self):
        """Refresca la foto y publica en el chatter la tabla de conciliación del
        facturado contable vs el presupuesto (solo presupuesto)."""
        self.ensure_one()
        if self.kind != 'presupuesto':
            raise UserError(
                "La conciliación de facturado aplica al presupuesto (el pronóstico "
                "mide compromiso, no facturación).")
        self._sgi_refresh_actuals()
        d = self._sgi_reconcile_data()
        ccy = (d['currency'].name or '') + ' '

        def _m(v):
            return "%s%s" % (ccy, '{:,.2f}'.format(v))
        cuadra = abs(d['residual']) < 0.01
        # 1.4.2: Markup, si no Odoo 19 escapa el HTML y el chatter muestra las etiquetas.
        body = (Markup(
            "<b>Conciliación facturado vs presupuesto — %s</b>"
            "<table class='table table-sm'>"
            "<tr><td><b>A. Facturado contable del equipo</b></td>"
            "<td style='text-align:right'>%s</td></tr>"
            "<tr><td>B. Capturado por el presupuesto (facturado de las líneas)</td>"
            "<td style='text-align:right'>%s</td></tr>"
            "<tr><td>+ Productos facturados NO presupuestados</td>"
            "<td style='text-align:right'>%s</td></tr>"
            "<tr><td>+ Líneas de factura sin producto (fletes/servicios)</td>"
            "<td style='text-align:right'>%s</td></tr>"
            "<tr><td>+ Presupuestado por cliente, facturado a OTROS clientes</td>"
            "<td style='text-align:right'>%s</td></tr>"
            "<tr><td><b>= Residuo (A − B − partidas)</b></td>"
            "<td style='text-align:right'><b>%s</b></td></tr>"
            "</table>"
            "%s"
            "<i>Informativo (fuera de A): facturas del año SIN equipo de ventas: "
            "%s.</i>") % (
                self.year, _m(d['A']), _m(d['B']), _m(d['unbudgeted']),
                _m(d['no_product']), _m(d['by_client_others']), _m(d['residual']),
                (Markup("<span class='text-success'>Cuadra exacto (residuo 0).</span><br/>")
                 if cuadra else
                 Markup("<span class='text-danger'>No cuadra: revisa el residuo "
                        "(foto real desactualizada o redondeos).</span><br/>")),
                _m(d['no_team'])))
        self.message_post(body=body)
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': "Conciliación de facturado",
                'message': "A=%s · B=%s · residuo=%s. Detalle en el chatter." % (
                    _m(d['A']), _m(d['B']), _m(d['residual'])),
                'type': 'success' if cuadra else 'warning',
                'sticky': False,
            },
        }

    # --- Una revisión aprobada y una abierta por año y compañía (3.5.0) -------
    @api.constrains('year', 'state', 'kind', 'company_id')
    def _check_unique_active(self):
        states = {'aprobado': "aprobada", 'borrador': "abierta", 'revisado': "abierta"}
        for budget in self.filtered(lambda b: b.kind == 'presupuesto' and b.state in states):
            group = ['aprobado'] if budget.state == 'aprobado' else ['borrador', 'revisado']
            dup = self.search([
                ('id', '!=', budget.id), ('kind', '=', 'presupuesto'),
                ('year', '=', budget.year), ('company_id', '=', budget.company_id.id),
                ('state', 'in', group)], limit=1)
            if dup:
                raise ValidationError(
                    "Ya hay una revisión %s del presupuesto %s (%s). Trabaje sobre ella o "
                    "márquela obsoleta antes de abrir otra." % (
                        states[budget.state], budget.year, dup.folio or dup.name))

    @api.model_create_multi
    def create(self, vals_list):
        if not self.env.context.get('qb_allow_old_forecast') and any(
                vals.get('kind') == 'pronostico' for vals in vals_list):
            raise UserError(
                "El pronóstico ya no es un documento por cliente y año: cárguelo en "
                "Presupuesto y pronóstico → Pronóstico vigente (con un release o con la "
                "carga del forecast de Excel).")
        return super().create(vals_list)

    def unlink(self):
        # Borra las líneas por ORM antes del DELETE en cascada de BD: así se
        # descartan sus cómputos pendientes (los Monetary con currency_field
        # calculado, p.ej. amount_currency/list_currency_id) y no se intenta hacer
        # flush sobre registros ya eliminados (evita MissingError al borrar).
        self.with_context(sgi_bypass_lock=True).mapped('line_ids').unlink()
        return super().unlink()

    # --- Flujo de estados -----------------------------------------------------
    def _sgi_no_price_pairs(self, limit=10):
        """(textos de hasta `limit` renglones con problema de precio, total)."""
        self.ensure_one()
        labels = dict(self.env['sgi.sales.budget.line']._fields['price_problem'].selection)
        lines = self.line_ids.filtered('price_problem')
        pairs = ["%s — %s — %s: %s" % (
            line._qb_label(), line.partner_id.name or "Sin cliente", line.date.strftime('%m/%Y'),
            labels.get(line.price_problem, '')) for line in lines[:limit]]
        return pairs, len(lines)

    def _sgi_can_review(self):
        """Admin de ventas (o Jefe MAST/SGI como fallback de sistema): manda a
        revisión el presupuesto y marca revisado el pronóstico (P-A28 4.2.2.3)."""
        return (self.env.user.has_group('sales_team.group_sale_manager')
                or self.env.user.has_group('quimibond_sgi.group_sgi_manager'))

    def action_send_to_review(self):
        """borrador → revisado. Presupuesto: 'Enviar a revisión' (Admin de ventas,
        4.2.2.3). Pronóstico: 'Marcar revisado' (mismo permiso). El gate de precios
        de la 5.4 corre aquí como AVISO (no bloquea): el bloqueo es al aprobar."""
        if not self._sgi_can_review():
            raise UserError(
                "Solo el Administrador de ventas (o el Jefe de MAST/SGI) puede "
                "enviar a revisión.")
        for budget in self:
            if budget.kind != 'presupuesto':
                raise UserError("Un pronóstico histórico no se envía a revisión: es solo de consulta.")
            if budget.state != 'borrador':
                raise UserError(
                    "Solo se envía a revisión desde borrador. El %s está en '%s'." % (
                        budget.folio or budget.name,
                        dict(self._fields['state'].selection)[budget.state]))
            if not budget.line_ids:
                raise UserError(
                    "El %s no tiene líneas: captura la matriz antes de enviarlo a "
                    "revisión." % (budget.folio or budget.name))
            # Aviso (no bloqueo) de precios en el presupuesto: el bloqueo es al aprobar.
            pairs, total = budget._sgi_no_price_pairs()
            if total:
                budget.message_post(
                    body=Markup("AVISO al enviar a revisión: %d renglón(es) cuentan 0 en el "
                                "importe. Corríjalos ANTES de aprobar (ahí sí bloquea).<br/>- %s") % (
                        total, Markup("<br/>- ").join(pairs)))
            budget.base_anio_anterior = budget._qb_company_net_invoiced(
                date(budget.year - 1, 1, 1), date(budget.year - 1, 12, 31))
            budget.state = 'revisado'
        return True

    def action_approve(self):
        """revisado → aprobado (solo presupuesto). Aprueba la ALTA DIRECCIÓN
        (P-A28 4.2.2.4): grupo Dirección o Jefe MAST/SGI (fallback de sistema).
        Exige haber pasado por 'revisado'. Bloquea si hay renglones de Industrial o
        Especiales sin cliente, y si hay renglones que cuentan 0 (sin precio, sin tipo de
        cambio o sin conversión), salvo contexto sgi_bypass_price_check, que deja
        constancia en el chatter. Congela el tipo de cambio de cada mes y deja
        «reemplazada» la aprobada anterior del año."""
        can_approve = self.env.user.has_group('quimibond_sgi.group_sgi_director') \
            or self.env.user.has_group('quimibond_sgi.group_sgi_manager')
        bypass_price = self.env.context.get('sgi_bypass_price_check')
        for budget in self:
            if budget.kind != 'presupuesto':
                raise UserError("Un pronóstico histórico no se aprueba: es solo de consulta.")
            if not can_approve:
                raise UserError(
                    "Solo la alta dirección (grupo Dirección) o el Jefe de MAST/SGI "
                    "puede aprobar un presupuesto de ventas.")
            if budget.state != 'revisado':
                raise UserError(
                    "Un presupuesto debe estar REVISADO antes de aprobarse "
                    "(P-A28 4.2.2.3 → 4.2.2.4). El %s está en '%s'." % (
                        budget.folio or budget.name,
                        dict(self._fields['state'].selection)[budget.state]))
            if not budget.line_ids:
                raise UserError(
                    "El presupuesto %s no tiene líneas: captura la matriz antes de "
                    "aprobarlo." % (budget.folio or budget.name))
            if budget.no_client_count:
                raise UserError(
                    "No se puede aprobar %s: %d renglón(es) de Industrial o Especiales sin "
                    "cliente. Póngales cliente o páselos al mercado que les toca." % (
                        budget.name, budget.no_client_count))
            pairs, total = budget._sgi_no_price_pairs()
            if total:
                if not bypass_price:
                    more = "\n…y %d más." % (total - len(pairs)) if total > len(pairs) else ""
                    raise UserError(
                        "No se puede aprobar %s: %d renglón(es) cuentan 0 en el importe "
                        "(sin precio, sin tipo de cambio o sin conversión).\n\n- %s%s\n\n"
                        "Corríjalos o apruebe con excepción (queda constancia en el chatter)." % (
                            budget.name, total, "\n- ".join(pairs), more))
                # sudo tras la revisión de grupo (entrega 4): Dirección aprueba
                # sin tener escritura en el presupuesto (ya no implica al Jefe
                # MAST). El autor del mensaje sigue siendo quien aprueba.
                budget.sudo().message_post(
                    body=Markup("Aprobado con EXCEPCIÓN: %d renglón(es) cuentan 0 en el "
                                "importe.<br/>- %s") % (total, Markup("<br/>- ").join(pairs)),
                    author_id=self.env.user.partner_id.id)
            budget.sudo()._qb_freeze_rates()
            previous = self.sudo().search([
                ('id', '!=', budget.id), ('kind', '=', 'presupuesto'), ('year', '=', budget.year),
                ('company_id', '=', budget.company_id.id), ('state', '=', 'aprobado')])
            previous.with_context(sgi_bypass_lock=True).write({'state': 'reemplazado'})
            for old in previous:
                old.message_post(body="Reemplazado por %s al aprobarse." % budget.name)
            budget.sudo().state = 'aprobado'
        return True

    def action_set_borrador(self):
        self.write({'state': 'borrador'})
        return True

    def action_set_obsoleto(self):
        self.write({'state': 'obsoleto'})
        return True

    def action_revise(self):
        """Nueva revisión (la de junio del P-A28, o cuando Dirección la pida): copia la
        aprobada en borrador con el número siguiente. La aprobada sigue vigente (y el
        VE-02 la sigue leyendo) hasta que se apruebe la nueva; entonces queda
        «reemplazada». Nunca se pisa lo aprobado. Solo MAST."""
        self.ensure_one()
        if self.kind != 'presupuesto':
            raise UserError("Un pronóstico histórico no se revisa: es solo de consulta.")
        if not self.env.user.has_group('quimibond_sgi.group_sgi_manager'):
            raise UserError("Solo el Jefe de MAST y SGI puede revisar un presupuesto.")
        if self.state != 'aprobado':
            raise UserError(
                "Solo se revisa un presupuesto aprobado. El %s está en '%s'." % (
                    self.folio or self.name, dict(self._fields['state'].selection)[self.state]))
        new = self.copy({
            'state': 'borrador',
            'revision': self.revision + 1,
            'folio': False,
            'revision_date': fields.Date.context_today(self),
        })
        new.message_post(body="Revisión nueva de %s." % self.name)
        return {
            'type': 'ir.actions.act_window',
            'name': new.name,
            'res_model': 'sgi.sales.budget',
            'res_id': new.id,
            'view_mode': 'form',
        }

    def _action_grid(self, view_xmlid, name):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "%s — %s" % (name, self.name),
            'res_model': 'sgi.sales.budget.line',
            'view_mode': 'grid,list,form',
            'views': [
                (self.env.ref(view_xmlid).id, 'grid'),
                (False, 'list'), (False, 'form')],
            'domain': [('budget_id', '=', self.id)],
            'context': {'default_budget_id': self.id,
                        'grid_anchor': fields.Date.to_string(
                            fields.Date.to_date('%s-01-01' % self.year))},
        }

    def action_open_grid_qty(self):
        return self._action_grid(
            'quimibond_ventas_presupuesto.sgi_sales_budget_line_grid_qty', "Cantidades")

    def action_open_comparison(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Comparación (ppto vs facturado vs pedido) — %s" % self.name,
            'res_model': 'sgi.sales.budget.line',
            'view_mode': 'pivot,graph,list',
            'domain': [('budget_id', '=', self.id)],
            'context': {'default_budget_id': self.id,
                        'search_default_group_uom': 1},
        }

    def action_open_analysis(self):
        """Botón inteligente: abre el análisis por cliente filtrado a ESTE
        presupuesto (sin el filtro de vigente: ya estás en una revisión concreta)."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': "Análisis por cliente — %s" % self.name,
            'res_model': 'sgi.sales.budget.line',
            'view_mode': 'pivot,list',
            'views': [
                (self.env.ref(
                    'quimibond_ventas_presupuesto.sgi_sales_analysis_pivot_mercado').id, 'pivot'),
                (self.env.ref(
                    'quimibond_ventas_presupuesto.sgi_sales_budget_line_view_list').id, 'list')],
            'domain': [('budget_id', '=', self.id)],
            'context': {'pivot_row_groupby': ['partner_id']},
        }

    # --- Matriz para el reporte F-P-A28-18 -----------------------------------
    def _report_matrix(self):
        """Estructura producto × 12 meses para el QWeb, agrupada por producto. Un
        producto presupuestado por cliente se desglosa en una fila por cliente con
        subtotal de producto; uno global lleva una sola fila. Las cantidades no se
        totalizan globalmente (unidades distintas); el total global es el de pesos."""
        self.ensure_one()
        months = list(range(1, 13))
        groups = {}
        for line in self.line_ids.sorted(lambda l: (
                l.product_id.default_code or '', l._qb_label(),
                l.partner_id.name or '', l.date)):
            gkey = line.product_id.id or ('proyecto', line.proyecto_descripcion or '')
            grp = groups.get(gkey)
            if not grp:
                grp = {
                    'product': line.product_id.display_name or line._qb_label(),
                    'by_client': False,
                    'rows': {},
                    'subtotal_amount': 0.0,
                }
                groups[gkey] = grp
            if line.partner_id:
                grp['by_client'] = True
            rkey = (line.partner_id.id, line.uom_id.id)
            row = grp['rows'].get(rkey)
            if not row:
                row = {
                    'client': line.partner_id.name or '',
                    'uom': line.uom_id.name or '',
                    'cells': {m: 0.0 for m in months},
                    'qty_total': 0.0,
                    'amount_total': 0.0,
                    'qty_real': 0.0,
                    'amount_real': 0.0,
                    'amount_currency': 0.0,
                    'list_currency': line.price_currency_id,
                }
                grp['rows'][rkey] = row
            row['cells'][line.date.month] += line.qty_budget
            row['qty_total'] += line.qty_budget
            row['amount_total'] += line.amount_budget
            row['qty_real'] += line.qty_real
            row['amount_real'] += line.amount_real
            row['amount_currency'] += line.amount_currency
            if line.price_currency_id:
                row['list_currency'] = line.price_currency_id
            grp['subtotal_amount'] += line.amount_budget
        ordered = []
        for grp in groups.values():
            rows = list(grp['rows'].values())
            for row in rows:
                row['price_budget'] = (
                    row['amount_total'] / row['qty_total'] if row['qty_total'] else 0.0)
                row['price_real'] = (
                    row['amount_real'] / row['qty_real'] if row['qty_real'] else 0.0)
            grp['rows'] = rows
            ordered.append(grp)
        return {
            'months': months,
            'groups': ordered,
            'qty_by_uom': self.qty_budget_text,
            'amount_total': self.amount_budget_total,
        }

    def action_print_budget(self):
        self.ensure_one()
        return self.env.ref(
            'quimibond_ventas_presupuesto.action_report_sales_budget').report_action(self)

    # --- Plantilla descargable (filas ya puestas) ----------------------------
    _SGI_TEMPLATE_MONTHS = [
        'ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO',
        'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']

    def _qb_market_teams(self):
        teams = self.env['crm.team']
        for base in ('industrial', 'confeccion', 'especiales'):
            teams |= mercado_team(self.env, base)
        return teams

    def _sgi_historical_pairs(self):
        """[(mercado, cliente comercial, producto)] FACTURADOS en los equipos de los
        mercados en los últimos 24 meses, por mercado, cliente y producto — filas del
        presupuesto. Confección va sin cliente."""
        self.ensure_one()
        from dateutil.relativedelta import relativedelta as _rd
        date_from = fields.Date.context_today(self) - _rd(months=24)
        amls = self.env['account.move.line'].search([
            ('parent_state', '=', 'posted'),
            ('move_id.move_type', 'in', _REAL_MOVE_TYPES),
            ('move_id.team_id', 'in', self._qb_market_teams().ids),
            ('company_id', '=', self.company_id.id),  # solo la compañía del ppto
            ('move_id.invoice_date', '>=', date_from),
            ('product_id', '!=', False),
        ])
        triples = set()
        for aml in amls:
            mercado = team_mercado(self.env, aml.move_id.team_id) or 'industrial'
            partner = (aml.move_id.commercial_partner_id if mercado in MERCADOS_CON_CLIENTE
                       else self.env['res.partner'])
            triples.add((mercado, partner, aml.product_id))
        order = [k for k, _l in MERCADOS]
        # sudo al ordenar: un producto de otra compañía en facturas de esta compañía
        # reventaría el prefetch (AccessError); solo se leen etiquetas para ordenar.
        return sorted(triples, key=lambda t: (
            order.index(t[0]), t[1].name or '', t[2].sudo().default_code or t[2].sudo().name or ''))

    def action_download_template(self):
        """Genera con openpyxl el Excel VACÍO con las FILAS ya puestas (productos
        históricos) para llenar cantidades y subir con el importador."""
        self.ensure_one()
        if self.state != 'borrador':
            raise UserError("La plantilla se descarga sobre un presupuesto en borrador.")
        import base64
        import io
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        if self.kind != 'presupuesto':
            raise UserError("La plantilla es solo para presupuestos.")
        ws.title = 'Presupuesto %s' % self.year
        months = ['%s m' % m for m in self._SGI_TEMPLATE_MONTHS]
        labels = dict(MERCADOS)
        # Instrucción discreta (fila sin producto: el importador la ignora).
        ws.append(['', "Instrucciones: MERCADO es Industrial, Proyectos industrial, "
                   "Confección, Proyectos confección o Especiales. Industrial y Especiales "
                   "llevan CLIENTE; Confección va sin cliente. Un mismo material no puede "
                   "ir con cliente y sin cliente a la vez. PRECIO es opcional (si falta, "
                   "se sugiere de la lista del cliente)."])
        ws.append(['PRODUCTO', 'CLIENTE', 'MERCADO', 'UNIDAD'] + months + ['PRECIO', 'MONEDA'])
        for mercado, partner, product in self._sgi_historical_pairs():
            product = product.sudo()  # prefetch multicompañía: uom/código
            ws.append([product.default_code or product.name, partner.name or '',
                       labels[mercado], product.uom_id.name] + [''] * 12)
        # Bloque de filas libres para clientes/productos nuevos (producto vacío
        # → el importador las ignora si quedan sin llenar).
        ws.append(['', '— Filas libres para clientes/productos nuevos —'])
        for _dummy in range(10):
            ws.append([''])
        buf = io.BytesIO()
        wb.save(buf)
        attachment = self.env['ir.attachment'].create({
            'name': 'Plantilla %s.xlsx' % (self.name or self.folio),
            'datas': base64.b64encode(buf.getvalue()),
            'res_model': 'sgi.sales.budget', 'res_id': self.id,
            'mimetype': 'application/vnd.openxmlformats-officedocument.'
                        'spreadsheetml.sheet',
        })
        return {
            'type': 'ir.actions.act_url',
            'url': '/web/content/%d?download=true' % attachment.id,
            'target': 'self',
        }

    def action_qb_suggest_prices(self):
        """Toma el precio de la lista del cliente en los renglones sin precio (abiertos)."""
        done = 0
        for budget in self:
            if budget.state not in ('borrador', 'revisado'):
                raise UserError("Los precios se sugieren en un presupuesto en borrador o revisado.")
            done += budget.line_ids._qb_suggest_price()
        return {
            'type': 'ir.actions.client', 'tag': 'display_notification',
            'params': {'title': "Precios de la lista",
                       'message': "%d renglón(es) tomaron el precio de la lista; %d siguen sin precio." % (
                           done, sum(self.mapped('no_price_count'))),
                       'type': 'success' if done else 'warning', 'sticky': False}}

    def action_refresh_actuals(self):
        """«Actualizar real» (engrane): recalcula la foto de facturado/pedido Y
        el precio de lista de las líneas (los computes almacenados no se
        refrescan solos al cambiar facturas o la lista de precios).

        1.3.0: el precio se recalcula en borrador Y en revisado. En revisado no
        se reabre el documento: el precio no se captura, sale de la lista, y lo
        que el Admin de ventas revisó (productos, clientes, cantidades) no
        cambia; el gate de «sin precio» de la aprobación pide justo corregir la
        lista y refrescar. Antes había que regresarlo a borrador, actualizar y
        volver a enviarlo. Si el precio cambia en un revisado, queda constancia
        en el chatter (líneas, importe antes y después) para que Dirección
        apruebe sabiendo. Aprobado y obsoleto siguen congelados."""
        self._sgi_refresh_actuals(price_states=('borrador', 'revisado'))
        return True

    def _sgi_refresh_actuals(self, price_states=('borrador',)):
        """Foto de facturado/pedido de todas las líneas y precio de lista de las
        de presupuestos en `price_states`. Los crons y la conciliación usan el
        default (solo borrador): un revisado solo cambia de precio a mano."""
        lines = self.mapped('line_ids')
        reviewed = self.filtered(
            lambda b: b.state == 'revisado' and 'revisado' in price_states)
        before = {
            budget.id: (budget.amount_budget_total, budget.no_price_count,
                        {l.id: (l.price_unit_budget, l.has_list_price)
                         for l in budget.line_ids})
            for budget in reviewed}
        to_price = lines.filtered(lambda l: l.budget_id.state in price_states)
        draft_lines = to_price.filtered(lambda l: l.budget_id.state != 'revisado')
        draft_lines._compute_price()
        reviewed_lines = to_price - draft_lines
        if reviewed_lines:
            # Asignar el precio pasa por write(), que regresa a borrador un
            # revisado al tocar price_unit_budget (campo de captura). Aquí el
            # cambio sale de la lista, no de la captura: se escribe con el
            # bypass del candado. sudo porque el bypass solo cuenta para
            # sistema o MAST y el Admin de ventas es quien lo usa; antes se
            # exige que el usuario pueda escribir las líneas.
            reviewed_lines.check_access('write')
            reviewed_lines.sudo().with_context(sgi_bypass_lock=True)._compute_price()
        # 3.5.0: los renglones que siguen sin precio toman el de la lista si ya tiene
        # regla (en revisado sin reabrirlo, como el precio de lista), y metros y kilos
        # con el rendimiento de hoy; solo en los abiertos (el aprobado no cambia de
        # importe si el cron corrige un rendimiento).
        draft_lines._qb_suggest_price()
        if reviewed_lines:
            reviewed_lines.sudo().with_context(sgi_bypass_lock=True)._qb_suggest_price()
        to_price._compute_qty_conv()
        lines._compute_real()
        lines._compute_ordered()
        for budget in reviewed:
            amount_before, no_price_before, prices = before[budget.id]
            changed = budget.line_ids.filtered(
                lambda l: prices.get(l.id) != (l.price_unit_budget, l.has_list_price))
            if not changed:
                continue
            ccy = budget.currency_id.name or ''
            budget.message_post(body=Markup(
                "Precio de lista recalculado en <b>revisado</b> por %s: %d "
                "línea(s) cambiaron. Importe %s %s → %s %s; sin precio de lista: "
                "%d → %d. El documento sigue revisado (el precio sale de la "
                "lista, no de la captura).") % (
                    self.env.user.name, len(changed),
                    '{:,.2f}'.format(amount_before), ccy,
                    '{:,.2f}'.format(budget.amount_budget_total), ccy,
                    no_price_before, budget.no_price_count))
        return True


    def action_preload_from_orders(self):
        """Precarga propuesta del presupuesto (solo borrador, P-A28 4.2.2.1): cantidad
        por producto(+cliente) por mes combinando el pronóstico vigente del año
        (qb.forecast.line, semana→mes) y el facturado real de los últimos 12 meses.
        Idempotente: nunca pisa celdas ya capturadas."""
        self.ensure_one()
        if self.kind != 'presupuesto':
            raise UserError("La precarga es solo para presupuestos.")
        if self.state != 'borrador':
            raise UserError("Solo se precarga un presupuesto en borrador.")
        return self._preload_budget_from_forecast_and_real()

    def _preload_budget_proposals(self):
        """{(product, commercial_partner, month): qty en la unidad de venta del
        producto} propuesta para el presupuesto, combinando (a) los pronósticos
        vigentes del año (agregando semana→mes) y (b) el facturado real de los
        últimos 12 meses. Combinación = max(pronóstico, real) por celda: no
        planear por debajo de lo pronosticado ni de lo que la historia demuestra.

        3.5.0: el real es el de los equipos de los cinco mercados (Ajustes), no el de un
        equipo; el mercado del renglón sale del equipo del cliente."""
        self.ensure_one()
        from dateutil.relativedelta import relativedelta as _rd

        def _to_sale_uom(product, uom, qty):
            # sudo puntual: el producto puede venir de facturas y ser de otra
            # compañía (mal configurado); leer uom_id sin sudo reventaría el
            # prefetch multicompañía (AccessError). Solo se lee la unidad.
            conv = _convert_qty(qty, uom, product.sudo().uom_id)
            return conv if conv is not None else qty

        forecast_qty = defaultdict(float)
        # 3.0.0: el pronóstico vigente (qb.forecast.line), ya en la unidad de venta.
        forecast_lines = self.env['qb.forecast.line'].search([
            ('company_id', '=', self.company_id.id), ('qty_venta', '>', 0),
            ('week', '>=', date(self.year, 1, 1)), ('week', '<=', date(self.year, 12, 31))])
        for line in forecast_lines:
            partner = line.partner_id.commercial_partner_id
            key = (line.product_id, partner, line.week.month)
            forecast_qty[key] += line.qty_venta

        real_qty = defaultdict(float)
        date_from = fields.Date.context_today(self) - _rd(months=12)
        amls = self.env['account.move.line'].sudo().search([
            ('parent_state', '=', 'posted'),
            ('move_id.move_type', 'in', _REAL_MOVE_TYPES),
            ('move_id.team_id', 'in', self._qb_market_teams().ids),
            ('company_id', '=', self.company_id.id),  # solo la compañía del ppto
            ('move_id.invoice_date', '>=', date_from),
            ('product_id', '!=', False),
        ])
        for aml in amls:
            product = aml.product_id
            month = aml.move_id.invoice_date.month
            partner = aml.move_id.commercial_partner_id
            sign = 1.0 if aml.move_id.move_type == 'out_invoice' else -1.0
            real_qty[(product, partner, month)] += _to_sale_uom(
                product, aml.product_uom_id, sign * aml.quantity)

        proposals = {}
        for key in set(forecast_qty) | set(real_qty):
            qty = max(forecast_qty.get(key, 0.0), real_qty.get(key, 0.0))
            if qty > 0:
                proposals[key] = qty
        return proposals

    def _preload_budget_from_forecast_and_real(self):
        """Presupuesto: crea líneas por producto+cliente+mes con la cantidad
        propuesta (ver _preload_budget_proposals). Respeta el anti-doble-conteo
        (omite productos que ya están capturados como global) y nunca pisa celdas
        existentes."""
        self.ensure_one()
        Line = self.env['sgi.sales.budget.line']
        existing = {(l.product_id.id, (l.partner_id.commercial_partner_id.id
                                       if l.partner_id else False), l.date)
                    for l in self.line_ids}
        # Producto ya presente como GLOBAL (sin cliente): no se puede mezclar con
        # líneas por cliente (constraint anti-doble-conteo) → se omite.
        global_products = {l.product_id.id for l in self.line_ids if not l.partner_id}
        created = skipped_scheme = skipped_company = 0
        for (product, partner, month), qty in sorted(
                self._preload_budget_proposals().items(),
                key=lambda kv: (kv[0][0].id, (kv[0][1].id if kv[0][1] else 0), kv[0][2])):
            when = date(self.year, month, 1)
            partner_id = partner.id if partner else False
            if (product.id, partner_id, when) in existing:
                continue
            # Producto de OTRA compañía facturado en esta (mal configurado): no se
            # presupuesta (crear la línea reventaría al leer su producto foráneo).
            prod_company = product.sudo().company_id
            if prod_company and prod_company.id != self.company_id.id:
                skipped_company += 1
                continue
            if product.id in global_products:
                skipped_scheme += 1
                continue
            Line.create({
                'budget_id': self.id, 'product_id': product.id, 'date': when,
                'uom_id': product.sudo().uom_id.id,  # prefetch multicompañía
                'partner_id': partner_id, 'qty_budget': qty,
                'mercado': default_mercado(self.env, partner),
            })
            existing.add((product.id, partner_id, when))
            created += 1
        note = ("Precarga (pronóstico vigente + facturado 12 m): %d línea(s) "
                "propuesta(s) por producto/cliente/mes (combinación = máximo de "
                "pronosticado y real; el comercial ajusta)." % created)
        if skipped_scheme:
            note += (" %d omitida(s): el producto ya está capturado como global "
                     "(no se mezcla con líneas por cliente)." % skipped_scheme)
        if skipped_company:
            note += (" %d omitida(s): producto de otra compañía facturado en "
                     "esta (mal configurado)." % skipped_company)
        self.message_post(body=note)
        return True

    def action_open_import(self):
        """Abre el asistente de importación del Excel F-P-A28-18 (solo borrador)."""
        self.ensure_one()
        if self.state != 'borrador':
            raise UserError("Solo se importa sobre un presupuesto en borrador.")
        return {
            'type': 'ir.actions.act_window',
            'name': "Importar desde Excel — %s" % self.name,
            'res_model': 'sgi.sales.budget.import',
            'view_mode': 'form',
            'target': 'new',
            'context': {'active_id': self.id, 'default_budget_id': self.id},
        }
