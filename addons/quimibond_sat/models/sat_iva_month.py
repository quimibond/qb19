# -*- coding: utf-8 -*-
"""IVA del mes: SAT vs contabilidad, antes de declarar.

Una fila por compañía, mes y concepto:

* **IVA acreditable** (CFDI recibidos) contra el saldo del mes de las cuentas
  de IVA acreditable pagado.
* **IVA trasladado** (CFDI emitidos) contra el de IVA trasladado cobrado.

El IVA es en base de flujo (los impuestos de Quimibond son "on_payment"), así
que del lado del SAT cuenta lo cobrado o pagado en el mes: los CFDI PUE en su
fecha de emisión y los PPD por sus complementos de pago del mes (parte
proporcional: importe pagado / total del CFDI × IVA del CFDI). Las notas de
crédito restan. Solo CFDI vigentes. El IVA del CFDI es el valueAddedTax de
Syntage (sin IEPS); si falta, el total de impuestos trasladados.

Del lado de Odoo, la suma del mes (por fecha contable) de las cuentas de los
parámetros ``quimibond_sat.iva_acreditable_cuentas`` y
``quimibond_sat.iva_trasladado_cuentas`` (códigos separados por coma). El IVA
de importación no va: no tiene CFDI de proveedor (sale del pedimento).
"""
from dateutil.relativedelta import relativedelta

from odoo import _, api, fields, models

DEFAULT_ACREDITABLE = '118.01.002,118.01.02,118.01.06,118.01.08'
DEFAULT_TRASLADADO = '208.01.01'


class SatIvaMonth(models.Model):
    _name = 'sat.iva.month'
    _description = 'IVA del mes: SAT vs contabilidad'
    _order = 'mes desc, concepto'

    company_id = fields.Many2one('res.company', string='Compañía', required=True, index=True)
    mes = fields.Date(string='Mes', required=True, index=True)
    concepto = fields.Selection([
        ('acreditable', 'IVA acreditable (pagado a proveedores)'),
        ('trasladado', 'IVA trasladado (cobrado a clientes)'),
    ], string='Concepto', required=True)
    sat_iva = fields.Float(string='Según el SAT (MXN)', digits=(16, 2))
    odoo_iva = fields.Float(string='Según la contabilidad (MXN)', digits=(16, 2))
    delta = fields.Float(string='Δ SAT − contabilidad', digits=(16, 2), compute='_compute_delta', store=True)
    cuadra = fields.Boolean(string='Cuadra', compute='_compute_delta', store=True,
                            help='La diferencia no pasa de la tolerancia (parámetro '
                                 'quimibond_sat.iva_tolerancia, default $1,000).')
    n_cfdi = fields.Integer(string='CFDI y pagos')
    calculado = fields.Datetime(string='Calculado el')

    _mes_uniq = models.Constraint('unique(company_id, mes, concepto)', 'Ya existe esa fila del IVA.')

    @api.model
    def _tolerance(self):
        return float(self.env['ir.config_parameter'].sudo().get_param('quimibond_sat.iva_tolerancia') or 1000)

    @api.depends('sat_iva', 'odoo_iva')
    def _compute_delta(self):
        tol = self._tolerance()
        for rec in self:
            rec.delta = rec.sat_iva - rec.odoo_iva
            rec.cuadra = abs(rec.delta) <= tol

    # ── cálculo ────────────────────────────────────────────────────────

    @api.model
    def _accounts(self, company, concepto):
        key = 'quimibond_sat.iva_%s_cuentas' % concepto
        default = DEFAULT_ACREDITABLE if concepto == 'acreditable' else DEFAULT_TRASLADADO
        raw = self.env['ir.config_parameter'].sudo().get_param(key) or default
        codes = [c.strip() for c in raw.split(',') if c.strip()]
        return self.env['account.account'].sudo().with_company(company).search([
            ('code', 'in', codes), ('company_ids', 'in', company.id)])

    @api.model
    def _sat_amounts(self, company, date_from, date_to):
        """{(direction, mes): (iva MXN, n)} de los CFDI vigentes, en base de flujo."""
        self.env.flush_all()
        self.env.cr.execute("""
            WITH cf AS (
                SELECT c.id, c.direction, c.tipo, coalesce(c.metodo_pago, 'PUE') AS metodo,
                       c.total, c.fecha_emision, coalesce(c.moneda, 'MXN') AS moneda,
                       coalesce(nullif(c.tipo_cambio, 0), 1) AS tc,
                       coalesce((c.raw_json -> 'transferredTaxes' ->> 'valueAddedTax')::numeric,
                                c.impuestos_trasladados, 0) AS iva
                  FROM sat_cfdi c
                 WHERE c.company_id = %(company)s AND c.estado_sat = 'vigente' AND c.tipo IN ('I', 'E')
            ),
            mov AS (
                -- PUE y notas de crédito: en su fecha de emisión
                SELECT direction, fecha_emision AS fecha,
                       CASE WHEN tipo = 'E' THEN -1 ELSE 1 END * iva * tc AS iva_mxn
                  FROM cf
                 WHERE tipo = 'E' OR metodo <> 'PPD'
                UNION ALL
                -- PPD: lo pagado en el mes según sus complementos
                SELECT cf.direction, p.fecha_pago,
                       abs(p.monto)
                       * CASE WHEN coalesce(p.moneda, 'MXN') = cf.moneda THEN 1
                              ELSE coalesce(nullif(p.tipo_cambio, 0), 1) / cf.tc END
                       / nullif(cf.total, 0) * cf.iva
                       * CASE WHEN cf.moneda = 'MXN' THEN 1
                              WHEN coalesce(p.moneda, 'MXN') = cf.moneda THEN coalesce(nullif(p.tipo_cambio, 0), cf.tc)
                              ELSE cf.tc END
                  FROM sat_cfdi_pago p
                  JOIN cf ON cf.id = p.invoice_cfdi_id
                 WHERE p.estado_sat = 'vigente' AND cf.tipo = 'I' AND cf.metodo = 'PPD'
            )
            SELECT direction,
                   date_trunc('month', (fecha AT TIME ZONE 'UTC') AT TIME ZONE 'America/Mexico_City')::date AS mes,
                   coalesce(sum(iva_mxn), 0), count(*)
              FROM mov
             WHERE fecha IS NOT NULL
               AND ((fecha AT TIME ZONE 'UTC') AT TIME ZONE 'America/Mexico_City')::date >= %(date_from)s
               AND ((fecha AT TIME ZONE 'UTC') AT TIME ZONE 'America/Mexico_City')::date < %(date_to)s
             GROUP BY 1, 2
        """, {'company': company.id, 'date_from': date_from, 'date_to': date_to})
        return {(direction, mes): (float(total or 0.0), n) for direction, mes, total, n in self.env.cr.fetchall()}

    @api.model
    def _odoo_amounts(self, company, concepto, date_from, date_to):
        """{mes: saldo MXN del mes} de las cuentas de IVA del concepto."""
        accounts = self._accounts(company, concepto)
        if not accounts:
            return {}
        groups = self.env['account.move.line'].sudo()._read_group(
            [('account_id', 'in', accounts.ids), ('company_id', '=', company.id),
             ('parent_state', '=', 'posted'), ('date', '>=', date_from), ('date', '<', date_to)],
            groupby=['date:month'], aggregates=['balance:sum'])
        sign = 1 if concepto == 'acreditable' else -1
        return {fields.Date.to_date(month).replace(day=1): sign * (balance or 0.0) for month, balance in groups}

    @api.model
    def action_compute(self, date_from, date_to=None, company_id=None):
        """Recalcula los meses de [date_from, date_to) (por default, hasta el
        mes en curso). Devuelve cuántas filas escribió."""
        company = self.env['res.company'].browse(company_id) if company_id else self.env.company
        date_from = fields.Date.to_date(date_from).replace(day=1)
        today = fields.Date.context_today(self)
        date_to = fields.Date.to_date(date_to) if date_to else today.replace(day=1) + relativedelta(months=1)
        sat = self._sat_amounts(company, date_from, date_to)
        odoo = {c: self._odoo_amounts(company, c, date_from, date_to) for c in ('acreditable', 'trasladado')}
        now = fields.Datetime.now()
        written = 0
        mes = date_from
        while mes < date_to:
            for concepto, direction in (('acreditable', 'received'), ('trasladado', 'issued')):
                sat_iva, n = sat.get((direction, mes), (0.0, 0))
                vals = {'sat_iva': round(sat_iva, 2), 'odoo_iva': round(odoo[concepto].get(mes, 0.0), 2),
                        'n_cfdi': n, 'calculado': now}
                row = self.sudo().search([('company_id', '=', company.id), ('mes', '=', mes),
                                          ('concepto', '=', concepto)], limit=1)
                if row:
                    row.write(vals)
                else:
                    self.sudo().create(dict(vals, company_id=company.id, mes=mes, concepto=concepto))
                written += 1
            mes += relativedelta(months=1)
        return written

    @api.model
    def _cron_compute(self):
        """Diario: los últimos tres meses de las compañías que se sincronizan
        (los complementos de pago del mes pasado llegan hasta el día 5)."""
        start = fields.Date.context_today(self).replace(day=1) - relativedelta(months=2)
        for company in self.env['res.company'].sudo().search([('sat_sync_enabled', '=', True)]):
            self.action_compute(start, company_id=company.id)

    def action_recompute_year(self):
        """Botón de la lista: recalcula el año de la fila (o el actual)."""
        year = (self[:1].mes or fields.Date.context_today(self)).year
        company = self[:1].company_id or self.env.company
        self.action_compute(fields.Date.to_date('%s-01-01' % year), fields.Date.to_date('%s-01-01' % (year + 1)),
                            company_id=company.id)
        return {'type': 'ir.actions.client', 'tag': 'reload'}

    def action_open_cfdi(self):
        """Los CFDI del mes y sentido de la fila."""
        self.ensure_one()
        direction = 'received' if self.concepto == 'acreditable' else 'issued'
        end = self.mes + relativedelta(months=1)
        return {
            'type': 'ir.actions.act_window', 'name': _('CFDI del mes'),
            'res_model': 'sat.cfdi', 'view_mode': 'list,form',
            'domain': [('company_id', '=', self.company_id.id), ('direction', '=', direction),
                       ('tipo', 'in', ('I', 'E')), ('estado_sat', '=', 'vigente'),
                       ('fecha_emision', '>=', fields.Datetime.to_datetime(self.mes)),
                       ('fecha_emision', '<', fields.Datetime.to_datetime(end))],
        }
