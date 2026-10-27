# -*- coding: utf-8 -*-
"""Pagos SAT vs Odoo: una fila por factura del SAT (tipo I, no ignorada) con
lo pagado según los complementos de pago vigentes y lo pagado según Odoo
(total − residual de la factura publicada). Vista SQL, solo lectura.

Hallazgos: ok, pue (sin complemento pero método PUE: no lo requiere),
sin_complemento (Odoo tiene pagos que el SAT no ve: falta emitir/recibir el
REP), complemento_sin_pago (el SAT tiene pago que Odoo no registró),
complemento_duplicado (los complementos vigentes suman más que la factura:
se timbró el mismo pago más de una vez y sobran REP por cancelar),
sin_factura (CFDI sin factura publicada en Odoo), cancelado.

Lo pagado según Odoo se expresa en términos del CFDI: la fracción pagada de
la factura (1 − residual / total) por el total del CFDI. Así no estorban los
pagos a terceros, la factura con varios CFDI, la otra moneda ni los centavos
de captura. Se tolera $1. Un pago de Odoo cuyo complemento todavía está en
plazo (día 5 del mes siguiente, más 4 días de extracción) es rep_en_plazo, y
una factura revertida con nota de crédito en Odoo es revertida (no es pago).
"""
from odoo import fields, models, tools


class SatPagoCompare(models.Model):
    _name = 'sat.pago.compare'
    _description = 'Pagos SAT vs Odoo'
    _auto = False
    _order = 'fecha desc, id desc'

    cfdi_id = fields.Many2one('sat.cfdi', string='CFDI', readonly=True)
    move_id = fields.Many2one('account.move', string='Factura en Odoo', readonly=True)
    move_name = fields.Char(string='Número en Odoo', readonly=True)
    company_id = fields.Many2one('res.company', string='Compañía', readonly=True)
    direction = fields.Selection([('issued', 'Emitida (cobros)'), ('received', 'Recibida (pagos)')],
                                 string='Sentido', readonly=True)
    partner_id = fields.Many2one('res.partner', string='Contacto', readonly=True)
    counterparty_rfc = fields.Char(string='RFC contraparte', readonly=True)
    counterparty_name = fields.Char(string='Contraparte', readonly=True)
    fecha = fields.Date(string='Fecha factura', readonly=True)
    mes = fields.Date(string='Mes', readonly=True)
    moneda = fields.Char(readonly=True)
    metodo_pago = fields.Char(string='Método (PUE/PPD)', readonly=True)
    total = fields.Float(string='Total factura', readonly=True, digits=(16, 2))
    estado_sat = fields.Char(string='Estado SAT', readonly=True)
    state_odoo = fields.Char(string='Estado Odoo', readonly=True)
    payment_state = fields.Char(string='Pago en Odoo', readonly=True)
    n_pagos = fields.Integer(string='Complementos', readonly=True)
    ultimo_pago = fields.Datetime(string='Último complemento', readonly=True)
    fecha_pago_odoo = fields.Date(string='Último pago en Odoo', readonly=True)
    pagado_sat = fields.Float(string='Pagado según SAT', readonly=True, digits=(16, 2))
    pagado_odoo = fields.Float(string='Pagado según Odoo', readonly=True, digits=(16, 2),
                               help='Fracción pagada de la factura de Odoo por el total del CFDI (en la moneda del CFDI).')
    saldo_sat = fields.Float(string='Saldo según SAT', readonly=True, digits=(16, 2))
    saldo_odoo = fields.Float(string='Saldo según Odoo', readonly=True, digits=(16, 2))
    delta = fields.Float(string='Δ pagado SAT − Odoo', readonly=True, digits=(16, 2))
    pagado_sat_mxn = fields.Float(string='Pagado SAT (MXN)', readonly=True, digits=(16, 2))
    pagado_odoo_mxn = fields.Float(string='Pagado Odoo (MXN)', readonly=True, digits=(16, 2))
    delta_mxn = fields.Float(string='Δ (MXN)', readonly=True, digits=(16, 2))
    issue = fields.Selection([
        ('ok', 'Cuadra'),
        ('pue', 'PUE sin complemento (no lo requiere)'),
        ('sin_complemento', 'Pagado en Odoo sin complemento en el SAT'),
        ('rep_en_plazo', 'Pagado en Odoo, complemento aún en plazo'),
        ('revertida', 'Revertida en Odoo con nota de crédito'),
        ('complemento_sin_pago', 'Complemento en el SAT sin pago en Odoo'),
        ('complemento_duplicado', 'Complementos exceden el total (REP duplicado)'),
        ('sin_factura', 'Sin factura publicada en Odoo'),
        ('cancelado', 'CFDI cancelado'),
    ], string='Hallazgo', readonly=True)

    def init(self):
        tools.drop_view_if_exists(self.env.cr, self._table)
        self.env.cr.execute("""
            CREATE OR REPLACE VIEW %s AS
            WITH pagos AS (
                SELECT p.invoice_cfdi_id AS cfdi_id, count(*) AS n_pagos, max(p.fecha_pago) AS ultimo_pago,
                       sum(p.monto * CASE WHEN coalesce(p.moneda, 'MXN') = coalesce(c.moneda, 'MXN') THEN 1
                                          ELSE coalesce(nullif(p.tipo_cambio, 0), 1)
                                               / coalesce(nullif(c.tipo_cambio, 0), 1) END) AS pagado_sat
                  FROM sat_cfdi_pago p
                  JOIN sat_cfdi c ON c.id = p.invoice_cfdi_id
                 WHERE p.estado_sat = 'vigente'
                 GROUP BY p.invoice_cfdi_id
            ),
            -- Fecha del último pago aplicado a la factura en Odoo (conciliación
            -- de su línea por cobrar / por pagar).
            pago_odoo AS (
                SELECT aml.move_id, max(apr.max_date) AS fecha_pago
                  FROM account_move_line aml
                  JOIN account_account aa ON aa.id = aml.account_id
                       AND aa.account_type IN ('asset_receivable', 'liability_payable')
                  JOIN account_partial_reconcile apr
                       ON apr.debit_move_id = aml.id OR apr.credit_move_id = aml.id
                 GROUP BY aml.move_id
            ),
            -- Lo saldado con notas de crédito (descuento, devolución): Odoo lo
            -- da por pagado, pero no es un pago y no lleva complemento
            -- (1.26.1: Blancos Milenium, Pieles Sintéticas salían «sin
            -- complemento» por la nota de crédito).
            nc_odoo AS (
                SELECT aml.move_id,
                       sum(CASE WHEN apr.debit_move_id = aml.id THEN apr.debit_amount_currency
                                ELSE apr.credit_amount_currency END) AS nc
                  FROM account_move_line aml
                  JOIN account_account aa ON aa.id = aml.account_id
                       AND aa.account_type IN ('asset_receivable', 'liability_payable')
                  JOIN account_partial_reconcile apr
                       ON apr.debit_move_id = aml.id OR apr.credit_move_id = aml.id
                  JOIN account_move_line otra
                       ON otra.id = CASE WHEN apr.debit_move_id = aml.id THEN apr.credit_move_id
                                         ELSE apr.debit_move_id END
                  JOIN account_move om ON om.id = otra.move_id
                 WHERE om.move_type IN ('out_refund', 'in_refund')
                   AND aml.move_id <> om.id
                 GROUP BY aml.move_id
            ),
            base AS (
                SELECT c.id AS cfdi_id, c.move_id, c.company_id, c.direction, c.partner_id,
                       c.counterparty_rfc, c.counterparty_name,
                       c.fecha_emision::date AS fecha, date_trunc('month', c.fecha_emision)::date AS mes,
                       coalesce(c.moneda, 'MXN') AS moneda, c.metodo_pago, c.total, c.estado_sat,
                       coalesce(nullif(c.tipo_cambio, 0), 1) AS tc,
                       m.id AS m_id, m.name AS move_name, m.state AS state_odoo, m.payment_state,
                       coalesce(pg.n_pagos, 0) AS n_pagos, pg.ultimo_pago,
                       coalesce(pg.pagado_sat, 0) AS pagado_sat,
                       CASE WHEN m.state = 'posted' AND m.amount_total <> 0
                            THEN greatest(abs(m.amount_total) - abs(m.amount_residual) - abs(coalesce(nc.nc, 0)), 0)
                                 / abs(m.amount_total) * c.total
                            ELSE 0 END AS pagado_odoo,
                       CASE WHEN m.state = 'posted' AND m.amount_total <> 0
                            THEN abs(m.amount_residual) / abs(m.amount_total) * c.total END AS saldo_odoo,
                       po.fecha_pago AS fecha_pago_odoo
                  FROM sat_cfdi c
                  LEFT JOIN account_move m ON m.id = c.move_id
                  LEFT JOIN pagos pg ON pg.cfdi_id = c.id
                  LEFT JOIN pago_odoo po ON po.move_id = c.move_id
                  LEFT JOIN nc_odoo nc ON nc.move_id = c.move_id AND m.move_type IN ('out_invoice', 'in_invoice')
                 WHERE c.tipo = 'I' AND c.match_status <> 'ignorado'
            )
            SELECT b.cfdi_id AS id, b.cfdi_id, b.move_id, b.company_id, b.direction, b.partner_id,
                   b.counterparty_rfc, b.counterparty_name, b.fecha, b.mes, b.moneda, b.metodo_pago,
                   b.total, b.estado_sat, b.move_name, b.state_odoo, b.payment_state,
                   b.n_pagos, b.ultimo_pago, b.fecha_pago_odoo, b.pagado_sat, b.pagado_odoo,
                   b.total - b.pagado_sat AS saldo_sat, b.saldo_odoo,
                   b.pagado_sat - b.pagado_odoo AS delta,
                   b.pagado_sat * b.tc AS pagado_sat_mxn,
                   b.pagado_odoo * b.tc AS pagado_odoo_mxn,
                   (b.pagado_sat - b.pagado_odoo) * b.tc AS delta_mxn,
                   CASE WHEN b.estado_sat <> 'vigente' THEN 'cancelado'
                        WHEN b.m_id IS NULL OR b.state_odoo <> 'posted' THEN 'sin_factura'
                        WHEN b.payment_state = 'reversed' THEN 'revertida'
                        WHEN b.pagado_sat > abs(b.total) + 1 THEN 'complemento_duplicado'
                        WHEN abs(b.pagado_sat - b.pagado_odoo) < 1.005 THEN 'ok'
                        WHEN b.metodo_pago = 'PUE' AND b.n_pagos = 0 THEN 'pue'
                        WHEN b.pagado_sat > b.pagado_odoo THEN 'complemento_sin_pago'
                        -- el complemento se emite a más tardar el día 5 del mes siguiente al pago
                        WHEN b.fecha_pago_odoo IS NOT NULL
                             AND current_date <= (date_trunc('month', b.fecha_pago_odoo) + interval '1 month 8 days')::date
                             THEN 'rep_en_plazo'
                        ELSE 'sin_complemento' END AS issue
              FROM base b
        """ % self._table)

    def action_open_cfdi(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'res_model': 'sat.cfdi',
                'res_id': self.cfdi_id.id, 'view_mode': 'form'}

    def action_open_move(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'res_model': 'account.move',
                'res_id': self.move_id.id, 'view_mode': 'form'}
