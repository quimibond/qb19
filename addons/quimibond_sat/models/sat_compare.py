# -*- coding: utf-8 -*-
"""Comparación SAT vs Odoo: una fila por CFDI del SAT (tipos I y E) y una por
factura publicada de Odoo que no tiene CFDI en el SAT. Vista SQL, solo lectura.

Cubetas: ambos / solo_sat / solo_odoo / solo_odoo_sin_uuid / ignorado.
Hallazgos: ok, monto, cancelado_sat, cancelado_odoo (solo en 'ambos').
"""
from odoo import fields, models, tools
from odoo.tools.sql import column_exists, table_exists


class SatCompareLine(models.Model):
    _name = 'sat.compare.line'
    _description = 'Comparación SAT vs Odoo'
    _auto = False
    _order = 'fecha desc, id desc'

    bucket = fields.Selection([
        ('ambos', 'En ambos'),
        ('solo_sat', 'Solo en el SAT'),
        ('solo_odoo', 'Solo en Odoo'),
        ('solo_odoo_sin_uuid', 'Solo en Odoo (sin UUID)'),
        ('ignorado', 'Ignorado'),
    ], string='Cubeta', readonly=True)
    issue = fields.Selection([
        ('ok', 'Cuadra'),
        ('monto', 'Monto distinto'),
        ('cancelado_sat', 'Cancelado en el SAT, publicado en Odoo'),
        ('cancelado_odoo', 'Cancelado en Odoo, vigente en el SAT'),
        ('solo_sat', 'Falta en Odoo'),
        ('solo_odoo', 'Falta en el SAT'),
        ('solo_odoo_sin_uuid', 'Sin UUID en Odoo'),
        ('ignorado', 'Ignorado'),
    ], string='Hallazgo', readonly=True)
    cfdi_id = fields.Many2one('sat.cfdi', string='CFDI', readonly=True)
    move_id = fields.Many2one('account.move', string='Factura en Odoo', readonly=True)
    move_name = fields.Char(string='Número en Odoo', readonly=True)
    uuid = fields.Char(string='UUID', readonly=True)
    direction = fields.Selection([('issued', 'Emitido'), ('received', 'Recibido')], string='Sentido', readonly=True)
    tipo = fields.Selection([('I', 'Ingreso'), ('E', 'Egreso')], string='Tipo', readonly=True)
    company_id = fields.Many2one('res.company', string='Compañía', readonly=True)
    partner_id = fields.Many2one('res.partner', string='Contacto', readonly=True)
    counterparty_rfc = fields.Char(string='RFC contraparte', readonly=True)
    counterparty_name = fields.Char(string='Contraparte', readonly=True)
    fecha = fields.Date(readonly=True)
    total_sat = fields.Float(string='Total SAT', readonly=True, digits=(16, 2))
    total_odoo = fields.Float(string='Total Odoo', readonly=True, digits=(16, 2))
    amount_diff = fields.Float(string='Diferencia', readonly=True, digits=(16, 2))
    estado_sat = fields.Char(string='Estado SAT', readonly=True)
    state_odoo = fields.Char(string='Estado Odoo', readonly=True)
    payment_state = fields.Char(string='Pago en Odoo', readonly=True)
    suggested_move_id = fields.Many2one('account.move', string='Factura sugerida', readonly=True)
    # Conciliación al centavo: E resta, solo vigentes / publicadas cuentan.
    mes = fields.Date(string='Mes', readonly=True)
    sat_vigente = fields.Float(string='SAT vigente', readonly=True, digits=(16, 2),
                               help='Total del CFDI si está vigente (egresos en negativo).')
    odoo_posted = fields.Float(string='Odoo publicado', readonly=True, digits=(16, 2),
                               help='Total de la factura si está publicada (notas de crédito en negativo).')
    delta = fields.Float(string='Δ SAT − Odoo', readonly=True, digits=(16, 2))

    def _odoo_uuid_sql(self):
        """UUID de la factura según lo que tenga instalada la base: el campo
        l10n_mx_edi_cfdi_uuid del asiento, el documento CFDI, o nada."""
        cr = self.env.cr
        parts = []
        if column_exists(cr, 'account_move', 'l10n_mx_edi_cfdi_uuid'):
            parts.append("NULLIF(m.l10n_mx_edi_cfdi_uuid, '')")
        if table_exists(cr, 'l10n_mx_edi_document'):
            parts.append("""(SELECT d.attachment_uuid FROM l10n_mx_edi_document d
                             WHERE d.move_id = m.id AND d.attachment_uuid IS NOT NULL
                             ORDER BY d.id DESC LIMIT 1)""")
        if not parts:
            return 'NULL::varchar'
        return 'lower(coalesce(%s))' % ', '.join(parts)

    def init(self):
        tools.drop_view_if_exists(self.env.cr, self._table)
        self.env.cr.execute("""
            CREATE OR REPLACE VIEW %s AS
            WITH odoo AS (
                SELECT m.id AS move_id, m.company_id, m.commercial_partner_id AS partner_id,
                       m.move_type, m.state, m.payment_state, m.invoice_date AS fecha,
                       abs(m.amount_total) AS total_odoo, m.name AS move_name,
                       %s AS odoo_uuid
                  FROM account_move m
                 WHERE m.move_type IN ('out_invoice', 'out_refund', 'in_invoice', 'in_refund')
                   AND m.state IN ('posted', 'cancel')
            ),
            rows AS (
                -- Lado SAT: en ambos, solo SAT o ignorado
                SELECT c.id AS cfdi_id, c.move_id, c.uuid, c.direction, c.tipo, c.company_id,
                       c.partner_id, c.counterparty_rfc, c.counterparty_name,
                       c.fecha_emision::date AS fecha, c.total AS total_sat, o.total_odoo,
                       c.estado_sat, o.state AS state_odoo, o.payment_state, o.move_name,
                       CASE WHEN c.match_status = 'ignorado' THEN 'ignorado'
                            WHEN c.move_id IS NOT NULL THEN 'ambos'
                            ELSE 'solo_sat' END AS bucket,
                       c.suggested_move_id
                  FROM sat_cfdi c
                  LEFT JOIN odoo o ON o.move_id = c.move_id
                 WHERE c.tipo IN ('I', 'E')
                UNION ALL
                -- Lado Odoo: facturas publicadas sin CFDI en el SAT
                SELECT NULL, o.move_id, o.odoo_uuid,
                       CASE WHEN o.move_type IN ('out_invoice', 'out_refund') THEN 'issued' ELSE 'received' END,
                       CASE WHEN o.move_type IN ('out_refund', 'in_refund') THEN 'E' ELSE 'I' END,
                       o.company_id, o.partner_id, upper(p.vat), p.name, o.fecha,
                       NULL, o.total_odoo, NULL, o.state, o.payment_state, o.move_name,
                       CASE WHEN o.odoo_uuid IS NULL THEN 'solo_odoo_sin_uuid' ELSE 'solo_odoo' END,
                       NULL::integer
                  FROM odoo o
                  LEFT JOIN res_partner p ON p.id = o.partner_id
                 WHERE o.state = 'posted'
                   AND NOT EXISTS (
                       SELECT 1 FROM sat_cfdi c
                        WHERE c.move_id = o.move_id
                           OR (o.odoo_uuid IS NOT NULL AND c.uuid = o.odoo_uuid))
            )
            SELECT row_number() OVER (ORDER BY fecha DESC NULLS LAST, cfdi_id, move_id) AS id,
                   r.*,
                   coalesce(r.total_sat, 0) - coalesce(r.total_odoo, 0) AS amount_diff,
                   date_trunc('month', r.fecha)::date AS mes,
                   CASE WHEN r.tipo = 'E' THEN -1 ELSE 1 END
                       * CASE WHEN r.estado_sat = 'vigente' THEN coalesce(r.total_sat, 0) ELSE 0 END AS sat_vigente,
                   CASE WHEN r.tipo = 'E' THEN -1 ELSE 1 END
                       * CASE WHEN r.state_odoo = 'posted' THEN coalesce(r.total_odoo, 0) ELSE 0 END AS odoo_posted,
                   CASE WHEN r.tipo = 'E' THEN -1 ELSE 1 END
                       * (CASE WHEN r.estado_sat = 'vigente' THEN coalesce(r.total_sat, 0) ELSE 0 END
                          - CASE WHEN r.state_odoo = 'posted' THEN coalesce(r.total_odoo, 0) ELSE 0 END) AS delta,
                   CASE WHEN r.bucket <> 'ambos' THEN r.bucket
                        WHEN r.estado_sat = 'cancelado' AND r.state_odoo = 'posted' THEN 'cancelado_sat'
                        WHEN r.estado_sat <> 'cancelado' AND r.state_odoo = 'cancel' THEN 'cancelado_odoo'
                        WHEN abs(coalesce(r.total_sat, 0) - coalesce(r.total_odoo, 0))
                             > greatest(1.0, 0.005 * abs(coalesce(r.total_sat, 0))) THEN 'monto'
                        ELSE 'ok' END AS issue
              FROM rows r
        """ % (self._table, self._odoo_uuid_sql()))

    def action_accept_suggestion(self):
        self.ensure_one()
        self.cfdi_id.action_accept_suggestion()

    def action_open_cfdi(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'res_model': 'sat.cfdi',
                'res_id': self.cfdi_id.id, 'view_mode': 'form', 'target': 'current'}

    def action_open_move(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'res_model': 'account.move',
                'res_id': self.move_id.id, 'view_mode': 'form', 'target': 'current'}
