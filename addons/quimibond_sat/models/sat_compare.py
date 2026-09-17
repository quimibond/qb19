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
        ('poliza', 'Se registra por póliza'),
        ('sin_cfdi', 'Sin CFDI esperado'),
        ('ignorado', 'Ignorado'),
    ], string='Cubeta', readonly=True)
    issue = fields.Selection([
        ('ok', 'Cuadra'),
        ('monto', 'Monto distinto'),
        ('moneda', 'Moneda distinta'),
        ('cancelado_sat', 'Cancelado en el SAT, publicado en Odoo'),
        ('cancelado_odoo', 'Cancelado en Odoo, vigente en el SAT'),
        ('solo_sat', 'Falta en Odoo'),
        ('solo_odoo', 'Falta en el SAT'),
        ('solo_odoo_sin_uuid', 'Sin UUID en Odoo'),
        ('poliza', 'Se registra por póliza'),
        ('sin_cfdi', 'Sin CFDI esperado'),
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
    moneda = fields.Char(string='Moneda', readonly=True, help='Moneda del CFDI; si no hay CFDI, la de la factura.')
    moneda_odoo = fields.Char(string='Moneda Odoo', readonly=True)
    total_sat = fields.Float(string='Total SAT', readonly=True, digits=(16, 2),
                             help='En la moneda del CFDI.')
    total_odoo = fields.Float(string='Total Odoo', readonly=True, digits=(16, 2),
                              help='En la moneda de la factura.')
    amount_diff = fields.Float(string='Diferencia', readonly=True, digits=(16, 2),
                               help='Total SAT − Total Odoo, en la moneda del documento.')
    total_sat_mxn = fields.Float(string='Total SAT (MXN)', readonly=True, digits=(16, 2),
                                 help='Total × tipo de cambio del CFDI.')
    total_odoo_mxn = fields.Float(string='Total Odoo (MXN)', readonly=True, digits=(16, 2),
                                  help='Si la factura está en la moneda del CFDI, total × tipo de cambio '
                                       'del CFDI (para que el Δ no cargue diferencias de tipo de cambio); '
                                       'si no, el total en moneda de la compañía según Odoo.')
    estado_sat = fields.Char(string='Estado SAT', readonly=True)
    state_odoo = fields.Char(string='Estado Odoo', readonly=True)
    payment_state = fields.Char(string='Pago en Odoo', readonly=True)
    suggested_move_id = fields.Many2one('account.move', string='Factura sugerida', readonly=True)
    # Conciliación al centavo, en MXN: E resta, solo vigentes / publicadas cuentan.
    # Fuera de conciliación (en cero): ignorado, por póliza y sin CFDI esperado.
    mes = fields.Date(string='Mes', readonly=True)
    sat_vigente = fields.Float(string='SAT vigente (MXN)', readonly=True, digits=(16, 2),
                               help='Total MXN del CFDI si está vigente (egresos en negativo).')
    odoo_posted = fields.Float(string='Odoo publicado (MXN)', readonly=True, digits=(16, 2),
                               help='Total MXN de la factura si está publicada (notas de crédito en negativo).')
    delta = fields.Float(string='Δ SAT − Odoo (MXN)', readonly=True, digits=(16, 2))

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
        mx_id = self.env.ref('base.mx').id
        self.env.cr.execute("""
            CREATE OR REPLACE VIEW %s AS
            WITH odoo AS (
                SELECT m.id AS move_id, m.company_id, m.commercial_partner_id AS partner_id,
                       m.move_type, m.state, m.payment_state, m.invoice_date AS fecha,
                       abs(m.amount_total) AS total_odoo,
                       abs(m.amount_total_signed) AS total_odoo_mxn,
                       cur.name AS moneda_odoo, m.name AS move_name,
                       %s AS odoo_uuid,
                       -- sin CFDI esperado: política del contacto o contacto extranjero
                       (cp.sat_cfdi_policy = 'sin_cfdi'
                        OR (cp.country_id IS NOT NULL AND cp.country_id <> %s)) AS sin_cfdi,
                       (cp.sat_cfdi_policy = 'poliza') AS por_poliza
                  FROM account_move m
                  LEFT JOIN res_currency cur ON cur.id = m.currency_id
                  LEFT JOIN res_partner cp ON cp.id = m.commercial_partner_id
                 WHERE m.move_type IN ('out_invoice', 'out_refund', 'in_invoice', 'in_refund')
                   AND m.state IN ('posted', 'cancel')
            ),
            rows AS (
                -- Lado SAT: en ambos, solo SAT o ignorado
                SELECT c.id AS cfdi_id, c.move_id, c.uuid, c.direction, c.tipo, c.company_id,
                       c.partner_id, c.counterparty_rfc, c.counterparty_name,
                       c.fecha_emision::date AS fecha,
                       coalesce(c.moneda, 'MXN') AS moneda, o.moneda_odoo,
                       c.total AS total_sat, o.total_odoo,
                       c.total * coalesce(nullif(c.tipo_cambio, 0), 1) AS total_sat_mxn,
                       CASE WHEN o.moneda_odoo = coalesce(c.moneda, 'MXN')
                            THEN o.total_odoo * coalesce(nullif(c.tipo_cambio, 0), 1)
                            ELSE o.total_odoo_mxn END AS total_odoo_mxn,
                       c.estado_sat, o.state AS state_odoo, o.payment_state, o.move_name,
                       CASE WHEN c.match_status = 'ignorado' THEN 'ignorado'
                            WHEN c.move_id IS NOT NULL THEN 'ambos'
                            WHEN sp.sat_cfdi_policy = 'poliza' THEN 'poliza'
                            ELSE 'solo_sat' END AS bucket,
                       c.suggested_move_id
                  FROM sat_cfdi c
                  LEFT JOIN odoo o ON o.move_id = c.move_id
                  LEFT JOIN res_partner sp ON sp.id = c.partner_id
                 WHERE c.tipo IN ('I', 'E')
                UNION ALL
                -- Lado Odoo: facturas publicadas sin CFDI en el SAT
                SELECT NULL, o.move_id, o.odoo_uuid,
                       CASE WHEN o.move_type IN ('out_invoice', 'out_refund') THEN 'issued' ELSE 'received' END,
                       CASE WHEN o.move_type IN ('out_refund', 'in_refund') THEN 'E' ELSE 'I' END,
                       o.company_id, o.partner_id, upper(p.vat), p.name, o.fecha,
                       o.moneda_odoo, o.moneda_odoo,
                       NULL, o.total_odoo, NULL, o.total_odoo_mxn,
                       NULL, o.state, o.payment_state, o.move_name,
                       CASE WHEN o.odoo_uuid IS NOT NULL THEN 'solo_odoo'
                            WHEN o.sin_cfdi THEN 'sin_cfdi'
                            WHEN o.por_poliza THEN 'poliza'
                            ELSE 'solo_odoo_sin_uuid' END,
                       NULL::integer
                  FROM odoo o
                  LEFT JOIN res_partner p ON p.id = o.partner_id
                 WHERE o.state = 'posted'
                   AND NOT EXISTS (
                       SELECT 1 FROM sat_cfdi c
                        WHERE c.move_id = o.move_id
                           OR (o.odoo_uuid IS NOT NULL AND c.uuid = o.odoo_uuid))
            )
            -- id estable (no row_number): la numeración no se corre al cambiar
            -- los datos, así la caché del ORM y los clics en la lista abren la fila correcta
            SELECT CASE WHEN r.cfdi_id IS NOT NULL THEN r.cfdi_id * 2 ELSE r.move_id * 2 + 1 END AS id,
                   r.cfdi_id, r.move_id, r.uuid, r.direction, r.tipo, r.company_id, r.partner_id,
                   r.counterparty_rfc, r.counterparty_name, r.fecha, r.moneda, r.moneda_odoo,
                   r.total_sat, r.total_odoo, r.total_sat_mxn, r.total_odoo_mxn,
                   r.estado_sat, r.state_odoo, r.payment_state, r.move_name, r.bucket, r.suggested_move_id,
                   coalesce(r.total_sat, 0) - coalesce(r.total_odoo, 0) AS amount_diff,
                   date_trunc('month', r.fecha)::date AS mes,
                   r.signo * CASE WHEN r.estado_sat = 'vigente' THEN coalesce(r.total_sat_mxn, 0) ELSE 0 END AS sat_vigente,
                   r.signo * CASE WHEN r.state_odoo = 'posted' THEN coalesce(r.total_odoo_mxn, 0) ELSE 0 END AS odoo_posted,
                   r.signo * (CASE WHEN r.estado_sat = 'vigente' THEN coalesce(r.total_sat_mxn, 0) ELSE 0 END
                              - CASE WHEN r.state_odoo = 'posted' THEN coalesce(r.total_odoo_mxn, 0) ELSE 0 END) AS delta,
                   CASE WHEN r.bucket <> 'ambos' THEN r.bucket
                        WHEN r.estado_sat = 'cancelado' AND r.state_odoo = 'posted' THEN 'cancelado_sat'
                        WHEN r.estado_sat <> 'cancelado' AND r.state_odoo = 'cancel' THEN 'cancelado_odoo'
                        WHEN r.moneda_odoo IS NOT NULL AND r.moneda <> r.moneda_odoo THEN 'moneda'
                        -- al centavo: un centavo de redondeo se tolera, dos ya no
                        WHEN abs(coalesce(r.total_sat, 0) - coalesce(r.total_odoo, 0)) > 0.015 THEN 'monto'
                        ELSE 'ok' END AS issue
              FROM (SELECT rows.*,
                           CASE WHEN rows.bucket IN ('ignorado', 'poliza', 'sin_cfdi') THEN 0
                                WHEN rows.tipo = 'E' THEN -1 ELSE 1 END AS signo
                      FROM rows) r
        """ % (self._table, self._odoo_uuid_sql(), mx_id))

    def action_accept_suggestion(self):
        self.ensure_one()
        self.cfdi_id.action_accept_suggestion()

    def action_reconcile(self):
        self.ensure_one()
        if self.cfdi_id:
            return self.cfdi_id.action_reconcile()
        if self.move_id:
            return self.move_id.action_sat_reconcile()
        return False

    def action_open_cfdi(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'res_model': 'sat.cfdi',
                'res_id': self.cfdi_id.id, 'view_mode': 'form', 'target': 'current'}

    def action_open_move(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'res_model': 'account.move',
                'res_id': self.move_id.id, 'view_mode': 'form', 'target': 'current'}
