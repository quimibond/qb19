# -*- coding: utf-8 -*-
"""Configuracion del Estado de flujo de efectivo NIF B-2.

``cash.flow.config`` es una por compania y contiene las reglas
(``cash.flow.rule``) que definen:

* ``cash``: que cuentas son efectivo y equivalentes (con exclusiones);
* ``indirect``: a que linea del metodo indirecto va cada movimiento de una
  cuenta que no es efectivo;
* ``direct``: a que linea del metodo directo va la contraparte de cada
  movimiento de efectivo.

Las reglas se evaluan por ``sequence``; la primera que coincide gana. Lo que
no coincide con ninguna regla se reporta en "Sin clasificar" (indirecto) u
"Otros (revisar)" (directo): nunca se descarta.
"""
import logging

from odoo import _, api, fields, models
from odoo.exceptions import ValidationError

from . import cash_flow_lines as L

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Defaults Quimibond
#
# Cada tupla: (method, line_key, criterion, value, extra)
#   extra: dict con side / cash_move_only / mode / mirror_line_key / exclude /
#          journal_name / partner_name / note
# El orden de la lista es la secuencia. Los prefijos se comparan con
# ``code.startswith(prefix)``; varios prefijos se separan por coma.
# ---------------------------------------------------------------------------

DEFAULT_RULES = [
    # ===== Efectivo y equivalentes ========================================
    ('cash', False, 'account_prefix', '109.23.02', {
        'exclude': True, 'note': 'Agente aduanal: tipificada como banco pero es un deudor.'}),
    ('cash', False, 'account_prefix', '107.03.001', {
        'exclude': True, 'note': 'Cuenta default del diario "5209 BBVA BANCOMER": es Grupo Quimibond (parte relacionada).'}),
    ('cash', False, 'account_type', 'asset_cash', {'note': 'Bancos y cajas.'}),
    ('cash', False, 'account_type', 'liability_credit_card', {'note': 'Tarjetas de crédito (204.01.02 Jeeves).'}),
    ('cash', False, 'account_prefix',
     '101.01.,102.01.00,102.02.02,102.02.03,103.01.03,103.01.04,103.01.05,102.01.0011,204.01.02',
     {'note': 'Bancos y cajas por código.'}),
    ('cash', False, 'account_prefix',
     '102.01.34,102.01.35,102.01.01,102.01.02,102.01.06,102.01.08,102.01.09,102.01.10,102.01.11,'
     '102.01.29,102.01.012,102.01.013,102.01.17,102.01.18',
     {'note': 'Recibos/Pagos pendientes: globales e históricas por banco.'}),
    ('cash', False, 'account_prefix', '102.01.', {
        'note': 'Cualquier otra cuenta de tránsito 102.01.xx (recibos/pagos pendientes archivados).'}),
    ('cash', False, 'account_prefix', '102.09.00', {'note': 'Transitoria de transferencias (transfer_account_id).'}),
    ('cash', False, 'account_prefix', '102.01.36', {'note': 'Suspense bancario.'}),

    # ===== Metodo indirecto: partidas virtuales (cuentas de resultados) ===
    ('indirect', 'nc_depreciation', 'account_prefix',
     '504.08.0001,504.09.0001,504.10.0001,504.11.0001,504.22.0001,504.23.0001,613.',
     {'mode': 'addback', 'mirror_line_key': 'nc_depreciation_ctr',
      'note': 'Depreciación y amortización del periodo.'}),
    ('indirect', 'nc_asset_result', 'account_prefix', '704.23.0003,701.01.0004',
     {'mode': 'addback', 'mirror_line_key': 'inv_disposals',
      'note': 'Utilidad (resta) / pérdida (suma) en venta de activo fijo; el flujo real va a inversión.'}),
    ('indirect', 'nc_casualty', 'account_prefix', '701.01.0003',
     {'mode': 'addback', 'mirror_line_key': 'inv_disposals', 'note': 'Pérdidas por siniestro.'}),
    ('indirect', 'nc_bad_debt', 'account_prefix', '701.01.0005',
     {'mode': 'addback', 'mirror_line_key': 'wc_receivables', 'note': 'Estimación de incobrables.'}),
    ('indirect', 'nc_fx_cash', 'account_prefix', '701.01.0001,701.01.0002,702.01.0001,702.01.0002',
     {'mode': 'addback', 'mirror_line_key': 'fx_effect', 'cash_move_only': True,
      'note': 'Diferencias cambiarias en pólizas que tocan efectivo → efecto por cambios en el valor del efectivo.'}),
    ('indirect', 'nc_interest', 'account_prefix', '701.04.',
     {'mode': 'addback', 'mirror_line_key': 'fin_interest', 'note': 'Intereses → financiamiento.'}),
    ('indirect', 'nc_lease', 'account_prefix', '701.11.0001',
     {'mode': 'addback', 'mirror_line_key': 'fin_lease', 'note': 'Arrendamiento financiero → financiamiento.'}),
    # ===== Metodo indirecto: resultado del periodo ========================
    ('indirect', 'result', 'account_prefix', '4,5,6,7',
     {'note': 'Resultado del periodo: cuentas 4xx–7xx (incluye 611 ISR). Se excluyen las pólizas de cierre.'}),
    # ===== Metodo indirecto: activo fijo y depreciacion acumulada =========
    ('indirect', 'nc_depreciation_ctr', 'account_prefix', '171.,183.,155.01.02',
     {'side': 'credit', 'note': 'Abonos a depreciación/amortización acumulada: contrapartida del gasto.'}),
    ('indirect', 'inv_disposals', 'account_prefix', '171.,183.,155.01.02',
     {'side': 'debit', 'note': 'Depreciación acumulada dada de baja.'}),
    ('indirect', 'nc_rou', 'account_prefix', '153.01.0002',
     {'note': 'Activo por derecho de uso (NIF D-5): sin flujo al reconocerse.'}),
    ('indirect', 'nc_rou', 'account_prefix', '205.02.02,205.02.03',
     {'side': 'credit', 'note': 'Pasivo por arrendamiento reconocido: sin flujo.'}),
    ('indirect', 'fin_lease', 'account_prefix', '205.02.02,205.02.03',
     {'side': 'debit', 'note': 'Pago del pasivo por arrendamiento.'}),
    ('indirect', 'inv_acquisitions', 'account_prefix',
     '153.01.01,154.01.01,155.01.01,156.01.01,168.01.01,181.01.01,188.03.01,184.03.01,184.03.02,184.03.03,184.03.04',
     {'side': 'debit', 'note': 'Cargos = adquisiciones y depósitos en garantía.'}),
    ('indirect', 'inv_disposals', 'account_prefix',
     '153.01.01,154.01.01,155.01.01,156.01.01,168.01.01,181.01.01,188.03.01,184.03.01,184.03.02,184.03.03,184.03.04',
     {'side': 'credit', 'note': 'Abonos = ventas/bajas a costo.'}),
    # ===== Metodo indirecto: capital de trabajo ===========================
    ('indirect', 'wc_receivables', 'account_prefix', '105.,107.05.01,108.', {'note': 'Clientes y estimación.'}),
    ('indirect', 'wc_inventory', 'account_prefix', '115.,107.05.02', {'note': 'Inventarios.'}),
    ('indirect', 'wc_other_receivables', 'account_prefix',
     '107.01.,107.05.001,107.05.03,109.01.,109.23.,110.,112.,114.01.01,120.,173.01.01,184.03.06',
     {'note': 'Otras cuentas por cobrar y pagos anticipados.'}),
    ('indirect', 'wc_tax_receivable', 'account_prefix', '113.,114.,118.,119.', {'note': 'Impuestos por recuperar.'}),
    ('indirect', 'wc_payroll', 'account_prefix',
     '210.,211.,215.,205.06.001,205.06.002,205.06.003,205.06.004',
     {'note': 'Nómina y prestaciones.'}),
    ('indirect', 'wc_payables', 'account_prefix',
     '201.,205.02.01,205.03.01,205.06.01,205.06.02,205.02.09',
     {'note': 'Proveedores y acreedores.'}),
    ('indirect', 'wc_customer_advances', 'account_prefix', '206.', {'note': 'Anticipos de clientes.'}),
    ('indirect', 'wc_taxes_payable', 'account_prefix',
     '205.02.05,205.02.06,205.02.07,205.02.08,208.,209.,213.,216.',
     {'note': 'Impuestos por pagar y retenciones.'}),
    # ===== Metodo indirecto: financiamiento ===============================
    ('indirect', 'fin_loans_received', 'account_prefix', '252.01.,205.06.03',
     {'side': 'credit', 'note': 'Abonos = disposiciones de préstamos.'}),
    ('indirect', 'fin_loans_paid', 'account_prefix', '252.01.,205.06.03',
     {'side': 'debit', 'note': 'Cargos = pagos de préstamos.'}),
    ('indirect', 'fin_related', 'account_prefix', '107.03.001,205.04.01', {'note': 'Partes relacionadas.'}),
    ('indirect', 'fin_equity', 'account_prefix', '301.,302.,303.,304.,305.',
     {'note': 'Capital y dividendos (el traspaso del resultado en el cierre se excluye).'}),
    # ===== Metodo indirecto: redes de seguridad por prefijo/tipo ==========
    ('indirect', 'inv_acquisitions', 'account_prefix', '15,16,18', {'side': 'debit', 'note': 'Otros activos fijos: cargos.'}),
    ('indirect', 'inv_disposals', 'account_prefix', '15,16,18', {'side': 'credit', 'note': 'Otros activos fijos: abonos.'}),
    ('indirect', 'nc_depreciation_ctr', 'account_prefix', '17', {'side': 'credit', 'note': 'Otra depreciación acumulada.'}),
    ('indirect', 'inv_disposals', 'account_prefix', '17', {'side': 'debit', 'note': 'Otra depreciación acumulada dada de baja.'}),
    ('indirect', 'fin_loans_received', 'account_prefix', '25', {'side': 'credit', 'note': 'Otros pasivos a largo plazo.'}),
    ('indirect', 'fin_loans_paid', 'account_prefix', '25', {'side': 'debit', 'note': 'Otros pasivos a largo plazo.'}),
    ('indirect', 'fin_equity', 'account_prefix', '3', {'note': 'Otras cuentas de capital.'}),
    ('indirect', 'wc_other_receivables', 'account_prefix', '107.', {'note': 'Otros deudores.'}),
    ('indirect', 'wc_payables', 'account_prefix', '205.', {'note': 'Otros acreedores.'}),
    ('indirect', 'wc_receivables', 'account_type', 'asset_receivable', {'note': 'Red de seguridad por tipo.'}),
    ('indirect', 'wc_payables', 'account_type', 'liability_payable', {'note': 'Red de seguridad por tipo.'}),
    ('indirect', 'result', 'account_type', 'income', {'note': 'Red de seguridad por tipo.'}),
    ('indirect', 'result', 'account_type', 'income_other', {'note': 'Red de seguridad por tipo.'}),
    ('indirect', 'result', 'account_type', 'expense', {'note': 'Red de seguridad por tipo.'}),
    ('indirect', 'result', 'account_type', 'expense_other', {'note': 'Red de seguridad por tipo.'}),
    ('indirect', 'result', 'account_type', 'expense_direct_cost', {'note': 'Red de seguridad por tipo.'}),
    ('indirect', 'result', 'account_type', 'expense_depreciation', {'note': 'Red de seguridad por tipo.'}),

    # ===== Metodo directo: por diario =====================================
    ('direct', 'd_fx', 'journal', False, {'journal_name': 'Diferencia de cambio',
                                          'note': 'Revaluación de bancos en USD.'}),
    ('direct', 'd_payroll', 'journal', False, {'journal_name': 'Nominas'}),
    ('direct', 'd_taxes', 'journal', False, {'journal_name': 'IMSS'}),
    ('direct', 'd_taxes', 'journal', False, {'journal_name': 'Impuestos'}),
    ('direct', 'd_taxes', 'journal', False, {'journal_name': 'IMPUESTOS FEDERALES'}),
    # ===== Metodo directo: diferencias cambiarias y tipo de asiento ========
    ('direct', 'd_fx', 'account_prefix', '701.01.0001,701.01.0002,702.01.0001,702.01.0002',
     {'note': 'Diferencia en ventas de USD (Mifel) y revaluaciones.'}),
    ('direct', 'd_customers', 'move_type', 'out_invoice,out_refund,out_receipt',
     {'note': 'Cobro registrado dentro de una factura de cliente: toda la póliza es cobro.'}),
    ('direct', 'd_suppliers', 'move_type', 'in_invoice,in_refund,in_receipt',
     {'note': 'Pago registrado dentro de una factura de proveedor: toda la póliza es pago.'}),
    # ===== Metodo directo: por contacto ===================================
    ('direct', 'd_taxes', 'partner', False, {'partner_name': 'Servicio de Administración Tributaria'}),
    ('direct', 'd_taxes', 'partner', False, {'partner_name': 'Instituto Mexicano del Seguro Social'}),
    ('direct', 'd_taxes', 'partner', False, {'partner_name': 'Gobierno de la Ciudad de México'}),
    ('direct', 'd_taxes', 'partner', False, {'partner_name': 'Gobierno del Estado de México'}),
    ('direct', 'd_lease', 'partner', False, {'partner_name': 'ICOMATEX'}),
    ('direct', 'd_lease', 'partner', False, {'partner_name': "Fong's"}),
    ('direct', 'd_lease', 'partner', False, {'partner_name': 'Interlock'}),
    ('direct', 'd_lease', 'partner', False, {'partner_name': 'Bianco'}),
    # ===== Metodo directo: por cuenta de contraparte =======================
    ('direct', 'd_customers', 'account_prefix', '105.,206.,4', {'note': 'Clientes, anticipos y ventas directas.'}),
    ('direct', 'd_payroll', 'account_prefix', '210.,211.,215.,205.06.001,205.06.002,205.06.003,205.06.004'),
    ('direct', 'd_taxes', 'account_prefix',
     '205.02.05,205.02.06,205.02.07,205.02.08,208.,209.,213.,216.,113.,114.,118.,119.'),
    ('direct', 'd_interest', 'account_prefix', '701.04.'),
    ('direct', 'd_bank_fees', 'account_prefix', '701.10.'),
    ('direct', 'd_interest_received', 'account_prefix', '702.04.'),
    ('direct', 'd_lease', 'account_prefix', '701.11.0001,205.02.02,205.02.03'),
    ('direct', 'd_loans_received', 'account_prefix', '252.01.,205.06.03', {'side': 'credit'}),
    ('direct', 'd_loans_paid', 'account_prefix', '252.01.,205.06.03', {'side': 'debit'}),
    ('direct', 'd_assets_sold', 'account_prefix', '704.23.0003,701.01.0004,17',
     {'note': 'Resultado en venta y depreciación acumulada dada de baja: parte del precio de venta.'}),
    ('direct', 'd_assets_bought', 'account_prefix', '15,16,18', {'side': 'debit'}),
    ('direct', 'd_assets_sold', 'account_prefix', '15,16,18', {'side': 'credit'}),
    ('direct', 'd_related', 'account_prefix', '107.03.001,205.04.01'),
    ('direct', 'd_equity', 'account_prefix', '3'),
    ('direct', 'd_suppliers', 'account_prefix',
     '201.,205.02.01,205.03.01,205.06.01,205.06.02,205.02.09,120.,115.,5,6',
     {'note': 'Proveedores, acreedores, anticipos a proveedores, inventario y gastos pagados directamente.'}),
    ('direct', 'd_suppliers', 'account_type', 'liability_payable', {'note': 'Red de seguridad por tipo.'}),
    ('direct', 'd_customers', 'account_type', 'asset_receivable', {'note': 'Red de seguridad por tipo.'}),
]


class CashFlowConfig(models.Model):
    _name = 'cash.flow.config'
    _description = 'Configuración del flujo de efectivo NIF B-2'
    _rec_name = 'company_id'

    company_id = fields.Many2one(
        'res.company', string='Compañía', required=True, index=True,
        default=lambda self: self.env.company)
    active = fields.Boolean(default=True)
    rule_ids = fields.One2many('cash.flow.rule', 'config_id', string='Reglas')
    cash_rule_ids = fields.One2many(
        'cash.flow.rule', 'config_id', string='Efectivo y equivalentes',
        domain=[('method', '=', 'cash')])
    indirect_rule_ids = fields.One2many(
        'cash.flow.rule', 'config_id', string='Método indirecto',
        domain=[('method', '=', 'indirect')])
    direct_rule_ids = fields.One2many(
        'cash.flow.rule', 'config_id', string='Método directo',
        domain=[('method', '=', 'direct')])
    other_threshold = fields.Float(
        string='Umbral de alerta "Otros" (%)', default=2.0,
        help='Se alerta cuando "Otros (revisar)" del método directo supera este porcentaje del total de salidas.')
    rule_count = fields.Integer(compute='_compute_rule_count')
    cash_account_count = fields.Integer(compute='_compute_cash_account_count', string='Cuentas de efectivo')

    _company_uniq = models.Constraint(
        'unique(company_id)',
        'Solo puede haber una configuración de flujo de efectivo por compañía.')

    @api.depends('rule_ids')
    def _compute_rule_count(self):
        for config in self:
            config.rule_count = len(config.rule_ids)

    def _compute_cash_account_count(self):
        for config in self:
            config.cash_account_count = len(config.get_cash_account_ids())

    # ------------------------------------------------------------------
    # Acceso
    # ------------------------------------------------------------------
    @api.model
    def _get_for_company(self, company, create=True):
        """Configuracion de ``company``; se crea vacia si no existe y ``create``."""
        config = self.with_context(active_test=False).search([('company_id', '=', company.id)], limit=1)
        if not config and create:
            config = self.create({'company_id': company.id})
        return config

    def _get_accounts(self):
        """Todas las cuentas de la compania (incluidas archivadas) como
        ``{id: (code, account_type, name)}``. El codigo se resuelve para la
        compania de la configuracion (``code_store`` es por compania)."""
        self.ensure_one()
        Account = self.env['account.account'].with_company(self.company_id).with_context(active_test=False).sudo()
        rows = Account.search_read(
            [('company_ids', 'in', self.company_id.ids)],
            ['code', 'account_type', 'name'], order='id')
        return {row['id']: (row['code'] or '', row['account_type'], row['name']) for row in rows}

    def get_cash_account_ids(self):
        """IDs de las cuentas que se consideran efectivo y equivalentes.

        Metodo publico (usable via JSON-2) para que scripts externos validen
        el saldo contable de efectivo con la misma definicion del reporte."""
        self.ensure_one()
        accounts = self._get_accounts()
        excluded, included = set(), set()
        for rule in self.rule_ids.filtered(lambda r: r.method == 'cash').sorted('sequence'):
            matched = rule._match_accounts(accounts)
            if rule.exclude:
                excluded |= matched
            else:
                included |= matched
        return sorted(included - excluded)

    # ------------------------------------------------------------------
    # Defaults
    # ------------------------------------------------------------------
    def action_load_quimibond_defaults(self):
        """Siembra el mapeo de Quimibond. Reemplaza las reglas actuales."""
        self.ensure_one()
        self.rule_ids.unlink()
        skipped = self._load_default_rules()
        message = _('Se cargaron %s reglas.', len(self.rule_ids))
        if skipped:
            message += ' ' + _('Se omitieron por no existir en esta compañía: %s.', ', '.join(skipped))
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {'title': _('Defaults Quimibond'), 'message': message, 'type': 'success' if not skipped else 'warning',
                       'next': {'type': 'ir.actions.act_window_close'}},
        }

    def _load_default_rules(self):
        """Crea las reglas de ``DEFAULT_RULES``. Devuelve los nombres de
        diarios/contactos que no se encontraron (esas reglas se omiten)."""
        self.ensure_one()
        Journal = self.env['account.journal'].with_context(active_test=False)
        Partner = self.env['res.partner']
        skipped = []
        values = []
        sequence = 10
        for item in DEFAULT_RULES:
            method, line_key, criterion, value = item[:4]
            extra = item[4] if len(item) > 4 else {}
            vals = {
                'config_id': self.id,
                'sequence': sequence,
                'method': method,
                'line_key': line_key or False,
                'criterion': criterion,
                'value': value or False,
                'side': extra.get('side', 'any'),
                'cash_move_only': extra.get('cash_move_only', False),
                'mode': extra.get('mode', 'flow'),
                'mirror_line_key': extra.get('mirror_line_key', False),
                'exclude': extra.get('exclude', False),
                'note': extra.get('note', False),
            }
            if criterion == 'journal':
                journal = Journal.search([('company_id', '=', self.company_id.id),
                                          ('name', '=ilike', extra['journal_name'])], limit=1)
                if not journal:
                    skipped.append(_('diario "%s"', extra['journal_name']))
                    continue
                vals['journal_id'] = journal.id
            elif criterion == 'partner':
                partner = Partner.search([('name', 'ilike', extra['partner_name']),
                                          ('company_id', 'in', [False, self.company_id.id])], limit=1)
                if not partner:
                    skipped.append(_('contacto "%s"', extra['partner_name']))
                    continue
                vals['partner_id'] = partner.commercial_partner_id.id or partner.id
            values.append(vals)
            sequence += 10
        self.env['cash.flow.rule'].create(values)
        return skipped

    # ------------------------------------------------------------------
    # API publica (JSON-2 / scripts)
    # ------------------------------------------------------------------
    def compute_summary(self, date_from, date_to):
        """Resumen del flujo de efectivo del periodo, serializable en JSON.

        Pensado para ``/json/2/cash.flow.config/compute_summary`` con
        ``{"ids": [id], "date_from": "2026-01-01", "date_to": "2026-08-31"}``."""
        self.ensure_one()
        result = self.env['cash.flow.engine'].compute(self, fields.Date.to_date(date_from), fields.Date.to_date(date_to))
        return self.env['cash.flow.engine'].to_summary(result)

    def action_open_report(self):
        self.ensure_one()
        action = self.env['ir.actions.actions']._for_xml_id('quimibond_cash_flow.action_cash_flow_nif_report')
        action['context'] = dict(self.env.context, report_id=self.env.ref('quimibond_cash_flow.cash_flow_nif_report').id)
        return action


class CashFlowRule(models.Model):
    _name = 'cash.flow.rule'
    _description = 'Regla de clasificación del flujo de efectivo'
    _order = 'config_id, method, sequence, id'
    _check_company_auto = True

    config_id = fields.Many2one('cash.flow.config', required=True, ondelete='cascade', index=True)
    company_id = fields.Many2one(related='config_id.company_id', store=True, index=True)
    sequence = fields.Integer(default=10)
    active = fields.Boolean(default=True)
    name = fields.Char(compute='_compute_name', store=True)
    method = fields.Selection([
        ('cash', 'Efectivo y equivalentes'),
        ('indirect', 'Método indirecto'),
        ('direct', 'Método directo'),
    ], required=True, default='indirect', string='Ámbito')
    line_key = fields.Selection(L.line_selection(), string='Línea del reporte')
    section = fields.Char(compute='_compute_section', string='Sección')
    criterion = fields.Selection([
        ('account_prefix', 'Prefijo de cuenta'),
        ('account', 'Cuenta'),
        ('account_type', 'Tipo de cuenta'),
        ('journal', 'Diario'),
        ('partner', 'Contacto'),
        ('move_type', 'Tipo de asiento'),
    ], required=True, default='account_prefix', string='Criterio')
    value = fields.Char(
        string='Valor',
        help='Prefijos de código de cuenta separados por coma (105., 206.), un tipo de cuenta '
             '(asset_cash, liability_payable, ...) o tipos de asiento separados por coma '
             '(out_invoice, in_invoice, entry).')
    account_id = fields.Many2one(
        'account.account', string='Cuenta', check_company=True,
        domain="[('company_ids', 'in', company_id)]")
    journal_id = fields.Many2one('account.journal', string='Diario', check_company=True)
    partner_id = fields.Many2one('res.partner', string='Contacto')
    side = fields.Selection([
        ('any', 'Cargos y abonos'),
        ('debit', 'Solo cargos'),
        ('credit', 'Solo abonos'),
    ], default='any', required=True, string='Lado')
    cash_move_only = fields.Boolean(
        string='Solo pólizas que tocan efectivo',
        help='Indirecto: la regla solo aplica a apuntes de pólizas con al menos una línea en una cuenta de efectivo.')
    mode = fields.Selection([
        ('flow', 'Flujo (efecto en efectivo)'),
        ('addback', 'Partida virtual (se suma de vuelta al resultado)'),
    ], default='flow', required=True, string='Signo',
        help='Flujo: el importe es -saldo (un abono en una cuenta que no es efectivo entra efectivo). '
             'Partida virtual: cuenta de resultados cuyo importe queda en el resultado, se revierte en esta '
             'línea y su efecto real se manda a la línea espejo.')
    mirror_line_key = fields.Selection(L.line_selection('indirect'), string='Línea espejo')
    exclude = fields.Boolean(string='Excluir', help='Efectivo: la cuenta NO es efectivo aunque otra regla la incluya.')
    note = fields.Char(string='Motivo')

    @api.depends('method', 'line_key', 'criterion', 'value', 'account_id', 'journal_id', 'partner_id', 'side', 'exclude')
    def _compute_name(self):
        for rule in self:
            if rule.criterion == 'account':
                target = rule.account_id.display_name or '?'
            elif rule.criterion == 'journal':
                target = rule.journal_id.display_name or '?'
            elif rule.criterion == 'partner':
                target = rule.partner_id.display_name or '?'
            else:
                target = rule.value or '?'
            if rule.method == 'cash':
                line = _('excluir') if rule.exclude else _('efectivo')
            else:
                line = L.LINE_LABELS.get(rule.line_key, '?')
            side = {'debit': ' (cargos)', 'credit': ' (abonos)'}.get(rule.side, '')
            rule.name = '%s%s → %s' % (target, side, line)

    @api.depends('line_key')
    def _compute_section(self):
        for rule in self:
            rule.section = L.SECTION_LABELS.get(L.LINE_SECTION.get(rule.line_key), '') if rule.line_key else ''

    @api.constrains('method', 'line_key', 'mode', 'mirror_line_key', 'criterion', 'value', 'account_id', 'journal_id', 'partner_id')
    def _check_rule(self):
        for rule in self:
            if rule.method != 'cash' and not rule.line_key:
                raise ValidationError(_('La regla "%s" necesita una línea del reporte.', rule.display_name))
            if rule.method != 'cash' and L.LINE_METHOD.get(rule.line_key) != rule.method:
                raise ValidationError(_('La línea "%s" no pertenece al método %s.',
                                        L.LINE_LABELS.get(rule.line_key), rule.method))
            if rule.mode == 'addback':
                if rule.method != 'indirect':
                    raise ValidationError(_('Las partidas virtuales solo existen en el método indirecto.'))
                if not rule.mirror_line_key:
                    raise ValidationError(_('La partida virtual "%s" necesita una línea espejo.', rule.display_name))
            if rule.criterion in ('account_prefix', 'account_type', 'move_type') and not rule.value:
                raise ValidationError(_('La regla "%s" necesita un valor.', rule.display_name))
            if rule.criterion == 'account' and not rule.account_id:
                raise ValidationError(_('La regla "%s" necesita una cuenta.', rule.display_name))
            if rule.criterion == 'journal' and not rule.journal_id:
                raise ValidationError(_('La regla "%s" necesita un diario.', rule.display_name))
            if rule.criterion == 'partner' and not rule.partner_id:
                raise ValidationError(_('La regla "%s" necesita un contacto.', rule.display_name))
            if rule.criterion == 'account_type':
                valid = dict(self.env['account.account']._fields['account_type'].selection)
                if rule.value not in valid:
                    raise ValidationError(_('"%s" no es un tipo de cuenta válido.', rule.value))

    # ------------------------------------------------------------------
    # Matching
    # ------------------------------------------------------------------
    def _values(self):
        """Lista de valores del campo ``value`` (separados por coma)."""
        self.ensure_one()
        return [v.strip() for v in (self.value or '').split(',') if v.strip()]

    def _match_accounts(self, accounts):
        """IDs de ``accounts`` (``{id: (code, type, name)}``) que cumplen el
        criterio de cuenta de la regla. ``None`` si la regla no restringe por
        cuenta (diario / contacto / tipo de asiento)."""
        self.ensure_one()
        if self.criterion == 'account_prefix':
            prefixes = tuple(self._values())
            return {aid for aid, (code, _type, _name) in accounts.items() if code.startswith(prefixes)}
        if self.criterion == 'account':
            return {self.account_id.id}
        if self.criterion == 'account_type':
            return {aid for aid, (_code, atype, _name) in accounts.items() if atype == self.value}
        return None

    def _compile(self, accounts):
        """Version compilada de la regla para el motor:
        ``(account_ids|None, journal_id|None, partner_id|None, move_types|None, side, cash_move_only)``."""
        self.ensure_one()
        return {
            'rule': self,
            'account_ids': self._match_accounts(accounts),
            'journal_id': self.journal_id.id if self.criterion == 'journal' else None,
            'partner_id': self.partner_id.id if self.criterion == 'partner' else None,
            'move_types': set(self._values()) if self.criterion == 'move_type' else None,
            'side': self.side,
            'cash_move_only': self.cash_move_only,
            'line_key': self.line_key,
            'mode': self.mode,
            'mirror_line_key': self.mirror_line_key,
        }

    def action_open_config(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'cash.flow.config',
            'res_id': self.config_id.id,
            'view_mode': 'form',
        }
