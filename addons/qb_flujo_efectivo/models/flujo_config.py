# -*- coding: utf-8 -*-
"""Supuestos del flujo de efectivo a 13 semanas (uno por empresa).

Todo lo que no sale de un documento de Odoo es un supuesto y vive aquí, a la
vista y editable: qué cuentas son caja, desde cuándo la cartera se considera
cobrable, qué contactos se revisan antes de pagar, los compromisos recurrentes
(nómina, IMSS, SAT…) y los ajustes manuales de una sola vez.
"""
from datetime import date

from odoo import _, api, fields, models
from odoo.exceptions import UserError

from . import calendario

PERIODICIDADES = [
    ('semanal', 'Semanal'),
    ('quincenal', 'Quincenal (15 y fin de mes)'),
    ('mensual', 'Mensual'),
    ('bimestral', 'Bimestral'),
    ('unica', 'Una sola vez'),
]
DIAS_SEMANA = [(str(i), n) for i, n in enumerate(
    ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo'])]


class QbFlujoConfig(models.Model):
    _name = 'qb.flujo.config'
    _description = 'Supuestos del flujo de efectivo a 13 semanas'
    _rec_name = 'company_id'

    company_id = fields.Many2one('res.company', string='Empresa', required=True, ondelete='cascade',
                                 default=lambda self: self.env.company)
    currency_id = fields.Many2one(related='company_id.currency_id')
    caja_ids = fields.One2many('qb.flujo.caja', 'config_id', string='Cuentas de caja')
    compromiso_ids = fields.One2many('qb.flujo.compromiso', 'config_id', string='Compromisos recurrentes')
    ajuste_ids = fields.One2many('qb.flujo.ajuste', 'config_id', string='Ajustes manuales')
    cartera_desde = fields.Date(
        string='Cartera cobrable desde', default=date(2026, 1, 1),
        help='Las facturas de cliente con fecha anterior se consideran incobrables: no entran a la '
             'cobranza y se muestran aparte.')
    dias_incobrable = fields.Integer(
        string='Vencido incobrable a más de (días)', default=90,
        help='Una factura de cliente vencida hace más de estos días no entra a la cobranza y se muestra aparte.')
    partner_revisar_ids = fields.Many2many(
        'res.partner', 'qb_flujo_config_partner_revisar_rel', 'config_id', 'partner_id',
        string='Proveedores por revisar antes de pagar',
        help='Sus facturas no entran a los pagos; se muestran aparte hasta que se aclaren.')
    incluir_pedidos = fields.Boolean(
        string='Incluir pedidos y órdenes de compra por facturar', default=True,
        help='Lo confirmado que aún no se factura entra como estimado: fecha de entrega más el plazo de pago.')
    dias_pedido_vigente = fields.Integer(
        string='Pedido vigente hasta (días)', default=90,
        help='Pedidos de venta y órdenes de compra con fecha de orden más antigua no se toman (se suponen '
             'abandonados).')
    saldo_minimo = fields.Monetary(
        string='Saldo mínimo de caja', currency_field='currency_id',
        help='El reporte marca las semanas cuyo saldo final queda por debajo.')

    _company_uniq = models.Constraint('unique(company_id)', 'Ya hay supuestos del flujo para esta empresa.')

    @api.model
    def _para_empresa(self, company):
        return self.search([('company_id', '=', company.id)], limit=1)

    @api.model
    def action_abrir(self):
        config = self._para_empresa(self.env.company) or self.create({'company_id': self.env.company.id})
        return {
            'type': 'ir.actions.act_window',
            'name': _('Supuestos del flujo a 13 semanas'),
            'res_model': self._name,
            'res_id': config.id,
            'view_mode': 'form',
            'target': 'current',
        }

    def action_diagnostico(self, fecha=None):
        """Calcula el flujo y arma el reporte; devuelve un resumen o el error
        completo. Sirve para revisar el módulo sin abrir el log del servidor."""
        import traceback
        self.ensure_one()
        hoy = fields.Date.to_date(fecha) if fecha else fields.Date.context_today(self)
        out = {'fecha': fields.Date.to_string(hoy)}
        try:
            res = self.env['qb.flujo.motor'].with_company(self.company_id).calcular(self, hoy)
            out['caja'] = [{k: (fields.Date.to_string(v) if isinstance(v, date) else v) for k, v in c.items()}
                           for c in res['caja']]
            out['caja_total'] = res['caja_total']
            out['filas'] = {k: [round(v, 2) for v in f['semanas']] for k, f in res['filas'].items()}
            out['info'] = {k: round(f['total'], 2) for k, f in res['info'].items()}
        except Exception:  # noqa: BLE001 - es un diagnóstico: se devuelve el error
            out['error_motor'] = traceback.format_exc()
            return out
        try:
            report = self.env.ref('qb_flujo_efectivo.qb_flujo_report').with_company(self.company_id)
            options = report.get_options({'date': {'date_to': fields.Date.to_string(hoy), 'mode': 'single',
                                                   'filter': 'custom'}})
            info = report.get_report_information(options)
            out['reporte'] = [(line.get('name'), [c.get('no_format') for c in line.get('columns', [])][-1:])
                              for line in info.get('lines', [])]
        except Exception:  # noqa: BLE001
            out['error_reporte'] = traceback.format_exc()
        return out

    def action_estimar_compromisos(self):
        self.compromiso_ids.filtered(lambda c: c.partner_id).action_estimar()
        return True

    # ------------------------------------------------------------------
    # Valores iniciales de Quimibond (el reporte diario de pagos y cobros)
    # ------------------------------------------------------------------
    def action_cargar_quimibond(self):
        """Carga la caja y los compromisos que usa el reporte diario. No borra
        nada: solo agrega lo que falta."""
        self.ensure_one()
        Journal = self.env['account.journal']
        cajas = {'8792': 'extracto', '5048': 'extracto', '5098': 'extracto', '5386': 'extracto',
                 '0546': 'extracto', '5109': 'contable', '7755': 'contable'}
        ya = set(self.caja_ids.mapped('journal_id.code'))
        for code, fuente in cajas.items():
            if code in ya:
                continue
            journal = Journal.search([('company_id', '=', self.company_id.id), ('code', '=', code),
                                      ('type', '=', 'bank')], limit=1)
            if journal:
                self.env['qb.flujo.caja'].create({'config_id': self.id, 'journal_id': journal.id, 'fuente': fuente})

        def partner(nombre):
            return self.env['res.partner'].search([('name', '=ilike', nombre)], limit=1)

        nomina = partner('NOMINA')
        imss = partner('Instituto Mexicano del Seguro Social')
        sat = partner('Servicio de Administracion Tributaria')
        base = [
            {'name': 'Nómina semanal', 'categoria': 'nomina', 'periodicidad': 'semanal', 'dia_semana': '4',
             'partner_id': nomina.id, 'texto_memo': 'SEM', 'periodos_historial': 8},
            {'name': 'Nómina quincenal', 'categoria': 'nomina', 'periodicidad': 'quincenal',
             'partner_id': nomina.id, 'texto_memo': 'Q ', 'periodos_historial': 4},
            {'name': 'IMSS cuotas del mes', 'categoria': 'imss', 'periodicidad': 'mensual', 'dia_mes': 17,
             'partner_id': imss.id, 'texto_memo': 'IMSS', 'texto_excluir': 'RCV', 'periodos_historial': 3},
            {'name': 'IMSS RCV e Infonavit', 'categoria': 'imss', 'periodicidad': 'bimestral', 'dia_mes': 17,
             'partner_id': imss.id, 'texto_memo': 'RCV', 'periodos_historial': 2},
            {'name': 'SAT pagos provisionales y retenciones', 'categoria': 'sat', 'periodicidad': 'mensual',
             'dia_mes': 17, 'partner_id': sat.id, 'texto_memo': 'PROVISI', 'periodos_historial': 3},
        ]
        nombres = set(self.compromiso_ids.mapped('name'))
        nuevos = self.env['qb.flujo.compromiso']
        for vals in base:
            if vals['name'] in nombres or not vals['partner_id']:
                continue
            nuevos |= nuevos.create(dict(vals, config_id=self.id))
        nuevos.action_estimar()
        revisar = self.env['res.partner'].search(
            ['|', ('name', '=ilike', 'GRUPO INDUSTRIAL MIRO'), ('name', '=ilike', 'INTRVC INTERACCION COMERCIAL')])
        if revisar:
            self.partner_revisar_ids = [fields.Command.link(p.id) for p in revisar]
        return True


class QbFlujoCaja(models.Model):
    _name = 'qb.flujo.caja'
    _description = 'Cuenta de caja del flujo a 13 semanas'
    _order = 'sequence, id'

    config_id = fields.Many2one('qb.flujo.config', required=True, ondelete='cascade')
    sequence = fields.Integer(default=10)
    journal_id = fields.Many2one('account.journal', string='Diario', required=True,
                                 domain="[('type', 'in', ('bank', 'cash')), ('company_id', '=', parent.company_id)]")
    fuente = fields.Selection(
        [('extracto', 'Último estado de cuenta'), ('contable', 'Saldo contable')],
        string='Saldo', required=True, default='extracto',
        help='Último estado de cuenta: saldo final del último extracto (lo que dice el banco). '
             'Saldo contable: saldo de la cuenta del diario en la contabilidad.')


class QbFlujoCompromiso(models.Model):
    _name = 'qb.flujo.compromiso'
    _description = 'Compromiso recurrente del flujo a 13 semanas'
    _order = 'sequence, id'

    config_id = fields.Many2one('qb.flujo.config', required=True, ondelete='cascade')
    currency_id = fields.Many2one(related='config_id.currency_id')
    sequence = fields.Integer(default=10)
    active = fields.Boolean(default=True)
    name = fields.Char(string='Concepto', required=True)
    categoria = fields.Selection(
        [('nomina', 'Nómina'), ('imss', 'IMSS e Infonavit'), ('sat', 'Impuestos (SAT)'),
         ('financiero', 'Créditos y arrendamientos'), ('otro', 'Otro')],
        string='Categoría', required=True, default='otro')
    tipo = fields.Selection([('salida', 'Salida'), ('entrada', 'Entrada')], required=True, default='salida')
    importe = fields.Monetary(string='Importe por pago', currency_field='currency_id', required=True, default=0.0,
                              help='En pesos y en positivo; el tipo dice si entra o sale.')
    periodicidad = fields.Selection(PERIODICIDADES, required=True, default='mensual')
    dia_semana = fields.Selection(DIAS_SEMANA, string='Día de la semana', default='4')
    dia_mes = fields.Integer(string='Día del mes', default=17)
    meses_pares = fields.Boolean(string='En meses pares', help='Bimestral: sin marcar paga en meses nones.')
    fecha = fields.Date(string='Fecha', help='Para «Una sola vez».')
    fecha_fin = fields.Date(string='Hasta', help='Vacío: sin fin.')
    # Estimación con lo pagado
    partner_id = fields.Many2one('res.partner', string='Contacto de los pagos',
                                 help='Se usa para estimar el importe con los pagos pasados. Sus facturas de '
                                      'proveedor no se suman otra vez en los pagos.')
    texto_memo = fields.Char(string='Memo contiene', help='Solo los pagos cuyo memo contiene este texto.')
    texto_excluir = fields.Char(string='Memo no contiene')
    periodos_historial = fields.Integer(string='Periodos para estimar', default=3)
    importe_estimado = fields.Monetary(string='Estimado con historial', currency_field='currency_id', readonly=True)
    fecha_estimacion = fields.Date(string='Estimado el', readonly=True)

    @api.constrains('dia_mes')
    def _check_dia_mes(self):
        for rec in self:
            if rec.periodicidad in ('mensual', 'bimestral') and not 1 <= (rec.dia_mes or 0) <= 31:
                raise UserError(_('El día del mes va de 1 a 31.'))

    def fechas_entre(self, desde, hasta):
        self.ensure_one()
        if self.fecha_fin and self.fecha_fin < hasta:
            hasta = self.fecha_fin
        return calendario.fechas(self.periodicidad, desde, hasta, dia_semana=int(self.dia_semana or 4),
                                 dia_mes=self.dia_mes or 17, meses_pares=self.meses_pares, fecha_unica=self.fecha)

    def _pagos_historial(self, hoy):
        self.ensure_one()
        desde = calendario.inicio_historial(self.periodicidad, hoy, self.periodos_historial or 3)
        domain = [('company_id', '=', self.config_id.company_id.id), ('partner_id', '=', self.partner_id.id),
                  ('payment_type', '=', 'outbound' if self.tipo == 'salida' else 'inbound'),
                  ('state', 'in', ('in_process', 'paid')), ('date', '>=', desde), ('date', '<', hoy)]
        if self.texto_memo:
            domain.append(('memo', 'ilike', self.texto_memo))
        if self.texto_excluir:
            domain.append(('memo', 'not ilike', self.texto_excluir))
        return self.env['account.payment'].search(domain)

    def action_estimar(self):
        """Importe por pago = promedio de lo pagado por periodo con pago."""
        hoy = fields.Date.context_today(self)
        for rec in self.filtered('partner_id'):
            company = rec.config_id.company_id
            pagos = [(p.date, p.currency_id._convert(p.amount, company.currency_id, company, p.date))
                     for p in rec._pagos_historial(hoy)]
            estimado = calendario.estimar(rec.periodicidad, pagos)
            vals = {'importe_estimado': estimado, 'fecha_estimacion': hoy}
            if estimado:
                vals['importe'] = round(estimado, -2)
            rec.write(vals)
        return True


class QbFlujoAjuste(models.Model):
    _name = 'qb.flujo.ajuste'
    _description = 'Ajuste manual del flujo a 13 semanas'
    _order = 'fecha, id'

    config_id = fields.Many2one('qb.flujo.config', required=True, ondelete='cascade')
    currency_id = fields.Many2one(related='config_id.currency_id')
    fecha = fields.Date(required=True)
    name = fields.Char(string='Concepto', required=True)
    tipo = fields.Selection([('entrada', 'Entrada'), ('salida', 'Salida')], required=True, default='entrada')
    importe = fields.Monetary(currency_field='currency_id', required=True, default=0.0,
                              help='En pesos y en positivo.')
    nota = fields.Char()
