# -*- coding: utf-8 -*-
"""Acuerdo de volumen con escalera incremental (1.8.0) y su liquidación por periodo.

Nace de una línea ``escalera_incremental`` de una negociación ganada: el precio base ya está en la tarifa del
cliente y el descuento de los tramos se devuelve al cierre de cada periodo con una nota de crédito.

Liquidación de un periodo (botón «Calcular periodo» o el cron diario, ``dias_espera`` días después del cierre):

1. Unidades facturadas del producto al cliente en el periodo, netas de notas de crédito (la nota de una
   refacturación cuenta en el periodo de la factura que corrige; las notas de los acuerdos no cuentan) y sin el
   triplete lista/descuento/neta de una misma factura (``QTY_DEDUP_SQL`` del costeo).
2. Con «Solo facturas pagadas en plazo», cada factura se revisa: pagada a más tardar en su vencimiento cuenta;
   tarde, sin pagar o aún por vencer se excluye (o solo se marca, según el acuerdo). Su nota de crédito sigue a
   la factura.
3. Descuento por tramos (``descuento_tramos``) sobre el precio base, con los umbrales × los meses del periodo.
4. Una nota de crédito ``out_refund`` EN BORRADOR (nunca se publica ni se timbra aquí) relacionada con las facturas
   del periodo, para que Contabilidad la revise.
5. Una línea de historial por periodo: correrlo otra vez actualiza esa línea y la nota mientras siga en borrador;
   con la nota publicada ya no se toca.
"""
import logging
from datetime import timedelta

from dateutil.relativedelta import relativedelta

from odoo import Command, api, fields, models
from odoo.exceptions import UserError

from .negociacion import MESES_PERIODO, PERIODOS, descuento_tramos

_logger = logging.getLogger(__name__)

TRATAMIENTOS = [('excluir', 'Excluir las no pagadas en plazo'), ('marcar', 'Contarlas y solo marcarlas')]
ESTADOS_PAGO = {'en_plazo': 'pagada en plazo', 'tarde': 'pagada tarde', 'pendiente': 'aún no vence',
                'vencida': 'vencida sin pagar'}


class QbCotizadorAcuerdoVolumen(models.Model):
    _name = 'qb.cotizador.acuerdo.volumen'
    _description = 'Acuerdo de descuento por volumen (escalera incremental)'
    _inherit = ['mail.thread']
    _order = 'fecha_inicio desc, id desc'

    name = fields.Char(compute='_compute_name', store=True)
    active = fields.Boolean(default=True, tracking=True)
    negociacion_id = fields.Many2one('qb.cotizador.negociacion', string='Negociación', readonly=True, index=True)
    linea_id = fields.Many2one('qb.cotizador.negociacion.linea', string='Línea de la negociación', readonly=True)
    company_id = fields.Many2one('res.company', required=True, default=lambda self: self.env.company)
    partner_id = fields.Many2one('res.partner', string='Cliente', required=True, tracking=True)
    product_id = fields.Many2one('product.product', string='Producto', required=True, tracking=True)
    currency_id = fields.Many2one('res.currency', string='Moneda', required=True)
    fecha_inicio = fields.Date(string='Vigencia desde', required=True)
    fecha_fin = fields.Date(string='Vigencia hasta', required=True)
    precio_base = fields.Float(string='Precio base', digits=(16, 4), required=True)
    periodo = fields.Selection(PERIODOS, string='Periodo de medición', required=True, default='trimestral')
    solo_pagadas_en_plazo = fields.Boolean(string='Solo facturas pagadas en plazo')
    tratamiento_no_pagadas = fields.Selection(TRATAMIENTOS, string='Facturas no pagadas en plazo',
                                              default='excluir')
    dias_espera = fields.Integer(string='Días después del cierre para liquidar', default=5,
                                 help='El cron calcula el periodo estos días después de cerrar, para que entren las '
                                      'facturas y los pagos de los últimos días.')
    tramo_ids = fields.One2many('qb.cotizador.negociacion.tramo', 'acuerdo_id', string='Tramos')
    liquidacion_ids = fields.One2many('qb.cotizador.acuerdo.liquidacion', 'acuerdo_id', string='Liquidaciones')
    descuento_total = fields.Float(string='Descuento liquidado', digits=(16, 2), compute='_compute_totales')
    unidades_total = fields.Float(string='Unidades liquidadas', digits=(16, 0), compute='_compute_totales')

    @api.depends('negociacion_id.folio', 'partner_id.name', 'product_id.default_code', 'fecha_inicio')
    def _compute_name(self):
        for rec in self:
            partes = [rec.negociacion_id.folio, rec.product_id.default_code or rec.product_id.name,
                      rec.partner_id.name, str(rec.fecha_inicio.year) if rec.fecha_inicio else '']
            rec.name = ' · '.join(p for p in partes if p)

    @api.depends('liquidacion_ids.descuento', 'liquidacion_ids.unidades')
    def _compute_totales(self):
        for rec in self:
            rec.descuento_total = sum(rec.liquidacion_ids.mapped('descuento'))
            rec.unidades_total = sum(rec.liquidacion_ids.mapped('unidades'))

    @api.model
    def _crear_de_linea(self, linea):
        neg = linea.negociacion_id
        acuerdo = self.create({
            'negociacion_id': neg.id, 'linea_id': linea.id, 'company_id': neg.company_id.id,
            'partner_id': neg.partner_id.commercial_partner_id.id, 'product_id': linea.product_id.id,
            'currency_id': neg.currency_id.id, 'fecha_inicio': neg.fecha_inicio, 'fecha_fin': neg.fecha_fin,
            'precio_base': linea.precio_base, 'periodo': linea.periodo or 'mensual',
            'solo_pagadas_en_plazo': linea.solo_pagadas_en_plazo,
            'tramo_ids': [Command.create({'desde': t.desde, 'hasta': t.hasta, 'descuento_pct': t.descuento_pct})
                          for t in linea.tramo_ids],
        })
        linea.write({'acuerdo_id': acuerdo.id})
        acuerdo.message_post(body='Creado al ganar la negociación %s.' % neg.folio)
        return acuerdo

    # ==================================================================
    # Periodos
    # ==================================================================
    def _periodos(self):
        """[(inicio, fin, meses)] de la vigencia, alineados a su inicio; el último se corta en la vigencia y sus
        umbrales se multiplican por los meses que alcanza a cubrir."""
        self.ensure_one()
        paso = MESES_PERIODO[self.periodo]
        out, inicio = [], self.fecha_inicio
        while inicio <= self.fecha_fin:
            fin = min(inicio + relativedelta(months=paso) - timedelta(days=1), self.fecha_fin)
            dif = relativedelta(fin + timedelta(days=1), inicio)
            meses = dif.years * 12 + dif.months + (1 if dif.days else 0)
            out.append((inicio, fin, max(1, meses)))
            inicio = inicio + relativedelta(months=paso)
        return out

    # ==================================================================
    # Ventas del periodo
    # ==================================================================
    @api.model
    def _unidades_por_movimiento(self, partner, product, desde, hasta, company):
        """{move_id: unidades con signo} de facturas y notas de crédito publicadas del producto al cliente con
        fecha atribuida entre ``desde`` y ``hasta`` (inclusive). La nota de crédito de una refacturación
        (``reversed_entry_id``) toma la fecha de la factura que corrige; las notas de los acuerdos de volumen no
        cuentan; el triplete lista/descuento/neta se cuenta una vez (``QTY_DEDUP_SQL`` del costeo)."""
        from odoo.addons.qb_costeo.models.costo import QTY_DEDUP_SQL
        self.env.flush_all()
        self.env.cr.execute("""
            WITH lines AS (
                SELECT aml.move_id, aml.product_id, am.currency_id, aml.quantity, am.move_type
                  FROM account_move_line aml
                  JOIN account_move am ON am.id = aml.move_id
                  JOIN account_account aa ON aa.id = aml.account_id
             LEFT JOIN account_move orig ON orig.id = am.reversed_entry_id AND am.move_type = 'out_refund'
                 WHERE am.move_type IN ('out_invoice', 'out_refund')
                   AND am.state = 'posted'
                   AND aml.display_type = 'product'
                   AND aml.product_id = %s
                   AND aa.account_type = 'income'
                   AND aml.company_id = %s
                   AND am.commercial_partner_id = %s
                   AND am.qb_liquidacion_id IS NULL
                   AND COALESCE(orig.invoice_date, am.invoice_date) BETWEEN %s AND %s
            ),
            dedup AS (""" + QTY_DEDUP_SQL + """)
            SELECT move_id, SUM(CASE WHEN move_type = 'out_refund' THEN -quantity ELSE quantity END)
              FROM dedup GROUP BY move_id
        """, (product.id, company.id, partner.commercial_partner_id.id, desde, hasta))
        return dict(self.env.cr.fetchall())

    @api.model
    def _estado_pago(self, factura, hoy):
        """'en_plazo', 'tarde', 'pendiente' (sin pagar y aún no vence) o 'vencida' (sin pagar y ya venció).
        Pagada = ``payment_state`` paid / in_payment / reversed; la fecha de pago es la última conciliación
        (``max_date`` de sus ``account.partial.reconcile``)."""
        vence = factura.invoice_date_due or factura.invoice_date
        if factura.payment_state in ('paid', 'in_payment', 'reversed'):
            lineas = factura.line_ids.filtered(lambda l: l.display_type == 'payment_term')
            fechas = [p.max_date for p in (lineas.matched_credit_ids | lineas.matched_debit_ids) if p.max_date]
            pago = max(fechas) if fechas else vence
            return 'en_plazo' if pago <= vence else 'tarde'
        return 'pendiente' if hoy <= vence else 'vencida'

    # ==================================================================
    # Liquidación
    # ==================================================================
    def action_calcular_periodo(self):
        """Calcula (o recalcula) cada periodo ya cerrado de la vigencia. Un periodo con la nota publicada no se
        toca."""
        hoy = fields.Date.context_today(self)
        for rec in self:
            cerrados = [p for p in rec._periodos() if p[1] < hoy]
            if not cerrados:
                raise UserError('Ningún periodo de %s ha cerrado todavía (el primero cierra el %s).' % (
                    rec.name, rec._periodos()[0][1] if rec._periodos() else rec.fecha_fin))
            for inicio, fin, meses in cerrados:
                rec._liquidar(inicio, fin, meses)
        return True

    @api.model
    def _cron_liquidar(self):
        """Diario: liquida cada periodo cerrado hace ``dias_espera`` días que no tenga liquidación."""
        hoy = fields.Date.context_today(self)
        for rec in self.search([('active', '=', True), ('fecha_inicio', '<', hoy)]):
            hechos = set(rec.liquidacion_ids.mapped('periodo_inicio'))
            for inicio, fin, meses in rec._periodos():
                if inicio in hechos or fin + timedelta(days=rec.dias_espera or 0) >= hoy:
                    continue
                try:
                    with self.env.cr.savepoint():
                        rec._liquidar(inicio, fin, meses)
                except Exception as e:  # noqa: BLE001 - un acuerdo con error no detiene a los demás
                    _logger.exception('Liquidación de %s %s–%s falló', rec.name, inicio, fin)
                    rec.message_post(body='La liquidación automática del periodo %s a %s falló: %s' % (
                        inicio, fin, e))
        return True

    def _liquidar(self, inicio, fin, meses):
        """Calcula el periodo y crea o actualiza su línea de historial y su nota de crédito en borrador."""
        self.ensure_one()
        Liq = self.env['qb.cotizador.acuerdo.liquidacion'].sudo()
        liq = Liq.search([('acuerdo_id', '=', self.id), ('periodo_inicio', '=', inicio)], limit=1)
        if liq.nota_id and liq.nota_id.state != 'draft':
            liq.acuerdo_id.message_post(body='Periodo %s a %s: la nota %s ya está %s; no se recalcula.' % (
                inicio, fin, liq.nota_id.name, dict(liq.nota_id._fields['state'].selection).get(liq.nota_id.state)))
            return liq
        calculo = self._calcular(inicio, fin, meses)
        vals = dict(calculo['vals'], acuerdo_id=self.id, periodo_inicio=inicio, periodo_fin=fin,
                    calculado_el=fields.Datetime.now(), calculado_por_id=self.env.uid)
        if liq:
            liq.write(vals)
        else:
            liq = Liq.create(vals)
        liq._sincronizar_nota(calculo['facturas'])
        self.message_post(body='Periodo %s a %s: %s unidades, descuento %s %.2f%s.' % (
            inicio, fin, '{:,.0f}'.format(liq.unidades), self.currency_id.name, liq.descuento,
            (' (nota en borrador %s)' % liq.nota_id.display_name) if liq.nota_id else ''))
        return liq

    def _calcular(self, inicio, fin, meses):
        self.ensure_one()
        hoy = fields.Date.context_today(self)
        movs = self._unidades_por_movimiento(self.partner_id, self.product_id, inicio, fin, self.company_id)
        Move = self.env['account.move'].sudo()
        moves = Move.browse(list(movs))
        facturas = moves.filtered(lambda m: m.move_type == 'out_invoice')
        estados = {}
        if self.solo_pagadas_en_plazo:
            estados = {f.id: self._estado_pago(f, hoy) for f in facturas}
        no_cuentan = {fid for fid, est in estados.items() if est != 'en_plazo'}
        excluir = self.tratamiento_no_pagadas == 'excluir'
        unidades = excluidas = 0.0
        for move in moves:
            origen = move.reversed_entry_id.id if move.move_type == 'out_refund' else move.id
            if excluir and origen in no_cuentan:
                excluidas += movs[move.id]
            else:
                unidades += movs[move.id]
        unidades = max(unidades, 0.0)
        tramos = [(t.desde, t.hasta, t.descuento_pct) for t in self.tramo_ids]
        descuento, detalle = descuento_tramos(unidades, tramos, self.precio_base, meses)
        lineas = ['Umbrales × %d mes(es), precio base %s %s:' % (meses, self.precio_base, self.currency_id.name)]
        for lo, hi, pct, en_tramo, desc in detalle:
            lineas.append('- %s a %s: %s u × %s %% = %.2f' % (
                '{:,.0f}'.format(lo), '{:,.0f}'.format(hi) if hi else 'sin límite', '{:,.0f}'.format(en_tramo),
                pct, desc))
        marcadas = []
        for f in facturas.filtered(lambda f: f.id in no_cuentan):
            marcadas.append('- %s (%s, vence %s): %s u, %s' % (
                f.name, f.invoice_date, f.invoice_date_due, '{:,.0f}'.format(movs[f.id]), ESTADOS_PAGO[estados[f.id]]))
        cuentan = moves.filtered(lambda m: not (excluir and (
            (m.reversed_entry_id.id if m.move_type == 'out_refund' else m.id) in no_cuentan)))
        return {
            'facturas': cuentan.filtered(lambda m: m.move_type == 'out_invoice'),
            'vals': {
                'meses': meses, 'unidades': unidades, 'unidades_excluidas': excluidas, 'descuento': descuento,
                'detalle_tramos': '\n'.join(lineas),
                'detalle_pagos': (('Facturas %s:\n' % ('excluidas' if excluir else 'marcadas (sí cuentan)'))
                                  + '\n'.join(marcadas)) if marcadas else False,
                'factura_ids': [Command.set(moves.ids)],
                'marcadas': len(marcadas),
            },
        }


class QbCotizadorAcuerdoLiquidacion(models.Model):
    _name = 'qb.cotizador.acuerdo.liquidacion'
    _description = 'Liquidación de un periodo de un acuerdo de volumen'
    _order = 'acuerdo_id, periodo_inicio desc'
    _rec_name = 'periodo_inicio'

    acuerdo_id = fields.Many2one('qb.cotizador.acuerdo.volumen', required=True, ondelete='cascade', index=True)
    currency_id = fields.Many2one(related='acuerdo_id.currency_id')
    partner_id = fields.Many2one(related='acuerdo_id.partner_id', string='Cliente')
    product_id = fields.Many2one(related='acuerdo_id.product_id', string='Producto')
    periodo_inicio = fields.Date(string='Desde', required=True)
    periodo_fin = fields.Date(string='Hasta', required=True)
    meses = fields.Integer(string='Meses del periodo')
    unidades = fields.Float(string='Unidades que cuentan', digits=(16, 0))
    unidades_excluidas = fields.Float(string='Unidades excluidas', digits=(16, 0),
                                      help='De facturas no pagadas en plazo (y sus notas de crédito).')
    marcadas = fields.Integer(string='Facturas no pagadas en plazo')
    descuento = fields.Float(string='Descuento', digits=(16, 2))
    detalle_tramos = fields.Text(string='Descuento por tramo')
    detalle_pagos = fields.Text(string='Facturas no pagadas en plazo (detalle)')
    factura_ids = fields.Many2many('account.move', 'qb_cot_liquidacion_move_rel', 'liquidacion_id', 'move_id',
                                   string='Facturas y notas del periodo')
    nota_id = fields.Many2one('account.move', string='Nota de crédito', readonly=True, copy=False)
    nota_estado = fields.Selection(related='nota_id.state', string='Estado de la nota')
    calculado_el = fields.Datetime(string='Calculada el', readonly=True)
    calculado_por_id = fields.Many2one('res.users', string='Calculó', readonly=True)

    _periodo_uniq = models.Constraint('unique(acuerdo_id, periodo_inicio)',
                                      'Ese periodo del acuerdo ya tiene su liquidación.')

    def _sincronizar_nota(self, facturas):
        """Crea o actualiza la nota de crédito EN BORRADOR del periodo; sin descuento, borra el borrador."""
        self.ensure_one()
        acuerdo = self.acuerdo_id
        nota = self.nota_id
        if nota and nota.state != 'draft':
            return nota
        if self.descuento <= 0:
            if nota:
                self.nota_id = False
                nota.unlink()
            return False
        texto = 'Descuento por volumen %s, periodo %s a %s: %s unidades' % (
            acuerdo.name, self.periodo_inicio, self.periodo_fin, '{:,.0f}'.format(self.unidades))
        linea = {'product_id': acuerdo.product_id.id, 'name': texto, 'quantity': 1, 'price_unit': self.descuento}
        Move = self.env['account.move'].sudo().with_company(acuerdo.company_id)
        vals = {
            'ref': texto[:250],
            'invoice_origin': ', '.join(facturas.mapped('name'))[:1000] or False,
        }
        if 'l10n_mx_edi_usage' in Move._fields:
            vals['l10n_mx_edi_usage'] = 'G02'  # Devoluciones, descuentos o bonificaciones
        if 'l10n_mx_edi_cfdi_origin' in Move._fields and 'l10n_mx_edi_cfdi_uuid' in Move._fields:
            uuids = [u for u in facturas.mapped('l10n_mx_edi_cfdi_uuid') if u]
            vals['l10n_mx_edi_cfdi_origin'] = ('01|' + ','.join(uuids)) if uuids else False
        if nota:
            producto = nota.invoice_line_ids.filtered(lambda l: l.display_type == 'product')[:1]
            vals['invoice_line_ids'] = [Command.update(producto.id, linea)] if producto else [Command.create(linea)]
            nota.write(vals)
            return nota
        vals.update({
            'move_type': 'out_refund', 'partner_id': acuerdo.partner_id.id, 'currency_id': acuerdo.currency_id.id,
            'company_id': acuerdo.company_id.id, 'qb_liquidacion_id': self.id,
            'invoice_line_ids': [Command.create(linea)],
        })
        nota = Move.with_context(default_move_type='out_refund').create(vals)
        self.nota_id = nota
        nota.message_post(body='Nota de crédito en borrador del acuerdo de volumen %s (periodo %s a %s). '
                               'Contabilidad la revisa, la publica y la timbra.' % (
                                   acuerdo.name, self.periodo_inicio, self.periodo_fin))
        return nota

    def action_recalcular(self):
        for rec in self:
            acuerdo = rec.acuerdo_id
            meses = next((m for i, _f, m in acuerdo._periodos() if i == rec.periodo_inicio), rec.meses or 1)
            acuerdo._liquidar(rec.periodo_inicio, rec.periodo_fin, meses)
        return True
