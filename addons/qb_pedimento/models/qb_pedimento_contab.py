# -*- coding: utf-8 -*-
"""Expediente contra contabilidad (1.2.0).

Logística captura las contribuciones del pedimento en el expediente y
contabilidad las vuelve a capturar en su factura de impuestos (líneas con los
productos IGI, DTA y PRV y el IVA de importación a su cuenta). Aquí se
comparan: lo que dice el expediente contra lo publicado en las facturas
ligadas (de la mercancía y de la agencia). El IVA del pedimento se compara
junto con el IVA/PRV, que contabilidad registra en la misma línea.

No crea ni cambia asientos. Si falta la factura de impuestos o no cuadra, el
aviso diario va al responsable en contabilidad.
"""
from odoo import _, api, fields, models

from .qb_pedimento import dias_habiles_entre

# Diferencia admitida por concepto (MXN): redondeos del pedimento.
TOLERANCIA = 1.0
ESTADOS_CON_PAGO = ('despachado', 'entregado', 'cerrado')


class QbPedimentoContab(models.Model):
    _inherit = 'qb.pedimento'

    contab_dta = fields.Monetary('DTA en contabilidad', compute='_compute_contab')
    contab_igi = fields.Monetary('IGI en contabilidad', compute='_compute_contab')
    contab_prv = fields.Monetary('PRV en contabilidad', compute='_compute_contab')
    contab_iva = fields.Monetary(
        'IVA en contabilidad', compute='_compute_contab',
        help='Saldo de la cuenta de IVA de importación (Ajustes) en las facturas publicadas ligadas. '
             'Se compara contra IVA + IVA/PRV del pedimento.')
    contab_estado = fields.Selection([
        ('sin_poliza', 'Sin factura de impuestos'),
        ('cuadra', 'Cuadra con contabilidad'),
        ('diferencia', 'No cuadra con contabilidad'),
    ], string='Contabilidad', compute='_compute_contab',
        help='Contribuciones del expediente contra las facturas publicadas ligadas. Solo importaciones '
             'con el pedimento pagado.')
    contab_detalle = fields.Char('Diferencias con contabilidad', compute='_compute_contab')

    def _contab_moves(self):
        return (self.invoice_ids | self.gasto_move_ids).filtered(lambda m: m.state == 'posted')

    @api.depends('operacion', 'state', 'dta', 'igi', 'prv', 'iva', 'iva_prv',
                 'invoice_ids.state', 'gasto_move_ids.state')
    def _compute_contab(self):
        for rec in self:
            company = rec.company_id
            lines = rec._contab_moves().line_ids
            por_producto = {}
            for line in lines.filtered('product_id'):
                por_producto[line.product_id] = por_producto.get(line.product_id, 0.0) + line.balance
            rec.contab_dta = por_producto.get(company.qb_pedimento_product_dta_id, 0.0)
            rec.contab_igi = por_producto.get(company.qb_pedimento_product_igi_id, 0.0)
            rec.contab_prv = por_producto.get(company.qb_pedimento_product_prv_id, 0.0)
            iva_account = company.qb_pedimento_iva_account_id
            rec.contab_iva = sum(lines.filtered(lambda l: l.account_id == iva_account).mapped('balance')) \
                if iva_account else 0.0
            if rec.operacion != 'importacion' or rec.state not in ESTADOS_CON_PAGO:
                rec.contab_estado = False
                rec.contab_detalle = False
                continue
            conceptos = [
                (_('DTA'), rec.dta, rec.contab_dta),
                (_('IGI'), rec.igi, rec.contab_igi),
                (_('PRV'), rec.prv, rec.contab_prv),
            ]
            if iva_account:
                conceptos.append((_('IVA'), rec.iva + rec.iva_prv, rec.contab_iva))
            if not any(abs(c) > TOLERANCIA for _n, _p, c in conceptos):
                rec.contab_estado = 'sin_poliza' if any(abs(p) > TOLERANCIA for _n, p, _c in conceptos) \
                    else False
                rec.contab_detalle = False
                continue
            difs = ['%s: pedimento %s, contabilidad %s' % (n, '{:,.2f}'.format(p), '{:,.2f}'.format(c))
                    for n, p, c in conceptos if abs(p - c) > TOLERANCIA]
            rec.contab_estado = 'diferencia' if difs else 'cuadra'
            rec.contab_detalle = '; '.join(difs) or False

    @api.model
    def _cron_avisos(self, hoy=None):
        res = super()._cron_avisos(hoy=hoy)
        self._cron_avisos_contab(hoy=hoy)
        return res

    @api.model
    def _cron_avisos_contab(self, hoy=None):
        """Importaciones pagadas hace más de los días de cierre sin factura de
        impuestos, o con contribuciones que no cuadran. El aviso se cierra
        solo cuando cuadra. Idempotente."""
        hoy = hoy or fields.Date.context_today(self)
        for company in self.env['res.company'].search([]):
            peds = self.search([('company_id', '=', company.id), ('operacion', '=', 'importacion'),
                                ('state', 'in', ESTADOS_CON_PAGO), ('fecha_pago', '!=', False)])
            for rec in peds:
                user = company.qb_pedimento_contabilidad_user_id or rec.user_id \
                    or company.qb_pedimento_responsable_id
                claves = {'sin_poliza': _('Sin factura de impuestos'), 'diferencia': _('No cuadra con contabilidad')}
                vigente = '[Pedimento] %s' % claves[rec.contab_estado] if rec.contab_estado in claves else None
                # El aviso que ya no aplica (cuadró, o pasó de «sin factura» a «no cuadra») se cierra.
                rec.activity_ids.filtered(
                    lambda a: a.summary in ['[Pedimento] %s' % c for c in claves.values()] and a.summary != vigente
                ).action_feedback(feedback=_('Revisión con contabilidad: %s.') % (
                    dict(self._fields['contab_estado']._description_selection(self.env)).get(rec.contab_estado)
                    or _('ya no aplica')))
                if rec.contab_estado == 'diferencia':
                    rec._avisar(user, _('No cuadra con contabilidad'), _(
                        'Las contribuciones del expediente no cuadran con las facturas publicadas ligadas: '
                        '%(d)s. Corrija el expediente o la factura de impuestos.', d=rec.contab_detalle))
                elif rec.contab_estado == 'sin_poliza' \
                        and dias_habiles_entre(rec.fecha_pago, hoy) >= company.qb_pedimento_dias_cierre:
                    rec._avisar(user, _('Sin factura de impuestos'), _(
                        'Pedimento pagado el %(f)s sin factura de impuestos publicada y ligada (DTA, IGI, PRV, '
                        'IVA de importación). Regístrela y líguela en la pestaña «Ligado a».', f=rec.fecha_pago))
