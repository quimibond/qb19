# -*- coding: utf-8 -*-
"""Recalcular el costeo de un rango de meses cualquiera.

El motor siempre supo costear cualquier período —`action_recompute_period`
recibe una fecha—, pero desde la UI solo se podía pedir el mes anterior o el
año EN CURSO: el menú llamaba `action_recompute_year()` sin argumento. Para
ver 2025 o 2024 había que entrar al shell, así que en la práctica no se veían.

Este asistente expone el rango. Recorre mes a mes, respeta los períodos
cerrados y reporta cuántos recalculó y cuántos se saltó.
"""
import logging

from dateutil.relativedelta import relativedelta

from odoo import _, api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

# Tope de meses por corrida. Cada mes recalcula ~1,250 productos con
# explosión de BOM y consultas al mayor sobre una ventana de 12 meses: en
# producción son ~10 s por mes. Treinta y seis meses son ~6 minutos, que un
# request web aguanta; más que eso conviene partirlo en dos corridas.
MAX_MESES = 36


class QbRecalculoWizard(models.TransientModel):
    _name = 'qb.recalculo.wizard'
    _description = 'Recalcular costeo por rango de meses'

    desde = fields.Date(
        string='Desde', required=True,
        help='Primer mes a recalcular. Se usa el día 1 del mes que elijas.')
    hasta = fields.Date(
        string='Hasta', required=True,
        help='Último mes a recalcular, inclusive.')
    incluir_cerrados = fields.Boolean(
        string='Incluir períodos cerrados', default=False,
        help='Por defecto NO: un período cerrado se congeló a propósito y '
             'recalcularlo cambiaría un número que ya se reportó. Márcalo '
             'solo si de verdad quieres reabrir la historia.')
    resultado = fields.Text(string='Resultado', readonly=True)

    @api.model
    def default_get(self, fields_list):
        res = super().default_get(fields_list)
        hoy = fields.Date.today().replace(day=1)
        # Por defecto, el año anterior completo: es lo que no se puede pedir
        # desde el menú y por lo que existe este asistente.
        res.setdefault('desde', hoy.replace(month=1) - relativedelta(years=1))
        res.setdefault('hasta', hoy.replace(month=12) - relativedelta(years=1))
        return res

    @api.constrains('desde', 'hasta')
    def _check_rango(self):
        for rec in self:
            if rec.desde and rec.hasta and rec.hasta < rec.desde:
                raise UserError(_('«Hasta» no puede ser anterior a «Desde».'))

    def _meses(self):
        """Los primeros de mes del rango, inclusive en los dos extremos."""
        self.ensure_one()
        mes = self.desde.replace(day=1)
        fin = self.hasta.replace(day=1)
        meses = []
        while mes <= fin:
            meses.append(mes)
            mes += relativedelta(months=1)
        return meses

    def action_recalcular(self):
        self.ensure_one()
        meses = self._meses()
        if len(meses) > MAX_MESES:
            raise UserError(_(
                'El rango son %(n)s meses y el tope por corrida es %(max)s. '
                'Cada mes recalcula ~1,250 productos, así que un rango muy '
                'largo deja el navegador esperando. Pártelo en dos.',
                n=len(meses), max=MAX_MESES))

        Costo = self.env['qb.costo.producto']
        Factores = self.env['qb.costo.factores']
        hechos, saltados = [], []
        for mes in meses:
            if not self.incluir_cerrados and Factores.periodo_cerrado(mes):
                saltados.append(mes)
                continue
            ctx = {'qb_forzar_periodo_cerrado': True} if self.incluir_cerrados \
                else {}
            Costo.with_context(**ctx).action_recompute_period(mes)
            hechos.append(mes)

        _logger.info(
            'qb.recalculo.wizard: %s meses recalculados (%s → %s), %s '
            'saltados por estar cerrados.', len(hechos), self.desde,
            self.hasta, len(saltados))

        lineas = [_('%s meses recalculados.') % len(hechos)]
        if saltados:
            lineas.append(_(
                '%(n)s saltados por estar CERRADOS: %(meses)s. Para '
                'recalcularlos marca «Incluir períodos cerrados» — pero '
                'piénsalo: se congelaron para poder defender el número.',
                n=len(saltados),
                meses=', '.join(m.strftime('%Y-%m') for m in saltados)))
        self.resultado = '\n'.join(lineas)
        return {
            'type': 'ir.actions.act_window',
            'res_model': self._name,
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'new',
        }
