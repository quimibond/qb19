# -*- coding: utf-8 -*-
"""FONACOT e Infonavit: lo que no se retuvo se recupera en las nóminas que
quedan, como NOI.

Las dos son cuotas fijas de un periodo más largo que la nómina: FONACOT, la
retención **mensual** de la cédula, repartida entre las nóminas que paga el
mes (``isr_mensual._qb_nomina_periodos_del_mes``); Infonavit, la cuota
**bimestral**, repartida entre las 9 semanas o 4 quincenas del bimestre
(``infonavit._qb_infonavit_retencion``). NOI hace dos cosas más:

1. **Sin sueldo no retiene.** Una semana entera de incapacidad (gravable en
   cero) no lleva FONACOT ni Infonavit. Filiberta Florentino, semana 42 de
   2026: NOI no le pagó ni le retuvo; Odoo le retenía 387.91 y 239.86.

2. **Lo que faltó se reparte entre las nóminas que quedan.** Si las nóminas
   anteriores del mes (FONACOT) o del bimestre (Infonavit) retuvieron menos de
   lo que les tocaba, el recibo retiene ``(lo que se debe del periodo − lo ya
   retenido) ÷ nóminas que quedan, contando esta``. Verificado contra NOI:

   - Filiberta, semanas 43 y 44: FONACOT (1,939.56 − 387.91 de la 41 − 0 de
     la 42) ÷ 3 = 517.22 (NOI 517.21); Infonavit (2,158.72 − 5 × 239.84 − 0) ÷
     3 = 319.84, y en la 44 (2,158.72 − 1,199.20 − 319.84) ÷ 2 = 319.84.
   - Jorge Matías Salinas: alta el 23-sep-2026 con crédito de 3,650.24
     bimestrales; septiembre–octubre se cobra por los días desde el alta
     (2,339.17, ver ``infonavit.py``) y el aviso llegó después de la semana 40,
     que no retuvo: desde la 41, 2,339.17 ÷ 5 = 467.83.

   Cuando las nóminas anteriores retuvieron lo suyo (lo normal), el recibo
   retiene la cuota de siempre, sin redondeos nuevos.

**Cuándo no se recupera** (y se retiene la cuota de siempre): si falta algún
recibo anterior del mes o del bimestre (el historial no está completo y no se
sabe cuánto se retuvo), o si alguno sigue en borrador. Para las corridas
piloto, que viven en borrador, cuenta el mismo parámetro que el ajuste mensual
del ISR (``quimibond_nomina.isr_mensual_incluye_borrador``). Lo ya retenido es
el total de las líneas ``FONACOT`` o ``INFONAVIT`` de esos recibos, incluido
el ajuste manual ``AJ_INFONAVIT``."""
import logging
from datetime import timedelta

from dateutil.relativedelta import relativedelta

from odoo import models

_logger = logging.getLogger(__name__)

# Centavos por periodo que se toleran al comparar lo retenido con lo que tocaba
# (las líneas van redondeadas a centavos; la cuota no).
TOLERANCIA = 0.01


class HrPayslip(models.Model):
    _inherit = 'hr.payslip'

    def _qb_retencion_recuperada(self, codigo, inicio, desde, cuota, esperado, debe, n_previos, n_restantes):
        """Lo que retiene este recibo de ``codigo`` (positivo).

        ``inicio``: primer día del mes o bimestre. ``desde``: desde cuándo
        corre la retención (los recibos anteriores que cuentan son los que
        terminan ese día o después). ``cuota``: lo normal de este recibo;
        ``esperado``: lo que debieron retener los ``n_previos`` recibos
        anteriores; ``debe``: el total del mes o bimestre desde ``desde``;
        ``n_restantes``: este recibo y los que faltan."""
        self.ensure_one()
        if not n_previos or n_restantes <= 0:
            return cuota
        recibos, abiertos = self._qb_recibos_previos_desde(inicio)
        if abiertos:
            _logger.info('quimibond_nomina: recibo %s: %s sin recuperar, hay recibos previos en borrador (%s)',
                         self.id, codigo, ', '.join(abiertos.mapped('display_name')))
            return cuota
        periodos = {(r.date_from, r.date_to) for r in recibos if r.date_to >= desde}
        if len(periodos) < n_previos:
            _logger.info('quimibond_nomina: recibo %s: %s sin recuperar, faltan recibos previos (%d de %d)',
                         self.id, codigo, len(periodos), n_previos)
            return cuota
        retenido = -sum(l.total for l in recibos.line_ids if l.code == codigo)
        if retenido >= esperado - TOLERANCIA * n_previos:
            return cuota
        return max(debe - retenido, 0.0) / n_restantes

    # ------------------------------------------------------------------
    # FONACOT: retención mensual entre las nóminas que paga el mes
    # ------------------------------------------------------------------
    def _qb_fonacot_periodos(self):
        """``(n_previos, n_restantes)`` del mes de pago del recibo: las nóminas
        anteriores del mes que caen desde el inicio del contrato, y esta más
        las que faltan."""
        self.ensure_one()
        version = self.version_id
        inicio_contrato = version.contract_date_start or version.date_start
        pago = self._qb_nomina_fecha_pago()
        primero = pago.replace(day=1)
        ultimo = primero + relativedelta(months=1, days=-1)
        if version.schedule_pay == 'weekly':
            dia = self._qb_nomina_dia_pago()
            d = primero + timedelta(days=(dia - primero.weekday()) % 7)
            fines = []
            while d <= ultimo:
                fines.append((d, d + timedelta(days=6 - d.weekday())))
                d += timedelta(days=7)
        elif version.schedule_pay == 'bi-weekly':
            fines = [(primero.replace(day=15),) * 2, (ultimo, ultimo)]
        else:
            return 0, 1
        previos = sum(1 for p, fin in fines if p < pago and not (inicio_contrato and fin < inicio_contrato))
        restantes = sum(1 for p, fin in fines if p >= pago)
        return previos, restantes

    def _qb_fonacot_retencion(self, gravable=None):
        """Lo que se retiene de FONACOT en este recibo (positivo). Lo llama la
        regla ``FONACOT`` de «Paga regular» con el gravable del recibo
        (``categories['GROSS']``); sin gravable no retiene."""
        self.ensure_one()
        if gravable is not None and gravable <= 0:
            return 0.0
        version = self.version_id
        creditos = version.l10n_mx_fonacot.filtered(lambda f: f.status == 'in_progress')
        importe = sum(c.monthly_import + c.extra_fixed_monthly_contribution for c in creditos)
        if not importe:
            return 0.0
        periodos = self._qb_nomina_periodos_del_mes() or (4 if version.schedule_pay == 'weekly' else 2)
        cuota = importe / periodos
        pago = self._qb_nomina_fecha_pago()
        if not pago:
            return cuota
        n_previos, n_restantes = self._qb_fonacot_periodos()
        inicio_contrato = version.contract_date_start or version.date_start
        inicio_mes = pago.replace(day=1)
        desde = max(inicio_mes, inicio_contrato) if inicio_contrato else inicio_mes
        return self._qb_retencion_recuperada(
            'FONACOT', inicio_mes, desde, cuota, cuota * n_previos, cuota * (n_previos + n_restantes),
            n_previos, n_restantes)
