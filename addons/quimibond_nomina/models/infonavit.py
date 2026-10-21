# -*- coding: utf-8 -*-
"""Retención de Infonavit en cuota fija, como NOI.

En la ficha de cada crédito (``l10n.mx.hr.infonavit``) se guarda la **cuota
bimestral** tal como la emite el Infonavit: cuota mensual × 2 + 15 pesos del
seguro de daños a la vivienda. En un bimestre completo se reparte entre los
periodos: 9 semanas o 4 quincenas (verificado contra la semana 38 y la
quincena 18 de 2026: los cinco créditos quincenales y los trece semanales al
centavo).

**El primer bimestre de un crédito que empieza a la mitad** (alta, reingreso o
aviso nuevo) no se cobra completo. El Infonavit pide, para cuota fija
(«Modalidades de descuento para el cálculo de las amortizaciones», enero
2026): cuota mensual × 2 ÷ días del bimestre × días cotizados en el bimestre
+ seguro. NOI cobra solo los días desde el inicio del descuento y reparte ese
monto en partes iguales entre los periodos del bimestre que caen desde el
inicio. Verificado: Reyes Sánchez, aviso del 22-sep-2026 por 2,668.35
mensuales; septiembre–octubre tiene 61 días y quedan 40:
2,668.35 × 2 ÷ 61 × 40 + 15 = 3,514.48, entre las quincenas 19, 20 y 21 =
1,171.49 por quincena, lo que retuvo NOI en la quincena 20 (Odoo retenía
1,337.93, el bimestre completo entre 4).

El inicio es el campo «Inicio del descuento» del crédito (la fecha del aviso
de retención); si el contrato empezó después, cuenta el contrato. Sin fecha,
el bimestre se cobra completo, como hasta 1.11.0. Un crédito cuyo inicio cae
después del periodo no se cobra en ese periodo.

Los bimestres son los del Infonavit (enero–febrero, marzo–abril…) y cada
nómina es del bimestre de su **fecha de pago** (viernes en la semanal,
``date_to`` en la quincenal), como en el resto del módulo."""
from datetime import date, timedelta

from dateutil.relativedelta import relativedelta

from odoo import fields, models

# Seguro de daños a la vivienda: 15 pesos por bimestre, incluido en la cuota
# bimestral de la ficha («Modalidades de descuento…», Infonavit, enero 2026).
SEGURO_VIVIENDA_BIMESTRAL = 15.0
PERIODOS_BIMESTRE = {'weekly': 9, 'bi-weekly': 4}


class L10nMxHrInfonavit(models.Model):
    _inherit = 'l10n.mx.hr.infonavit'

    qb_fecha_inicio = fields.Date(
        string='Inicio del descuento',
        help="Fecha desde la que el aviso de retención del Infonavit pide descontar. Si cae a la "
             "mitad de un bimestre, ese primer bimestre se cobra solo por los días desde esta fecha "
             "(como NOI). Vacío = el bimestre completo.",
    )


class HrPayslip(models.Model):
    _inherit = 'hr.payslip'

    @staticmethod
    def _qb_bimestre(fecha):
        """``(primer_dia, ultimo_dia)`` del bimestre del Infonavit de ``fecha``."""
        mes = fecha.month - (fecha.month - 1) % 2
        inicio = date(fecha.year, mes, 1)
        return inicio, inicio + relativedelta(months=2, days=-1)

    def _qb_infonavit_periodos_desde(self, bim_ini, bim_fin, desde):
        """Cuántos periodos de este recibo (su periodicidad) tiene el bimestre
        que terminan en ``desde`` o después."""
        self.ensure_one()
        if self.version_id.schedule_pay == 'weekly':
            dia = self._qb_nomina_dia_pago()
            pago = bim_ini + timedelta(days=(dia - bim_ini.weekday()) % 7)
            n = 0
            while pago <= bim_fin:
                # La semana de nómina va de lunes a domingo.
                if pago + timedelta(days=6 - pago.weekday()) >= desde:
                    n += 1
                pago += timedelta(days=7)
            return n
        n = 0
        mes = bim_ini
        while mes <= bim_fin:
            for fin in (mes.replace(day=15), mes + relativedelta(months=1, days=-1)):
                if fin >= desde:
                    n += 1
            mes += relativedelta(months=1)
        return n

    def _qb_infonavit_retencion(self):
        """Lo que se retiene de Infonavit en este recibo (positivo). Lo llama
        la regla ``INFONAVIT`` de «Paga regular»."""
        self.ensure_one()
        version = self.version_id
        periodos = PERIODOS_BIMESTRE.get(version.schedule_pay, 4)
        pago = self._qb_nomina_fecha_pago()
        if not pago:
            return 0.0
        bim_ini, bim_fin = self._qb_bimestre(pago)
        inicio_contrato = version.contract_date_start or version.date_start
        total = 0.0
        for credito in version.l10n_mx_infonavit.filtered(lambda c: c.status == 'in_progress'):
            cuota = credito.fixed_monetary_fee + credito.monthly_insurance + credito.extra_fixed_monthly_contribution
            if not cuota:
                continue
            inicio = credito.qb_fecha_inicio
            if inicio and inicio_contrato and inicio_contrato > inicio:
                inicio = inicio_contrato
            if inicio and self.date_to and inicio > self.date_to:
                continue
            if not inicio or not (bim_ini < inicio <= bim_fin):
                total += cuota / periodos
                continue
            seguro = min(SEGURO_VIVIENDA_BIMESTRAL, cuota)
            dias_bimestre = (bim_fin - bim_ini).days + 1
            dias = (bim_fin - inicio).days + 1
            debe = (cuota - seguro) * dias / dias_bimestre + seguro
            n = self._qb_infonavit_periodos_desde(bim_ini, bim_fin, inicio)
            total += debe / n if n else 0.0
        return total
