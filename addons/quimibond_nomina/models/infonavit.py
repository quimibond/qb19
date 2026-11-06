# -*- coding: utf-8 -*-
"""Retención de Infonavit en cuota fija, como NOI.

En la ficha de cada crédito (``l10n.mx.hr.infonavit``) se guarda la **cuota
bimestral** tal como la emite el Infonavit: cuota mensual × 2 + 15 pesos del
seguro de daños a la vivienda. En un bimestre completo se reparte entre los
periodos del bimestre: 8 o 9 semanas o 4 quincenas (verificado contra la semana 38 y la
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
``date_to`` en la quincenal), como en el resto del módulo.

**El divisor del bimestre completo** (1.17.0) es el número de pagos que caen
en el bimestre (``_qb_pagos_en_bimestre``): en la semanal, cuántas veces cae
el día de pago (``quimibond_nomina.dia_pago_semanal``, viernes) entre el día 1
del primer mes y el último del segundo; en la quincenal, 4 siempre. Hasta
1.16.2 era 9 fijo en la semanal, cierto en septiembre–octubre de 2026 (9
viernes, lo que usó NOI en las semanas 37 a 41) y falso en noviembre–diciembre
(8 viernes): en la semana 46 Odoo retenía 8/9 de lo de NOI en los 14 créditos
y el lote quedaba 788.78 arriba del neto de NOI.

Desde 1.16.0 un recibo sin sueldo no retiene, y lo que no retuvieron los
recibos anteriores del bimestre se reparte entre los que quedan, como NOI
(``models/retenciones.py``)."""
from datetime import timedelta

from dateutil.relativedelta import relativedelta

from odoo import fields, models

from .calendario import QUINCENAS_POR_BIMESTRE, _qb_bimestre_de, _qb_pagos_en_ventana

# Seguro de daños a la vivienda: 15 pesos por bimestre, incluido en la cuota
# bimestral de la ficha («Modalidades de descuento…», Infonavit, enero 2026).
SEGURO_VIVIENDA_BIMESTRAL = 15.0


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
        return _qb_bimestre_de(fecha)

    def _qb_pagos_en_bimestre(self):
        """Pagos de nómina que caen en el bimestre de este recibo: el divisor
        de la cuota bimestral completa. Semanal: veces que cae el día de pago
        en el bimestre (8 o 9); quincenal (y cualquier otra periodicidad, como
        hasta 1.16.2): 4. Nunca 0."""
        self.ensure_one()
        if self.version_id.schedule_pay != 'weekly':
            return QUINCENAS_POR_BIMESTRE
        referencia = self._qb_nomina_fecha_pago() or self.date_to
        if not referencia:
            return QUINCENAS_POR_BIMESTRE
        inicio, fin = _qb_bimestre_de(referencia)
        return _qb_pagos_en_ventana(inicio, fin, self._qb_nomina_dia_pago()) or 1

    def _qb_infonavit_periodos_desde(self, bim_ini, bim_fin, desde):
        """Cuántos periodos de este recibo (su periodicidad) tiene el bimestre
        que terminan en ``desde`` o después."""
        self.ensure_one()
        if self.version_id.schedule_pay == 'weekly':
            dia = self._qb_nomina_dia_pago()
            # La semana de nómina va de lunes a domingo: cuenta el pago cuya
            # semana termina en ``desde`` o después, es decir, el que cae a
            # partir de ``desde − (6 − día de pago)``.
            return _qb_pagos_en_ventana(max(bim_ini, desde - timedelta(days=6 - dia)), bim_fin, dia)
        n = 0
        mes = bim_ini
        while mes <= bim_fin:
            for fin in (mes.replace(day=15), mes + relativedelta(months=1, days=-1)):
                if fin >= desde:
                    n += 1
            mes += relativedelta(months=1)
        return n

    def _qb_infonavit_retencion(self, gravable=None):
        """Lo que se retiene de Infonavit en este recibo (positivo). Lo llama
        la regla ``INFONAVIT`` de «Paga regular» con el gravable del recibo
        (``categories['GROSS']``): sin gravable no retiene, y lo que no
        retuvieron los recibos anteriores del bimestre se reparte entre los que
        quedan (``models/retenciones.py``)."""
        self.ensure_one()
        if gravable is not None and gravable <= 0:
            return 0.0
        version = self.version_id
        pago = self._qb_nomina_fecha_pago()
        if not pago:
            return 0.0
        periodos = self._qb_pagos_en_bimestre()
        bim_ini, bim_fin = self._qb_bimestre(pago)
        inicio_contrato = version.contract_date_start or version.date_start
        n_restantes = self._qb_infonavit_periodos_desde(bim_ini, bim_fin, self.date_from)
        cuota = esperado = debe = 0.0
        n_previos = 0
        desde = bim_fin
        for credito in version.l10n_mx_infonavit.filtered(lambda c: c.status == 'in_progress'):
            cuota_bimestral = credito.fixed_monetary_fee + credito.monthly_insurance + credito.extra_fixed_monthly_contribution
            if not cuota_bimestral:
                continue
            inicio = credito.qb_fecha_inicio
            if inicio and inicio_contrato and inicio_contrato > inicio:
                inicio = inicio_contrato
            if inicio and self.date_to and inicio > self.date_to:
                continue
            if not inicio or not (bim_ini < inicio <= bim_fin):
                debe_credito, n_credito, desde_credito = cuota_bimestral, periodos, bim_ini
            else:
                seguro = min(SEGURO_VIVIENDA_BIMESTRAL, cuota_bimestral)
                dias_bimestre = (bim_fin - bim_ini).days + 1
                dias = (bim_fin - inicio).days + 1
                debe_credito = (cuota_bimestral - seguro) * dias / dias_bimestre + seguro
                n_credito = self._qb_infonavit_periodos_desde(bim_ini, bim_fin, inicio)
                desde_credito = inicio
            if not n_credito:
                continue
            cuota_credito = debe_credito / n_credito
            previos_credito = max(n_credito - n_restantes, 0)
            cuota += cuota_credito
            esperado += cuota_credito * previos_credito
            debe += debe_credito
            n_previos = max(n_previos, previos_credito)
            desde = min(desde, desde_credito)
        if not cuota:
            return 0.0
        return self._qb_retencion_recuperada(
            'INFONAVIT', bim_ini, desde, cuota, esperado, debe, n_previos, n_restantes)
