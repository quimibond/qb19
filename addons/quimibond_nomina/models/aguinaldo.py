# -*- coding: utf-8 -*-
"""Aguinaldo con los días de cada grupo de nómina, como NOI.

La regla ``BASIC`` de la estructura «Aguinaldo» (id 104) multiplica el salario
diario por ``l10n_mx_christmas_bonus``, un parámetro de la localización con un
solo valor por país: 15 días, el mínimo de ley. Quimibond paga **25 días** a
la planta semanal (sindicalizados, acuerdo con el sindicato) y **16 días** a
los quincenales de Toluca y CDMX (empleados, desde el incremento de agosto de
2025). Lo confirma la tabla de factores de integración de «MOD SALARIAL
VARIABLES 5o BIMESTRE 2026» (MSTOL: 25; MQTOL y MQMEX: 16) y Lorena por correo
el 5-nov-2026. Con un valor único no se pueden tener los dos: el ensayo del
lote 133 (5-nov-2026) lo probó y se deshizo.

Desde 1.18.0 los días salen del **tipo de estructura** del contrato
(``hr.payroll.structure.type.qb_dias_aguinaldo``), con el parámetro de la
localización como respaldo cuando el campo está en cero.

El prorrateo es el de la regla 104, sin cambios: días del año desde el 1-ene
(o desde el alta) menos los días no pagados (``LEAVE90``, ``LEAVE1000``,
``LEAVE1100``, ``LEAVE1200``) de los recibos regulares del año, entre los días
del año. La regla solo lee recibos validados o pagados; en la piloto todo vive
en borrador y el descuento salía en cero (Gerónimo Bartolo cobró 23.63 días en
2025 por sus faltas; Odoo le daba los completos). Con el mismo interruptor que
el ISR mensual (``quimibond_nomina.isr_mensual_incluye_borrador``) también
cuentan los borradores, un recibo por periodo (el más reciente), para poder
comparar contra NOI. Ojo: la piloto solo tiene recibos desde la semana 42, así
que las ausencias de enero a septiembre de 2026 no están en Odoo y el
aguinaldo de 2026 en Odoo no puede descontarlas. Eso solo cuadra completo con
el aguinaldo de 2027, cuando todo el año viva en Odoo."""
from datetime import date

from odoo import api, fields, models

from .isr_mensual import ESTADOS_CERRADOS, PARAM_INCLUYE_BORRADOR

DIAS_SEMANAL = 25.0       # planta semanal (sindicalizados)
DIAS_QUINCENAL = 16.0     # quincenales de Toluca y CDMX (empleados)
TIPO_QUINCENAL_XMLID = 'l10n_mx_hr_payroll.l10n_mx_employee'
CODIGOS_NO_PAGADOS = ['LEAVE90', 'LEAVE1000', 'LEAVE1100', 'LEAVE1200']
ESTRUCTURA_REGULAR = 'MX_REGULAR'


class HrPayrollStructureType(models.Model):
    _inherit = 'hr.payroll.structure.type'

    qb_dias_aguinaldo = fields.Float(
        string='Días de aguinaldo',
        digits=(16, 2),
        help="Días de salario que paga el aguinaldo a este grupo de nómina (Quimibond: 25 la planta "
             "semanal, 16 los quincenales). En cero, se usa el parámetro de la localización (15).",
    )

    @api.model
    def _qb_sembrar_dias_aguinaldo(self):
        """Pone 16 en «Mexico: Employee» (quincenales) y 25 en los tipos semanales de México,
        solo donde el campo sigue en cero. Corre al instalar y al actualizar a 1.18.0."""
        quincenal = self.env.ref(TIPO_QUINCENAL_XMLID, raise_if_not_found=False)
        if quincenal and not quincenal.qb_dias_aguinaldo:
            quincenal.qb_dias_aguinaldo = DIAS_QUINCENAL
        semanales = self.search([
            ('country_id.code', '=', 'MX'),
            ('default_schedule_pay', '=', 'weekly'),
            ('qb_dias_aguinaldo', '=', 0),
        ])
        semanales.write({'qb_dias_aguinaldo': DIAS_SEMANAL})


class HrPayslip(models.Model):
    _inherit = 'hr.payslip'

    def _qb_aguinaldo_dias(self):
        """Días de aguinaldo del grupo del contrato; el parámetro de país si no tiene."""
        self.ensure_one()
        dias = self.version_id.structure_type_id.qb_dias_aguinaldo
        return dias or self._rule_parameter('l10n_mx_christmas_bonus')

    def _qb_aguinaldo_recibos_regulares(self, desde, hasta):
        """Recibos regulares del año que cuentan para el prorrateo: validados o
        pagados; con el interruptor de la piloto, también borradores, un recibo
        por periodo (el más reciente)."""
        self.ensure_one()
        estados = list(ESTADOS_CERRADOS)
        if self.env['ir.config_parameter'].sudo().get_param(PARAM_INCLUYE_BORRADOR):
            estados.append('draft')
        recibos = self.env['hr.payslip'].search([
            ('employee_id', '=', self.employee_id.id),
            ('structure_code', '=', ESTRUCTURA_REGULAR),
            ('state', 'in', estados),
            ('date_from', '>=', desde),
            ('date_to', '<=', hasta),
            ('id', '!=', self.id),
        ], order='id desc')
        por_periodo = {}
        for r in recibos:
            por_periodo.setdefault((r.date_from, r.date_to), r)
        return self.env['hr.payslip'].browse([r.id for r in por_periodo.values()])

    def _qb_aguinaldo_base(self):
        """Aguinaldo del recibo antes de exención e ISR: salario diario × días del
        grupo × proporción del año trabajada. Lo llama la regla ``BASIC`` de la
        estructura «Aguinaldo» (104): ``result = payslip._qb_aguinaldo_base()``."""
        self.ensure_one()
        inicio_anio = date(self.date_to.year, 1, 1)
        primer_dia = max(inicio_anio, self.employee_id._get_first_version_date())
        dias_contrato = (self.date_to - primer_dia).days + 1
        recibos = self._qb_aguinaldo_recibos_regulares(inicio_anio, self.date_to)
        no_pagados = 0.0
        if recibos:
            valores = recibos._get_worked_days_line_values(CODIGOS_NO_PAGADOS, ['number_of_days'], True)
            no_pagados = sum(v['sum']['number_of_days'] for v in valores.values())
        proporcion = (dias_contrato - no_pagados) / self.l10n_mx_days_of_year
        return self.l10n_mx_daily_salary * self._qb_aguinaldo_dias() * proporcion
