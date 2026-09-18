# -*- coding: utf-8 -*-
"""Valores del CFDI de nómina que el módulo de Odoo saca mal.

``l10n_mx_hr_payroll_account_edi`` arma el CFDI con
``_l10n_mx_edi_add_payslip_cfdi_values``. Aquí se llama al original y se
agrega ``cfdi_values['qb_nomina']``; la plantilla heredada
(``data/cfdi_nomina_templates.xml``) lee de ahí los tres atributos.

Las líneas del recibo se buscan POR CÓDIGO y nunca con ``env.ref()``. Es
deliberado: el módulo resuelve las reglas por XML id, y cuando la regla de
fábrica está archivada y en su lugar corre una copia activa con el mismo código
(la situación de esta base) el ``env.ref()`` devuelve la archivada, no produce
línea y el CFDI sale con ``0.00`` sin error ni aviso."""
import logging

from odoo import models

_logger = logging.getLogger(__name__)

# Códigos de las líneas del recibo de donde salen los salarios del Receptor.
CODE_SDI = 'INT_DAY_WAGE_BASE'     # SalarioDiarioIntegrado: el SDI declarado
CODE_SBC = 'INT_DAY_WAGE'          # SalarioBaseCotApor: el mismo, topado a 25 UMA
UMA_CAP = 25


class HrPayslip(models.Model):
    _inherit = 'hr.payslip'

    def _qb_nomina_line_total(self, code):
        """Total de las líneas del recibo con ese código, o None si no hay."""
        self.ensure_one()
        lines = self.line_ids.filtered(lambda l: l.code == code)
        if not lines:
            return None
        return sum(lines.mapped('total'))

    def _qb_nomina_cfdi_values(self):
        """Dict con valores planos (sin recordsets: ``_clean_cfdi_values`` los
        destruye) para los atributos que la plantilla heredada sobrescribe."""
        self.ensure_one()
        version = self.version_id.sudo()
        company = self.company_id.sudo()
        registro = (version.l10n_mx_employer_registration or company.l10n_mx_imss_id or '').strip()

        sdi = self._qb_nomina_line_total(CODE_SDI)
        sbc = self._qb_nomina_line_total(CODE_SBC)
        if not sdi:
            # Lo que hacía el módulo: el salario diario simple. No mejora nada,
            # pero tampoco deja el atributo vacío.
            sdi = self.l10n_mx_daily_salary or 0.0
            _logger.warning('quimibond_nomina: recibo %s sin línea %s; SalarioDiarioIntegrado '
                            'sale del salario diario simple (%.2f)', self.id, CODE_SDI, sdi)
        if not sbc:
            sbc = sdi
            try:
                sbc = min(sbc, UMA_CAP * self._rule_parameter('l10n_mx_uma')['daily'])
            except Exception:  # noqa: BLE001 — sin parámetro de UMA se manda sin topar
                pass
            _logger.warning('quimibond_nomina: recibo %s sin línea %s; SalarioBaseCotApor '
                            'sale del SDI (%.2f)', self.id, CODE_SBC, sbc)
        return {
            'registro_patronal': registro or False,
            'salario_diario_integrado': '%.2f' % sdi,
            'salario_base_cot_apor': '%.2f' % sbc,
        }

    def _l10n_mx_edi_add_payslip_cfdi_values(self, cfdi_values, *args, **kwargs):
        res = super()._l10n_mx_edi_add_payslip_cfdi_values(cfdi_values, *args, **kwargs)
        if len(self) == 1:
            cfdi_values['qb_nomina'] = self._qb_nomina_cfdi_values()
        return res
