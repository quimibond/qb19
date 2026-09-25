# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Nómina (CFDI y centinela de reglas)',
    'version': '19.0.1.6.0',
    'license': 'LGPL-3',
    'category': 'Human Resources/Payroll',
    'summary': 'Registro patronal por contrato, SDI y SBC del recibo y nodo HorasExtra en el CFDI de nómina, '
               'y un centinela que avisa cuando una regla salarial cambia por detrás.',
    'description': """
Nómina Quimibond
================

Tres cosas que el módulo de Odoo (``l10n_mx_hr_payroll_account_edi``) no
resuelve y que hacen falta para timbrar la nómina de Quimibond:

* **Registro patronal por empleado.** Odoo guarda uno solo en la compañía
  (``res.company.l10n_mx_imss_id``); Quimibond tiene dos (Toluca y México).
  Se agrega el campo al contrato (``hr.version``) y se inyecta en
  ``nomina12:Emisor/@RegistroPatronal``, con la compañía como respaldo.
* **SalarioDiarioIntegrado y SalarioBaseCotApor.** El módulo los alimenta
  con el salario diario simple. Aquí salen de las líneas ``INT_DAY_WAGE_BASE``
  (SDI declarado) e ``INT_DAY_WAGE`` (topado a 25 UMA) del recibo ya
  calculado, buscadas POR CÓDIGO y nunca con ``env.ref()``: la regla de
  fábrica está archivada y en su lugar corre una copia con el mismo código.
* **Centinela de reglas.** Guarda un SHA-256 de la condición y la fórmula de
  las reglas vigiladas, revisa a diario y avisa a los responsables de nómina
  si cambiaron. No revierte nada solo.
* **Nodo ``nomina12:HorasExtra``.** Obligatorio en toda percepción 019 y el
  módulo de Odoo nunca lo emite. Sale de las entradas ``HE_DOBLE``/``HE_TRIPLE``
  y de las líneas ``HE_EXEMPT`` + ``HE_TAX``; ``Dias`` es la entrada ``HE_DIAS``
  que captura RH (días en que hubo tiempo extra) y, si falta, se estima.
  La herencia de la plantilla se configura sola al instalar y se apaga si Odoo
  ya emite el nodo.
    """,
    'author': 'Quimibond',
    'website': 'https://www.quimibond.com',
    'depends': [
        'hr_payroll',
        'l10n_mx_hr_payroll',
        'l10n_mx_hr_payroll_account_edi',
    ],
    'data': [
        'security/ir.model.access.csv',
        'data/cfdi_nomina_templates.xml',
        'data/cfdi_horas_extra.xml',
        'data/payslip_input_types.xml',
        'data/ir_cron_data.xml',
        'views/hr_employee_views.xml',
        'views/rule_sentinel_views.xml',
        'views/hr_payslip_run_views.xml',
    ],
    'post_init_hook': 'post_init_hook',
    'installable': True,
    'application': False,
    'auto_install': False,
}
