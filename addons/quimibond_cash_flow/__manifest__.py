# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Estado de flujo de efectivo NIF B-2',
    'version': '19.0.1.0.0',
    'license': 'LGPL-3',
    'category': 'Accounting/Accounting',
    'summary': 'Estado de flujo de efectivo conforme a la NIF B-2 (metodo indirecto y directo) sobre el motor de reportes Enterprise.',
    'description': """
Estado de flujo de efectivo NIF B-2 (Mexico)
============================================

Reporte de flujo de efectivo con dos presentaciones que cuadran contra la
variacion real de efectivo:

- Metodo indirecto: resultado + partidas sin efecto en efectivo +/- cambios
  en capital de trabajo = operacion; inversion; financiamiento; efecto por
  cambios en el valor del efectivo; efectivo inicial y final.
- Metodo directo resumido: cobros a clientes, pagos a proveedores, nomina,
  impuestos, intereses, arrendamientos, prestamos, activo fijo, partes
  relacionadas, otros. Clasificado por la contraparte del movimiento de
  efectivo, sin descomponer la factura en sus lineas.

La definicion de efectivo y el mapeo de cuentas viven en una configuracion
por compania (Contabilidad > Configuracion > Flujo de efectivo NIF B-2) con
un boton para cargar los defaults de Quimibond.
    """,
    'author': 'Quimibond',
    'website': 'https://quimibond.com',
    'depends': [
        'account',
        'account_reports',
        'l10n_mx',
    ],
    'data': [
        'security/ir.model.access.csv',
        'data/account_report_data.xml',
        'data/ir_cron_data.xml',
        'views/cash_flow_config_views.xml',
        'views/cash_flow_snapshot_views.xml',
        'views/menus.xml',
    ],
    'installable': True,
    'application': False,
    'auto_install': False,
}
