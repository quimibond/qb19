# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - SAT (Syntage) en Mis pendientes del SGI',
    'version': '19.0.1.1.0',
    'license': 'LGPL-3',
    'category': 'Accounting/Localizations',
    'summary': 'Lo que no cuadra entre el SAT y Odoo llega a Mis pendientes de quien ocupa el puesto.',
    'description': """
Puente entre quimibond_sat y quimibond_sgi
==========================================

Cada día revisa la conciliación SAT vs Odoo y los complementos de pago y
manda un aviso a **Mis pendientes** del SGI, colgado de la actividad del mapa
de procesos que corresponde (S1.21 registrar la factura del proveedor desde el
SAT, S1.25 complementos de los proveedores, S2.21 timbrar el complemento de
pago, S2.13 cancelar y refacturar, S3.14 IVA del mes que no cuadra). Lo recibe quien ocupa el puesto
responsable de esa actividad; si nadie lo ocupa, el Jefe MAST. «Ir» abre la
lista del SAT ya filtrada. Cuando ya no hay nada, el aviso se cierra solo.

Se instala solo (auto_install) cuando están los dos módulos.
    """,
    'author': 'Quimibond',
    'website': 'https://quimibond.com',
    'depends': ['quimibond_sat', 'quimibond_sgi'],
    'data': ['data/ir_cron_data.xml'],
    'installable': True,
    'application': False,
    'auto_install': True,
}
