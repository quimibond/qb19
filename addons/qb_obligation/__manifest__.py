# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Obligaciones',
    'version': '19.0.1.1.0',
    'license': 'LGPL-3',
    'category': 'Accounting/Accounting',
    'summary': 'Obligaciones vivas: qué hay que hacer, de quién es, sobre qué documento, cómo se prueba y cuándo vence.',
    'description': """
Obligaciones Quimibond (piloto: cobranza)
=========================================

Una obligación es un compromiso con cinco datos: qué hay que hacer, de quién
es (usuario de Odoo), sobre qué documento, cómo se prueba que ya se hizo y
cuándo vence. Si no tiene los cinco, no entra.

Piloto cobranza:

- ``collection.overdue_invoice``: nace sola de Odoo (factura de cliente
  publicada, vencida, con saldo). Nace ya confirmada: es un hecho, no una
  hipótesis. Se cierra sola cuando el saldo baja a la tolerancia o el estado
  de pago es pagado / en proceso / revertido. Si la factura se cancela o se
  bloquea, la obligación se cancela.
- ``collection.apply_payment``: si el SAT tiene un complemento de pago vigente
  por más de lo que Odoo registra cobrado, el cliente ya pagó y contabilidad no
  lo aplicó: la obligación de cobro se convierte en obligación de aplicar el
  pago (requiere el módulo quimibond_sat; sin él la regla no corre).
- ``collection.payment_promise``: nace del correo (canal externo) como
  candidata y solo cuenta cuando el dueño la confirma.

Dueño: ``collection_user_id`` del contacto comercial, o el default de la
compañía. Sin dueño configurado el módulo no crea nada.

Escalación: la obligación confirmada que sigue abierta ``N`` días después de
confirmarse (default 3) y además rebasa el umbral (saldo ≥ monto configurado o
más de X días vencida) se escala al usuario de Dirección de la compañía.

Recordatorio único: un correo diario al dueño agrupado por cliente (no por
factura) y otro a Dirección solo con lo escalado. No se crean actividades de
Odoo para no avisar dos veces.
    """,
    'author': 'Quimibond',
    'website': 'https://quimibond.com',
    'depends': ['account'],
    'data': [
        'security/ir.model.access.csv',
        'data/ir_cron_data.xml',
        'views/qb_obligation_views.xml',
        'views/res_company_views.xml',
        'views/res_partner_views.xml',
        'views/account_move_views.xml',
    ],
    'installable': True,
    'application': True,
}
