# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Obligaciones',
    'version': '19.0.2.0.0',
    'license': 'LGPL-3',
    'category': 'Accounting/Accounting',
    'summary': 'Obligaciones vivas: qué hay que hacer, de quién es, sobre qué documento, cómo se prueba y cuándo vence.',
    'description': """
Obligaciones vivas
==================

Una obligación es un compromiso con cinco datos: qué hay que hacer, de quién
es (usuario de Odoo), sobre qué documento, cómo se prueba que ya se hizo y
cuándo vence. Si no tiene los cinco, no entra. Misma mecánica en comercial,
operaciones, compras, finanzas, SGI y RH: cambia la regla de cierre, no el
sistema.

De dónde nace: de Odoo (hechos: factura vencida), del correo (la memoria en
Supabase detecta compromisos de entrega, cotizaciones, documentos solicitados,
promesas de pago) o a mano. Lo que nace de Odoo nace confirmado; lo que nace
del correo espera que el dueño lo confirme.

Cómo se cierra: por evidencia en Odoo (saldo en cero, pedido entregado, compra
recibida), por la memoria (el pendiente quedó resuelto en el correo) o por
acuse del dueño. Nunca por el juicio de un modelo.

Quién es el dueño: el buzón que recibió el correo si es un usuario de Odoo;
si no, el dueño del área configurado en la compañía. Escalación a Dirección
tras N días abierta. Un recordatorio diario por dueño agrupado por cliente.
    """,
    'author': 'Quimibond',
    'website': 'https://quimibond.com',
    'depends': ['account', 'qb_memoria'],
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
