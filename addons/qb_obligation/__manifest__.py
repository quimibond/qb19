# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Obligaciones',
    'version': '19.0.3.0.0',
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

No es una app aparte: cada obligación abierta es una actividad nativa de Odoo
sobre su documento (pedido, compra, factura) o su contacto, asignada al dueño
con la fecha. Se ve en el reloj de actividades y en el chatter; marcarla hecha
es el acuse del dueño, cancelarla la descarta, y cuando Odoo o la memoria
comprueban que ya se cumplió, la actividad se marca hecha sola.

De dónde nace: del correo (la memoria en Supabase detecta compromisos de
entrega, cotizaciones, documentos solicitados, promesas de pago, RFQ) o a
mano / por MCP. La cobranza de facturas vencidas NO vive aquí: ya está en
Contabilidad.

Quién es el dueño: el buzón que recibió el correo si es usuario de Odoo; si
no, el encargado que la memoria aprendió para ese contacto y área
(qb_memoria); si no, el dueño del área configurado en la compañía.
Escalación a Dirección tras N días abierta y recordatorio diario por dueño.
    """,
    'author': 'Quimibond',
    'website': 'https://quimibond.com',
    'depends': ['account', 'contacts', 'qb_memoria'],
    'data': [
        'security/ir.model.access.csv',
        'data/ir_cron_data.xml',
        'data/mail_activity_type_data.xml',
        'views/qb_obligation_views.xml',
        'views/res_company_views.xml',
        'views/res_partner_views.xml',
        'views/account_move_views.xml',
    ],
    'installable': True,
    'application': False,
}
