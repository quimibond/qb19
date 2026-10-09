# -*- coding: utf-8 -*-
{
    'name': 'Cotizador Quimibond',
    'summary': 'Cotizaciones de costo con aprobación por puesto, seguimiento, '
               'vencimiento y precio en tarifa al ganar',
    'description': """
Cotizador de la segunda generación del costeo (spec
docs/superpowers/specs/2026-10-06-qb-costeo-v2-diseno.md, §6 y §6.1).

Toma el costo del último período cerrado de `qb_costeo` (o de un producto
hermano, o capturado a mano con fuente), guarda la foto del costo, calcula
pisos, márgenes y semáforo sobre el precio vigente, y lleva el ciclo que pide
el procedimiento C1: Borrador → Por aprobar (puesto que aprueba, con suplente)
→ Presentada → Ganada / Perdida / Vencida, con motivos de lista, seguimiento
automático a Ventas, aprobación del cliente con evidencia y el precio en la
tarifa del cliente al ganar. Importa las cotizaciones de `qb_capacidad_costeo`
sin tocarlas.
    """,
    'author': 'Quimibond',
    'website': 'https://www.quimibond.com',
    'category': 'Sales',
    'version': '19.0.1.3.0',
    'license': 'LGPL-3',
    'application': False,
    'depends': ['qb_costeo', 'sale_management', 'hr', 'mail'],
    'data': [
        'security/groups.xml',
        'security/ir.model.access.csv',
        'data/sequence.xml',
        'data/motivos.xml',
        'data/ir_cron.xml',
        'report/report_cotizacion.xml',
        'wizard/decision_wizard_views.xml',
        'views/cotizacion_views.xml',
        'views/motivo_views.xml',
        'views/settings_views.xml',
        'views/menus.xml',
    ],
    'post_init_hook': 'post_init_hook',
    'installable': True,
}
