# -*- coding: utf-8 -*-
{
    'name': 'Costeo Quimibond',
    'summary': 'Tarifas por centro desde el mayor, períodos con compuerta y '
               'validación de recetas',
    'description': """
Motor de costeo de Quimibond, segunda generación (spec
docs/superpowers/specs/2026-10-06-qb-costeo-v2-diseno.md).

Esta versión trae el esqueleto: centros con su capacidad normal, clasificación
de cuentas del mayor por bucket y centro, parámetros con ayuda, períodos con
tarifa por centro ($/h fija + variable), capacidad ociosa, publicación de la
tarifa a los centros de trabajo de Odoo y el validador de recetas, precios y
pesos. El costo por producto, la conciliación y el cotizador llegan en las
versiones siguientes.
    """,
    'author': 'Quimibond',
    'website': 'https://www.quimibond.com',
    'category': 'Manufacturing',
    'version': '19.0.1.0.1',
    'license': 'LGPL-3',
    'application': True,
    'depends': ['mrp', 'stock_account', 'account', 'hr', 'purchase', 'uom',
                'mail'],
    'data': [
        'security/ir.model.access.csv',
        'data/parametros.xml',
        'data/ir_cron.xml',
        'views/centro_views.xml',
        'views/cuenta_clase_views.xml',
        'views/parametro_views.xml',
        'views/periodo_views.xml',
        'views/validacion_views.xml',
        'views/menus.xml',
    ],
    'post_init_hook': 'post_init_hook',
    'installable': True,
}
