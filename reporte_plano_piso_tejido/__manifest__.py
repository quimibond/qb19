# -*- coding: utf-8 -*-
{
    'name': 'Reporte Plano de Piso Tejido',
    'version': '19.0.1.0.0',
    'category': 'Manufacturing/Manufacturing',
    'summary': 'Reporte tipo vista de plano de piso para órdenes de fabricación de la operación Tejido Circular',
    'description': """
Reporte Plano de Piso Tejido
=============================
Muestra en tiempo real todas las órdenes de fabricación (mrp.production) de la
operación Tejido Circular (centros de trabajo que inician con "CIRCULAR" y
operación de ruta que inicia con "TEJIDO") que se encuentren en estatus
"En progreso" al momento de generar el reporte.

La vista se genera bajo demanda (snapshot) y se ordena por Producto y
después por Centro de Trabajo.
""",
    'author': 'Jose - Quimibond',
    'depends': ['mrp'],
    'data': [
        'security/ir.model.access.csv',
        'views/reporte_plano_piso_tejido_views.xml',
    ],
    'installable': True,
    'application': False,
    'auto_install': False,
    'license': 'LGPL-3',
}
