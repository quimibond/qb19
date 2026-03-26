# -*- coding: utf-8 -*-
{
    'name': 'Pesaje de Rollos Tejido (IoT & Manual)',
    'version': '19.0.1.2.0',
    'category': 'Manufacturing',
    'summary': 'Control de pesaje de rollos, lotes automáticos y etiquetas ZPL para logística',
    'author': 'CONSOLTI',
    'depends': ['mrp', 'stock', 'iot', 'mrp_workorder'],
    'data': [
        'security/ir.model.access.csv',
        'views/mrp_weigh_wizard_view.xml',      # 1. Primero el diseño del asistente
        'views/mrp_production_view.xml',
        'views/mrp_workorder_tablet_view.xml', # 2. Al final el botón que llama al asistente
    ],
    'installable': True,
    'application': True,
    'license': 'LGPL-3',
}