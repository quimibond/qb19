# -*- coding: utf-8 -*-
{
    'name': 'Devolución de Hilo Sobrante de Manufactura',
    'version': '19.0.1.0.0',
    'category': 'Manufacturing/Inventory',
    'summary': 'Proceso de re-empaque, pesaje y devolución de hilo a la ubicación de almacenamiento principal.',
    'author': 'Jose Sacramento Ruiz Arizmendi',
    'depends': ['stock', 'mrp', 'iot'],
    'data': [
        'security/ir.model.access.csv',
        'report/ir_actions_report.xml',
        'report/report_zpl_templates.xml',
        'wizard/mrp_yarn_return_wizard_views.xml',
    ],
    'installable': True,
    'application': False,
    'license': 'LGPL-3',
}
