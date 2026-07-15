{
    'name': 'Reporte de Producción Diaria',
    'version': '19.0.1.0.0',
    'summary': 'Reporte en vista (lista) de órdenes de fabricación con subproductos, '
               'centros de trabajo y costos, para la operación Tejido Circular.',
    'category': 'Manufacturing',
    'author': 'Jose Sacramento Ruiz Arizmendi Consolti',
    'depends': ['mrp'],
    'data': [
        'security/ir.model.access.csv',
        'wizard/production_report_wizard.xml',
        'views/production_report_views.xml',
        'views/production_report_line_views.xml',
    ],
    'installable': True,
    'application': False,
    'license': 'LGPL-3',
}
