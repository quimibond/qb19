{
    'name': 'Reporte de Producción Diaria',
    'version': '1.0',
    'summary': 'Reporte PDF de órdenes de fabricación con subproductos y centros de trabajo.',
    'category': 'Manufacturing',
    'author': 'Jose Sacramento Ruiz Arizmendi',
    'depends': ['mrp'],
    'data': [
        'security/ir.model.access.csv',
        'reports/production_report_action.xml',
        'reports/production_report_template.xml',
        'wizard/production_report_wizard.xml',
        'views/production_report_views.xml',
    ],
    'installable': True,
    'application': False,
    'license': 'LGPL-3',
}