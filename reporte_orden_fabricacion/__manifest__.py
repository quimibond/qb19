{
    'name': 'Reporte Orden de Fabricación',
    'version': '19.0.1.0.0',
    'category': 'Manufacturing',
    'summary': 'Reporte técnico PDF de trazabilidad completa para MO',
    'author': 'Jose Sacramento Ruiz Arizmendi',
    'depends': ['mrp', 'stock'],
    'data': [
        'views/mo_report_views.xml',
        'report/mo_report_summary_templates.xml',
    ],
    'installable': True,
    'application': False,
    'license': 'LGPL-3',
}