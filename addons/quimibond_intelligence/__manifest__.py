{
    'name': 'Quimibond Intelligence System',
    'version': '19.0.24.0.0',
    'category': 'Productivity',
    'summary': 'Deep Intelligence: Gmail + 10 modelos Odoo + Knowledge Graph + Claude AI.',
    'description': (
        'Cerebro de inteligencia empresarial con enriquecimiento profundo de Odoo. '
        'Lee emails de Gmail, cruza con ventas, facturas, pagos, entregas, '
        'producción, CRM, actividades, calendario y chatter de Odoo. '
        'Genera briefings ejecutivos, alertas, scoring de clientes y '
        'perfiles acumulativos de contactos con Claude AI.'
    ),
    'author': 'Quimibond',
    'website': 'https://quimibond.com',
    'license': 'LGPL-3',
    'depends': [
        'base',
        'mail',
        'sale',
        'account',
        'purchase',
        'crm',
        'stock',
        'calendar',
    ],
    'external_dependencies': {
        'python': [
            'google.auth',
            'googleapiclient',
            'httpx',
        ],
    },
    'data': [
        'security/ir.model.access.csv',
        'data/ir_config_parameter.xml',
        'data/ir_cron_data.xml',
        'views/intelligence_briefing_views.xml',
        'views/intelligence_alert_views.xml',
        'views/intelligence_action_views.xml',
        'views/intelligence_query_views.xml',
        'views/res_partner_views.xml',
        'views/intelligence_config_views.xml',
    ],
    'installable': True,
    'application': True,
    'auto_install': False,
}
