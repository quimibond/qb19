{
    'name': 'Quimibond Intelligence System',
    'version': '19.0.29.0.0',
    'category': 'Productivity',
    'summary': 'Sincroniza datos de Odoo con el sistema de inteligencia en Supabase.',
    'description': (
        'Sincroniza contactos, facturas, entregas, CRM y actividades de Odoo '
        'hacia Supabase. El analisis con IA, dashboard, alertas y briefings '
        'se gestionan en el frontend (quimibond-intelligence.vercel.app).'
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
            'anthropic',
            'google.auth',
            'googleapiclient',
            'httpx',
        ],
    },
    'data': [
        'security/ir.model.access.csv',
        'data/ir_config_parameter.xml',
        'data/ir_cron_data.xml',
        'data/disable_migrated_crons.xml',
        'views/intelligence_briefing_views.xml',
        'views/intelligence_alert_views.xml',
        'views/intelligence_action_views.xml',
        'views/res_partner_views.xml',
        'views/intelligence_config_views.xml',
    ],
    'installable': True,
    'application': True,
    'auto_install': False,
}
