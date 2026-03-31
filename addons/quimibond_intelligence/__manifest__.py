{
    'name': 'Quimibond Intelligence',
    'version': '19.0.31.0.0',
    'license': 'LGPL-3',
    'category': 'Productivity',
    'summary': 'Minimal Odoo↔Supabase data bridge for Quimibond Intelligence.',
    'description': """
        Syncs Odoo operational data to Supabase and pulls back
        intelligence-generated commands. All analysis, alerts,
        briefings, and scoring run on Vercel.
    """,
    'author': 'Quimibond',
    'website': 'https://quimibond.com',
    'depends': ['base', 'sale', 'purchase', 'account', 'stock', 'crm', 'mail'],
    'external_dependencies': {
        'python': ['httpx'],
    },
    'data': [
        'security/ir.model.access.csv',
        'data/ir_cron_data.xml',
        'views/sync_status_views.xml',
    ],
    'installable': True,
    'application': True,
    'auto_install': False,
}
