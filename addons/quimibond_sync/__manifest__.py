{
    'name': 'Quimibond Sync',
    'version': '19.0.1.0.0',
    'category': 'Productivity',
    'summary': 'Minimal Odoo↔Supabase data bridge. Pushes ERP data, pulls intelligence commands.',
    'description': """
        Lightweight addon that syncs Odoo operational data to Supabase
        and pulls back intelligence-generated commands (new contacts, completed actions).

        All intelligence logic (Gmail sync, Claude analysis, alerts, briefings, scoring)
        runs on Vercel. This addon only handles the Odoo↔Supabase data bridge.
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
    ],
    'installable': True,
    'application': False,
    'auto_install': False,
}
