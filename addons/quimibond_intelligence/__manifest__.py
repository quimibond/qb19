{
    'name': 'Quimibond Intelligence System',
    'version': '19.0.1.0.0',
    'category': 'Productivity',
    'summary': 'Communication Intelligence — Lee 22 cuentas de email, analiza con Claude AI, cruza con datos de Odoo y genera briefings ejecutivos diarios.',
    'description': """
Quimibond Intelligence System v6
================================

Sistema de inteligencia de comunicaciones que:

* Lee emails de 22 cuentas vía Gmail API (Service Account + Domain-Wide Delegation)
* Deduplica emails entre cuentas (fingerprint-based)
* Analiza con Claude API: sentimiento, urgencia, temas, riesgos
* Cruza contactos con datos de Odoo (ventas, facturas, compras) vía ORM directo
* Calcula scores de relación por cliente (0-100, 4 dimensiones)
* Genera embeddings con Voyage AI para memoria semántica
* Persiste todo en Supabase con pgvector
* Envía briefing ejecutivo diario y análisis semanal por email
* Se auto-mejora: detecta patrones, auto-resuelve alertas, aprende
    """,
    'author': 'Quimibond',
    'website': 'https://quimibond.com',
    'license': 'LGPL-3',
    'depends': [
        'base',
        'mail',
        'sale',
        'account',
        'purchase',
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
        'views/intelligence_config_views.xml',
    ],
    'installable': True,
    'application': True,
    'auto_install': False,
}
