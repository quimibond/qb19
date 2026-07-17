# -*- coding: utf-8 -*-
{
    'name': "Quimibond SGI",
    'summary': "Sistema de Gestión Integral (ISO 9001/14001/45001) sobre apps nativas de Odoo 19",
    'description': """
Sistema de Gestión Integral de Productora de No Tejidos Quimibond (PNTQ).

Fase 1 — Núcleo documental, no conformidades, reclamaciones, mejora y mapa de procesos.

Extiende apps nativas (Documentos, Aprobaciones, Calidad, Helpdesk, Proyecto) sin
duplicarlas y agrega los modelos que Odoo no tiene (mapa de procesos, acuses,
catálogos SGI). Toda la lógica de negocio vive en este módulo (cero Studio).
    """,
    'author': "Quimibond",
    'website': "https://www.quimibond.com",
    'category': 'Services/SGI',
    'version': '19.0.2.0.0',
    'license': 'LGPL-3',
    'application': True,
    'depends': [
        'base',
        'mail',
        'hr',
        'stock',
        'purchase',
        'documents',
        'approvals',
        'approvals_purchase',
        'quality_control',
        'quality_mrp',
        'helpdesk',
        'project',
        'sale_management',
        'maintenance',
        'survey',
    ],
    'data': [
        # security
        'security/sgi_security.xml',
        'security/ir.model.access.csv',
        # data
        'data/sgi_sequences.xml',
        'data/sgi_sequences_fase2.xml',
        'data/sgi_areas.xml',
        'data/sgi_norms.xml',
        'data/sgi_process_data.xml',
        'data/sgi_stages.xml',
        'data/sgi_objectives.xml',
        'data/sgi_indicators_data.xml',
        'data/sgi_risk_data.xml',
        'data/sgi_audit_data.xml',
        'data/sgi_helpdesk_interno.xml',
        'data/sgi_mgmt_review_data.xml',
        'data/sgi_cron.xml',
        'data/sgi_cron_fase2.xml',
        # views
        'views/sgi_area_views.xml',
        'views/sgi_norm_views.xml',
        'views/sgi_process_views.xml',
        'views/sgi_document_views.xml',
        'views/sgi_doc_change_views.xml',
        'views/sgi_nonconformity_views.xml',
        'views/sgi_complaint_views.xml',
        'views/sgi_improvement_views.xml',
        'views/sgi_integration_views.xml',
        'views/sgi_objective_views.xml',
        'views/sgi_indicator_views.xml',
        'views/sgi_audit_views.xml',
        'views/sgi_risk_views.xml',
        'views/sgi_supplier_eval_views.xml',
        'views/sgi_res_partner_views.xml',
        'views/sgi_management_review_views.xml',
        # reports
        'report/report_nc.xml',
        'report/report_news.xml',
        'report/report_audit.xml',
        'report/report_mgmt_review.xml',
        # menus
        'views/sgi_menus.xml',
    ],
    'installable': True,
}
