# -*- coding: utf-8 -*-
{
    'name': 'Pesaje de Rollos y Subproductos Tejido',
    'version': '19.0.1.3.0',
    'category': 'Manufacturing',
    'summary': 'Control de pesaje de rollos, subproductos, lotes automáticos y etiquetas ZPL',
    'description': """
        Este módulo permite:
        - Registro de pesaje rollo por rollo sin movimiento inicial total.
        - Generación de lotes automáticos con formato MO-Número de Rollo.
        - Registro único de subproductos con pesaje y lote.
        - Aprobación automática de controles de calidad de tipo 'Registrar Subproductos'.
        - Generación de etiquetas ZPL para ambos procesos.
    """,
    'author': 'Jose Sacramento',
    'depends': [
        'mrp', 
        'stock', 
        'mrp_workorder', 
        'quality_control'  # Requerido para la lógica de calidad del subproducto
    ],
    'data': [
        'security/ir.model.access.csv',
        'views/mrp_weigh_wizard_view.xml',      # Vistas de los asistentes
        'views/mrp_subproduct_wizard_view.xml',  # Vista del nuevo asistente de subproductos
        'views/mrp_production_view.xml',        # Pestaña ZPL en la MO
        'views/mrp_workorder_tablet_view.xml',  # Botones PESAR y SUBPRODUCTO en tableta
        'views/res_config_settings_view.xml',   # Configuraciones adicionales
        'report/ir_actions_report.xml',
        'report/report_zpl_templates.xml',
    ],
    'installable': True,
    'application': True,
    'license': 'LGPL-3',
}