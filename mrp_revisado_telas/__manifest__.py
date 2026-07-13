# -*- coding: utf-8 -*-
{
    'name': 'Mrp Revisado Telas (Botones Directos)',
    'version': '19.0.1.3',
    'summary': 'Control de pesaje y revisión desde el tablero de centros de trabajo',
    'category': 'Manufacturing',
    'author': 'Jose Sacramento Ruiz Arizmendi',
    'website': 'https://github.com/jsra2025',
    'depends': [
        'mrp', 
        'stock',
        'pesaje_rollos_tejido',
        'quality_control',
        'iot',
        'iot_scale_common',
    ],
    'data': [
        # 1. Seguridad siempre primero
        'security/ir.model.access.csv',
        
        # 2. Definición de acciones (Wizards) ANTES de usarlas en las vistas
        'wizard/mrp_revisado_wizard_views.xml',
        
        # 3. Parametro % Revisado
        'views/res_config_settings_views.xml',
        
        # 4. Vistas que heredan y agregan botones
        'views/mrp_production_views.xml',
        'views/stock_lot_views.xml',
        'report/ir_actions_report.xml',
        'report/report_revisado_templates.xml',
    ],
    # El widget de báscula (scale_capture_field.js/.xml) ahora vive en el
    # módulo común 'iot_scale_common' y se carga automáticamente por ser
    # una dependencia -- ya no se declara aquí. iot_handler.js se eliminó
    # (era código muerto de lectura de báscula, mal etiquetado como
    # "impresión ZPL"). No queda ningún asset propio de este módulo, por
    # eso no hay clave 'assets'.
    'installable': True,
    'application': False,
    'license': 'LGPL-3',
}