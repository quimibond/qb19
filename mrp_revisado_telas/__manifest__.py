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
        'quality_control'
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
    ],
    'assets': {
        'web.assets_backend': [
            # Mantenemos solo el manejador de la báscula IoT si lo usas
            'mrp_revisado_telas/static/src/js/iot_handler.js',
        ],
    },
    'installable': True,
    'application': False,
    'license': 'LGPL-3',
}