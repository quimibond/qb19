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
        'iot'
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
    # OJO: antes existían DOS claves 'assets' en este mismo diccionario.
    # En Python, un dict literal con claves repetidas simplemente descarta
    # la primera y se queda con la última -> 'scale_revisado_handler.js'
    # nunca se estaba cargando en el navegador. Se deja UNA sola clave.
    'assets': {
        'web.assets_backend': [
            # Lógica original de impresión de etiquetas Zebra (IoT printer)
            'mrp_revisado_telas/static/src/js/iot_handler.js',

            # Widget Owl de captura de peso vía báscula IoT (reemplaza al
            # antiguo scale_revisado_handler.js basado en dispatchEvent,
            # que rompía la reactividad de Owl en el wizard)
            'mrp_revisado_telas/static/src/js/scale_capture_field.js',
            'mrp_revisado_telas/static/src/xml/scale_capture_field.xml',
        ],
    },
    'installable': True,
    'application': False,
    'license': 'LGPL-3',
}
