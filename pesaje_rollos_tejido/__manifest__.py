# -*- coding: utf-8 -*-
{
    'name': 'Pesaje de Rollos y Subproductos Tejido',
    'version': '19.0.1.4.1',
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
        'quality_control',  # Requerido para la lógica de calidad del subproducto
        'iot',
        'iot_scale_common',
    ],
    # scale_tejido_handler.js queda eliminado: parcheaba FormController.prototype
    # globalmente (afectando TODOS los formularios de Odoo, no solo estos
    # wizards) e ignoraba iot_device_id para resolver la URL de la báscula.
    # El widget 'peso_bascula' del módulo iot_scale_common lo reemplaza,
    # scoped únicamente a los campos donde se declara explícitamente.
    # No queda ningún asset propio de este módulo, por eso se retira la
    # clave 'assets' (si en tu versión real tenías más JS/CSS aquí,
    # consérvala con esos otros archivos únicamente).
    'data': [
        'security/ir.model.access.csv',
        'security/security_groups.xml',
        'views/mrp_tara_view.xml',              # Modelo de Taras
        'views/mrp_rollo_estandar_view.xml',
        'views/mrp_weigh_wizard_view.xml',      # Vistas de los asistentes
        'views/mrp_subproduct_wizard_view.xml', # Vista del nuevo asistente de subproductos
        'views/wizard_corregir_view.xml',       # Vista de boton para corregir numero Rollo Circular
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