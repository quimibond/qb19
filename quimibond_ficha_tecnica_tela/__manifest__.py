# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Ficha Técnica de Tela',
    'version': '19.0.2.29.0',
    'summary': 'Fichas técnicas de tela en las pantallas nativas: tela, crudo, ruta, máquinas y revisión de listas',
    'description': """
Fichas técnicas de tela
=======================
Qué es cada tela y cómo se hace, en las pantallas nativas de Odoo:

- Producto: pestaña «Ficha técnica» (la tela, su crudo, la ruta de sus listas de materiales con el tiempo de cada
  operación contra el de la ficha, la especificación y los pendientes).
- Operación de la lista de materiales: tiempo desde la ficha y parámetros de la tela en la máquina y sus alternativas.
- Centro de trabajo: ficha de la máquina por fecha, capacidad de los jets por producto y telas que corren ahí.
- App «Fichas técnicas»: concentrado de telas y crudos, revisión de las listas en kg y pendientes por área, fichas de
  tejido y de acabado (datos de origen), máquinas, materias primas, catálogos e importación desde Excel.

Manual: README.md. Historia por versión: CHANGELOG.md.
""",

    'category': 'Manufacturing',
    'author': 'Jose Sacramento Consolti',
    'depends': ['mrp', 'product', 'account', 'hr'],
    'data': [
        'security/ir.model.access.csv',
        'data/ficha_tecnica_unidad_data.xml',  # 2.10.0: antes de los datos que llevan unidad
        'data/ficha_tecnica_caracteristica_data.xml',
        'data/ficha_tecnica_maquina_tipo_data.xml',  # 2.4.0
        'data/ficha_tecnica_parametros_data.xml',  # 2.23.0: parámetros del cálculo (solo los que falten)
        'data/ficha_tecnica_material_data.xml',  # 2.9.0: después de los tipos de máquina (usa tintorería)
        'views/ficha_tecnica_tejido_views.xml',
        'views/ficha_tecnica_acabado_views.xml',
        'views/ficha_tecnica_caracteristica_views.xml',  # 2.1.0: después del menú raíz que usa
        'views/maquina_config_views.xml',  # 2.3.0: después del menú «Configuración» que usa
        'views/mrp_aviso_maquina_views.xml',  # 2.5.0
        'views/tiempo_ficha_views.xml',  # 2.22.0 (2.23.0: antes tiempo_rama_views.xml)
        'views/capacidad_conversion_views.xml',  # 2.23.0
        'views/revision_listas_views.xml',  # 2.23.0
        'views/ficha_producto_views.xml',  # 2.23.0: después de maquina_config_views (usa ft_tipo_id en el centro)
        'views/ruta_familia_views.xml',  # 2.23.0
        'data/mcp_data.xml',  # 2.23.0: después del modelo de rutas
        'data/ir_cron_revision.xml',  # 2.23.0: después del modelo de la revisión
        'views/proceso_views.xml',  # 2.8.0
        'views/materia_prima_views.xml',  # 2.9.0
        'views/product_template_views.xml',
        'wizard/ficha_tecnica_tejido_import_wizard_views.xml',
        'wizard/ficha_tecnica_acabado_import_wizard_views.xml',
        'wizard/proceso_import_wizard_views.xml',  # 2.8.0
        'wizard/proceso_faltante_views.xml',  # 2.13.0
        'views/ficha_tecnica_menus.xml',  # 2.7.0: todos los menús, al final (las acciones ya existen)
    ],
    'installable': True,
    'application': True,
    'license': 'LGPL-3',
}
