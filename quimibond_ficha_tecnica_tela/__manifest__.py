# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Ficha Técnica de Tela',
    'version': '19.0.1.0.0',
    'summary': 'Carga y administración de fichas técnicas de tela (Tejido + Acabado)',
    'description': """
Ficha Técnica de Tela (Tejido + Acabado)
=========================================
Módulo para capturar, importar y administrar la ficha técnica completa de
cada artículo de tela, cubriendo:

- Datos de Tejido: máquina, hilos, especificaciones de tejido, tela
  acondicionada (peso, ancho, espesor, elongación).
- Datos de Acabado: rendimiento de tela acabada (mts/kg), peso, ancho,
  espesor, encogimiento (largo/ancho) y elongación (largo/ancho).

Incluye:
- Vínculo con el producto de "Tela en Proceso" (kg) y "Tela Acabada"
  (m, varios colores/variantes que comparten la misma ficha).
- Wizard de importación masiva desde Excel (formato de ficha técnica de
  tejido, tal como se genera hoy en Quimibond).
- Validaciones de tolerancia (ej. encogimiento máximo 5%).
""",
    'category': 'Manufacturing',
    'author': 'Quimibond',
    'depends': ['mrp', 'product'],
    'data': [
        'security/ir.model.access.csv',
        'views/ficha_tecnica_tela_views.xml',
        'views/product_template_views.xml',
        'wizard/ficha_tecnica_import_wizard_views.xml',
    ],
    'installable': True,
    'application': True,
    'license': 'LGPL-3',
}
