# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Ficha Técnica de Tela',
    'version': '19.0.2.0.0',
    'summary': 'Fichas técnicas de Tejido y de Acabado, e importación masiva desde Excel',
    'description': """
Fichas Técnicas de Tela (Tejido y Acabado)
===========================================
Módulo para capturar, importar y administrar las fichas técnicas de tela,
separadas en 2 modelos:

- **Ficha Técnica de Tejido**: máquina, hilos, especificaciones de tejido,
  tela acondicionada (peso, ancho, espesor, elongación). Se vincula al
  producto "Tela en Proceso" (kg).
- **Ficha Técnica de Acabado**: rendimiento de tela acabada (mts/kg), peso,
  ancho, espesor, encogimiento (largo/ancho) y elongación (largo/ancho).
  Cada producto de "Tela Acabada" (m) — por ejemplo cada color — tiene su
  propia ficha de acabado, vinculada a la ficha de tejido que le sirve de
  base. Una misma ficha de tejido puede ser la base de varias fichas de
  acabado (varios colores/variantes).

Incluye:
- Importación masiva desde Excel (tabular, fila por fila, muchos
  artículos a la vez) para ambos modelos, con encabezados flexibles.
- Validaciones de tolerancia (ej. encogimiento máximo 5%).
- Botones inteligentes en la ficha de producto para acceder directo a la
  ficha de tejido y/o de acabado vinculada.
""",
    'category': 'Manufacturing',
    'author': 'Jose Sacramento Consolti',
    'depends': ['mrp', 'product', 'account'],
    'data': [
        'security/ir.model.access.csv',
        'views/ficha_tecnica_tejido_views.xml',
        'views/ficha_tecnica_acabado_views.xml',
        'views/product_template_views.xml',
        'wizard/ficha_tecnica_tejido_import_wizard_views.xml',
        'wizard/ficha_tecnica_acabado_import_wizard_views.xml',
    ],
    'installable': True,
    'application': True,
    'license': 'LGPL-3',
}
