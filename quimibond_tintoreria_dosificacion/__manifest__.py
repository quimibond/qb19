# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Dosificación de Tintorería',
    'version': '19.0.1.2.0',
    'category': 'Manufacturing',
    'summary': 'Catálogo de fórmulas de tintorería y cálculo de requerimiento de químicos (g/L) y colorantes (%)',
    'description': """
Dosificación de Tintorería
==========================
* Catálogo de fórmulas de tintorería (dato maestro) con sus ingredientes.
* Campos de dosificación en las líneas de BoM: g/L (químicos) o % sobre kg (colorantes).
* El tipo de componente se deriva de la categoría del producto.
* Cabecera de BoM con la fórmula de tintorería, carga de ingredientes desde el catálogo
  y validación contra el catálogo.
* En la MO el requerimiento se calcula con la relación de baño del artículo en el jet o, si no
  hay, la del jet (fichas técnicas) y se recalcula si cambia la máquina, mientras la requisición
  no se haya surtido.
* 1.2.0: el catálogo de fórmulas pasa a Fichas técnicas → Tintorería → Fórmulas (antes
  Fabricación → Configuración) y el aviso de relación de baño faltante dice dónde se captura.
""",
    'author': 'Jose Sacramento Consolti',
    'license': 'LGPL-3',
    'depends': ['mrp', 'quimibond_ficha_tecnica_tela'],
    'data': [
        'security/ir.model.access.csv',
        'data/ir_config_parameter.xml',
        'views/tintoreria_formula_views.xml',
        'views/mrp_bom_views.xml',
        'views/mrp_production_views.xml',
    ],
    'installable': True,
    'application': False,
}
