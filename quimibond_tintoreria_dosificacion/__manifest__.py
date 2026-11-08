# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Dosificación de Tintorería',
    'version': '19.0.1.4.1',
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
* 1.3.0: el formulario de la fórmula lleva el nombre como título y explica la regla de dosificación
  (g/L con la relación de baño, % sobre kg); los colorantes se distinguen en la lista, y la
  lista de fórmulas trae el número de ingredientes.
* 1.4.0: la dosis va sobre los kg que entran al jet (el crudo, componente de una categoría de
  tejido), no sobre los kg de tela de la orden: con 1.1505 kg de crudo por kg de tela (IWJ038),
  la orden pedía alrededor de 13 % menos químico y colorante. Sin componente de tejido, como antes.
* 1.4.1: da a las cargas y avisos por jet de fichas técnicas (2.28.0) los kg de crudo (`_ft_kg_carga_jet`).
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
