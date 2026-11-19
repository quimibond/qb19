# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Cierre de inventario',
    'version': '19.0.1.1.0',
    'license': 'LGPL-3',
    'category': 'Inventory/Inventory',
    'summary': 'Casilla «No valuar en el cierre» en la categoría de producto: el cierre y el reporte de valuación de Odoo 19 ignoran esas categorías (refacciones, activo fijo).',
    'description': """
Cierre de inventario
====================

El cierre de inventario de Odoo 19 (``stock_account``) toma todo producto
almacenable. Para Quimibond eso mete al inventario las refacciones (que van a
gasto al comprarse) y el activo fijo (que ya está en 153), y reclasifica años
de consumos de esas categorías.

Este módulo agrega en la categoría de producto la casilla **No valuar en el
cierre**. Una categoría marcada (cada subcategoría se marca aparte):

* no entra al valor del almacén del cierre ni del reporte de valuación;
* no entra a la reclasificación por ubicación (merma, desecho, consumo);
* su cuenta de inventario no se compara contra el almacén, si ninguna
  categoría que sí se valúa la usa.

Las existencias y el costo de esos productos no cambian (Mantenimiento y el
surtido de refacciones los siguen usando).
    """,
    'author': 'Quimibond',
    'website': 'https://www.quimibond.com',
    'depends': ['stock_account'],
    'data': [
        'views/product_category_views.xml',
    ],
    'installable': True,
    'application': False,
    'auto_install': False,
}
