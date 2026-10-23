# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Pedimentos',
    'version': '19.0.1.0.1',
    'license': 'LGPL-3',
    'category': 'Inventory/Inventory',
    'summary': 'Expediente de cada pedimento de importación y exportación: documentos, contribuciones, '
               'cuenta de gastos, garantías y avisos.',
    'description': """
Pedimentos
==========

Un registro por pedimento con todo su expediente, en lugar de los Excel
F-P-A-16-05 IMPORTACIONES y F-P-A-16-06 EXPORTACIONES:

- Número de pedimento validado (año, aduana, patente, consecutivo), clave (A1,
  V1, R1…), agencia, compras, recepciones o entregas y facturas ligadas.
- Etapas con fecha: preparación, en tránsito, en aduana, despachado,
  entregado / cruzado, cerrado.
- Checklist de documentos por tipo de operación, cada uno con su adjunto.
- Contribuciones (DTA, IGI, IVA, PRV), partidas con fracción arancelaria,
  cuenta de gastos contra anticipo, garantías navieras y de cuenta aduanera
  hasta su devolución.
- El número se copia a los campos nativos de la localización mexicana
  (entrega y costo en destino) cuando ``l10n_mx_edi`` está instalado.
- Avisos diarios como actividades: despachado sin cruzar, entregado sin
  cerrar, garantía sin devolver.
- Facturas a clientes con pedimento virtual (o con comercio exterior) sin
  pedimento ligado, para la auditoría.

Diseño y evidencia: docs/superpowers/specs/2026-10-23-pedimentos-design.md
    """,
    'author': 'Quimibond',
    'website': 'https://quimibond.com',
    'depends': ['purchase_stock', 'sale_stock', 'stock_landed_costs'],
    'data': [
        'security/qb_pedimento_security.xml',
        'security/ir.model.access.csv',
        'data/qb_pedimento_data.xml',
        'data/ir_cron_data.xml',
        'views/qb_pedimento_catalog_views.xml',
        'views/qb_pedimento_views.xml',
        'views/qb_pedimento_garantia_views.xml',
        'views/res_partner_views.xml',
        'views/res_config_settings_views.xml',
        'views/document_views.xml',
        'views/menus.xml',
    ],
    'installable': True,
    'application': True,
}
