# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Portal de proveedores',
    'version': '19.0.1.1.0',
    'license': 'LGPL-3',
    'category': 'Website',
    'summary': 'Portal de proveedores del sitio B2B: órdenes de compra, subir la factura (PDF y XML) '
               'contra la orden, estado de pago y alta de proveedor en línea.',
    'description': """
Portal de proveedores de Quimibond
==================================

Extiende «Mi cuenta» del sitio B2B (``website_quimibond``) para el proveedor:

* **Órdenes de compra** con lo pedido, lo recibido y lo facturado.
* **Subir factura** (PDF y XML) contra la orden: se lee el CFDI, se valida el
  RFC del emisor y del receptor, que no esté repetido, el estado en el SAT
  (si ``quimibond_sat`` está instalado) y el importe contra lo recibido.
  Queda como «Factura recibida» para Cuentas por pagar; no crea asientos.
* **Facturas y pagos**: qué facturas están en revisión, cuáles tienen pago
  programado y con qué fecha, y cuáles ya se pagaron.
* **Alta de proveedor en línea** (``/proveedores/alta``) con constancia
  fiscal, opinión de cumplimiento y carátula bancaria. Compras la revisa y la
  aprueba.

Todo se activa con la casilla «Diseño B2B de Quimibond» del sitio web.
    """,
    'author': 'Quimibond',
    'website': 'https://www.quimibond.com',
    'depends': [
        'website_quimibond',
        'purchase_stock',
    ],
    'data': [
        'security/ir.model.access.csv',
        'data/ir_cron_data.xml',
        'views/backend_views.xml',
        'views/website_views.xml',
        'views/portal.xml',
        'views/alta.xml',
    ],
    'assets': {
        'web.assets_frontend': [
            'website_quimibond_proveedores/static/src/css/proveedores.css',
        ],
    },
    'installable': True,
    'application': False,
    'auto_install': False,
}
