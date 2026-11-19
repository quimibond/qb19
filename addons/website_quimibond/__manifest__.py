# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Sitio web B2B',
    'version': '19.0.1.0.0',
    'license': 'LGPL-3',
    'category': 'Website',
    'summary': 'Sitio web B2B de Quimibond: catálogo técnico desde los productos de Odoo, '
               'cotizaciones a CRM, mercados, planta, calidad y animaciones de tejido.',
    'description': """
Sitio web B2B de Quimibond
==========================

Diseño propio (estilo shadcn: tipografía Geist, bordes finos, componentes
sobrios) con animaciones de tejido circular y de no tejido, conectado a las
apps de Odoo:

* **Catálogo técnico** (``/catalogo``) con los productos publicados y sus
  categorías del sitio web, sin precios, con botón de cotización.
* **Solicitud de cotización o muestra** (``/cotizar``) que crea una
  oportunidad en CRM con el equipo de ventas del sitio.
* **Empleos** desde Reclutamiento, **portal de clientes** y **Helpdesk**
  para reportes de calidad.

Todo se activa por sitio web con la casilla «Diseño B2B de Quimibond»
(Sitio web → Configuración → Sitios web). Sin la casilla, el sitio se queda
como está.
    """,
    'author': 'Quimibond',
    'website': 'https://www.quimibond.com',
    'depends': [
        'website',
        'website_crm',
        'website_sale',
        'website_hr_recruitment',
    ],
    'data': [
        'views/website_views.xml',
        'views/components.xml',
        'views/layout.xml',
        'views/pages.xml',
        'views/catalogo.xml',
    ],
    'assets': {
        'web.assets_frontend': [
            'website_quimibond/static/src/css/quimibond.css',
            'website_quimibond/static/src/js/quimibond.js',
        ],
    },
    'installable': True,
    'application': False,
    'auto_install': False,
}
