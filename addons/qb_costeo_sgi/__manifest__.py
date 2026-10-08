# -*- coding: utf-8 -*-
{
    'name': 'Costeo Quimibond - puente con el SGI',
    'summary': 'Cotizaciones ligadas al proyecto de desarrollo de producto (C1): '
               'datos del proyecto, recálculo por revisión y medición del SGI',
    'description': """
Puente entre el cotizador (`qb_cotizador`) y el SGI (`quimibond_sgi`), spec
§6.1 y §8.4. Se instala solo cuando los tres módulos están.

- La cotización se liga a un proyecto de desarrollo de producto y toma de él
  cliente, artículo, gramaje, ancho y galga (tabla de características),
  volumen y precio objetivo (solicitud).
- El proyecto muestra sus cotizaciones y, al subir de revisión, recalcula la
  cotización viva: si el piso cambia, vuelve a «Por aprobar» y el proyecto se
  detiene hasta que el puesto que aprueba la libere.
- Las fichas C1.05 (costo y precio aprobados) y C1.06 (cotización enviada) se
  miden con la cotización.
- Liga las cotizaciones importadas del cotizador anterior con su proyecto FT.
    """,
    'author': 'Quimibond',
    'website': 'https://www.quimibond.com',
    'category': 'Sales',
    'version': '19.0.1.2.0',
    'license': 'LGPL-3',
    'auto_install': True,
    'depends': ['qb_costeo', 'qb_cotizador', 'quimibond_sgi'],
    'data': [
        'views/cotizacion_views.xml',
        'views/project_views.xml',
    ],
    'post_init_hook': 'post_init_hook',
    'installable': True,
}
