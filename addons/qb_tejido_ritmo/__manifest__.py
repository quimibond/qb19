# -*- coding: utf-8 -*-
{
    'name': 'Ritmo de tejido desde el pesaje',
    'version': '19.0.1.0.2',
    'category': 'Manufacturing',
    'summary': 'Cuánto teje cada circular, cuánto corre, cuánto para y cuánto '
               'pierde en cambios, medido rollo por rollo desde el pesaje.',
    'description': """
El cronómetro de las órdenes de trabajo no sirve para medir el tejido: muchas
se quedan abiertas. Todos los rollos se pesan al salir de la máquina, así que
el tiempo entre un rollo y el siguiente es el tiempo real de tejido. Este
módulo clasifica cada intervalo (corrida, paro, cambio de artículo, captura
atrasada), detecta paros generales de planta, calcula ritmos por máquina y
artículo (kg/h en corrida, técnica, típica, lenta, efectiva, rango de
confianza) y alimenta el costeo (`qb_costeo`): horas por unidad con fuente
«pesaje» y capacidad medida del centro Tejido.
    """,
    'author': 'Quimibond',
    'license': 'LGPL-3',
    'depends': ['mrp', 'resource', 'qb_costeo'],
    'data': [
        'security/ir.model.access.csv',
        'data/parametros.xml',
        'data/descansos.xml',
        'data/ir_cron.xml',
        'views/descanso_views.xml',
        'views/intervalo_views.xml',
        'views/paro_general_views.xml',
        'views/ritmo_views.xml',
        'views/centro_views.xml',
        'wizards/recalcular_views.xml',
        'views/menus.xml',
    ],
    'post_init_hook': 'post_init_hook',
    'installable': True,
    'application': False,
}
