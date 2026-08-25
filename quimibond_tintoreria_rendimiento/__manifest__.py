# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Tabla de Rendimientos y RB Tintorería',
    'version': '19.0.1.0.0',
    'summary': 'Capacidad por rendimiento y Relación de Baño (RB) por centro de trabajo de Tintorería',
    'description': """
Tabla de Rendimientos y RB Tintorería
======================================
Módulo de configuración para Manufactura que define, por centro de trabajo
de Tintorería:

- Capacidad máxima (kg).
- Capacidad por banda de rendimiento de tela (Grupo A 3-6, Grupo B 7-10,
  Grupo C 11-15 m/kg) — usada para calcular el tamaño de orden de trabajo
  y el split automático cuando el rendimiento del artículo no permite
  procesar toda la cantidad en un solo baño.
- Relación de Baño — RB (litros por kg de tela) — usada para calcular la
  cantidad de químicos/agua necesarios según el tamaño en kg de la orden.

Accesible desde Manufactura > Configuración > Tabla de Rendimientos y RB
Tintorería.
""",
    'category': 'Manufacturing',
    'author': 'Jose Sacramento Consolti',
    'depends': ['mrp'],
    'data': [
        'security/ir.model.access.csv',
        'views/tintoreria_capacidad_rendimiento_views.xml',
        'data/tintoreria_capacidad_rendimiento_data.xml',
    ],
    'installable': True,
    'application': False,
    'license': 'LGPL-3',
}
