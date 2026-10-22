# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Programación de Tintorería',
    'version': '19.0.1.1.0',
    'category': 'Manufacturing',
    'summary': 'Asignación de HTJ por orden, cálculo del tamaño de baño y división simétrica de la MO de tela acabada en kg',
    'description': """
Programación de Tintorería
==========================
* Las órdenes de fabricación de Producto Terminado en kg (código con "K" en la posición 9,
  DAT P-D02-01) se crean por MPS/MTO por el total requerido, sin calcular el tamaño de baño.
* Opción «Programar Tintorería»: el área de Tintorería asigna el HTJ de cada orden. Al confirmar:
  - se calcula la capacidad por baño con ``tintoreria.capacidad.rendimiento`` (banda de
    rendimiento de la ficha técnica de acabado);
  - si los kg exceden la capacidad, la MO se divide en N órdenes simétricas (mismo HTJ);
  - se asigna el HTJ a la operación de Tintorería de cada MO, lo que dispara el recálculo de
    la dosificación (módulo ``quimibond_tintoreria_dosificacion``).
* Validación: ninguna operación de una MO que requiere programación puede iniciarse sin haberla
  programado.
""",
    'author': 'Jose Sacramento Consolti',
    'license': 'LGPL-3',
    'depends': [
        'mrp',
        'quimibond_tintoreria_dosificacion',
        'quimibond_ficha_tecnica_tela',
    ],
    'data': [
        'security/ir.model.access.csv',
        'views/tintoreria_programacion_views.xml',
        'wizard/tintoreria_programacion_wizard_views.xml',
        'views/mrp_production_views.xml',
        'views/menus.xml',
    ],
    'installable': True,
    'application': False,
}
