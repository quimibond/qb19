# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Programación de Tintorería',
    'version': '19.0.1.5.0',
    'category': 'Manufacturing',
    'summary': 'Asignación de HTJ por orden, cálculo del tamaño de baño y división simétrica de la MO de tela acabada en kg',
    'description': """
Programación de Tintorería
==========================
* Las órdenes de fabricación del Producto A (tela acabada en proceso, en kg; código de la tela
  acabada con la «I» al frente, DAT P-D02-01 rev. 05) se crean por MPS/MTO por el total requerido,
  sin calcular el tamaño de baño.
* Una orden requiere programación cuando su producto es el «Acabado en proceso» de una ficha
  técnica de acabado (``product_proceso_id``) y su BoM tiene una operación de Tintorería. El
  vínculo kg <-> metros vive en la ficha, no en el código del artículo.
* Opción «Programar Tintorería»: el área de Tintorería asigna el HTJ de cada orden. Al confirmar:
  - se toma el rendimiento de tejido del artículo (``_ft_rendimiento``) y se obtiene su banda
    (A 3 a <7, B 7 a <11, C 11 a 16, los mismos grupos del módulo de fichas);
  - la capacidad por baño sale de la configuración vigente del jet en las fichas técnicas
    (``_ft_capacidad_carga``): primero los «kg por carga» del artículo en ese jet; si no hay,
    la capacidad de la banda (no mayor que la capacidad general del jet); un jet sin bandas
    capturadas (p. ej. HTJ5) usa su capacidad general con aviso;
  - si los kg exceden la capacidad, la MO se divide en N órdenes simétricas (mismo HTJ) con la
    división nativa de Odoo 19, que conserva el enlace MTO hacia la MO en metros;
  - se asigna el HTJ a la operación de Tintorería de cada MO, lo que dispara el recálculo de
    la dosificación (módulo ``quimibond_tintoreria_dosificacion``).
* Duración de Tintorería: el tiempo es por ORDEN (un solo ciclo, no por kg). Al programar se
  fija en la workorder de Tintorería de cada MO: 1) tiempo de ciclo del artículo en el jet
  (ficha técnica, en horas); 2) minutos de la fórmula en ese HTJ (``tintoreria.formula.tiempo``);
  3) la operación de Tintorería de la BoM, con aviso: su tiempo es el de Odoo, por unidad de la
  BoM (en kg, minutos por kg), y se multiplica por los kg del baño (1.5.0). Sin ninguno, se bloquea.
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
        'views/tintoreria_formula_tiempo_views.xml',
        'wizard/tintoreria_programacion_wizard_views.xml',
        'views/mrp_production_views.xml',
        'views/menus.xml',
    ],
    'installable': True,
    'application': False,
}
