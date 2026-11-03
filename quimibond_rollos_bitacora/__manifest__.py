# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Taras y Bitácora de Rollos (Tela Acabada)',
    'version': '19.0.3.0.0',
    'category': 'Manufacturing',
    'summary': 'Tabla de taras por producto y operación, bitácora de rollos y pesaje en Rama de la MO en kg',
    'description': """
Taras y bitácora de rollos — tela acabada
=========================================
Diseño técnico v5.4, §8, §9, §12.1 y §13.

* ``mrp.tara.producto``: una tara por producto y operación (Rama, Clasificación final). Sin tara
  capturada el pesaje se bloquea; nunca se asume 0. Botón «Cargar taras iniciales» desde los
  archivos de Quimibond (ACABADO TARA RAMAS e INSPECCION TARAS).
* ``mrp.rollo.bitacora``: un registro por rollo, con los datos de Rama, Inspección y Clasificación
  final (§9.1). Esta primera entrega captura Rama; Inspección y Clasificación final llenan el resto.
* Pesaje en Rama: único pesaje del rollo en la orden en kg. Peso bruto (báscula IoT o captura manual
  con motivo) menos la tara de Rama = kg netos = cantidad del lote del Producto A. Lote-serie =
  Orden de Fabricación + No. de rollo (inmutable) y etiqueta pequeña ZPL solo con el código de barras.
* ``es_desarrollo`` en la MO (se hereda de la MO en metros a la MO en kg) para el ruteo al Almacén 31.
* Parámetros de alerta de metros en Ajustes de Manufactura.
""",
    'author': 'Jose Sacramento Consolti',
    'license': 'LGPL-3',
    'depends': [
        'mrp',
        'hr',
        'stock',
        'iot_scale_common',
        'quimibond_ficha_tecnica_tela',
    ],
    'data': [
        'security/ir.model.access.csv',
        'data/calidad.defecto.tela.csv',
        'views/calidad_defecto_tela_views.xml',
        'views/mrp_inspeccion_restriccion_views.xml',
        'views/mrp_tara_producto_views.xml',
        'views/mrp_rollo_bitacora_views.xml',
        'views/mrp_production_views.xml',
        'views/mrp_workorder_views.xml',
        'views/res_config_settings_views.xml',
        'views/hr_employee_views.xml',
        'wizard/rollo_pesaje_rama_wizard_views.xml',
        'wizard/rollo_clasificacion_wizard_views.xml',
        'wizard/rollo_inspeccion_wizard_views.xml',
        'wizard/rollo_desperdicio_rama_wizard_views.xml',
        'report/report_etiqueta_rama.xml',
        'views/menus.xml',
    ],
    'installable': True,
    'application': False,
}
