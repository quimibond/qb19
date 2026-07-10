# -*- coding: utf-8 -*-
{
    'name': 'IoT Scale Common (Widget de Báscula)',
    'version': '19.0.1.0.0',
    'category': 'Manufacturing/Technical',
    'summary': 'Widget Owl y mixin común para capturar peso desde básculas IoT en wizards',
    'description': """
Módulo técnico compartido, sin uso directo por sí solo. Otros módulos
(mrp_revisado_telas, pesaje_rollos_tejido, etc.) dependen de este para:

- El mixin 'scale.wizard.mixin': aporta a cualquier TransientModel los
  campos estándar 'weighing_mode', 'iot_device_id' y 'scale_read_url'.
- La extensión de 'iot.device' con el campo 'scale_read_url', donde se
  configura el endpoint HTTP(S) real de cada báscula (evita tener IPs
  quemadas repetidas en varios archivos JS).
- El widget de campo Owl 'peso_bascula': un <field widget="peso_bascula"/>
  que agrega un botón "Capturar" y escribe el peso leído directamente en
  el estado reactivo del record (record.update()), sin tocar el DOM y sin
  parchear FormController globalmente.
    """,
    'author': 'Jose Sacramento Ruiz Arizmendi',
    'website': 'https://github.com/jsra2025',
    'depends': ['iot', 'web'],
    'assets': {
        'web.assets_backend': [
            'iot_scale_common/static/src/js/scale_capture_field.js',
            'iot_scale_common/static/src/xml/scale_capture_field.xml',
        ],
    },
    'data': [
        'views/iot_device_views.xml',
    ],
    'installable': True,
    'application': False,
    'license': 'LGPL-3',
}
