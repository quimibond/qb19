# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Checadores ZKTeco y Hikvision',
    'version': '19.0.1.8.0',
    'license': 'LGPL-3',
    'category': 'Human Resources/Attendances',
    'summary': 'Recibe las checadas de los relojes ZKTeco de la planta (protocolo ADMS/PUSH o por API) '
               'y las convierte en asistencias de Odoo.',
    'description': """
Checadores ZKTeco
=================

Los relojes checadores de la planta (ZKTeco MB10-VL y UA860, Toluca) empujan
sus registros a ``/iclock/cdata`` con el protocolo ADMS/PUSH de ZKTeco. El
módulo guarda cada checada tal cual llega (equipo, usuario, fecha y hora,
tipo de verificación) y un cron la empareja por persona en asistencias
(``hr.attendance``): una por turno (entrada, comida y salida juntas, la
comida dentro de la jornada), con reglas para turnos de 12 horas que cruzan
la medianoche.

Lo que no se puede emparejar (entrada sin salida, usuario sin empleado,
registro fuera de orden) queda marcado para que el supervisor lo resuelva;
nunca se inventan horas.

El MB10-VL tiene HTTPS y empuja directo a Odoo. El UA860 no lo tiene:
empuja en HTTP plano a un puente (``puente/``) en una PC de la planta o un
servidor chico, que reenvía tal cual a Odoo por HTTPS.

Los terminales Hikvision (DS-K1T…, oficina de Ciruelos) mandan sus eventos
por ISAPI («Escucha HTTP») a ``/qb_checador/hikvision/<token>``; el token
está en la ficha del reloj. Para otro tipo de equipo queda
``qb.checada.ingresar_api``.
    """,
    'author': 'Quimibond',
    'website': 'https://www.quimibond.com',
    'depends': ['hr', 'hr_attendance'],
    'data': [
        'security/ir.model.access.csv',
        'views/equipo_views.xml',
        'views/checada_views.xml',
        'views/hr_employee_views.xml',
        'views/sincronizacion_views.xml',
        'views/menus.xml',
        'data/ir_cron_data.xml',
    ],
    'installable': True,
    'application': False,
    'auto_install': False,
}
