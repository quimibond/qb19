# -*- coding: utf-8 -*-
{
    'name': 'Quimibond - Checadores ZKTeco',
    'version': '19.0.1.0.0',
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
(``hr.attendance``) con reglas para turnos de 12 horas que cruzan la
medianoche.

Lo que no se puede emparejar (entrada sin salida, usuario sin empleado,
registro fuera de orden) queda marcado para que el supervisor lo resuelva;
nunca se inventan horas.

Para un equipo cuyo firmware no pueda empujar, un puente en una PC de la
planta manda las mismas checadas con ``qb.checada.ingresar_api``.
    """,
    'author': 'Quimibond',
    'website': 'https://www.quimibond.com',
    'depends': ['hr', 'hr_attendance'],
    'data': [
        'security/ir.model.access.csv',
        'views/equipo_views.xml',
        'views/checada_views.xml',
        'views/hr_employee_views.xml',
        'views/menus.xml',
        'data/ir_cron_data.xml',
    ],
    'installable': True,
    'application': False,
    'auto_install': False,
}
