# -*- coding: utf-8 -*-


def sgi_exec_start_open(env, start='2000-01-01'):
    """57.149.0: fecha de arranque del registro de cumplimiento para las
    pruebas. Por omisión el arranque real es el 05/10/2026: un atraso
    relativo a hoy (una mensual que venció el día hábil 1) quedaría antes del
    arranque y no escalaría. Las pruebas que no son de la fecha de arranque la
    abren al pasado para seguir viendo el atraso; las que la prueban la ponen
    a propósito (test_registro_cumplimiento.test_09/test_10)."""
    env['ir.config_parameter'].sudo().set_param('quimibond_sgi.exec_start_date', start)
