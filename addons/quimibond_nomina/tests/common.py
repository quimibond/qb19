# -*- coding: utf-8 -*-
"""Ayudas de las pruebas de nómina."""


def liberar_referencias(env, *referencias):
    """Las pruebas también corren sobre la copia de producción (staging), donde
    las referencias de empleado que usan (S-1, S-250, Q-9…) ya existen y la base
    no deja repetirlas. Se quitan a los empleados reales dentro de la
    transacción de la prueba, que se deshace al terminar: no cambia ningún dato."""
    env['hr.employee'].sudo().with_context(active_test=False).search(
        [('registration_number', 'in', list(referencias))]).write({'registration_number': False})
