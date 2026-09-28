# -*- coding: utf-8 -*-
"""Usuario de prueba sin superusuario.

El env de TransactionCase es el superusuario y los candados del SGI se saltan
con ``env.su`` (el sistema cierra lo que haga falta). Las pruebas de candados
llaman la acción con un usuario real del SGI para que el candado sí corra.
"""
from odoo.exceptions import AccessError, UserError
from odoo.tests.common import new_test_user


def sgi_test_user(env, login='sgi_candado_prueba',
                  groups='quimibond_sgi.group_sgi_manager'):
    return new_test_user(env, login=login, groups='base.group_user,' + groups)


def assert_locked(test, method, *args, **kwargs):
    """La acción se detiene por el candado (UserError), no por permisos."""
    with test.assertRaises(UserError) as caught:
        method(*args, **kwargs)
    test.assertNotIsInstance(
        caught.exception, AccessError,
        "Debió detenerla el candado, no un permiso: %s" % caught.exception)
