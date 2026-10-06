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


def sgi_set_mast(env, login='sgi_mast_avisos'):
    """Jefe MAST que recibe los avisos sin dueño (``_sgi_manager_user_id``).

    En una base nueva nadie está en el grupo: el env de la prueba es OdooBot,
    que está archivado, y ``_sgi_first_user_id`` solo toma usuarios activos.
    Sin esto los avisos que van al Jefe MAST no se agendan (en la copia de
    producción sí había a quién: ``quimibond_sgi.mast_user_id``)."""
    user = new_test_user(env, login=login, groups='base.group_user,quimibond_sgi.group_sgi_manager')
    env['ir.config_parameter'].sudo().set_param('quimibond_sgi.mast_user_id', str(user.id))
    return user


def sgi_set_director(env, login='sgi_direccion_avisos'):
    """Dirección de Operaciones que recibe las escalaciones (primer miembro
    activo del grupo; en una base nueva no hay ninguno).

    57.66.0: en la copia de producción el grupo ya tiene miembros reales con
    id menor (Dirección, id 9) y ``_sgi_first_user_id`` los elegía a ellos.
    Dentro de la prueba (se deshace al final) los demás miembros directos
    activos salen del grupo; el usuario de prueba queda como el primero."""
    user = new_test_user(env, login=login, groups='base.group_user,quimibond_sgi.group_sgi_director')
    group = env.ref('quimibond_sgi.group_sgi_director')
    others = group.user_ids.filtered('active') - user
    if others:
        group.write({'user_ids': [(3, other.id) for other in others]})
    return user
