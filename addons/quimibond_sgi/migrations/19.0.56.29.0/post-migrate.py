# -*- coding: utf-8 -*-
"""56.29.0 (entrega 4 de la auditoría, decisiones de Jose del 2026-09-29).

A. Salarios de eficiencias: los importes pasan del grupo de Nómina
   (hr_payroll.group_hr_payroll_user, que en producción incluye a usuarios que
   no deben verlos) al grupo propio «Salarios de eficiencias (SGI)». Los
   miembros iniciales se ponen aquí, por login, y no en el XML, para que un
   update no los reasigne. Nadie más.
B. Dirección de Operaciones dejó de implicar al Jefe MAST: sus miembros
   conservan, asignadas directo, las administraciones de Proyecto,
   Aprobaciones y Soporte (sgi.config._sgi_director_keep_admin_groups).

Idempotente: solo agrega lo que falta.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

SALARY_LOGINS = (
    'rhmexico@quimibond.com',  # Lorena
    'recursoshumanos@quimibond.com',  # Miguel
    'jose.mizrahi@quimibond.com',  # Jose
)


def _salary_members(env):
    group = env.ref('quimibond_sgi.group_sgi_salary', raise_if_not_found=False)
    if not group:
        _logger.warning("SGI 56.29.0: no existe el grupo group_sgi_salary; no se asignan miembros.")
        return
    users = env['res.users'].with_context(active_test=False).search([('login', 'in', SALARY_LOGINS)])
    found = set(users.mapped('login'))
    missing_logins = [login for login in SALARY_LOGINS if login not in found]
    to_add = users.filtered(lambda u: group not in u.group_ids)
    if to_add:
        to_add.write({'group_ids': [(4, group.id)]})
    _logger.info("SGI 56.29.0: Salarios de eficiencias: agregados %s; ya estaban %s; no encontrados %s.",
                 to_add.mapped('login'), (users - to_add).mapped('login'), missing_logins)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {'tracking_disable': True})
    _salary_members(env)
    env['sgi.config']._sgi_director_keep_admin_groups()
