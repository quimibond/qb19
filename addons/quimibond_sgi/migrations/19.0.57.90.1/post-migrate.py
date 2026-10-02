# -*- coding: utf-8 -*-
"""57.90.1: completa la base de versiones de los despliegues (S6-04).

La migración 57.90.0 guardó ``quimibond_sgi.deploy_versions`` contando solo
los módulos en estado «instalado». Los que se actualizaban en esa misma
corrida estaban en «por actualizar» y quedaron fuera; en producción quedaron
8 de los módulos del repositorio y faltó quimibond_sgi. Sin esta corrección,
el cron de NC habría creado una solicitud «nuevo» por cada módulo faltante,
aunque su versión no hubiera cambiado.

Aquí:

- cada módulo del repositorio que falta en la base se agrega con su versión
  instalada;
- quimibond_sgi se agrega con 19.0.57.89.0, la versión anterior al despliegue
  de 57.90.0. Así, el cron crea una sola solicitud que cubre 57.90.0 y 57.90.1.

Lo que ya estaba en la base no se toca. Solo en producción (base no
neutralizada). Idempotente.
"""
import json
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

PARAM = 'quimibond_sgi.deploy_versions'
SGI_BEFORE_5790 = '19.0.57.89.0'


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Param = env['ir.config_parameter'].sudo()
    if Param.get_param('database.is_neutralized'):
        return
    try:
        known = json.loads(Param.get_param(PARAM) or '{}')
    except ValueError:
        known = {}
    if not known:
        # Sin base: la guarda la primera corrida (ya con «por actualizar»).
        env['sgi.cron']._sgi_register_deploys()
        return
    added = []
    if 'quimibond_sgi' not in known:
        known['quimibond_sgi'] = SGI_BEFORE_5790
        added.append('quimibond_sgi')
    for name, (installed, _path) in env['sgi.cron']._sgi_repo_modules().items():
        if name not in known:
            known[name] = installed
            added.append(name)
    Param.set_param(PARAM, json.dumps(dict(sorted(known.items()))))
    _logger.info("SGI 57.90.1: base de despliegues completada con %s módulos: %s",
                 len(added), ', '.join(sorted(added)))
