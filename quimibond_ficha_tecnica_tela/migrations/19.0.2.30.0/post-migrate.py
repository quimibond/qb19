# -*- coding: utf-8 -*-
"""2.30.0 (auditoría del 10-nov-2026): los parámetros de banda de los jets se dieron de alta en producción a mano con
el nombre «Capacidad grupo A (rendimiento 3 a 6)», que contradice la regla (A de 3 a menos de 7 m/kg). Se les pone el
nombre de ``PARAMETROS_DEL_CALCULO``; la clave, los valores y los cálculos no cambian."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    from odoo.addons.quimibond_ficha_tecnica_tela.models.maquina_config import FichaTecnicaMaquinaTipo
    env = api.Environment(cr, SUPERUSER_ID, {})
    Param = env['ficha.tecnica.maquina.parametro'].with_context(active_test=False)
    for tipo_code, code, name, _unit, _kind, _ambito in FichaTecnicaMaquinaTipo.PARAMETROS_DEL_CALCULO:
        if not code.startswith('capacidad_grupo_'):
            continue
        for param in Param.search([('tipo_id.code', '=', tipo_code), ('code', '=', code)]):
            if param.name != name:
                _logger.info('fichas 2.30.0: parámetro %s «%s» → «%s»', code, param.name, name)
                param.name = name
