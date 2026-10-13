# -*- coding: utf-8 -*-
"""19.0.57.149.0 — Escalamientos accionables y fecha de arranque del
registro de cumplimiento.

Caso de Dirección (2026-10-13): el escalamiento de la S3.22 llevaba al
catálogo y no al periodo de Irma Luna (anual 2026, venció el 31/03, creado
el 05/10/2026 al instalar el registro). Este paso, idempotente:

1. Deja el parámetro ``quimibond_sgi.exec_start_date`` (05/10/2026) si no
   existe, para que se vea y se edite en Ajustes del SGI.
2. Expone al MCP (auditoría UX 2026-10-13, fase 1) los asistentes que
   resuelven pendientes (avance, cancelar o forzar cierre de NC, aprobación,
   evaluación legal, rechazo de medición, cierre de checklist, firma) con
   alta, edición y llamada de métodos, y en lectura el registro de
   cumplimiento, Mis pendientes (también sus métodos), Mi procedimiento y el
   Diagnóstico. ``MCP_MODELS`` en models/sgi_dev_project.py.
3. Saneamiento: pone «No aplica este periodo» en los renglones «Pendiente»
   sin nota de avance que vencieron antes del arranque (los 12 anuales del
   05/10/2026), con su motivo y nota en el historial. Se puede volver a
   correr desde Registro de cumplimiento («No aplica: anteriores al
   arranque»); la segunda vez no hace nada.
"""
import logging

from odoo import SUPERUSER_ID, api

from odoo.addons.quimibond_sgi.models.sgi_activity_execution import EXEC_START_DEFAULT, EXEC_START_PARAM

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Param = env['ir.config_parameter'].sudo()
    if not Param.get_param(EXEC_START_PARAM):
        Param.set_param(EXEC_START_PARAM, EXEC_START_DEFAULT)
        _logger.info("SGI 57.149.0: %s = %s.", EXEC_START_PARAM, EXEC_START_DEFAULT)
    models = env['project.project']._sgi_dev_enable_mcp_models()
    _logger.info("SGI 57.149.0: modelos habilitados o ampliados en el MCP: %s.", models.mapped('model'))
    closed = env['sgi.activity.execution']._sgi_close_prestart()
    _logger.info("SGI 57.149.0: saneamiento, %d periodo(s) anteriores al arranque en «No aplica»: %s.",
                 len(closed), closed.ids)
