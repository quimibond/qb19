# -*- coding: utf-8 -*-
"""19.0.57.129.0 — Envío de muestra y respuesta del cliente (Jose 2026-10-08, 3.3).

- Parámetros: tipo de operación de la baja de muestras («Baja de Muestras»
  por nombre, producción 267) y puesto que avisa al cliente («Administrador
  de Ventas» por nombre, producción 207), solo si están vacíos. Los días de
  seguimiento se quedan vacíos (el brief no los define).
- Mediciones: C1.12 por el envío registrado (entregable nuevo C1-ENVIO) y
  C1.13 por la respuesta registrada (C1-RESPUESTA deja el paso por la etapa).
- Modelos nuevos expuestos por MCP (solo lectura/escritura, sin borrar).

Idempotente. Prefijo en el log: «SGI 57.129.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

PARAM_OUT_PICKING_TYPE = 'quimibond_sgi.dev_sample_out_picking_type_id'
PARAM_NOTIFY_JOB = 'quimibond_sgi.dev_shipment_notify_job_id'
NOTIFY_JOB_NAME = 'ADMINISTRADOR DE VENTAS'


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    lang = env.company.partner_id.lang or 'en_US'
    env = api.Environment(cr, SUPERUSER_ID, {'lang': lang})
    Param = env['ir.config_parameter'].sudo()
    Project = env['project.project']
    Shipment = env['sgi.dev.shipment']
    filled = []
    if not (Param.get_param(PARAM_OUT_PICKING_TYPE, '') or '').strip():
        ptype = Shipment._out_picking_type()
        if ptype:
            Param.set_param(PARAM_OUT_PICKING_TYPE, str(ptype.id))
            filled.append('baja: %s (%d)' % (ptype.display_name, ptype.id))
    if not (Param.get_param(PARAM_NOTIFY_JOB, '') or '').strip():
        jobs = Project._sgi_dev_search_langs('hr.job', [('name', 'ilike', NOTIFY_JOB_NAME)])
        job = (jobs.filtered('active') or jobs)[:1]
        if job:
            Param.set_param(PARAM_NOTIFY_JOB, str(job.id))
            filled.append('aviso: %s (%d)' % (job.name, job.id))
    report = Project._sgi_dev_apply_c1_measures()
    mcp = Project._sgi_dev_enable_mcp_models()
    _logger.info("SGI 57.129.0: parámetros %s; mediciones %s; MCP %s.",
                 "; ".join(filled) or 'ya estaban', report, mcp.mapped('model') or '—')
