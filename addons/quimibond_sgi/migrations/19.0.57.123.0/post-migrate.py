# -*- coding: utf-8 -*-
"""19.0.57.123.0 — Ajustes al modelo del SGI para C1 (Jose, punto 3).

1. C1.04 se parte: C1.04b «ruta y centros de trabajo» (Diseño de Procesos),
   sin renumerar; C1.04 se queda con artículo y lista de materiales; C1.05
   recibe también la ruta. Entregable C1-RUTA ligado a C1.04b.
2. Aprobaciones de C1.02, C1.03, C1.07 y C1.11 ligadas a su botón real y
   sincronizadas si el satélite de Studio está instalado.
3. Escalamiento de segundo nivel a Dirección de Operaciones donde ejecuta
   Diseño y Desarrollo: solo si el parámetro tiene días (hoy vacío).

Idempotente. Prefijo en el log: «SGI 57.123.0»."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    lang = env.company.partner_id.lang or 'en_US'
    env = api.Environment(cr, SUPERUSER_ID, {'lang': lang})
    Activity = env['sgi.process.activity']
    new = Activity._sgi_dev_split_c1_04()
    _logger.info("SGI 57.123.0: C1.04b %s (paso %s, secuencia %s); entrega %s; C1.05 recibe %s.",
                 "creada" if new else "no creada (C1.04 no existe)", new.step or '—', new.sequence or '—',
                 ", ".join(new.output_deliverable_ids.mapped('code')) or '—',
                 ", ".join(Activity._sgi_dev_c1_activity('C1.05').input_ids.deliverable_id.mapped('code')) or '—')
    roles = Activity._sgi_dev_link_c1_approvals()
    for role in roles:
        _logger.info("SGI 57.123.0: aprobación %s · %s → %s.%s [%s]", role.activity_id.number,
                     role.job_id.name or role.family_id.name or role.relative_role,
                     role.approval_model_id.model, role.approval_method, role.approval_state)
    escalations = Activity._sgi_dev_sync_c1_escalation()
    _logger.info("SGI 57.123.0: escalamiento de segundo nivel: parámetro %s día(s); %d rol(es) creados o "
                 "actualizados en %s.", Activity._sgi_dev_escalation_days() or "vacío", len(escalations),
                 ", ".join(escalations.activity_id.mapped('number')) or '—')
