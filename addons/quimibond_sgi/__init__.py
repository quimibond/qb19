# -*- coding: utf-8 -*-
import logging

from . import models

_logger = logging.getLogger(__name__)


def post_init_hook(env):
    """Retro-vincula los puntos de control reales del piso (que pueden existir o
    no según la instancia) a los planes de control del SGI. Búsqueda segura: si
    el equipo no está en la BD, se registra en el log y se continúa."""
    mapping = [
        ('CALIDAD Materia Prima', 'quimibond_sgi.sgi_control_plan_mp'),
        ('Revisado de Tela', 'quimibond_sgi.sgi_control_plan_revisado'),
    ]
    for team_name, plan_xmlid in mapping:
        try:
            plan = env.ref(plan_xmlid, raise_if_not_found=False)
            if not plan:
                continue
            team = env['quality.alert.team'].search([('name', '=', team_name)], limit=1)
            if not team:
                _logger.info(
                    "SGI F4: el equipo de calidad «%s» no existe en esta BD; "
                    "se omite la retro-vinculación del plan %s.", team_name, plan_xmlid)
                continue
            points = env['quality.point'].search([
                ('team_id', '=', team.id),
                ('sgi_control_plan_id', '=', False),
            ])
            if points:
                points.write({'sgi_control_plan_id': plan.id})
                _logger.info(
                    "SGI F4: %d punto(s) de «%s» vinculados al plan %s.",
                    len(points), team_name, plan.folio or plan.name)
        except Exception as exc:  # noqa: BLE001 - la retro-vinculación nunca debe romper el update
            _logger.warning(
                "SGI F4: la retro-vinculación de «%s» falló y se omitió: %s",
                team_name, exc)
