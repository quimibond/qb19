# -*- coding: utf-8 -*-
"""19.0.57.151.0 — La secuencia del folio FT no va atrás de los folios que ya existen.

Dirección General 2026-10-13: la secuencia ``sgi.dev.ft`` (un rango por año)
creó el rango 2026 en 1 cuando los folios FT-001-2026 a FT-039-2026 se
habían capturado a mano, y asignó FT-001-2026 al proyecto 499, que ya era del
380. Jose corrigió el 499 a FT-040-2026 a mano.

Este paso pone cada rango de la secuencia en el folio más alto que ya existe
en su año + 1 (proyectos archivados incluidos), solo si va atrás; nunca lo
regresa. En producción el rango 2026 queda en 41. No toca ningún proyecto ni
ningún folio. Idempotente: deja una línea en el log por rango."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

TAG = "57.151.0"


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    Project = env['project.project'].with_context(active_test=False)
    sequences = env['ir.sequence'].search([('code', '=', 'sgi.dev.ft')])
    if not sequences:
        _logger.info("%s: no hay secuencia sgi.dev.ft; nada que ajustar.", TAG)
        return
    for sequence in sequences:
        companies = sequence.company_id or env['res.company'].search([])
        for date_range in sequence.date_range_ids.sorted('date_from'):
            year = date_range.date_from.year
            top = max((Project._sgi_dev_ft_max_number(year, company) for company in companies), default=0)
            before = date_range.number_next_actual
            if before <= top:
                date_range.number_next_actual = top + 1
                _logger.info("%s: secuencia sgi.dev.ft (id %s), rango %s: iba en %s, el folio más alto es "
                             "FT-%03d-%s; pasa a %s.", TAG, sequence.id, year, before, top, year, top + 1)
            else:
                _logger.info("%s: secuencia sgi.dev.ft (id %s), rango %s: va en %s y el folio más alto es "
                             "%s; sin cambio.", TAG, sequence.id, year, before, top)
