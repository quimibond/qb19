# -*- coding: utf-8 -*-
"""19.0.57.100.0 (K-04): las mediciones ya validadas guardan las metas de hoy
(las mismas con las que hoy se pintan), para que un cambio de meta posterior
no las recoloree. La fecha guardada es la del despliegue. No cambia ningún
color: se comprueba y se registra. Idempotente (solo toma las validadas que
aún no tienen metas guardadas); no toca ningún otro dato."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Measure = env['sgi.indicator.measure'].with_context(active_test=False)
    todo = Measure.search([('state', '=', 'validado'), ('sgi_targets_frozen', '=', False)],
                          order='id')
    before = {measure.id: measure.semaphore for measure in todo}
    todo._sgi_freeze_targets()
    env.flush_all()
    todo.invalidate_recordset(['semaphore'])
    changed = [measure.id for measure in todo if measure.semaphore != before[measure.id]]
    _logger.info("SGI 57.100.0 (K-04): %d medición(es) validada(s) con metas congeladas "
                 "(ids %s); %d cambiaron de color.", len(todo), todo.ids, len(changed))
    if changed:
        _logger.warning("SGI 57.100.0 (K-04): estas mediciones cambiaron de color al guardar "
                        "sus metas: %s. Revisar con el Jefe MAST.", changed)
