# -*- coding: utf-8 -*-
"""57.67.0: quita lo que 57.53.0 y 57.64.0 volvieron a crear con xmlids que
45.0.0 había retirado (``SGI_REMOVED_XMLIDS``, ``test_cleanup_45``).

- ``menu_sgi_deliverables`` (57.64.0, SGI → Procesos → Entregables)
- ``sgi_deliverable_action`` (57.64.0)
- ``sgi_process_flow_action`` (57.64.0, la usa el menú Flujos entre procesos)
- ``sgi_audit_finding_action`` (57.53.0, la usa el menú Hallazgos)

Los menús y acciones siguen, con xmlids nuevos (``menu_sgi_deliverable_list``,
``sgi_deliverable_list_action``, ``sgi_process_flow_list_action``,
``sgi_audit_finding_list_action``), ya cargados cuando corre este script. Aquí
solo se borra el registro que quedó con el xmlid viejo, y su xmlid. Odoo lo
borraría también al final del update (el xmlid ya no está en los datos); se
hace explícito para que el log diga qué se quitó. Producción (57.13.0) nunca
tuvo estos registros: no hay nada que borrar. Idempotente.

Además recalcula «General del modelo» (``sgi.format.map.is_general``): desde
57.67.0 los criterios leen también tipos de operación y centros de trabajo
archivados, y un mapeo con solo criterios archivados se había guardado como
general. Lo que cambie queda en el log.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

# Menú primero: apunta a la acción.
OLD_XMLIDS = (
    'menu_sgi_deliverables',
    'sgi_deliverable_action',
    'sgi_process_flow_action',
    'sgi_audit_finding_action',
)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    Data = env['ir.model.data']
    for name in OLD_XMLIDS:
        data = Data.search([('module', '=', 'quimibond_sgi'), ('name', '=', name)])
        if not data:
            continue
        record = env[data.model].browse(data.res_id).exists()
        if record:
            _logger.info("SGI 57.67.0: se quita %s (%s id %s, xmlid retirado en 45.0.0).",
                         name, data.model, record.id)
            record.unlink()
        data.unlink()

    Map = env['sgi.format.map'].with_context(active_test=False)
    maps = Map.search([])
    before = {fmap.id: fmap.is_general for fmap in maps}
    env.add_to_compute(Map._fields['is_general'], maps)
    maps.flush_recordset(['is_general'])
    for fmap in maps.filtered(lambda m: m.is_general != before[m.id]):
        _logger.info("SGI 57.67.0: mapeo de formato %s (id %s): general %s → %s.",
                     fmap.sgi_code, fmap.id, before[fmap.id], fmap.is_general)
