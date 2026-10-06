# -*- coding: utf-8 -*-
"""56.32.0 (auditoría 2026-09, C-004 + L-004, C-005, C-007; entrega 3
«documentos», bloque 3). Corre después de 56.30.0 (C-006: los pies de formato
ya están ligados al documento, decisión 4 de Jose).

1. Clave anterior (C-004 + L-004): copia ``sgi_code`` → ``sgi_previous_code``
   en los documentos controlados del Dropbox, activos o archivados, salvo
   «Mi procedimiento» y externos, solo donde la clave anterior está vacía. Por
   SQL, sin chatter; no toca ``sgi_code`` ni el nombre. Fuera: 5556 (clave
   inválida «P-A14 MEDIO AMBIENTE…», se archiva aparte, C-008).
   Los 64 «Formulario de Odoo (vista)» SÍ entran (L-004): son formatos del
   Dropbox ya migrados y conservan su clave vieja (D-02).

   Verificado en producción por MCP (solo lectura, 2026-09-29):
   ``aggregate_records documents.document groupby [sgi_doc_type_id, active]
   domain [sgi_is_controlled = True, sgi_code != False, sgi_previous_code =
   False, active in (True, False), sgi_doc_type_id not in (14 externo, 16 Mi
   procedimiento), id != 5556]`` → **490**, todos activos: MIID 1,
   procedimiento 51, instructivo 48, formato 193, F-IT 65, DAT 42, PROT 5, DF
   1, R 5, anexo 15, formulario de Odoo 64 (= 426 + 64). Ninguno sin tipo.
   Esperado: 490 la primera vez, 0 la segunda.

2. Nomenclatura nueva (C-007, D-02): en ``sgi.document.type`` (noupdate, así
   que el XML no llega a producción) procedimiento pasa a ``PR-{process}`` y
   se vuelven a encender «Exige clave» y «Exige proceso» en procedimiento y
   formato; F-IT toma ``F-{process}-{seq:02d}`` (formato de su proceso) y DAT
   ``DA-{process}-{seq:02d}``. Producción el 2026-09-29: procedimiento
   ``P-{process}`` sin exigir clave ni proceso, formato sin exigir ninguno,
   F-IT y DAT sin patrón. Las claves heredadas siguen aceptándose en los
   documentos que ya las tienen (``legacy_code_regex``); solo 5556 no la
   cumple (verificado: procedimientos y formatos cuya clave no es P-___ ni
   F-P-___-__ → solo 5556).
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

# Decisión fechada (C-008): clave inválida, fuera de la migración.
EXCLUDED_IDS = (5556,)

TYPE_VALUES = {
    'procedimiento': {'prefix_pattern': 'PR-{process}', 'name': 'Procedimiento (PR)',
                      'code_required': True, 'requires_process': True},
    'formato': {'prefix_pattern': 'F-{process}-{seq:02d}',
                'code_required': True, 'requires_process': True},
}
# Solo si siguen sin patrón (no pisa lo que MAST haya puesto).
TYPE_PATTERN_IF_EMPTY = {
    'formato_it': 'F-{process}-{seq:02d}',
    'dat': 'DA-{process}-{seq:02d}',
}


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {'active_test': False})
    Type = env['sgi.document.type']
    for code, values in TYPE_VALUES.items():
        for dtype in Type.search([('code', '=', code)]):
            changed = {k: v for k, v in values.items() if dtype[k] != v}
            if changed:
                dtype.write(changed)
                _logger.info("SGI 56.32.0 (C-007): tipo %s → %s", code, changed)
    for code, pattern in TYPE_PATTERN_IF_EMPTY.items():
        for dtype in Type.search([('code', '=', code), ('prefix_pattern', '=', False)]):
            dtype.write({'prefix_pattern': pattern})
            _logger.info("SGI 56.32.0 (C-007): tipo %s con patrón %s", code, pattern)

    count = env['documents.document']._sgi_migrate_previous_codes(exclude_ids=EXCLUDED_IDS)
    env.flush_all()
    _logger.info("SGI 56.32.0 (C-004/L-004): clave del Dropbox copiada a «Clave anterior» en "
                 "%d documento(s) (esperado en producción: 490 la primera vez, 0 después).",
                 count)
