# -*- coding: utf-8 -*-
"""56.31.0 (auditoría 2026-09, L-005 que funde C-003; entrega 3 «documentos»,
bloque 2): estado de migración de los procedimientos según la decisión de
Jose (decisiones.md, «Estado de migración de los procedimientos (C-003)»):

- los 23 sustituidos (con ``sgi_replaced_by_process_id``) → «En curso» (pasan
  a «Baja tramitada» cuando su proceso entre en vigor);
- los 21 con pendientes (entre ellos los 13 que se quedan vacíos a
  propósito) → «En curso»;
- los 5 de control operacional (P-A17, P-A18, P-A19, P-A20, P-S03) → «No
  aplica (se queda)»;
- P-I01 no se toca: se retira aparte (credenciales).

Verificado en producción por MCP (solo lectura, 2026-09-29):
``search_records documents.document [sgi_doc_type = procedimiento,
sgi_is_controlled = True]`` → 52: 50 vigentes en «migrado» (23 con proceso
que los sustituye, 27 sin él, P-I01 incluido), 5119 (P-A28 obsoleto, «baja»)
y 5556 (borrador, «pendiente»). Esperado: 49 filas (44 «en_curso» y 5 «na»)
la primera vez y 0 la segunda. Solo cambia lo que dice «migrado», por SQL y
sin chatter.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

# Decisión fechada de Jose (2026-09-29), no estructura del módulo.
CONTROL_OPERACIONAL = ('P-A17', 'P-A18', 'P-A19', 'P-A20', 'P-S03')
APARTE = ('P-I01',)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    result = env['documents.document']._sgi_migrate_procedure_states(
        na_codes=CONTROL_OPERACIONAL, skip_codes=APARTE)
    _logger.info("SGI 56.31.0 (L-005): procedimientos de «migrado» a «en_curso» %d y a "
                 "«na» %d (esperado en producción: 44 y 5; P-I01 sin tocar).",
                 result.get('en_curso', 0), result.get('na', 0))
