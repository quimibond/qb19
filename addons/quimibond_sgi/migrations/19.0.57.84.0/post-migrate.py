# -*- coding: utf-8 -*-
"""57.84.0 (bloque 3 de formularios 3/3, D-02; decisión de Jose del
2026-09-30: «con script, al final del bloque 3»): clave nueva
``F-{proceso}-{nn}``, ``IT-{proceso}-{nn}`` y ``DA-{proceso}-{nn}`` con la
del Dropbox como clave anterior (``documents.document._sgi_apply_d02``).

No renombra archivos ni toca ``sgi_previous_code`` (la del Dropbox, copiada
por 56.32.0): el buscador «Del Dropbox a Odoo», la búsqueda «Clave SGI» y
``_sgi_find_by_code`` siguen encontrando cada documento por su clave vieja,
sin límite de tiempo (C-005). Todas las revisiones de una clave reciben la
misma clave nueva, con chatter. Numeración determinista: por proceso, por
prefijo y por clave anterior; F y F-IT comparten el consecutivo
``F-{proceso}-{nn}``. Los mapeos de formato ligados al documento imprimen
solos la clave nueva; su «Clave al ligar» se actualiza si era la vieja.
Idempotente: la segunda corrida no cambia nada.

Fuera: formularios de Odoo (L-004, conservan su clave), procedimientos
(pregunta abierta: ``PR-{proceso}`` no lleva consecutivo y hay hasta 13
procedimientos del Dropbox por proceso), tipos sin patrón (anexo, protocolo,
reglamento, MIID, diagrama), obsoletos y P-I01 con su familia.

Esperado en producción (MCP, solo lectura, 2026-10-01, después de 57.82.0):
323 claves nuevas: C1 33, C2 21, C3 3, C4 67, C5 64, C6 6, E1 1, E2 44, S1 11,
S2 7, S3 9, S4 41, S5 16 (instructivos 42, formatos 183, F-IT 63, DAT 35).
IT-C4-01 (3644) ya tenía clave nueva: los instructivos de C4 siguen en
IT-C4-02. «Clave al ligar» al día en los ~25 mapeos ligados a un formato o
F-IT.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    result = env['documents.document']._sgi_apply_d02()
    _logger.info("SGI 57.84.0 (D-02): %d clave(s) nuevas; %d sin cambio (motivos en el log).",
                 len(result['done']), len(result['skipped']))
