# -*- coding: utf-8 -*-
"""57.84.0 (bloque 3 de formularios 3/3, D-02; decisiones de Jose del
2026-09-30, «con script, al final del bloque 3», y del 2026-10-01 sobre
procedimientos, formularios, protocolos, anexos y reglamentos): clave nueva
con la del Dropbox como clave anterior (``documents.document._sgi_apply_d02``).

1. Patrones de tipo (solo si siguen con el de fábrica): «Control
   operacional» ``CO-{seq:02d}`` → ``CO-{proceso}-{nn}``; «Protocolo» vacío →
   ``PROT-{proceso}-{nn}``.
2. Los procedimientos del Dropbox «No aplica (se queda)» que ningún proceso
   sustituye pasan a tipo «Control operacional» con ``CO-{proceso}-{nn}``.
3. Formatos y F-IT → ``F-{proceso}-{nn}`` (consecutivo compartido),
   instructivos → ``IT-…``, DAT → ``DA-…``, protocolos → ``PROT-…``.

No renombra archivos ni toca ``sgi_previous_code`` (la del Dropbox, copiada
por 56.32.0): el buscador «Del Dropbox a Odoo», la búsqueda «Clave SGI» y
``_sgi_find_by_code`` siguen encontrando cada documento por su clave vieja,
sin límite de tiempo (C-005). Todas las revisiones de una clave reciben la
misma clave nueva, con chatter. Numeración determinista: por proceso, por
prefijo y por clave anterior. Los mapeos de formato ligados al documento
imprimen solos la clave nueva; su «Clave al ligar» se actualiza si era la
vieja. Idempotente: la segunda corrida no cambia nada.

Conservan su clave: procedimientos «En curso» (hasta que su proceso entre en
vigor y queden obsoletos), formularios de Odoo (L-004), anexos (siguen a su
documento padre), reglamentos (registrados ante la autoridad con ese
nombre), MIID, diagramas, obsoletos y P-I01 con su familia.

Esperado en producción (MCP, solo lectura, 2026-10-01, después de 57.82.0):
- 5 controles operacionales, todos de E2: P-A17 → CO-E2-01, P-A18 → CO-E2-02,
  P-A19 → CO-E2-03, P-A20 → CO-E2-04, P-S03 → CO-E2-05. Siguen como
  procedimiento los 21 «En curso», 5556 (borrador, clave inválida, C-008) y
  P-I01 (3560).
- 328 claves nuevas: C1 33, C2 21, C3 3, C4 67, C5 64, C6 6, E1 1, E2 49, S1
  11, S2 7, S3 9, S4 41, S5 16 (instructivos 42, formatos 183, F-IT 63, DAT
  35, protocolos 5: PROT-01…05 → PROT-E2-01…05). IT-C4-01 (3644) ya tenía
  clave nueva: los instructivos de C4 siguen en IT-C4-02.
- «Clave al ligar» al día en los 25 mapeos ligados a un formato o F-IT. El
  mapeo de bloqueo y etiquetado (``sgi.loto``, «P-A20», sin documento) lo
  sigue encontrando por la clave anterior: imprime CO-E2-04.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    result = env['documents.document']._sgi_apply_d02()
    _logger.info("SGI 57.84.0 (D-02): %d clave(s) nuevas; %d procedimiento(s) a control "
                 "operacional; patrones de tipo %s; %d sin cambio (motivos en el log).",
                 len(result['done']), len(result['co']), result['patterns'] or "sin cambio",
                 len(result['skipped']))
