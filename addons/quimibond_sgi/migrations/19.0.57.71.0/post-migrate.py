# -*- coding: utf-8 -*-
"""57.71.0 (bloque 3 de formularios 2/3, propuesta de formatos §2, decisión
de Jose del 2026-09-30): el responsable SGI de cada formato vigente es el
usuario del dueño de su proceso (``documents.document._sgi_owner_from_process``).

Corre después de 57.70.0 (fusiones): los formatos dados de baja ya no
cuentan. Solo toca los formatos (formato, F-IT, DAT, anexo y formulario de
Odoo) vigentes o en piloto que hoy tiene el Jefe MAST (custodio de 56.28.0);
lo que alguien ya reasignó se respeta. Si el dueño no tiene usuario activo e
interno, el formato se queda con MAST y sale en el log (la lista para Jose
está en ``docs/sgi/transicion/formatos-bloque-3.md``). Fuera P-I01 y su
familia. Idempotente. Nada se borra.

Esperado en producción (MCP, solo lectura, 2026-10-01, después de 57.70.0):
238 formatos cambian de Blanca Ballesteros (128) a su dueño: C1 32 y C2 31
(con F-P-A28-13) → Jessica Francisco (22); C3 7 → Paris Villordo (6); C5 60
→ Oscar González (33); C6 10 → Cynthia Santana (15); E1 5 y S1 14 → Jorge
Manuel Ortiz (35); S2 7 y S3 9 → Irma Luna (68); S4 45 → Miguel Medina (88);
S5 18 → Manuel Juárez (135). Se quedan con MAST: E2 70 (la dueña es MAST;
incluye F-P-A28-11) y C4 64 (Francisco González, empleado 564, sin usuario):
54 en el log y 10 de P-I01, que la migración no toca. La familia P-A13 (7
en S2) no se movió a S4: sus formatos son reportes administrativos
(anticipos, compras, facturación, inventario, importaciones), no solo de RH;
queda para MAST.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    result = env['documents.document']._sgi_owner_from_process()
    _logger.info("SGI 57.71.0 (responsables): cambian %d formato(s) (%s); se quedan con MAST por "
                 "dueño sin usuario %d (%s) y por ser de MAST %d (%s).",
                 sum(len(ids) for _user, ids in result['changed'].values()),
                 ", ".join(sorted(result['changed'])) or "ninguno",
                 sum(len(ids) for ids in result['no_user'].values()),
                 ", ".join(sorted(result['no_user'])) or "ninguno",
                 sum(len(ids) for ids in result['mast'].values()),
                 ", ".join(sorted(result['mast'])) or "ninguno")
