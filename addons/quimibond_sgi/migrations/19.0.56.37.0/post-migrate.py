# -*- coding: utf-8 -*-
"""56.37.0 (auditoría 2026-09, entrega 8a, línea «avisos»; I-003 con G-004 y
G-005): los avisos de MAST que quedaron en otras bandejas.

1. ``quimibond_sgi.mast_user_id``: si no está puesto (o vale 0), se fija al
   usuario activo con login ``mas@quimibond.com`` (en producción, el 128,
   Blanca Areli Ballesteros). Se resuelve por login, no por id fijo.
2. Respaldo en la tabla ``sgi_mail_activity_bak_563700`` de cada actividad que
   se va a tocar (copia completa de la fila de ``mail_activity``).
3. ``sgi.cron._sgi_migrate_mast_notices``: las actividades de los crons que
   están con Ana Silvia Colin (67, ``control.almacen@``) y con el CEO (7,
   ``jose.mizrahi@``) pasan a quien le tocan hoy según las reglas de los
   crons, y de cada registro con el mismo resumen queda una sola; las
   repetidas se cierran con ``action_feedback`` y la nota «aviso repetido
   (migración 56.37.0)» (se archivan; nada se borra). Solo se tocan las de 67,
   7 y el destinatario; las de terceros se quedan.

Verificado en producción por MCP (solo lectura, 2026-09-29):
- ``aggregate_records mail.activity groupby [user_id, res_model]
  domain [user_id in [7, 67, 128]]``: 67 → documents.document 42,
  sgi.process 3, survey.survey 1 (46); 7 → maintenance.equipment 143,
  documents.document 15, sgi.interested.party 10, sgi.process 2, sgi.risk 2,
  sgi.indicator 1, account.move 1 (174); 128 → 6.
- ``aggregate_records mail.activity groupby [user_id] domain [summary =like
  'Procedimiento vivo cambió: %']``: 67 → 42, 7 → 15 (57) sobre 17
  documentos vigentes con ``sgi_procedure_dirty`` y sin propietario (3501,
  3507, 3509, 3520, 3534, 3536, 3538, 3540, 3551, 3560, 3563, 3567, 3570,
  3572, 3573, 3574, 3583): van a MAST.
- ``search_records mail.activity`` sobre sgi.process, sgi.interested.party,
  sgi.risk y survey.survey: eslabón atorado 45613, 45614, 45616 (67) y 45954,
  46799 (7), todos sobre procesos archivados (15, 21, 22, 11) → MAST; 45954
  repite a 45613 (proceso 15). 46789 (Ariadna, 131, mismo aviso) no se toca.
  Partes interesadas 45955-45964 (7) → MAST. Riesgos 47808 y 47810 (7): su
  proceso C4 (102) tiene dueño sin usuario (empleado 564) → MAST. DNC 45523
  (67) → RH (``quimibond_sgi.rh_user_id`` = 88).
Esperado la primera vez: 33 a MAST (17 procedimiento vivo, 10 partes
interesadas, 4 eslabones, 2 riesgos), 1 a RH y 41 cerradas (40 procedimiento
vivo, 1 eslabón). La segunda vez, 0. No se tocan aquí las 143 «Calibración
VENCIDA» (las cierra 56.38.0), ni «Conceder aprobación» (account.move, no es
del SGI) ni «Abrir mercado mexicano» (acción del SGI asignada al 7 a
propósito).
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

BACKUP = 'sgi_mail_activity_bak_563700'
# Bandejas donde quedaron los avisos de MAST (09-roles I-003).
FROM_LOGINS = ('control.almacen@quimibond.com', 'jose.mizrahi@quimibond.com')


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    Cron = env['sgi.cron']
    Param = env['ir.config_parameter'].sudo()
    raw = Param.get_param('quimibond_sgi.mast_user_id') or ''
    if not raw.isdigit() or not int(raw):
        mast = Cron._sgi_mast_user_by_login()
        if mast:
            Param.set_param('quimibond_sgi.mast_user_id', mast.id)
            _logger.info("SGI 56.37.0: quimibond_sgi.mast_user_id = %s (%s).", mast.id, mast.login)
        else:
            _logger.warning("SGI 56.37.0: no hay usuario %s; mast_user_id sin cambio.",
                            'mas@quimibond.com')
    from_users = env['res.users'].sudo().with_context(active_test=False).search(
        [('login', 'in', FROM_LOGINS)])
    if not from_users:
        _logger.info("SGI 56.37.0: no existen los usuarios de origen; nada que reasignar.")
        return
    result = Cron._sgi_migrate_mast_notices(from_users.ids, BACKUP)
    moved = result['moved']
    _logger.info(
        "SGI 56.37.0 (I-003): avisos reasignados %s; cerrados por repetidos %d (%s). "
        "Respaldo en %s. Esperado en producción la primera vez: 33 a MAST, 1 a RH, 41 cerrados.",
        {user_id: len(ids) for user_id, ids in moved.items()}, len(result['closed']),
        result['closed'], BACKUP)
