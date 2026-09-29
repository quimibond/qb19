# -*- coding: utf-8 -*-
"""56.27.0: archivar una regla de aprobación cierra sus avisos abiertos.

Cuando una regla de aprobación de Studio (`studio.approval.rule`, cualquiera,
no solo las del SGI) pasa de activa a archivada, las actividades que avisaban
a los aprobadores (`studio.approval.request.mail_activity_id`) se quedaban
vivas: seguían en el reloj de actividades y en Mis pendientes de cada quien
aunque ya nadie tuviera que aprobar nada.

Ahora se marcan como **hechas** con la nota «Regla de aprobación archivada el
AAAA-MM-DD por <usuario>; ya no se requiere esta aprobación.», sin aprobar ni
rechazar (no se crea ningún `studio.approval.entry`). En Odoo 19 marcar hecha
una actividad la ARCHIVA (no la borra) y publica en el chatter del documento
el mensaje «actividad hecha» con la nota; si la actividad no está sobre el
documento, la nota se publica aparte en el documento.

La `studio.approval.request` se borra después: web_studio la mantiene solo
mientras la aprobación está pendiente (en producción no hay ni una solicitud
con su actividad archivada: al aprobar, Studio borra actividad y solicitud),
y una solicitud huérfana haría que, si la regla se reactiva, Studio crea que
ya avisó y no vuelva a avisar en ese documento. La actividad archivada queda
como historia. Reactivar la regla no recrea nada: Studio vuelve a avisar
cuando alguien pulse otra vez el botón.
"""
import logging

from odoo import fields, models

_logger = logging.getLogger(__name__)


class StudioApprovalRuleArchive(models.Model):
    _inherit = 'studio.approval.rule'

    def write(self, vals):
        archiving = self.browse()
        if 'active' in vals and not vals['active']:
            archiving = self.filtered('active')
        res = super().write(vals)
        if archiving:
            archiving._sgi_close_open_requests()
        return res

    def _sgi_archive_note(self):
        return "Regla de aprobación archivada el %s por %s; ya no se requiere esta aprobación." % (
            fields.Date.context_today(self).isoformat(), self.env.user.name)

    def _sgi_close_open_requests(self):
        """Marca hechas (sin aprobar ni rechazar) las actividades de las
        solicitudes pendientes de estas reglas y borra las solicitudes.

        sudo(): quien archiva es administrador de Studio, pero las actividades
        son de otros usuarios (los aprobadores) y las solicitudes solo las
        escribe Studio en sudo. El autor del mensaje sigue siendo quien
        archiva: sudo() no cambia el usuario."""
        if not self or 'studio.approval.request' not in self.env:
            return
        requests = self.env['studio.approval.request'].sudo().search([('rule_id', 'in', self.ids)])
        if not requests:
            return
        note = self._sgi_archive_note()
        noted = set()  # (modelo, id) de los documentos que ya llevan la nota en el chatter
        for activity in requests.mail_activity_id.filtered('active'):
            # action_feedback: el «Marcar como hecho» nativo de Odoo 19 (archiva
            # la actividad, guarda la nota en `feedback` y la publica en el
            # documento). Una por una para saber en qué documento quedó.
            message_id = activity.action_feedback(feedback=note)
            if message_id:
                message = self.env['mail.message'].sudo().browse(message_id)
                noted.add((message.model, message.res_id))
        for request in requests:
            rule = request.rule_id
            key = (rule.model_name, request.res_id)
            if key in noted or not rule.model_name or rule.model_name not in self.env:
                continue
            noted.add(key)
            record = self.env[rule.model_name].sudo().browse(request.res_id).exists()
            if record and hasattr(record, 'message_post'):
                record.message_post(body=note, message_type='comment',
                                    subtype_xmlid='mail.mt_note',
                                    author_id=self.env.user.partner_id.id)
        _logger.info("SGI: reglas de aprobación %s archivadas; %s solicitudes cerradas",
                     self.ids, len(requests))
        requests.unlink()
