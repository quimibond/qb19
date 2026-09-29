# -*- coding: utf-8 -*-
"""N-001 (auditoría 2026-09): todo documento controlado tiene responsable.

P-I01, con credenciales, estaba abierto a todos los usuarios internos y sin
responsable: nadie lo revisaba. Desde 56.28.0:

- un documento controlado nace con responsable SGI (el que se capture o, si
  no, el Jefe MAST: `quimibond_sgi.mast_user_id` o el primer miembro
  directo del grupo);
- un documento controlado no puede quedarse sin responsable.

La migración 19.0.56.28.0 puso a Areli (mas@quimibond.com) como responsable
de los 489 que no tenían, antes de encender la regla; ella reasigna con el
tiempo (decisión de Jose, 2026-09-29).
"""
from odoo import api, models
from odoo.exceptions import ValidationError


class DocumentsDocumentOwner(models.Model):
    _inherit = 'documents.document'

    @api.model_create_multi
    def create(self, vals_list):
        default_owner = None
        for vals in vals_list:
            if vals.get('sgi_is_controlled') and not vals.get('sgi_owner_id'):
                if default_owner is None:
                    default_owner = self.env['sgi.cron']._sgi_manager_user_id() or self.env.user.id
                vals['sgi_owner_id'] = default_owner
        return super().create(vals_list)

    @api.constrains('sgi_is_controlled', 'sgi_owner_id')
    def _check_sgi_controlled_owner(self):
        for doc in self:
            if doc.sgi_is_controlled and not doc.sgi_owner_id:
                raise ValidationError(
                    "El documento controlado «%s» necesita un responsable SGI: es quien lo "
                    "revisa y quien decide quién lo puede abrir." % (doc.name or doc.sgi_code or ''))
