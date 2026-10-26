# -*- coding: utf-8 -*-
from odoo import models

from .sgi_cron import SAT_PENDING_KEYS


class SgiMyPendingSat(models.Model):
    _inherit = 'sgi.my.pending'

    def action_open(self):
        """El aviso del SAT abre la lista del SAT ya filtrada, no la ficha de
        la actividad del proceso de la que cuelga."""
        if len(self) == 1 and self.kind == 'aviso' and self.res_model == 'mail.activity' \
                and self.group_count <= 1:
            act = self.env['mail.activity'].sudo().browse(self.res_id).exists()
            if act and act.sgi_cron_kind in SAT_PENDING_KEYS:
                action = self.env['sgi.cron']._sat_pending_action(act.sgi_cron_kind)
                if action:
                    return action
        return super().action_open()
