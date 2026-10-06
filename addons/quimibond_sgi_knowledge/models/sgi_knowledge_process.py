# -*- coding: utf-8 -*-
"""1.2.0: ligas a Conocimiento desde la ficha del proceso y desde la tarjeta
de Mi procedimiento.

``mp_article_id`` es un campo de pantalla: no pasa por
``_sgi_mp_entry_extra`` ni por ``_sgi_my_procedure_data``, así que no entra a
la huella de Mi procedimiento ni al PDF."""
from odoo import api, fields, models
from odoo.exceptions import AccessError, UserError


class SgiProcessKnowledge(models.Model):
    _inherit = 'sgi.process'

    sgi_article_id = fields.Many2one(
        'knowledge.article', string="Artículo en Conocimiento", readonly=True, copy=False,
        help="Artículo del proceso en Conocimiento → SGI, con sus instructivos, controles operacionales y "
             "protocolos.")

    def action_sgi_open_article(self):
        """«Artículo en Conocimiento»."""
        self.ensure_one()
        if not self.sgi_article_id:
            raise UserError("Este proceso todavía no tiene artículo en Conocimiento.")
        return self.sgi_article_id._sgi_kb_open_action()


class SgiActivityRoleKnowledge(models.Model):
    _inherit = 'sgi.activity.role'

    mp_article_id = fields.Many2one(
        'knowledge.article', string="Instructivo en Conocimiento", compute='_compute_mp_article_id',
        help="El instructivo de la actividad para leerlo en Conocimiento, cuando ya está publicado.")

    @api.depends('activity_id.instruction_article_id')
    @api.depends_context('uid')
    def _compute_mp_article_id(self):
        for role in self:
            article = role.activity_id.sudo().instruction_article_id
            if article and article.sgi_publish_state in ('publicado', 'cambio'):
                try:
                    article.with_user(self.env.user).check_access('read')
                except AccessError:
                    article = article.browse()
            else:
                article = article.browse()
            role.mp_article_id = article.id or False

    def action_mp_article(self):
        """«Leer en Conocimiento»."""
        self.ensure_one()
        if not self.mp_article_id:
            raise UserError("El instructivo de esta actividad no está publicado en Conocimiento.")
        return self.mp_article_id._sgi_kb_open_action()
