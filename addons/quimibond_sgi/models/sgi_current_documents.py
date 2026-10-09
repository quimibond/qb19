# -*- coding: utf-8 -*-
"""Inicio → Documentos vigentes (I-015 / D-21, entrega 4).

El Usuario SGI no tenía dentro del SGI una lista de los documentos vigentes:
Documentos, Lista maestra y Documentos externos viven en Sistema → Documentos
(antes Administración SGI), solo para Auditor, Jefe MAST y Dirección.
Esta entrada le muestra solo título y revisión de los documentos controlados
vigentes (decisión 11 de la tanda 2: sin la clave del Dropbox) y, en el mismo
renglón, su acuse de lectura pendiente para firmarlo ahí (agregado de Jose a
D-21). El acuse es el de siempre: sgi.document.ack.action_mark_read, con su
candado de identidad.
"""
from odoo import api, fields, models
from odoo.exceptions import UserError

ACK_STATES = [
    ('pendiente', "Por leer"),
    ('leido', "Leído"),
    ('no_aplica', "Sin acuse"),
]


class DocumentsDocumentCurrent(models.Model):
    _inherit = 'documents.document'

    sgi_my_ack_state = fields.Selection(
        ACK_STATES, string="Mi acuse", compute='_compute_sgi_my_ack_state',
        search='_search_sgi_my_ack_state',
        help="Su acuse de lectura de este documento: «Por leer» si le toca "
             "firmarlo, «Leído» si ya lo firmó y «Sin acuse» si no aplica a su puesto.")

    def _sgi_my_acks(self):
        """Acuses del usuario actual (por su empleado) de estos documentos."""
        return self.env['sgi.document.ack'].sudo().search([
            ('document_id', 'in', self.ids), ('user_id', '=', self.env.uid)])

    @api.depends_context('uid')
    def _compute_sgi_my_ack_state(self):
        by_doc = {}
        for ack in self._sgi_my_acks():
            # Un pendiente manda sobre un leído (hay un acuse por empleado).
            if by_doc.get(ack.document_id.id) != 'pendiente':
                by_doc[ack.document_id.id] = ack.state
        for doc in self:
            doc.sgi_my_ack_state = by_doc.get(doc.id, 'no_aplica')

    def _search_sgi_my_ack_state(self, operator, value):
        if operator not in ('=', '!=', 'in', 'not in'):
            raise UserError("Búsqueda no soportada en «Mi acuse».")
        states = {value} if isinstance(value, str) else set(value or [])
        if operator in ('!=', 'not in'):
            states = {key for key, _label in ACK_STATES} - states
        acks = self.env['sgi.document.ack'].sudo().search([('user_id', '=', self.env.uid)])
        pending = set(acks.filtered(lambda a: a.state == 'pendiente').document_id.ids)
        read = set(acks.document_id.ids) - pending
        ids = set()
        if 'pendiente' in states:
            ids |= pending
        if 'leido' in states:
            ids |= read
        if 'no_aplica' in states:
            return ['|', ('id', 'in', list(ids)), ('id', 'not in', list(pending | read))]
        return [('id', 'in', list(ids))]

    def action_sgi_mark_my_ack_read(self):
        """Firma MI acuse pendiente de este documento (Documentos vigentes)."""
        acks = self.env['sgi.document.ack'].search([
            ('document_id', 'in', self.ids), ('user_id', '=', self.env.uid),
            ('state', '=', 'pendiente')])
        if not acks:
            raise UserError("No tiene acuse de lectura pendiente de este documento.")
        acks.action_mark_read()
        return True
