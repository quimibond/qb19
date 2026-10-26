# -*- coding: utf-8 -*-
"""Folio fiscal en la factura de proveedor cuando una persona liga el CFDI.

El cruce por UUID ya encuentra el folio en la factura; el que liga una
persona (asistente Conciliar, sugerencia aceptada) solo quedaba en
``sat.cfdi`` y la factura seguía sin folio (o con el de otro CFDI, si el XML
estaba cruzado), y de ahí lo toman la DIOT y la contabilidad electrónica.

Se hace como lo hace Odoo al subir el XML de un proveedor: un documento CFDI
(``l10n_mx_edi.document``, estado "Recibido") con el XML del SAT adjunto; el
folio del asiento (``l10n_mx_edi_cfdi_uuid``) se calcula de ahí. Si la factura
ya tenía el documento de otro CFDI, se quita (el XML viejo se queda en el
chatter) y se deja constancia. Solo facturas de proveedor: las de cliente las
timbra Odoo. Sin l10n_mx_edi no hace nada.
"""
import logging

from odoo import Command, _, api, fields, models

_logger = logging.getLogger(__name__)


class SatCfdiFolio(models.Model):
    _inherit = 'sat.cfdi'

    @api.model
    def _has_mx_edi(self):
        return 'l10n_mx_edi.document' in self.env

    def _xml_attachment_on(self, move):
        """El XML de este CFDI adjunto a la factura (lo crea si falta)."""
        self.ensure_one()
        name = '%s.xml' % (self.uuid or '').upper()
        Attachment = self.env['ir.attachment'].sudo()
        attachment = Attachment.search([('res_model', '=', 'account.move'), ('res_id', '=', move.id),
                                        ('name', '=ilike', name)], limit=1)
        if attachment:
            return attachment
        self._attach_xml_to_move(move)
        return Attachment.search([('res_model', '=', 'account.move'), ('res_id', '=', move.id),
                                  ('name', '=ilike', name)], limit=1)

    def _write_fiscal_folio(self, move=None):
        """Deja el folio fiscal de este CFDI en su factura de proveedor.
        Devuelve un texto con lo que pasó; nunca revienta."""
        self.ensure_one()
        move = move or self.move_id
        if not self._has_mx_edi():
            return _('sin localización mexicana: el folio no se escribe en la factura')
        if not move or move.move_type not in ('in_invoice', 'in_refund') or self.direction != 'received':
            return _('solo facturas de proveedor')
        Doc = self.env['l10n_mx_edi.document'].sudo()
        docs = Doc.search([('move_id', '=', move.id), ('state', '=', 'invoice_received')])
        mine = docs.filtered(lambda d: (d.attachment_uuid or '').lower() == (self.uuid or '').lower())
        if mine:
            return _('la factura ya tenía este folio fiscal')
        try:
            attachment = self._xml_attachment_on(move)
            if not attachment:
                return _('el XML no está disponible en Syntage: el folio no se escribió')
            old = docs.mapped('attachment_uuid')
            with self.env.cr.savepoint():
                # El documento del CFDI equivocado se quita; su XML se queda en el chatter.
                docs.unlink()
                Doc.create({
                    'move_id': move.id,
                    'invoice_ids': [Command.set(move.ids)],
                    'state': 'invoice_received',
                    'sat_state': 'cancelled' if self.estado_sat == 'cancelado' else 'valid',
                    'datetime': fields.Datetime.now(),
                    'attachment_id': attachment.id,
                })
        except Exception as exc:  # noqa: BLE001 — la liga ya está grabada
            _logger.exception('sat.cfdi %s: no se pudo escribir el folio en %s', self.uuid, move.name)
            return _('el folio no se escribió (%s)') % exc
        if old:
            body = _('Folio fiscal cambiado de %(old)s a %(new)s (CFDI del SAT conciliado).') % {
                'old': ', '.join(u for u in old if u), 'new': (self.uuid or '').upper()}
        else:
            body = _('Folio fiscal %s registrado desde el CFDI del SAT.') % (self.uuid or '').upper()
        move.sudo().with_context(disable_attachment_import=True).message_post(
            body=body, message_type='comment', subtype_xmlid='mail.mt_note')
        return body

    def _reconcile_with(self, move, attach_xml=True):
        body = super()._reconcile_with(move, attach_xml=attach_xml)
        if attach_xml:
            self._write_fiscal_folio(move)
        return body

    def action_accept_suggestion(self):
        targets = [(rec, rec.suggested_move_id) for rec in self if rec.suggested_move_id]
        res = super().action_accept_suggestion()
        for rec, move in targets:
            if rec.move_id == move:
                rec._write_fiscal_folio(move)
        return res

    def action_write_fiscal_folio(self):
        """Botón (y llamada por MCP para las ligas que ya existen): escribe el
        folio de los CFDI ligados a facturas de proveedor y avisa qué pasó."""
        lines = ['%s: %s' % (rec.name, rec._write_fiscal_folio())
                 for rec in self.filtered(lambda c: c.move_id and c.direction == 'received')]
        return {
            'type': 'ir.actions.client', 'tag': 'display_notification',
            'params': {'title': _('Folio fiscal en la factura'), 'message': '\n'.join(lines) or _('Nada que hacer'),
                       'sticky': False},
        }
