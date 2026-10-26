# -*- coding: utf-8 -*-
"""Factura de proveedor en borrador a partir del XML del SAT.

Un CFDI recibido que no tiene factura en Odoo (ni candidata del mismo RFC y
monto, publicada o en borrador) se vuelve una factura en borrador con el
importador de XML de Odoo (el decodificador de CFDI lo pone l10n_mx_edi): así
nace con su folio fiscal, el proveedor y los importes exactos del CFDI, y la
persona solo la revisa y la publica. Nunca publica.

Se enciende por compañía con el diario donde se crean
(``sat_auto_bill_journal_id``). Cron diario sobre los CFDI de hace
``quimibond_sat.auto_bill_min_days`` (default 3: tiempo para que se capturen
por el camino normal) a ``quimibond_sat.auto_bill_max_days`` días (default 45).

El importador de Odoo hace ``commit`` (``rollbackable_transaction``): por eso
cada CFDI se procesa completo y se confirma antes de pasar al siguiente, y no
se usa savepoint alrededor de la importación.
"""
import logging
from datetime import timedelta

import psycopg2

from odoo import _, api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)


class SatCfdiFactura(models.Model):
    _inherit = 'sat.cfdi'

    auto_bill_state = fields.Selection([
        ('created', 'Factura creada desde el XML'),
        ('error', 'No se pudo crear la factura'),
    ], string='Factura desde el XML', copy=False, index=True)
    auto_bill_note = fields.Char(string='Factura desde el XML: detalle', copy=False)

    AUTO_BILL_CANDIDATE_DAYS = 45

    def _auto_bill_blocker(self):
        """Por qué este CFDI no se vuelve factura sola (texto), o False."""
        self.ensure_one()
        if self.direction != 'received' or self.tipo not in ('I', 'E'):
            return _('solo CFDI recibidos de ingreso o egreso')
        if self.estado_sat != 'vigente':
            return _('el CFDI no está vigente en el SAT')
        if self.move_id or self.match_status != 'solo_sat':
            return _('el CFDI ya está ligado a una factura o ignorado')
        if abs(self.total) < 1:
            return _('total menor a $1')
        policy = self.partner_id.commercial_partner_id.sat_cfdi_policy
        if self._is_poliza() or (self.partner_id and policy != 'factura'):
            return _('el contacto no se registra como factura')
        if self.suggested_move_id:
            return _('hay una factura sugerida en Odoo: acéptela o recházela')
        # Publicada o en borrador, mismo RFC y monto: probablemente ya se capturó.
        if self._reconcile_candidates(days=self.AUTO_BILL_CANDIDATE_DAYS):
            return _('hay una factura de Odoo del mismo proveedor y monto: concílielas')
        return False

    def _has_cfdi_decoder(self, move_type, attachment):
        """¿Odoo tiene un decodificador para este XML? Sin l10n_mx_edi no lo
        hay y el importador dejaría una factura vacía. Si la pregunta misma
        falla, se intenta la importación y se revisa el resultado."""
        Move = self.env['account.move'].with_company(self.company_id).with_context(default_move_type=move_type)
        try:
            files_data = Move._to_files_data(attachment)
            infos = [Move._get_edi_decoder(file_data, new=True) for file_data in files_data]
        except Exception:  # noqa: BLE001 — se decide con el resultado de la importación
            _logger.warning('sat.cfdi %s: no se pudo consultar el decodificador', self.uuid, exc_info=True)
            return True
        return any(info and info.get('priority', 0) for info in infos)

    def _create_bill_from_xml(self, journal):
        """Crea la factura de proveedor en borrador desde el XML del SAT y la
        liga al CFDI. Devuelve la factura; UserError si no se pudo."""
        self.ensure_one()
        blocker = self._auto_bill_blocker()
        if blocker:
            raise UserError(_('No se crea la factura del CFDI %(uuid)s: %(why)s.') % {
                'uuid': self.uuid, 'why': blocker})
        content = self._fetch_xml()
        move_type = 'in_refund' if self.tipo == 'E' else 'in_invoice'
        attachment = self.env['ir.attachment'].create({
            'name': '%s.xml' % (self.uuid or '').upper(), 'raw': content, 'mimetype': 'application/xml'})
        if not self._has_cfdi_decoder(move_type, attachment):
            attachment.unlink()
            raise UserError(_('Odoo no tiene con qué leer el XML del CFDI (falta la localización mexicana, '
                              'l10n_mx_edi).'))
        Move = self.env['account.move'].with_company(self.company_id).with_context(
            default_move_type=move_type, default_journal_id=journal.id)
        moves = Move._create_records_from_attachments(attachment)
        move = moves[:1]
        if not move.invoice_line_ids:
            moves.filtered(lambda m: m.state == 'draft').unlink()
            raise UserError(_('El importador de Odoo no pudo leer el XML del CFDI %s.') % self.uuid)
        if move.state != 'draft':
            # Nunca se publica sola: Contabilidad la revisa.
            move.button_draft()
        self._match_move()
        if self.move_id != move:
            self.write({'move_id': move.id, 'match_method': 'conciliado', 'match_status': 'matched'})
            # El importador no dejó el documento CFDI: el folio se escribe aparte.
            self._write_fiscal_folio(move)
        self.write({
            'auto_bill_state': 'created',
            'auto_bill_note': _('Factura creada en borrador desde el XML del SAT'),
            'note': _('Factura creada en borrador desde el XML del SAT'),
        })
        move.message_post(
            body=_('Factura creada en borrador desde el CFDI del SAT (Syntage) %(uuid)s, total %(total)s %(cur)s. '
                   'Revísela y publíquela.') % {
                'uuid': (self.uuid or '').upper(), 'total': '{:,.2f}'.format(self.total or 0.0),
                'cur': self.moneda or 'MXN'},
            message_type='comment', subtype_xmlid='mail.mt_note')
        return move

    @api.model
    def _auto_bill_journal(self, company):
        return company.sat_auto_bill_journal_id or self.env['account.journal'].search([
            ('type', '=', 'purchase'), ('company_id', '=', company.id)], order='sequence, id', limit=1)

    def action_create_bill(self):
        """Botón del CFDI: crea su factura en borrador y la abre."""
        self.ensure_one()
        journal = self._auto_bill_journal(self.company_id)
        if not journal:
            raise UserError(_('La compañía %s no tiene diario de compras.') % self.company_id.name)
        move = self._create_bill_from_xml(journal)
        return {'type': 'ir.actions.act_window', 'res_model': 'account.move', 'res_id': move.id,
                'view_mode': 'form', 'target': 'current'}

    @api.model
    def _cron_auto_bills(self):
        """Crea en borrador las facturas de los CFDI recibidos que no están en
        Odoo, en las compañías con diario configurado. Devuelve cuántas."""
        icp = self.env['ir.config_parameter'].sudo()
        min_days = int(icp.get_param('quimibond_sat.auto_bill_min_days') or 3)
        max_days = int(icp.get_param('quimibond_sat.auto_bill_max_days') or 45)
        limit = int(icp.get_param('quimibond_sat.auto_bill_limit') or 50)
        client = self.env['sat.syntage.client']
        now = fields.Datetime.now()
        created = 0
        companies = self.env['res.company'].sudo().search([
            ('sat_sync_enabled', '=', True), ('sat_auto_bill_journal_id', '!=', False)])
        for company in companies:
            domain = [
                ('company_id', '=', company.id), ('direction', '=', 'received'), ('tipo', 'in', ('I', 'E')),
                ('estado_sat', '=', 'vigente'), ('match_status', '=', 'solo_sat'), ('auto_bill_state', '=', False),
                ('fecha_emision', '<=', now - timedelta(days=min_days)),
                ('fecha_emision', '>=', now - timedelta(days=max_days)),
            ]
            if company.sat_data_since:
                domain.append(('fecha_emision', '>=', fields.Datetime.to_datetime(company.sat_data_since)))
            for cfdi in self.sudo().search(domain, order='fecha_emision, id', limit=limit):
                blocker = cfdi._auto_bill_blocker()
                if blocker:
                    if cfdi.auto_bill_note != blocker:
                        cfdi.write({'auto_bill_note': blocker})
                    continue
                try:
                    cfdi._create_bill_from_xml(company.sat_auto_bill_journal_id)
                    created += 1
                except Exception as exc:  # noqa: BLE001 — un CFDI malo no detiene a los demás
                    if isinstance(exc, psycopg2.Error):
                        self.env.cr.rollback()
                    _logger.warning('sat.cfdi %s: factura desde el XML no creada: %s', cfdi.uuid, exc)
                    cfdi.write({'auto_bill_state': 'error', 'auto_bill_note': str(exc)[:500]})
                client._commit()
        _logger.info('sat.cfdi: %s facturas de proveedor creadas desde el XML del SAT', created)
        return created
