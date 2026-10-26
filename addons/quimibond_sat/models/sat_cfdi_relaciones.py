# -*- coding: utf-8 -*-
"""CFDI relacionados (nodo CfdiRelacionados, ``relations`` en Syntage).

Dos casos que el cruce por UUID no resuelve y que el SAT ya dice cómo
resolver:

* Sustitución (tipo 04): el proveedor canceló un CFDI y timbró otro que lo
  sustituye. Si el cancelado estaba ligado a una factura de Odoo, el nuevo
  toma su lugar (y su folio fiscal pasa a la factura); el cancelado queda
  "cancelado en el SAT, sin factura".
* Compensado (tipos 01 nota de crédito y 03 devolución): un ingreso que una
  o varias notas de crédito anulan completo, sin factura en Odoo de ninguno
  de los dos (2026-10-26: Productos Eléctricos $243,716, Bemexinox, A&H
  Haiat, Higo). Neto cero: se ignoran con el motivo y salen de "Falta en
  Odoo".
"""
from odoo import _, api, fields, models

CREDIT_TYPES = {'01', '03'}
SUBSTITUTION = '04'


class SatCfdiRelaciones(models.Model):
    _inherit = 'sat.cfdi'

    relacion_tipo = fields.Char(string='Tipo de relación', help='c_TipoRelacion del SAT (01 nota de crédito, '
                                '03 devolución, 04 sustitución, 07 anticipo...).')
    relacionados = fields.Char(string='CFDI relacionados', index=True,
                               help='Folios fiscales (UUID) de los CFDI que este relaciona, separados por coma.')
    match_method = fields.Selection(selection_add=[('sustitucion', 'Sustituye a un CFDI cancelado')])

    @api.model
    def _vals_from_syntage(self, obj, company, deleted=False):
        vals = super()._vals_from_syntage(obj, company, deleted=deleted)
        relations = [r for r in (obj.get('relations') or []) if isinstance(r, dict)]
        vals['relacion_tipo'] = ','.join(sorted({str(r.get('type') or '').zfill(2) for r in relations
                                                 if r.get('type') not in (None, '')})) or False
        vals['relacionados'] = ','.join((r.get('relatedInvoiceUuid') or '').strip().lower() for r in relations
                                        if r.get('relatedInvoiceUuid')) or False
        return vals

    def _relation_types(self):
        return {t for t in (self.relacion_tipo or '').split(',') if t}

    def _related_cfdis(self):
        uuids = [u for u in (self.relacionados or '').split(',') if u]
        return self.sudo().search([('uuid', 'in', uuids), ('company_id', '=', self.company_id.id)]) if uuids \
            else self.browse()

    def _match_by_substitution(self):
        """Este CFDI sustituye a uno cancelado que estaba ligado: toma su factura."""
        self.ensure_one()
        if SUBSTITUTION not in self._relation_types() or self.estado_sat == 'cancelado':
            return False
        old = self._related_cfdis().filtered(lambda c: c.move_id and c.estado_sat == 'cancelado')[:1]
        if not old:
            return False
        move = old.move_id
        old.write({'move_id': False, 'match_method': False, 'match_status': 'solo_sat',
                   'note': _('Sustituido por el CFDI %s, que quedó ligado a %s') % (self.uuid, move.name)})
        self.write({'move_id': move.id, 'match_method': 'sustitucion', 'match_status': 'matched',
                    'note': _('Sustituye al CFDI cancelado %s') % old.uuid})
        self._write_fiscal_folio(move)
        return True

    def _credit_group(self):
        """(ingreso, notas de crédito que lo acreditan) o (vacío, vacío)."""
        self.ensure_one()
        if self.tipo == 'E' and self._relation_types() & CREDIT_TYPES:
            invoice = self._related_cfdis().filtered(lambda c: c.tipo == 'I')[:1]
        elif self.tipo == 'I':
            invoice = self
        else:
            return self.browse(), self.browse()
        if not invoice:
            return self.browse(), self.browse()
        credits = self.sudo().search([
            ('tipo', '=', 'E'), ('estado_sat', '=', 'vigente'), ('company_id', '=', invoice.company_id.id),
            ('relacionados', 'ilike', invoice.uuid)])
        credits = credits.filtered(lambda c: c._relation_types() & CREDIT_TYPES
                                   and invoice.uuid.lower() in (c.relacionados or '').split(','))
        return invoice, credits

    def _match_by_credit(self):
        """Ingreso anulado completo por notas de crédito, sin factura en Odoo
        de ninguno: neto cero, se ignoran con el motivo."""
        self.ensure_one()
        if self.estado_sat != 'vigente':
            return False
        invoice, credits = self._credit_group()
        group = invoice | credits
        if not credits or invoice.estado_sat != 'vigente' or group.filtered('move_id'):
            return False
        if group.filtered(lambda c: c.match_status != 'solo_sat'):
            return False
        if abs(sum(credits.mapped('total')) - invoice.total) >= self.ROUNDING_TOLERANCE:
            return False
        reason = _('Compensado: el ingreso %(inv)s lo anula(n) la(s) nota(s) de crédito %(cred)s; neto cero') % {
            'inv': invoice.name, 'cred': ', '.join(credits.mapped('name'))}
        group.write({'match_status': 'ignorado', 'match_method': False, 'ignore_reason': reason})
        return True

    def _match_move(self):
        res = super()._match_move()
        for rec in self:
            if rec.move_id or rec.match_status != 'solo_sat':
                continue
            if rec.relacionados or rec.tipo == 'I':
                rec._match_by_substitution() or rec._match_by_credit()
        return res
