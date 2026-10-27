# -*- coding: utf-8 -*-
"""Estado del CFDI consultado directo al SAT (1.27.0).

La extracción diaria de Syntage pide los CFDI emitidos en los últimos días,
así que una cancelación posterior no se vuelve a traer: el CFDI se queda
«vigente» aunque en el SAT ya esté cancelado (INV/2026/10/0041, emitida el
7-oct y cancelada el 13-oct; Syntage la seguía mostrando vigente). Aquí se le
pregunta al SAT con su servicio público de consulta (el mismo que usa la
localización mexicana para el estado SAT de las facturas):

* un cron diario repasa los CFDI vigentes, primero los que Odoo ya tiene
  cancelados, luego los que nunca se han consultado o se consultaron hace
  más tiempo;
* «Consultar estado en el SAT» lo hace a mano desde el CFDI o la comparación.

Una cancelación en el SAT no tiene vuelta: si el SAT dijo «cancelado», una
descarga posterior de Syntage que todavía diga «vigente» no lo regresa.
"""
import logging
import re
import time
from datetime import timedelta
from xml.sax.saxutils import escape

import requests

from odoo import _, api, fields, models

_logger = logging.getLogger(__name__)

SAT_URL = 'https://consultaqr.facturaelectronica.sat.gob.mx/ConsultaCFDIService.svc'
SAT_ACTION = 'http://tempuri.org/IConsultaCFDIService/Consulta'
SAT_ESTADOS = {'vigente': 'vigente', 'cancelado': 'cancelado', 'no encontrado': 'no_encontrado'}
# CFDI que se consultan por día y antigüedad máxima (la cancelación se puede
# pedir hasta el ejercicio siguiente).
DEFAULT_POR_DIA = 300
VENTANA_DIAS = 400


class SatCfdiEstado(models.Model):
    _inherit = 'sat.cfdi'

    sat_verificado = fields.Datetime(string='Estado consultado al SAT', readonly=True, copy=False)
    sat_estado_verificado = fields.Selection([
        ('vigente', 'Vigente'),
        ('cancelado', 'Cancelado'),
        ('no_encontrado', 'No encontrado'),
    ], string='Estado según el SAT', readonly=True, copy=False,
        help='Respuesta del servicio de consulta del SAT la última vez que se le preguntó. Gana sobre '
             'Syntage cuando el SAT dice «Cancelado».')

    @api.model
    def _sat_query(self, emisor, receptor, total, uuid):
        """Estado del CFDI en el SAT: 'vigente', 'cancelado', 'no_encontrado' o
        None si el servicio no contestó."""
        expresion = '?re=%s&rr=%s&tt=%.6f&id=%s' % (
            escape(emisor or ''), escape(receptor or ''), abs(total or 0.0), (uuid or '').upper())
        body = (
            '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
            'xmlns:tem="http://tempuri.org/"><soapenv:Header/><soapenv:Body><tem:Consulta>'
            '<tem:expresionImpresa><![CDATA[%s]]></tem:expresionImpresa>'
            '</tem:Consulta></soapenv:Body></soapenv:Envelope>' % expresion)
        try:
            resp = requests.post(SAT_URL, data=body.encode(), timeout=20, headers={
                'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': SAT_ACTION})
            resp.raise_for_status()
        except requests.RequestException as exc:
            _logger.warning('Consulta al SAT de %s falló: %s', uuid, exc)
            return None
        match = re.search(r'<(?:\w+:)?Estado>([^<]*)</(?:\w+:)?Estado>', resp.text)
        return SAT_ESTADOS.get((match.group(1) if match else '').strip().lower())

    def _verify_sat_status(self, pause=0.0):
        """Pregunta al SAT por cada CFDI y aplica la respuesta. Devuelve
        {estado: n} con lo que contestó el SAT."""
        cuenta = {}
        now = fields.Datetime.now()
        for rec in self:
            estado = self._sat_query(rec.emisor_rfc, rec.receptor_rfc, rec.total, rec.uuid)
            cuenta[estado or 'sin_respuesta'] = cuenta.get(estado or 'sin_respuesta', 0) + 1
            if not estado:
                continue
            vals = {'sat_verificado': now, 'sat_estado_verificado': estado}
            if estado == 'cancelado' and rec.estado_sat != 'cancelado':
                vals.update({
                    'estado_sat': 'cancelado',
                    'fecha_cancelacion': rec.fecha_cancelacion or now,
                    'note': ((rec.note + '\n') if rec.note else '') + _(
                        'Cancelado según el SAT (consulta del %s); Syntage lo tenía como %s.') % (
                        fields.Date.to_string(fields.Date.context_today(rec)), rec.estado_sat),
                })
            elif estado == 'vigente' and rec.estado_sat == 'cancelacion_pendiente':
                vals['estado_sat'] = 'vigente'
            rec.write(vals)
            if pause:
                time.sleep(pause)
        return cuenta

    def action_verify_sat_status(self):
        """Botón: consulta el estado de estos CFDI en el SAT."""
        cuenta = self._verify_sat_status()
        labels = {'vigente': _('vigentes'), 'cancelado': _('cancelados'), 'no_encontrado': _('no encontrados'),
                  'sin_respuesta': _('sin respuesta del SAT')}
        message = ', '.join('%s %s' % (n, labels.get(k, k)) for k, n in sorted(cuenta.items())) or _('Nada que consultar')
        return {
            'type': 'ir.actions.client', 'tag': 'display_notification',
            'params': {'title': _('Estado en el SAT'), 'message': message, 'sticky': False,
                       'next': {'type': 'ir.actions.act_window_close'}},
        }

    @api.model
    def _verify_candidates(self, limit):
        """Primero los vigentes que Odoo tiene cancelados (la factura o su
        documento CFDI), luego los nunca consultados y los consultados hace
        más tiempo, de los últimos VENTANA_DIAS días."""
        base = [('tipo', 'in', ('I', 'E')), ('estado_sat', 'in', ('vigente', 'cancelacion_pendiente')),
                ('uuid', '!=', False)]
        found = self.sudo().search(base + [('move_id.state', '=', 'cancel')], limit=limit)
        if 'l10n_mx_edi.document' in self.env and len(found) < limit:
            docs = self.env['l10n_mx_edi.document'].sudo().search([('sat_state', '=', 'cancelled')])
            uuids = [u.lower() for u in docs.mapped('attachment_uuid') if u]
            if uuids:
                found |= self.sudo().search(base + [('uuid', 'in', uuids), ('id', 'not in', found.ids)],
                                            limit=limit - len(found))
        if len(found) < limit:
            since = fields.Datetime.now() - timedelta(days=VENTANA_DIAS)
            found |= self.sudo().search(base + [('fecha_emision', '>=', since), ('id', 'not in', found.ids)],
                                        order='sat_verificado asc nulls first, fecha_emision desc',
                                        limit=limit - len(found))
        return found

    @api.model
    def _cron_verify_sat_status(self, limit=None):
        limit = limit or int(self.env['ir.config_parameter'].sudo().get_param(
            'quimibond_sat.verificar_sat_por_dia') or DEFAULT_POR_DIA)
        cfdis = self._verify_candidates(limit)
        cuenta = cfdis._verify_sat_status(pause=0.2)
        _logger.info('sat.cfdi: estado consultado al SAT de %s CFDI: %s', len(cfdis), cuenta)
        return cuenta

    @api.model
    def _upsert_from_syntage(self, obj, company, event_type=None):
        rec = super()._upsert_from_syntage(obj, company, event_type=event_type)
        # Lo que el SAT ya dijo cancelado no lo regresa una descarga atrasada.
        if rec.sat_estado_verificado == 'cancelado' and rec.estado_sat != 'cancelado':
            rec.write({'estado_sat': 'cancelado'})
        return rec
