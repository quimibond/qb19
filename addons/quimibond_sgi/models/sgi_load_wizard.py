# -*- coding: utf-8 -*-
"""Asistente «Cargar catálogo»: la misma carga de sgi.process.load_payload
desde la pantalla, para no depender del conector MCP. Se pega o se sube el
JSON; «Probar» corre en dry-run y muestra qué se crearía, actualizaría o
archivaría y los errores; «Cargar» escribe."""
import base64
import json

from odoo import models, fields, api
from odoo.exceptions import UserError

_ACTIONS = [
    ('created', "Se crea"),
    ('updated', "Se actualiza"),
    ('archived', "Se archiva"),
    ('deleted', "Se quita"),
    ('error', "Error"),
    ('warning', "Advertencia"),
]


class SgiCatalogLoadWizard(models.TransientModel):
    _name = 'sgi.catalog.load.wizard'
    _description = "Cargar catálogo SGI"

    payload_text = fields.Text(string="JSON")
    payload_file = fields.Binary(string="Archivo JSON")
    payload_filename = fields.Char(string="Nombre del archivo")
    state = fields.Selection([
        ('draft', "Captura"),
        ('tested', "Probado"),
        ('loaded', "Cargado"),
    ], default='draft', readonly=True)
    dry_run_ok = fields.Boolean(readonly=True)
    summary = fields.Text(string="Resumen", readonly=True)
    line_ids = fields.One2many(
        'sgi.catalog.load.wizard.line', 'wizard_id', string="Resultado", readonly=True)
    error_count = fields.Integer(compute='_compute_counts')
    change_count = fields.Integer(compute='_compute_counts')

    @api.depends('line_ids.action')
    def _compute_counts(self):
        for wizard in self:
            wizard.error_count = len(wizard.line_ids.filtered(lambda l: l.action == 'error'))
            wizard.change_count = len(wizard.line_ids.filtered(
                lambda l: l.action not in ('error', 'warning')))

    def _payload(self):
        self.ensure_one()
        if self.payload_file:
            raw = base64.b64decode(self.payload_file).decode('utf-8-sig')
        else:
            raw = self.payload_text or ''
        if not raw.strip():
            raise UserError("Pega el JSON o sube el archivo.")
        try:
            return json.loads(raw)
        except ValueError as exc:
            raise UserError("El JSON no es válido: %s" % exc)

    def _run(self, dry_run):
        self.ensure_one()
        result = self.env['sgi.process'].load_payload(self._payload(), dry_run=dry_run)
        lines = [(5, 0, 0)]
        for change in result['changes']:
            lines.append((0, 0, {
                'kind': change['kind'], 'key': str(change.get('key') or ''),
                'action': change['action'],
                'message': ', '.join(change.get('fields') or []),
            }))
        for level, entries in (('error', result['errors']), ('warning', result['warnings'])):
            for entry in entries:
                lines.append((0, 0, {
                    'kind': entry['kind'], 'key': str(entry.get('key') or ''),
                    'action': level, 'message': entry['message'],
                }))
        summary = "; ".join(
            "%s: %s" % (dict(_ACTIONS).get(action, action),
                        ", ".join("%d %s" % (n, kind) for kind, n in kinds.items()))
            for action, kinds in result['summary'].items()) or "Sin cambios."
        self.write({
            'line_ids': lines,
            'summary': summary,
            'dry_run_ok': dry_run and result['ok'],
            'state': 'tested' if dry_run else 'loaded',
        })
        return {
            'type': 'ir.actions.act_window',
            'res_model': self._name,
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'new',
            'name': "Cargar catálogo",
        }

    def action_test(self):
        return self._run(dry_run=True)

    def action_load(self):
        return self._run(dry_run=False)


class SgiCatalogLoadWizardLine(models.TransientModel):
    _name = 'sgi.catalog.load.wizard.line'
    _description = "Resultado de la carga del catálogo SGI"
    _order = 'id'

    wizard_id = fields.Many2one('sgi.catalog.load.wizard', required=True, ondelete='cascade')
    kind = fields.Char(string="Qué")
    key = fields.Char(string="Clave")
    action = fields.Selection(_ACTIONS, string="Resultado")
    message = fields.Char(string="Detalle")
