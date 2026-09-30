# -*- coding: utf-8 -*-
"""Asistente «Cargar mapa de procesos» (decisión 13 de la auditoría: carga
manual, con modo de prueba primero).

1. **Probar** corre ``sgi.process.load_payload`` en modo de prueba (dry-run:
   todo dentro de un savepoint que se deshace) y muestra qué se crearía,
   actualizaría o archivaría, los errores y las advertencias.
2. **Cargar** solo se habilita después de probar ESE mismo archivo y de
   marcar «Entiendo que se escribe en la base»; entonces escribe. Se puede
   repetir sin duplicar (todo va por llave natural).

El archivo es ``data/mapa.json`` del módulo o uno que se sube (por ejemplo, el
que se descarga aquí mismo de otra base con «Descargar el mapa de esta base»,
que es ``sgi.process.export_payload``). Solo un Administrador SGI.
"""
import base64
import hashlib
import json

from odoo import api, fields, models
from odoo.exceptions import AccessError, UserError
from odoo.tools import file_open

MAPA_PATH = 'quimibond_sgi_mapa/data/mapa.json'

_ACTIONS = [
    ('created', "Se crea"),
    ('updated', "Se actualiza"),
    ('archived', "Se archiva"),
    ('moved', "Se mueve"),
    ('deleted', "Se quita"),
    ('error', "Error"),
    ('warning', "Advertencia"),
]


def _check_admin(env):
    if not (env.su or env.user.has_group('quimibond_sgi.group_sgi_admin')):
        raise AccessError("Solo un Administrador SGI puede cargar o descargar el mapa.")


class SgiMapaLoadWizard(models.TransientModel):
    """Asistente «Cargar mapa de procesos» de ``quimibond_sgi_mapa``: prueba y carga el JSON del
    módulo o uno subido, y descarga el de la base."""
    _name = 'sgi.mapa.load.wizard'
    _description = "Cargar mapa de procesos SGI"

    source = fields.Selection([
        ('modulo', "Mapa del módulo (producción, 2026-09-29)"),
        ('archivo', "Otro archivo JSON"),
    ], string="Qué mapa", default='modulo', required=True)
    payload_file = fields.Binary(string="Archivo JSON")
    payload_filename = fields.Char(string="Nombre del archivo")
    state = fields.Selection([
        ('draft', "Sin probar"),
        ('tested', "Probado"),
        ('loaded', "Cargado"),
    ], default='draft', readonly=True)
    tested_hash = fields.Char(readonly=True)
    confirm = fields.Boolean(string="Entiendo que se escribe en la base")
    dry_run_ok = fields.Boolean(readonly=True)
    summary = fields.Text(string="Resumen", readonly=True)
    counts = fields.Char(string="El mapa trae", readonly=True)
    line_ids = fields.One2many('sgi.mapa.load.wizard.line', 'wizard_id', string="Resultado",
                               readonly=True)
    error_count = fields.Integer(compute='_compute_counts')
    warning_count = fields.Integer(compute='_compute_counts')
    change_count = fields.Integer(compute='_compute_counts')

    @api.depends('line_ids.action')
    def _compute_counts(self):
        for wizard in self:
            actions = wizard.line_ids.mapped('action')
            wizard.error_count = actions.count('error')
            wizard.warning_count = actions.count('warning')
            wizard.change_count = len(actions) - wizard.error_count - wizard.warning_count

    @api.model_create_multi
    def create(self, vals_list):
        _check_admin(self.env)
        return super().create(vals_list)

    # ------------------------------------------------------------------
    def _raw(self):
        self.ensure_one()
        if self.source == 'archivo':
            if not self.payload_file:
                raise UserError("Sube el archivo JSON del mapa.")
            return base64.b64decode(self.payload_file)
        with file_open(MAPA_PATH, 'rb') as handle:
            return handle.read()

    def _payload(self, raw):
        try:
            payload = json.loads(raw.decode('utf-8-sig'))
        except ValueError as exc:
            raise UserError("El archivo no es JSON válido: %s" % exc)
        if not isinstance(payload, dict):
            raise UserError("El mapa debe ser un objeto JSON.")
        return payload

    def _reopen(self):
        return {
            'type': 'ir.actions.act_window',
            'res_model': self._name,
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'new',
            'name': "Cargar mapa de procesos",
        }

    def _run(self, dry_run):
        self.ensure_one()
        _check_admin(self.env)
        raw = self._raw()
        digest = hashlib.sha256(raw).hexdigest()
        if not dry_run:
            if self.state != 'tested' or self.tested_hash != digest:
                raise UserError("Primero corre «Probar» con este mismo mapa: la carga real "
                                "solo va después del modo de prueba.")
            if not self.confirm:
                raise UserError("Marca «Entiendo que se escribe en la base» para cargar.")
        payload = self._payload(raw)
        counts = (payload.get('meta') or {}).get('counts') or {}
        result = self.env['sgi.process'].load_payload(payload, dry_run=dry_run)
        lines = [(5, 0, 0)]
        for change in result['changes']:
            lines.append((0, 0, {
                'kind': change['kind'], 'key': str(change.get('key') or ''),
                'action': change['action'] if change['action'] in dict(_ACTIONS) else 'updated',
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
                        ", ".join("%d %s" % (n, kind) for kind, n in sorted(kinds.items())))
            for action, kinds in sorted(result['summary'].items())) or "Sin cambios."
        self.write({
            'line_ids': lines,
            'summary': ("Modo de prueba (no se escribió nada). " if dry_run else "Cargado. ")
            + summary,
            'counts': ", ".join("%s %s" % (n, kind) for kind, n in counts.items()),
            'dry_run_ok': dry_run and result['ok'],
            'state': 'tested' if dry_run else 'loaded',
            'tested_hash': digest if dry_run else self.tested_hash,
            'confirm': False,
        })
        return self._reopen()

    def action_test(self):
        return self._run(dry_run=True)

    def action_load(self):
        return self._run(dry_run=False)

    def action_export(self):
        """Descarga el mapa de ESTA base (export_payload) como JSON."""
        self.ensure_one()
        _check_admin(self.env)
        text = self.env['sgi.process'].export_payload_json()
        attachment = self.env['ir.attachment'].create({
            'name': 'mapa_%s.json' % fields.Date.context_today(self),
            'raw': text.encode('utf-8'),
            'mimetype': 'application/json',
            'res_model': self._name,
            'res_id': self.id,
        })
        return {
            'type': 'ir.actions.act_url',
            'url': '/web/content/%d?download=true' % attachment.id,
            'target': 'self',
        }


class SgiMapaLoadWizardLine(models.TransientModel):
    """Renglón del resultado de la carga del mapa."""
    _name = 'sgi.mapa.load.wizard.line'
    _description = "Resultado de la carga del mapa SGI"
    _order = 'id'

    wizard_id = fields.Many2one('sgi.mapa.load.wizard', required=True, ondelete='cascade')
    kind = fields.Char(string="Qué")
    key = fields.Char(string="Clave")
    action = fields.Selection(_ACTIONS, string="Resultado")
    message = fields.Char(string="Detalle")
