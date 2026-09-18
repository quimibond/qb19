# -*- coding: utf-8 -*-
"""Lectura de la memoria (Supabase, API REST/PostgREST): GET de tablas/vistas
y POST a funciones RPC de solo lectura. La llave de servicio vive en
ir.config_parameter (la puso quimibond_intelligence)."""
import requests

from odoo import _, api, models
from odoo.exceptions import UserError


class QbMemoriaClient(models.AbstractModel):
    _name = 'qb.memoria.client'
    _description = 'Cliente de la memoria (Supabase)'

    TIMEOUT = 8

    @api.model
    def _conf(self):
        icp = self.env['ir.config_parameter'].sudo()
        url = (icp.get_param('quimibond_intelligence.supabase_url') or '').rstrip('/')
        key = icp.get_param('quimibond_intelligence.supabase_service_key') or ''
        if not url or not key:
            raise UserError(_('La memoria no está configurada: faltan quimibond_intelligence.supabase_url / '
                              'supabase_service_key.'))
        return url, key

    @api.model
    def get(self, table, params):
        """Filas (lista de dicts) de una tabla/vista con filtros PostgREST."""
        url, key = self._conf()
        try:
            resp = requests.get('%s/rest/v1/%s' % (url, table), params=params, timeout=self.TIMEOUT,
                                headers={'apikey': key, 'Authorization': 'Bearer %s' % key})
        except requests.RequestException as exc:
            raise UserError(_('No se pudo leer la memoria (%s): %s') % (table, exc)) from exc
        if resp.status_code >= 400:
            raise UserError(_('La memoria respondió %s en %s: %s') % (resp.status_code, table, resp.text[:300]))
        data = resp.json()
        return data if isinstance(data, list) else []

    @api.model
    def rpc(self, name, params):
        """Resultado (JSON) de una función RPC de la memoria (``/rest/v1/rpc/<name>``)."""
        url, key = self._conf()
        try:
            resp = requests.post('%s/rest/v1/rpc/%s' % (url, name), json=params or {}, timeout=self.TIMEOUT,
                                 headers={'apikey': key, 'Authorization': 'Bearer %s' % key,
                                          'Content-Type': 'application/json'})
        except requests.RequestException as exc:
            raise UserError(_('No se pudo leer la memoria (%s): %s') % (name, exc)) from exc
        if resp.status_code >= 400:
            raise UserError(_('La memoria respondió %s en %s: %s') % (resp.status_code, name, resp.text[:300]))
        return resp.json()
