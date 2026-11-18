# -*- coding: utf-8 -*-
"""Traer los archivos del expediente desde la memoria de correo (Supabase).

Las agencias mandan el pedimento pagado, el DODA, la cuenta de gastos y los
demás documentos por correo a Logística, Contabilidad y CxC. La memoria ya
guarda cada adjunto en Storage (bucket ``email-attachments``) con su texto.
Aquí se buscan los correos del pedimento (el consecutivo en el asunto o en
el texto de un adjunto, cerca de sus fechas), se bajan sus adjuntos
y se pasan por el mismo asistente de «Cargar expediente»: un expediente a la
vez se revisa antes de guardar; varios a la vez se guardan solos, sin pisar
nada y dejando fuera los archivos que nombran otro pedimento.

La URL y la llave de servicio son las de quimibond_intelligence
(``ir.config_parameter``); el módulo no depende de él.
"""
import base64
import hashlib
import logging
import re
from datetime import timedelta

import requests

from odoo import _, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

BUCKET = 'email-attachments'
TIMEOUT = 20
# Ventana de correos alrededor de las fechas del pedimento: el consecutivo se
# repite entre años y las agencias mandan papeles semanas antes y después.
DIAS_ANTES = 60
DIAS_DESPUES = 120
MAX_ARCHIVOS = 60
MAX_BYTES_ARCHIVO = 15 * 1024 * 1024
# Firmas, logotipos y correos reenviados no son documentos del expediente.
RUIDO = re.compile(r'^(image\d*|outlook-|logo|~)|\.(eml|ics|vcf|htm|html)$', re.I)
IMAGEN = re.compile(r'\.(png|jpe?g|gif|bmp)$', re.I)


# Expedientes por corrida del cron y por selección en la lista: cada uno son
# cuatro consultas y varias descargas, y la petición web tiene tiempo límite.
LOTE_CRON = 10
LOTE_LISTA = 20
# Un expediente abierto se vuelve a buscar cada semana (llegan papeles tarde).
DIAS_REVISAR = 7


class QbPedimentoMemoria(models.Model):
    _inherit = 'qb.pedimento'

    memoria_fecha = fields.Datetime(
        'Última búsqueda en la memoria', copy=False, readonly=True,
        help='Cuándo se buscaron por última vez los archivos de este pedimento en la memoria de correo.')

    # ── conexión ─────────────────────────────────────────────────────
    def _memoria_conf(self):
        icp = self.env['ir.config_parameter'].sudo()
        url = (icp.get_param('quimibond_intelligence.supabase_url') or '').rstrip('/')
        key = icp.get_param('quimibond_intelligence.supabase_service_key') or ''
        if not url or not key:
            raise UserError(_('La memoria de correo no está configurada: faltan los parámetros '
                              'quimibond_intelligence.supabase_url y supabase_service_key.'))
        return url, key

    def _memoria_get(self, tabla, params):
        url, key = self._memoria_conf()
        try:
            resp = requests.get('%s/rest/v1/%s' % (url, tabla), params=params, timeout=TIMEOUT,
                                headers={'apikey': key, 'Authorization': 'Bearer %s' % key})
        except requests.RequestException as exc:
            raise UserError(_('No se pudo leer la memoria de correo: %s', exc)) from exc
        if resp.status_code >= 400:
            raise UserError(_('La memoria de correo respondió %(c)s: %(t)s', c=resp.status_code, t=resp.text[:300]))
        data = resp.json()
        return data if isinstance(data, list) else []

    def _memoria_descargar(self, storage_path):
        url, key = self._memoria_conf()
        try:
            resp = requests.get('%s/storage/v1/object/%s/%s' % (url, BUCKET, storage_path), timeout=TIMEOUT,
                                headers={'apikey': key, 'Authorization': 'Bearer %s' % key})
        except requests.RequestException as exc:
            _logger.warning('qb_pedimento: no se bajó %s de la memoria: %s', storage_path, exc)
            return None
        if resp.status_code >= 400:
            _logger.warning('qb_pedimento: la memoria respondió %s al bajar %s', resp.status_code, storage_path)
            return None
        return resp.content

    # ── búsqueda ─────────────────────────────────────────────────────
    def _memoria_ventana(self):
        self.ensure_one()
        fechas = [f for f in (self.fecha_pago, self.fecha_arribo,
                              self.fecha_despacho and self.fecha_despacho.date(),
                              self.fecha_entrega and self.fecha_entrega.date()) if f]
        if not fechas:
            fechas = [self.create_date.date() if self.create_date else fields.Date.context_today(self)]
        return min(fechas) - timedelta(days=DIAS_ANTES), max(fechas) + timedelta(days=DIAS_DESPUES)

    def _memoria_adjuntos(self):
        """Adjuntos de la memoria que corresponden al pedimento: todos los de
        los correos que traen el consecutivo en el asunto o en el texto de un
        adjunto (el pedimento pagado, el DODA), dentro de la ventana de fechas.
        Sin repetidos (sha256): el mismo correo llega a varios buzones."""
        self.ensure_one()
        consecutivo = (self.numero or '')[-7:]
        if not re.fullmatch(r'\d{7}', consecutivo):
            raise UserError(_('Capture el número de pedimento antes de buscar sus archivos en la memoria.'))
        desde, hasta = self._memoria_ventana()
        rango = [('email_date', 'gte.%s' % desde), ('email_date', 'lte.%sT23:59:59' % hasta)]
        # Texto del adjunto: índice GIN (texto_tsv). El nombre del archivo no
        # se busca con ilike: recorre toda la tabla y pasa del tiempo límite.
        por_texto = self._memoria_get('email_attachments', [
            ('select', 'email_id'), ('texto_tsv', 'fts(simple).%s' % consecutivo), ('limit', '200')])
        ids = {a['email_id'] for a in por_texto}
        if ids:
            ids = {c['id'] for c in self._memoria_get('emails', [
                ('select', 'id'), ('id', 'in.(%s)' % ','.join(str(i) for i in sorted(ids)))] + rango)}
        ids |= {c['id'] for c in self._memoria_get('emails', [
            ('select', 'id'), ('subject', 'ilike.*%s*' % consecutivo), ('limit', '200')] + rango)}
        if not ids:
            return []
        filas = self._memoria_get('email_attachments', [
            ('select', 'id,filename,mime_type,size_bytes,sha256,storage_path'),
            ('email_id', 'in.(%s)' % ','.join(str(i) for i in sorted(ids))),
            ('storage_path', 'not.is.null'), ('order', 'id'), ('limit', '500')])
        vistos, adjuntos = set(), []
        for fila in filas:
            nombre = fila.get('filename') or ''
            tam = fila.get('size_bytes') or 0
            if RUIDO.search(nombre) or (IMAGEN.search(nombre) and tam < 50 * 1024) or tam > MAX_BYTES_ARCHIVO:
                continue
            clave = fila.get('sha256') or fila['storage_path']
            if clave in vistos:
                continue
            vistos.add(clave)
            adjuntos.append(fila)
        return adjuntos[:MAX_ARCHIVOS]

    def _memoria_preparar_carga(self):
        """Asistente de carga con los archivos de la memoria que el expediente
        aún no tiene, ya clasificados. None si no hay nada nuevo."""
        self.ensure_one()
        existentes = set(self.env['ir.attachment'].search([
            ('res_model', '=', 'qb.pedimento'), ('res_id', '=', self.id)]).mapped('checksum'))
        archivos = self.env['ir.attachment']
        for fila in self._memoria_adjuntos():
            datos = self._memoria_descargar(fila['storage_path'])
            if not datos or hashlib.sha1(datos).hexdigest() in existentes:
                continue
            existentes.add(hashlib.sha1(datos).hexdigest())
            archivos |= self.env['ir.attachment'].create({
                'name': fila['filename'], 'datas': base64.b64encode(datos),
                'mimetype': fila.get('mime_type') or False})
        if not archivos:
            return None
        carga = self.env['qb.pedimento.carga'].create({
            'pedimento_id': self.id, 'archivo_ids': [fields.Command.set(archivos.ids)]})
        carga.action_clasificar()
        return carga

    # ── acciones ─────────────────────────────────────────────────────
    def _memoria_guardar_solo(self):
        """Trae y guarda sin revisión: los archivos que nombran otro pedimento
        se dejan fuera. Devuelve cuántos archivos guardó."""
        self.ensure_one()
        carga = self._memoria_preparar_carga()
        self.memoria_fecha = fields.Datetime.now()
        if not carga:
            return 0
        carga.linea_ids.filtered('aviso').write({'omitir': True})
        guardados = len(carga.linea_ids.filtered(lambda linea: not linea.omitir))
        carga.action_guardar()
        return guardados

    def action_traer_memoria(self):
        """Uno: abre la carga ya clasificada para revisarla. Varios: guarda solo."""
        if len(self) == 1:
            carga = self._memoria_preparar_carga()
            self.memoria_fecha = fields.Datetime.now()
            if not carga:
                raise UserError(_('La memoria de correo no tiene archivos nuevos del pedimento %s.', self.name))
            return carga._reabrir()
        if len(self) > LOTE_LISTA:
            raise UserError(_('Seleccione hasta %s pedimentos a la vez; el resto lo va trayendo solo la tarea '
                              'programada «Pedimentos - Traer archivos de la memoria».', LOTE_LISTA))
        guardados = sum(rec._memoria_guardar_solo() for rec in self.filtered('numero'))
        return {'type': 'ir.actions.client', 'tag': 'display_notification', 'params': {
            'type': 'success' if guardados else 'warning', 'sticky': False,
            'message': _('Se trajeron %s archivos de la memoria de correo.', guardados) if guardados
            else _('La memoria de correo no tiene archivos nuevos de estos pedimentos.')}}

    def _cron_traer_memoria(self):
        """Llena poco a poco los expedientes: primero los que nunca se buscaron,
        luego los abiertos cuya última búsqueda tiene más de una semana."""
        hace = fields.Datetime.now() - timedelta(days=DIAS_REVISAR)
        base = [('numero', '!=', False), ('state', '!=', 'cancelado')]
        pendientes = self.search(base + [('memoria_fecha', '=', False)], limit=LOTE_CRON, order='fecha_pago desc, id desc')
        if len(pendientes) < LOTE_CRON:
            pendientes |= self.search(base + [('state', '!=', 'cerrado'), ('memoria_fecha', '<', hace)],
                                      limit=LOTE_CRON - len(pendientes), order='memoria_fecha')
        for rec in pendientes:
            try:
                with self.env.cr.savepoint():
                    n = rec._memoria_guardar_solo()
                if n:
                    _logger.info('qb_pedimento: %s archivos de la memoria en %s', n, rec.name)
            except UserError as exc:
                _logger.warning('qb_pedimento: memoria de %s: %s', rec.name, exc)
                rec.memoria_fecha = fields.Datetime.now()
