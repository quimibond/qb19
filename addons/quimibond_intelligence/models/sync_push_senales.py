# -*- coding: utf-8 -*-
"""Push de señales Odoo → Supabase (`senales`), spec §4 regla 2.

Una llamada a senales_ingestar por señal con la lista COMPLETA de claves
activas; Supabase resuelve lo que no viene. Turnos por cada_horas en el
parámetro quimibond_intelligence.senales_last_run. rpc_strict: un lote
solo cuenta si Supabase respondió bien; cualquier error deja el método en
error en el Historial de Sync (y en pipeline_logs vía _run_push).
"""
import json
import logging
import time
import uuid
from datetime import datetime, timedelta

from odoo import models

from .senales.base import REGISTRO
from .supabase_client import SupabaseError

_logger = logging.getLogger(__name__)

PARAM_LAST_RUN = 'quimibond_intelligence.senales_last_run'
HOLGURA = timedelta(minutes=5)  # el cron horario no cae exacto: 55 min cuentan como 1 h


class QuimibondSyncSenales(models.TransientModel):
    _inherit = 'quimibond.sync'

    def _push_senales(self, client, last_sync=None) -> int:
        ICP = self.env['ir.config_parameter'].sudo()
        config = client.fetch('senales_config', {'fuente': 'eq.odoo', 'activa': 'eq.true',
                                                 'select': 'senal,umbrales,reglas_calidad,cada_horas,agrupar_por', 'order': 'senal'})
        if not config:
            raise SupabaseError('senales_config vacío o inaccesible: no se manda ningún lote')
        try:
            last_run = json.loads(ICP.get_param(PARAM_LAST_RUN) or '{}')
        except ValueError:
            last_run = {}
        corrida = str(uuid.uuid4())
        ahora = datetime.utcnow()
        total, errores, detalle = 0, [], {}

        for cfg in config:
            nombre = cfg['senal']
            fn = REGISTRO.get(nombre)
            if fn is None:
                _logger.info('senal %s: sin consulta en Odoo (¿plan B o fuente=memoria?)', nombre)
                continue
            prev = last_run.get(nombre)
            if prev:
                try:
                    if ahora - datetime.fromisoformat(prev) < timedelta(hours=int(cfg.get('cada_horas') or 1)) - HOLGURA:
                        detalle[nombre] = 'fuera de turno'
                        continue
                except ValueError:
                    pass
            t0 = time.monotonic()
            try:
                with self.env.cr.savepoint():
                    filas = fn(self.env, cfg)
                if filas is None:
                    detalle[nombre] = 'no aplica'
                    continue
                res = client.rpc_strict('senales_ingestar', {'p_senal': nombre, 'p_fuente': 'odoo', 'p_corrida': corrida, 'p_filas': filas})
                if not isinstance(res, dict) or not res.get('ok'):
                    raise SupabaseError('senales_ingestar %s: %s' % (nombre, (res or {}).get('error') if isinstance(res, dict) else res))
                total += len(filas)
                last_run[nombre] = ahora.isoformat(timespec='seconds')
                detalle[nombre] = {'n': len(filas), 'nuevas': res.get('nuevas'), 'resueltas': res.get('resueltas'), 's': round(time.monotonic() - t0, 1)}
            except Exception as exc:  # noqa: BLE001 — una señal no tumba a las demás
                _logger.exception('senal %s falló', nombre)
                errores.append('%s: %s' % (nombre, str(exc)[:200]))
                detalle[nombre] = {'error': str(exc)[:200], 's': round(time.monotonic() - t0, 1)}

        ICP.set_param(PARAM_LAST_RUN, json.dumps(last_run))
        try:
            client.rpc_strict('senales_push_terminado', {'p_corrida': corrida, 'p_origen': 'odoo'})
        except Exception as exc:  # noqa: BLE001
            errores.append('push_terminado: %s' % str(exc)[:200])

        try:
            client.insert('pipeline_logs', [{'level': 'error' if errores else 'info', 'phase': 'odoo_push_senales',
                                             'message': '[senales] %d filas en %d señales, %d errores' % (total, len([d for d in detalle.values() if isinstance(d, dict) and 'n' in d]), len(errores)),
                                             'details': {'corrida': corrida, 'senales': detalle, 'errores': errores}}])
        except Exception as exc:  # noqa: BLE001
            _logger.warning('log de señales: %s', exc)

        if errores:
            self.env['quimibond.sync.log'].sudo().create({
                'name': 'Push señales con errores', 'direction': 'push', 'status': 'error',
                'summary': '\n'.join(errores)[:2000],
            })
            raise SupabaseError('%d señal(es) fallaron: %s' % (len(errores), '; '.join(errores)[:500]))
        return total
