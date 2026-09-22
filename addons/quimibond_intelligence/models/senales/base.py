# -*- coding: utf-8 -*-
"""Señales de Odoo para el mapa de situación (Supabase `senales`).

Una señal = una función registrada con @senal('nombre') que devuelve la lista
COMPLETA de filas activas (spec §4 regla 2): `[]` resuelve todo lo abierto de
esa señal; `None` = no aplica en esta base (modelo no instalado), no se manda
lote. Nunca cifras en masa: una fila por hecho con modelo+id del documento.
"""
import logging
from datetime import datetime, timedelta

from odoo import fields

_logger = logging.getLogger(__name__)

REGISTRO = {}   # nombre de señal → fn(env, cfg) -> list[dict] | None


def senal(nombre):
    def deco(fn):
        REGISTRO[nombre] = fn
        fn.senal = nombre
        return fn
    return deco


def hoy():
    return fields.Date.today()


def hace(n):
    """Fecha de hace n días (para dominios `('campo', '<', hace(7))`)."""
    return hoy() - timedelta(days=n)


def dias(desde, hasta=None):
    """Días entre una fecha (date o datetime) y hoy; None si no hay fecha."""
    if not desde:
        return None
    if isinstance(desde, datetime):
        desde = desde.date()
    return ((hasta or hoy()) - desde).days


def umbral(cfg, clave, default):
    try:
        v = (cfg or {}).get('umbrales') or {}
        v = v.get(clave, default)
        return type(default)(v) if default is not None and not isinstance(v, type(default)) else v
    except (TypeError, ValueError):
        return default


def tiene_modelo(env, nombre):
    return nombre in env


def companias(env):
    return env['quimibond.sync']._get_company_ids()


def doc(rec, nombre=None, **extra):
    d = {'modelo': rec._name, 'id': rec.id, 'nombre': nombre or rec.display_name}
    d.update({k: v for k, v in extra.items() if v not in (None, False)})
    return d


def fila(clave, documentos, valor=None, valor_texto=None, vence=None, partner=None, user=None, payload=None):
    """Arma una fila para senales_ingestar. `partner` se resuelve a su empresa
    comercial (así 7 facturas de 7 contactos del mismo cliente son una
    situación); el RFC viaja en payload para la regla de partes relacionadas."""
    p = partner.commercial_partner_id if partner else None
    pl = dict(payload or {})
    if p is not None and p.vat and 'rfc' not in pl:
        pl['rfc'] = p.vat
    return {
        'clave': clave,
        'documentos': documentos or [],
        'odoo_partner_id': p.id if p else None,
        'responsable_odoo_user_id': user.id if user else None,
        'valor': float(valor) if valor is not None else None,
        'valor_texto': (valor_texto or '')[:300] or None,
        'vence': str(vence) if vence else None,
        'payload': pl,
    }


def agrupar(records, key):
    """{clave: [records]} conservando el orden de aparición."""
    out = {}
    for r in records:
        out.setdefault(key(r), []).append(r)
    return out


def texto_monto(m, moneda='MXN'):
    return f'${m:,.0f} {moneda}'
