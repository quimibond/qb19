# -*- coding: utf-8 -*-
"""Firma de los webhooks de Syntage (formato estilo Stripe).

    Header:   X-Satws-Signature: t=<unix_timestamp>,s=<hex_hmac>
    Firmado:  "<timestamp>.<cuerpo crudo>" con HMAC-SHA256 y el secreto.

Sin dependencias de Odoo para poder probarla sola.
"""
import hashlib
import hmac
import time


def parse_signature_header(header):
    """Devuelve (timestamp, firma_hex) o None si el header no tiene forma."""
    t = None
    s = None
    for part in (header or '').split(','):
        if '=' not in part:
            continue
        key, value = part.split('=', 1)
        key = key.strip()
        value = value.strip()
        if key == 't':
            try:
                t = int(value)
            except ValueError:
                return None
        elif key == 's':
            s = value
    if t is None or not s:
        return None
    return t, s


def compute_signature(raw_body, secret, timestamp):
    if isinstance(raw_body, str):
        raw_body = raw_body.encode('utf-8')
    signed = str(int(timestamp)).encode('ascii') + b'.' + raw_body
    return hmac.new(secret.encode('utf-8'), signed, hashlib.sha256).hexdigest()


def verify_signature(raw_body, header, secret, tolerance=300, now=None):
    """True si la firma es válida y el timestamp está dentro de la tolerancia
    (segundos; 0 desactiva la ventana)."""
    if not header or not secret:
        return False
    parsed = parse_signature_header(header)
    if not parsed:
        return False
    t, s = parsed
    if tolerance > 0:
        now_s = int(now() if now else time.time())
        if abs(now_s - t) > tolerance:
            return False
    expected = compute_signature(raw_body, secret, t)
    return hmac.compare_digest(s.lower(), expected)
