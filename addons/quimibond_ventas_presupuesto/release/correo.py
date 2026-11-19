# -*- coding: utf-8 -*-
"""Qué perfil de release le toca a un archivo que llegó por correo (19.0.3.30.0).

Python puro. Un correo puede llegar directo del cliente o reenviado por Ventas (entonces el remitente es de
Quimibond y el del cliente viene en el cuerpo: «De: … @lear.com»). Por perfil se configura:

- ``domains``: dominios del cliente (lear.com, fxi.com). Sin dominios el perfil no se reconoce por correo.
- ``subject``: textos que debe traer el asunto (cualquiera; vacío = cualquier asunto).
- ``files``: patrones del nombre del archivo con comodines (``SUM Quimibond*.xlsx``; vacío = cualquier
  archivo de release: pdf, xls, xlsx, csv, txt, eml).

Gana el perfil que cumple más condiciones (remitente directo > dominio en el cuerpo; asunto y archivo
configurados suman). Si empatan dos, no se elige: Ventas escoge el perfil.
"""
import fnmatch

RELEASE_EXT = ('.pdf', '.xlsx', '.xls', '.csv', '.txt', '.eml')


def lista(value):
    """«a, b ,c» → ['a', 'b', 'c'] en minúsculas."""
    return [x.strip().lower() for x in (value or '').split(',') if x.strip()]


def es_archivo_release(filename):
    return (filename or '').strip().lower().endswith(RELEASE_EXT)


def puntaje(perfil, email_from, subject, body, filename):
    """Puntos del perfil para este archivo; 0 si no le toca."""
    domains = perfil.get('domains') or []
    if not domains:
        return 0
    sender = (email_from or '').lower()
    texto = ' '.join([sender, (subject or '').lower(), (body or '').lower()])
    if any(d in sender for d in domains):
        score = 2
    elif any(d in texto for d in domains):
        score = 1
    else:
        return 0
    subjects = perfil.get('subject') or []
    if subjects:
        if not any(s in (subject or '').lower() for s in subjects):
            return 0
        score += 2
    name = (filename or '').strip().lower()
    patterns = perfil.get('files') or []
    if patterns:
        if not any(fnmatch.fnmatch(name, p) for p in patterns):
            return 0
        score += 2
    elif not es_archivo_release(name):
        return 0
    return score


def elegir(perfiles, email_from, subject, body, filename):
    """(perfil_id o None, ids de los empatados). ``perfiles``: dicts con id, domains, subject y files."""
    scores = [(puntaje(p, email_from, subject, body, filename), p['id']) for p in perfiles]
    scores = [s for s in scores if s[0] > 0]
    if not scores:
        return None, []
    best = max(s for s, _id in scores)
    ids = [i for s, i in scores if s == best]
    return (ids[0] if len(ids) == 1 else None), ids
