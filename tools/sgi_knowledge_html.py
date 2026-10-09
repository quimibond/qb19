#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Convierte los manuales del SGI (Markdown del repo) al HTML que siembra
``quimibond_sgi_knowledge`` en Conocimiento (decisión N2, entrega 1.2.0).

Uso (desde la raíz del repo):

    python3 tools/sgi_knowledge_html.py           # regenera data/manuales/*.html
    python3 tools/sgi_knowledge_html.py --check   # compara con lo comprometido

Sin dependencias: Odoo 19 no trae convertidor de Markdown del lado del
servidor y el HTML comprometido evita depender de uno en Odoo.sh. Entiende el
subconjunto que usan los manuales: títulos ``#`` a ``####``, párrafos, listas
``-`` y ``1.`` (con renglones de continuación sangrados), tablas con ``|---|``,
citas ``>``, ``**negritas**``, ``*cursivas*``, ``` `código` ``` y ligas.

Ligas:

- a otro manual sembrado (``operador-o-supervisor.md``,
  ``../usuarios/mast.md``…) → ``sgi-kb:manual:<nombre>``, marcador que
  ``knowledge.article._sgi_kb_seed`` cambia por la liga del artículo;
- a ``tecnica/`` o ``transicion/`` (o cualquier otro archivo del repo) → el
  texto en negritas con «(documentación técnica del repositorio)»;
- ``http(s)`` → tal cual, en otra pestaña.

``--check`` genera en memoria y sale con 1 si algún ``.html`` difiere (mismo
contrato que ``tools/sgi_docs.py``). Corre en el CI.
"""
import argparse
import html
import os
import re
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DOCS = os.path.join(ROOT, 'docs', 'sgi')
OUT = os.path.join(ROOT, 'addons', 'quimibond_sgi_knowledge', 'data', 'manuales')

# (archivo de salida, fuente relativa a docs/sgi). Misma tabla que
# SGI_KB_MANUALS en models/sgi_knowledge_article.py (la prueba test_kb_seed
# revisa que cada archivo exista).
MANUALS = (
    ('primeros-pasos.html', 'primeros-pasos.md'),
    ('operador-o-supervisor.html', 'usuarios/operador-o-supervisor.md'),
    ('jefe-de-area.html', 'usuarios/jefe-de-area.md'),
    ('mast.html', 'usuarios/mast.md'),
    ('manual-jefe-mast.html', 'administracion/manual-jefe-mast.md'),
    ('direccion.html', 'usuarios/direccion.md'),
    ('rh.html', 'usuarios/rh.md'),
    ('auditor.html', 'usuarios/auditor.md'),
    ('glosario.html', 'glosario.md'),
)
# Fuente → clave de siembra del artículo (sgi_seed_key).
SEEDED = {src: 'manual:' + out[:-len('.html')] for out, src in MANUALS}

INLINE = re.compile(
    r'`(?P<code>[^`]+)`'
    r'|\[(?P<text>[^\]]+)\]\((?P<href>[^)\s]+)\)'
    r'|\*\*(?P<bold>.+?)\*\*'
    r'|(?<![\w*])\*(?P<em>[^*\s][^*]*?)\*(?![\w*])'
    r'|(?<![\w_])_(?P<em2>[^_\s][^_]*?)_(?![\w_])')
HEADING = re.compile(r'^(#{1,4})\s+(.*?)\s*#*\s*$')
UL_ITEM = re.compile(r'^[-*]\s+(.*)$')
OL_ITEM = re.compile(r'^\d+[.)]\s+(.*)$')
TABLE_SEP = re.compile(r'^\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)*\|?\s*$')


def _link(text_html, href, source):
    """HTML de una liga ``[texto](href)`` vista desde ``source``."""
    if re.match(r'^https?://', href):
        return '<a href="%s" target="_blank">%s</a>' % (html.escape(href, quote=True), text_html)
    path = href.split('#', 1)[0]
    if path:
        target = os.path.normpath(os.path.join(os.path.dirname(source), path)).replace(os.sep, '/')
        key = SEEDED.get(target)
        if key:
            return '<a href="sgi-kb:%s">%s</a>' % (key, text_html)
    return '<strong>%s</strong> (documentación técnica del repositorio)' % text_html


def inline(text, source):
    """Marcas en línea de un texto Markdown, con lo demás escapado."""
    out, pos = [], 0
    for match in INLINE.finditer(text):
        out.append(html.escape(text[pos:match.start()], quote=False))
        if match.group('code') is not None:
            out.append('<code>%s</code>' % html.escape(match.group('code'), quote=False))
        elif match.group('href') is not None:
            out.append(_link(inline(match.group('text'), source), match.group('href'), source))
        elif match.group('bold') is not None:
            out.append('<strong>%s</strong>' % inline(match.group('bold'), source))
        else:
            out.append('<em>%s</em>' % inline(match.group('em') or match.group('em2'), source))
        pos = match.end()
    out.append(html.escape(text[pos:], quote=False))
    return ''.join(out)


def _cells(line):
    line = line.strip()
    if line.startswith('|'):
        line = line[1:]
    if line.endswith('|') and not line.endswith('\\|'):
        line = line[:-1]
    cells = re.split(r'(?<!\\)\|', line)
    return [c.strip().replace('\\|', '|') for c in cells]


def convert(markdown, source):
    """HTML de un manual. ``source`` es la ruta relativa a docs/sgi (para las ligas)."""
    lines = markdown.replace('\r\n', '\n').split('\n')
    blocks = []
    i, n = 0, len(lines)
    while i < n:
        line = lines[i]
        stripped = line.strip()
        if not stripped:
            i += 1
            continue
        heading = HEADING.match(stripped)
        if heading and not line.startswith(' '):
            level = len(heading.group(1))
            blocks.append('<h%d>%s</h%d>' % (level, inline(heading.group(2), source), level))
            i += 1
            continue
        if stripped.startswith('|') and i + 1 < n and TABLE_SEP.match(lines[i + 1].strip()):
            head = _cells(stripped)
            rows = []
            i += 2
            while i < n and lines[i].strip().startswith('|'):
                rows.append(_cells(lines[i]))
                i += 1
            parts = ['<table class="table table-bordered">', '<thead><tr>']
            parts += ['<th>%s</th>' % inline(c, source) for c in head]
            parts.append('</tr></thead>')
            parts.append('<tbody>')
            for row in rows:
                parts.append('<tr>%s</tr>' % ''.join('<td>%s</td>' % inline(c, source) for c in row))
            parts.append('</tbody></table>')
            blocks.append(''.join(parts))
            continue
        if stripped.startswith('>'):
            quote = []
            while i < n and lines[i].strip().startswith('>'):
                quote.append(lines[i].strip()[1:].strip())
                i += 1
            blocks.append('<blockquote><p>%s</p></blockquote>' % inline(' '.join(q for q in quote if q), source))
            continue
        ul, ol = UL_ITEM.match(stripped), OL_ITEM.match(stripped)
        if (ul or ol) and not line.startswith(' '):
            tag, pattern = ('ul', UL_ITEM) if ul else ('ol', OL_ITEM)
            items = []
            while i < n:
                current = lines[i]
                item = pattern.match(current.strip()) if not current.startswith(' ') else None
                if item:
                    items.append([item.group(1)])
                    i += 1
                elif current.strip() and current.startswith(' ') and items:
                    items[-1].append(current.strip())
                    i += 1
                elif not current.strip() and i + 1 < n and items and (
                        pattern.match(lines[i + 1].strip()) and not lines[i + 1].startswith(' ')):
                    i += 1
                else:
                    break
            blocks.append('<%s>%s</%s>' % (tag, ''.join(
                '<li>%s</li>' % inline(' '.join(parts), source) for parts in items), tag))
            continue
        paragraph = []
        while i < n and lines[i].strip():
            nxt = lines[i].strip()
            if paragraph and (HEADING.match(nxt) or nxt.startswith(('|', '>'))
                              or ((UL_ITEM.match(nxt) or OL_ITEM.match(nxt)) and not lines[i].startswith(' '))):
                break
            paragraph.append(nxt)
            i += 1
        blocks.append('<p>%s</p>' % inline(' '.join(paragraph), source))
    return '\n'.join(blocks) + '\n'


def generate():
    """{archivo de salida: HTML} de los nueve manuales."""
    files = {}
    for out, src in MANUALS:
        with open(os.path.join(DOCS, src), encoding='utf-8') as handle:
            body = convert(handle.read(), src)
        header = ('<!-- Generado por tools/sgi_knowledge_html.py desde docs/sgi/%s; no editar a mano: '
                  'correr `python3 tools/sgi_knowledge_html.py`. -->\n' % src)
        files[out] = header + body
    return files


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--check', action='store_true',
                    help='no escribe; sale con 1 si lo generado difiere de lo comprometido')
    opts = ap.parse_args()
    files = generate()
    if not opts.check:
        os.makedirs(OUT, exist_ok=True)
        for name, text in files.items():
            with open(os.path.join(OUT, name), 'w', encoding='utf-8') as handle:
                handle.write(text)
        print('addons/quimibond_sgi_knowledge/data/manuales: %d archivos generados.' % len(files))
        return 0
    stale = []
    for name, text in sorted(files.items()):
        path = os.path.join(OUT, name)
        current = open(path, encoding='utf-8').read() if os.path.exists(path) else None
        if current != text:
            stale.append(name)
    for name in stale:
        print('DESACTUALIZADO addons/quimibond_sgi_knowledge/data/manuales/%s' % name)
    if stale:
        print('%d manual(es) desactualizado(s): correr `python3 tools/sgi_knowledge_html.py` y comprometer.'
              % len(stale))
        return 1
    print('Manuales del SGI en Conocimiento al día (%d archivos).' % len(files))
    return 0


if __name__ == '__main__':
    sys.exit(main())
