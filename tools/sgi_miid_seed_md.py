#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Genera docs/sgi/transicion/miid-rev03-borrador.md desde la siembra del
Manual del SGI (MIID) y del P-G01 (addons/quimibond_sgi/data/sgi_miid_sections.xml).

Es la referencia a la que el aviso de la siembra manda comparar cuando una
sección editada a mano recibe texto nuevo. No se edita a mano: se cambia la
siembra y se vuelve a correr:

    python3 tools/sgi_miid_seed_md.py

Marcas: [VIVO: bloque] = datos que pone el sistema; `[[datos]]` = dónde van;
[POR CONFIRMAR: nota] = sección sembrada por confirmar."""
import ast
import os
import re
import sys

from lxml import etree, html as lhtml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SEED = os.path.join(ROOT, 'addons', 'quimibond_sgi', 'data', 'sgi_miid_sections.xml')
MODEL = os.path.join(ROOT, 'addons', 'quimibond_sgi', 'models', 'sgi_miid.py')
OUT = os.path.join(ROOT, 'docs', 'sgi', 'transicion', 'miid-rev03-borrador.md')


def _constants():
    src = open(MODEL, encoding='utf-8').read()
    blocks = dict(ast.literal_eval('[' + re.search(r'MIID_BLOCKS = \[(.*?)\n\]', src, re.S).group(1) + ']'))
    kinds = ast.literal_eval(re.search(r'MIID_KINDS = (\{.*?\n\})\n', src, re.S).group(1))
    return blocks, kinds


def _cell(el):
    return ' '.join(''.join(el.itertext()).split())


def _inline(el):
    parts = [el.text or '']
    for child in el:
        text = _cell(child)
        if child.tag in ('b', 'strong'):
            parts.append('**%s**' % text)
        elif child.tag in ('i', 'em'):
            parts.append('*%s*' % text)
        else:
            parts.append(text)
        parts.append(child.tail or '')
    return ' '.join(''.join(parts).split()).replace('[[datos]]', '`[[datos]]`')


def _html_to_md(fragment):
    if not fragment.strip():
        return ''
    root = lhtml.fragment_fromstring('<div>%s</div>' % fragment)
    out = []
    for el in root:
        if el.tag == 'p':
            out.append(_inline(el))
        elif el.tag in ('ul', 'ol'):
            for index, li in enumerate(el.findall('li'), 1):
                out.append(('- ' if el.tag == 'ul' else '%d. ' % index) + _inline(li))
        elif el.tag == 'table':
            lines = []
            for row_index, tr in enumerate(el.xpath('.//tr')):
                cells = [_cell(c) for c in tr if c.tag in ('th', 'td')]
                lines.append('| ' + ' | '.join(cells) + ' |')
                if row_index == 0:
                    lines.append('| ' + ' | '.join('---' for _ in cells) + ' |')
            out.append('\n'.join(lines))
        elif el.tag in ('h1', 'h2', 'h3', 'h4'):
            out.append('**' + _cell(el) + '**')
        else:
            out.append(_cell(el))
    return '\n\n'.join(x for x in out if x)


def main():
    blocks, kinds = _constants()
    tree = etree.parse(SEED)
    sections, notes = [], {}
    for record in tree.xpath('//record[@model="sgi.miid.section"]'):
        data = {'xmlid': record.get('id'), 'kind': 'miid', 'to_confirm': False}
        for field in record.findall('field'):
            name = field.get('name')
            if name == 'body':
                data['body'] = (field.text or '') + ''.join(etree.tostring(c, encoding='unicode') for c in field)
            elif name == 'to_confirm':
                data['to_confirm'] = field.get('eval') in ('True', '1')
            elif name == 'body_fallback':
                data['body_fallback'] = field.get('eval') in ('True', '1')
            else:
                data[name] = (field.text or '').strip()
        sections.append(data)
    for record in tree.xpath('//record[@model="sgi.miid.row.note"]'):
        data = {f.get('name'): (f.get('ref') or f.text or '').strip() for f in record.findall('field')}
        notes.setdefault(data['section_id'], []).append((data['key'], data['text']))

    lines = ["""<!-- Texto fijo del Manual del SGI (MIID) y del P-G01 generados desde Odoo.
GENERADO por tools/sgi_miid_seed_md.py desde addons/quimibond_sgi/data/sgi_miid_sections.xml;
es la referencia a la que el aviso de la siembra manda comparar cuando una
sección editada a mano recibe texto nuevo. No se edita a mano.
El borrador Rev. 03 del 5-oct-2026 (46 secciones en prosa) ya no es la fuente
(Dirección 2026-10-13, «el MIID está como en el Dropbox»); desde 57.148.0 el
P-G01 es un documento generado propio con el mismo motor.
Marcas: [VIVO: bloque] = datos que pone el sistema; `[[datos]]` = dónde van. -->

# Texto fijo de los documentos generados (siembra)
"""]
    for kind, values in kinds.items():
        mine = sorted((s for s in sections if s['kind'] == kind), key=lambda d: int(d['sequence']))
        lines.append("\n# %s\n\n%d secciones; bloques vivos: %s.\n" % (
            values['title'], len(mine), ', '.join(values['blocks'])))
        for section in mine:
            level = int(section.get('heading_level') or 2)
            heading = ' '.join(x for x in (section.get('clause'), section.get('name')) if x)
            lines.append('#' * (level + 1) + ' ' + heading)
            meta = []
            if section.get('live_block'):
                meta.append('[VIVO: %s — %s]' % (section['live_block'], blocks.get(section['live_block'], '')))
            if section['to_confirm']:
                meta.append('[POR CONFIRMAR: %s]' % section.get('to_confirm_note', ''))
            if section.get('body_fallback'):
                meta.append('[Texto solo si no hay datos]')
            if meta:
                lines.append('\n'.join(meta))
            body = _html_to_md(section.get('body', ''))
            if body:
                lines.append(body)
            if section['xmlid'] in notes:
                lines.append('Notas por renglón (`sgi.miid.row.note`):\n\n' + '\n'.join(
                    '- **%s**: %s' % kv for kv in notes[section['xmlid']]))
            lines.append('')
    text = '\n\n'.join(lines)
    while '\n\n\n' in text:
        text = text.replace('\n\n\n', '\n\n')
    with open(OUT, 'w', encoding='utf-8') as handle:
        handle.write(text)
    print("%s: %d secciones (%s)" % (os.path.relpath(OUT, ROOT), len(sections),
                                     ', '.join('%s %d' % (k, sum(1 for s in sections if s['kind'] == k))
                                               for k in kinds)))
    return 0


if __name__ == '__main__':
    sys.exit(main())
