#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Genera docs/sgi/transicion/miid-rev03-borrador.md desde la siembra del
Manual del SGI (MIID) y del P-G01 (addons/quimibond_sgi/data/sgi_miid_sections.xml y, desde
57.161.0, el formato y los renglones de data/sgi_miid_items.xml).

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
ITEMS = os.path.join(ROOT, 'addons', 'quimibond_sgi', 'data', 'sgi_miid_items.xml')
MENUS = os.path.join(ROOT, 'addons', 'quimibond_sgi', 'views', 'sgi_menus.xml')
TITLES = ('item_label_title', 'item_clause_title', 'item_text_title', 'item_text2_title', 'item_text3_title')
COLUMNS = ('label', 'clause', 'text', 'text_2', 'text_3')
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


def _menu_paths():
    """{xmlid: «SGI → Sistema → …»} de los menús del módulo."""
    menus = {}
    for item in etree.parse(MENUS).xpath('//menuitem'):
        menus[item.get('id')] = (item.get('name'), item.get('parent'))
    paths = {}
    for xmlid in menus:
        parts, node = [], xmlid
        while node in menus:
            parts.append(menus[node][0])
            node = menus[node][1]
        paths[xmlid] = ' → '.join(reversed(parts))
    return paths


def _items():
    """57.161.0: ({sección: formato}, {sección: [renglón]}) de la siembra de renglones."""
    paths = _menu_paths()
    tree = etree.parse(ITEMS)
    formats, items = {}, {}
    for record in tree.xpath('//record[@model="sgi.miid.section"]'):
        data = {}
        for field in record.findall('field'):
            if field.get('ref'):
                data[field.get('name')] = paths.get(field.get('ref'), field.get('ref'))
            else:
                data[field.get('name')] = (field.text or '').strip()
        formats[record.get('id')] = data
    for record in tree.xpath('//record[@model="sgi.miid.item"]'):
        data = {}
        for field in record.findall('field'):
            if field.get('name') == 'menu_id':
                data['menu'] = paths.get(field.get('ref'), field.get('ref'))
            else:
                data[field.get('name')] = (field.get('ref') or field.text or '').strip()
        items.setdefault(data['section_id'], []).append(data)
    for rows in items.values():
        rows.sort(key=lambda d: int(d.get('sequence') or 0))
    return formats, items


def _items_md(fmt, rows):
    if not rows:
        return ''
    with_menu = any(r.get('menu') for r in rows)
    if fmt.get('item_style') == 'list':
        out = []
        for r in rows:
            line = '- **%s**' % r['label'] if r.get('text') else '- %s' % r['label']
            if r.get('clause'):
                line += ' (%s)' % r['clause']
            if r.get('text'):
                line += ': ' + r['text']
            if r.get('menu'):
                line += ' — En Odoo: ' + r['menu']
            out.append(line)
        return '\n'.join(out)
    cols = [(c, fmt.get(t) or ('Concepto' if c == 'label' else '')) for c, t in zip(COLUMNS, TITLES)]
    cols = [(c, t) for c, t in cols if c == 'label' or t]
    head = [t for _c, t in cols] + (['En Odoo'] if with_menu else [])
    lines = ['| ' + ' | '.join(head) + ' |', '| ' + ' | '.join('---' for _ in head) + ' |']
    for r in rows:
        cells = [('**%s**' % r['label']) if c == 'label' else r.get(c, '') for c, _t in cols]
        if with_menu:
            cells.append(r.get('menu', ''))
        lines.append('| ' + ' | '.join(x.replace('|', '/') for x in cells) + ' |')
    return '\n'.join(lines)


def main():
    blocks, kinds = _constants()
    formats, items = _items()
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
Desde 57.161.0 las listas y tablas son renglones (data/sgi_miid_items.xml) y las
rutas, menús ([En Odoo: …]).
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
            fmt = formats.get(section['xmlid'], {})
            if fmt.get('menu_id'):
                meta.append('[En Odoo: %s]' % fmt['menu_id'])
            if meta:
                lines.append('\n'.join(meta))
            body = _html_to_md(section.get('body', ''))
            rows = _items_md(fmt, items.get(section['xmlid'], []))
            if body and rows and '`[[datos]]`' in body:
                before, after = body.split('`[[datos]]`', 1)
                body = before.rstrip() + '\n\n' + rows + '\n\n`[[datos]]`' + after
            elif rows:
                body = (body + '\n\n' if body else '') + rows
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
