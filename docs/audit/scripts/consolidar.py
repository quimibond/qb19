# -*- coding: utf-8 -*-
"""Junta las tablas de hallazgos de docs/audit/NN-*.md en un CSV (fase 2)."""
import collections
import csv
import glob
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
AUDIT = os.path.dirname(HERE)
COLS = ['id', 'elemento', 'hallazgo', 'evidencia', 'severidad', 'accion', 'propuesta', 'esfuerzo', 'depende']


def split_row(line):
    cells, cur, tick, i = [], '', False, 0
    s = line.strip()
    if s.startswith('|'):
        s = s[1:]
    if s.endswith('|'):
        s = s[:-1]
    while i < len(s):
        ch = s[i]
        if ch == '\\' and i + 1 < len(s) and s[i + 1] == '|':
            cur += '|'
            i += 2
            continue
        if ch == '`':
            tick = not tick
        if ch == '|' and not tick:
            cells.append(cur.strip())
            cur = ''
        else:
            cur += ch
        i += 1
    cells.append(cur.strip())
    return cells


def clean(value):
    return re.sub(r'[*_]', '', value or '').strip()


def load():
    rows = []
    for path in sorted(glob.glob(os.path.join(AUDIT, '[01][0-9]-*.md'))):
        for line in open(path, encoding='utf-8'):
            if not re.match(r'^\|\s*[A-L]-\d{3}\s*\|', line):
                continue
            cells = split_row(line)
            if len(cells) > 9:  # celdas con | sin backticks: se pegan en «hallazgo»
                extra = len(cells) - 9
                cells = cells[:2] + [' | '.join(cells[2:3 + extra])] + cells[3 + extra:]
            cells += [''] * (9 - len(cells))
            row = dict(zip(COLS, cells))
            row['archivo'] = os.path.basename(path)
            sev = clean(row['severidad']).split()
            row['sev'] = sev[0] if sev else ''
            acc = clean(row['accion']).split()
            row['acc'] = acc[0] if acc else ''
            nums = re.findall(r'\d+(?:\.\d+)?', row['esfuerzo'])
            row['horas'] = float(nums[0]) if nums else 0.0
            rows.append(row)
    return rows


if __name__ == '__main__':
    rows = load()
    print(len(rows))
    print(collections.Counter(r['sev'] for r in rows))
    print(collections.Counter(r['acc'] for r in rows))
    bad = [r['id'] for r in rows if r['sev'] not in ('Crítica', 'Alta', 'Media', 'Baja')]
    print('sev raras', bad)
    bad = [r['id'] for r in rows if r['acc'] not in ('Eliminar', 'Corregir', 'Agregar', 'Mover', 'Documentar', 'Decidir')]
    print('acc raras', bad)
    print('horas', sum(r['horas'] for r in rows))
    with open(os.path.join(AUDIT, '99-consolidado', 'hallazgos.csv'), 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['id', 'archivo', 'sev', 'acc', 'horas'] + COLS[1:4] + COLS[6:])
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, '') for k in w.fieldnames})
