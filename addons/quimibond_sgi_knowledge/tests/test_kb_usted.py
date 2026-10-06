# -*- coding: utf-8 -*-
"""1.2.0: «usted» y glosario (Jefe MAST, NC, CoA) en el satélite y en los
manuales que siembra. Mismas expresiones que quimibond_sgi/tests/test_usted.py
(que solo recorre el núcleo)."""
import html
import os
import re

from odoo.tests import tagged
from odoo.tests.common import BaseCase

from odoo.addons.quimibond_sgi.tests.test_usted import (
    GLOSARIO, GLOSARIO_EXCEPCIONES, QUOTED, SUSTANTIVOS, TUTEO)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SKIP_DIRS = {'tests', 'migrations', 'static', 'manuales'}
# En los manuales cada celda o renglón de lista es un «inicio de oración»:
# rutas de menú y nombres de grupo que empiezan como un imperativo de «tú».
MANUAL_SUSTANTIVOS = re.compile(r"\bPrograma →|\bCaptura de eficiencias\b")
BLOCK = re.compile(r'<(p|li|td|th|h[1-6]|blockquote)\b[^>]*>(.*?)</\1>', re.S)


def _clean(text, pattern):
    if pattern is TUTEO:
        return SUSTANTIVOS.sub('', text)
    return GLOSARIO_EXCEPCIONES.sub('', text)


def _offending_sources(pattern):
    hits = []
    for folder, dirs, files in os.walk(ROOT):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for name in files:
            if not name.endswith(('.py', '.xml')):
                continue
            path = os.path.join(folder, name)
            with open(path, encoding='utf-8') as handle:
                for number, line in enumerate(handle, 1):
                    stripped = line.strip()
                    if stripped.startswith(('#', '<!--', '"""', "'''")):
                        continue
                    for match in QUOTED.finditer(line):
                        text = _clean(match.group(1) or match.group(2), pattern)
                        if ' ' in text and pattern.search(text):
                            hits.append("%s:%d: %s" % (os.path.relpath(path, ROOT), number, text))
    return hits


def _offending_manuals(pattern):
    hits = []
    folder = os.path.join(ROOT, 'data', 'manuales')
    for name in sorted(os.listdir(folder)):
        if not name.endswith('.html'):
            continue
        with open(os.path.join(folder, name), encoding='utf-8') as handle:
            source = handle.read()
        for match in BLOCK.finditer(source):
            text = html.unescape(re.sub(r'<[^>]+>', '', match.group(2)))
            text = _clean(MANUAL_SUSTANTIVOS.sub('', text), pattern)
            if pattern.search(text):
                hits.append("%s: %s" % (name, text[:160]))
    return hits


@tagged('post_install', '-at_install')
class TestKbUsted(BaseCase):

    def test_01_sin_tuteo(self):
        hits = _offending_sources(TUTEO) + _offending_manuals(TUTEO)
        self.assertFalse(hits, "Textos en «tú»:\n" + "\n".join(hits))

    def test_02_glosario(self):
        hits = _offending_sources(GLOSARIO) + _offending_manuals(GLOSARIO)
        self.assertFalse(hits, "Fuera del glosario:\n" + "\n".join(hits))
