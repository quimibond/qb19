# -*- coding: utf-8 -*-
"""57.92.0 (U-06): textos para el usuario en «usted» y con el glosario del
README (Jefe MAST, NC, CoA). Lee las fuentes del módulo: no necesita datos."""
import os
import re

from odoo.tests import tagged
from odoo.tests.common import BaseCase

# Solo formas que no pueden ser tercera persona. «pide», «revisa», «agrega»,
# «elige», «levanta» o «contesta» también son «usted/él» («Persona que levanta
# la NC»): como imperativo de «tú» solo cuentan al inicio de oración o tras «¿».
TUTEO = re.compile(
    r"\b(tienes|puedes|quieres|confirmas|leíste|tu|tus|Tu|Tus|Tú|te ligue|apruébala|apágalo|adjúntala)\b"
    r"|(?:^|[.!?¿:]\s*)(Pide|Elige|Revisa|Agrega|Captura|Programa|Contesta|Levanta|Usa|Pega|Sube)\b"
    # Imperativos de «tú» con pronombre pegado (el acento cae donde el de
    # «usted» no: «apágalo» / «apáguelo», «corrígelo» / «corríjalo») y
    # frases que solo dicen «tú». Lista explícita de lo que ya se corrigió.
    r"|\b([Pp]ulsa|vuelve a (?:probar|generar)|[Cc]orrígel[oa]s?|[Aa]pruébal[oa]s?"
    r"|[Cc]onfigúral[oa]s?|[Dd]esactíval[oa]s?|[Ss]elecciónal[oa]s?|[Pp]ublícal[oa]s?"
    r"|[Rr]ecalcúlal[oa]s?|[Ee]nvíal[oa]s?|[Rr]evísal[oa]s?|[Mm]ándal[oa]s?|[Qq]uítal[oa]s?"
    r"|[Aa]págal[oa]s?|[Mm]árcal[oa]s?|[Ii]nstálal[oa]s?|[Ee]nlázal[oa]s?|[Aa]sígnal[oa]s?"
    r"|[Cc]aptúral[oa]s?|[Dd]ecláral[oa]s?|[Dd]ecídel[oa]s?|[Dd]efínel[oa]s?|[Ii]mprímel[oa]s?"
    r"|[Dd]éjal[oa]s?|[Ll]lámale)\b"
    # «Corrige» / «Corrija», «Pasa el mouse» / «Pase», «da clic» / «dé clic».
    r"|\bCorrige\b|\b[Pp]asa el mouse\b|\bda clic\b")
# Excepciones explícitas: sustantivos que empiezan como un imperativo de «tú»
# («Programa de auditorías», el modo de indicador «Captura manual», la clase de
# valor «Agrega valor»). Se quitan del texto antes de buscar; no relajan TUTEO.
SUSTANTIVOS = re.compile(
    r"\bPrograma (?:de|anual|semanal|sugerido|por)\b|\bCaptura manual\b|^Agrega valor$")
GLOSARIO = re.compile(r"Jefe de MAST|\bNCs\b|\bCOA\b|No Conformidad(?:es)?\b")
# Excepciones explícitas del glosario (texto exacto):
# - «SGI > No Conformidades»: ruta de menú de Odoo que se interpreta para hallar
#   el menú real (sgi_document.py, «Destino en Odoo»); no es redacción.
# - Nombres de registros noupdate que ya existen en producción y no se tocan
#   desde el XML: el cron «SGI: Seguimiento de No Conformidades» y el
#   indicador «Cierre de No Conformidades (NCA/NCD)».
GLOSARIO_EXCEPCIONES = re.compile(
    r"SGI > No Conformidades|^SGI: Seguimiento de No Conformidades$"
    r"|^Cierre de No Conformidades \(NCA/NCD\)$")
# Cadenas entre comillas en .py y valores/atributos en .xml.
QUOTED = re.compile(r"\"([^\"\n]{4,})\"|'([^'\n]{4,})'")
SKIP_DIRS = {'tests', 'migrations', 'static', 'tools', 'demo'}


def _offending(pattern):
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    hits = []
    for folder, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for name in files:
            if not name.endswith(('.py', '.xml')):
                continue
            path = os.path.join(folder, name)
            with open(path, encoding='utf-8') as handle:
                for number, line in enumerate(handle, 1):
                    stripped = line.strip()
                    # Comentarios y docstrings («Agrega al modelo…» describe el
                    # método en tercera persona; no lo ve el usuario).
                    if stripped.startswith(('#', '<!--', '"""', "'''")):
                        continue
                    for match in QUOTED.finditer(line):
                        text = match.group(1) or match.group(2)
                        if pattern is TUTEO:
                            text = SUSTANTIVOS.sub('', text)
                        elif pattern is GLOSARIO:
                            text = GLOSARIO_EXCEPCIONES.sub('', text)
                        if ' ' in text and pattern.search(text):
                            hits.append("%s:%d: %s" % (os.path.relpath(path, root), number, text))
    return hits


@tagged('post_install', '-at_install')
class TestUsted(BaseCase):

    def test_01_sin_tuteo(self):
        hits = _offending(TUTEO)
        self.assertFalse(hits, "Textos en «tú»:\n" + "\n".join(hits))

    def test_02_glosario(self):
        hits = _offending(GLOSARIO)
        self.assertFalse(hits, "Fuera del glosario:\n" + "\n".join(hits))
