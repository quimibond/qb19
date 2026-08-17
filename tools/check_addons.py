#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Checks estáticos de los addons: cazan en el PR los errores que hoy sólo
aparecen cuando Odoo intenta arrancar.

No necesita Odoo, ni Postgres, ni licencia de Enterprise — por eso cubre también
los módulos que dependen de Enterprise, que es donde vive la complejidad del SGI
y donde un CI con Odoo Community no puede llegar.

Cada check nació de un build caído de verdad. Ver el bloque de cada uno.

Uso:  python3 tools/check_addons.py [--base-ref origin/main]
"""
import argparse
import ast
import glob
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict

PROBLEMS = []       # (severidad, ruta, mensaje)


def error(path, msg):
    PROBLEMS.append(('ERROR', path, msg))


def warn(path, msg):
    PROBLEMS.append(('WARN', path, msg))


# ---------------------------------------------------------------------------
# Descubrimiento
# ---------------------------------------------------------------------------

def find_modules(root='.'):
    """{nombre_modulo: ruta} de todo dir con __manifest__.py."""
    mods = {}
    for man in glob.glob(os.path.join(root, '**', '__manifest__.py'), recursive=True):
        if '/node_modules/' in man or '/.git/' in man:
            continue
        path = os.path.dirname(man)
        mods[os.path.basename(path)] = path
    return mods


def read_manifest(path):
    try:
        return ast.literal_eval(open(os.path.join(path, '__manifest__.py')).read())
    except Exception as exc:
        error(os.path.join(path, '__manifest__.py'), "manifest ilegible: %s" % exc)
        return None


def xml_files(path):
    return sorted(glob.glob(os.path.join(path, '**', '*.xml'), recursive=True))


def py_files(path):
    return [f for f in sorted(glob.glob(os.path.join(path, '**', '*.py'), recursive=True))
            if '__pycache__' not in f]


def parse_xml(f):
    try:
        return ET.parse(f).getroot()
    except ET.ParseError as exc:
        error(f, "XML mal formado: %s" % exc)
        return None


# ---------------------------------------------------------------------------
# Check 1 — archivos del manifest que no existen
#   Un `data:` apuntando a un archivo inexistente mata la carga del módulo.
# ---------------------------------------------------------------------------

def check_manifest_files(mods, manifests):
    for name, path in mods.items():
        man = manifests.get(name)
        if not man:
            continue
        for key in ('data', 'demo', 'qweb'):
            for rel in man.get(key) or []:
                if not os.path.exists(os.path.join(path, rel)):
                    error(os.path.join(path, '__manifest__.py'),
                          "'%s' lista '%s', que no existe" % (key, rel))


# ---------------------------------------------------------------------------
# Check 2 — `_sql_constraints` (sintaxis muerta en Odoo 19)
#   Odoo 19 IGNORA el atributo: la restricción se declara y no se aplica.
#   Nos costó 7 restricciones de unicidad inexistentes en qb_capacidad_costeo.
# ---------------------------------------------------------------------------

def check_sql_constraints(mods):
    for name, path in mods.items():
        for f in py_files(path):
            for i, line in enumerate(open(f, encoding='utf-8'), 1):
                if re.match(r'\s*_sql_constraints\s*=', line):
                    error("%s:%d" % (f, i),
                          "_sql_constraints ya no se soporta en Odoo 19 y se ignora "
                          "en silencio — usa models.Constraint")


# ---------------------------------------------------------------------------
# Check 3 — claves de ir.config_parameter declaradas dos veces
#   Una clave declarada como <record> en un módulo y sembrada por código en otro
#   revienta la carga del registry entero en una instalación limpia:
#   la siembra crea la fila SIN xmlid, el <record> intenta el INSERT, y choca
#   contra ir_config_parameter_key_uniq. Nos tumbó el build con
#   quimibond_sgi.pesaje_tolerance_kg.
# ---------------------------------------------------------------------------

def check_config_param_dupes(mods):
    xml_keys = defaultdict(list)        # clave -> [(modulo, archivo)]
    for name, path in mods.items():
        for f in xml_files(path):
            root = parse_xml(f)
            if root is None:
                continue
            for rec in root.iter('record'):
                if rec.get('model') != 'ir.config_parameter':
                    continue
                for fl in rec:
                    if fl.get('name') == 'key' and fl.text:
                        xml_keys[fl.text.strip()].append((name, f))

    if not xml_keys:
        return

    py_keys = defaultdict(list)         # clave -> [(modulo, archivo)]
    for name, path in mods.items():
        for f in py_files(path):
            src = open(f, encoding='utf-8').read()
            for key in xml_keys:
                if "'%s'" % key in src or '"%s"' % key in src:
                    py_keys[key].append((name, f))

    for key, decls in xml_keys.items():
        seeders = [(m, f) for m, f in py_keys.get(key, []) if m != decls[0][0]]
        if seeders:
            error(decls[0][1],
                  "la clave '%s' se declara como <record> aquí y también la maneja "
                  "'%s' (%s). En una instalación limpia la siembra la crea sin xmlid "
                  "y este INSERT choca contra ir_config_parameter_key_uniq, tumbando "
                  "el registry. Déjala en UN solo lugar."
                  % (key, seeders[0][0], seeders[0][1]))
        elif len(decls) > 1:
            error(decls[0][1],
                  "la clave '%s' se declara como <record> en %d archivos: %s"
                  % (key, len(decls), ', '.join(f for _, f in decls)))
        elif py_keys.get(key):
            warn(decls[0][1],
                 "la clave '%s' se declara como <record> y además aparece en %s "
                 "(mismo módulo). Hoy funciona sólo por el orden de carga del "
                 "manifest; es frágil."
                 % (key, py_keys[key][0][1]))


# ---------------------------------------------------------------------------
# Check 4 — xmlids referenciados desde Python que no existen
#   Un env.ref() a un xmlid inexistente truena en runtime, no al cargar,
#   así que puede llegar hasta producción sin que nadie lo note.
# ---------------------------------------------------------------------------

REF_RE = re.compile(r"""(?:env\.ref|ref)\(\s*['"]([a-z0-9_]+)\.([a-zA-Z0-9_.\-]+)['"]""")


def check_env_refs(mods):
    declared = defaultdict(set)         # modulo -> {xmlid}
    for name, path in mods.items():
        for f in xml_files(path):
            root = parse_xml(f)
            if root is None:
                continue
            for tag in ('record', 'menuitem', 'template', 'report', 'act_window'):
                for node in root.iter(tag):
                    rid = node.get('id')
                    if not rid:
                        continue
                    if '.' in rid:
                        mod, rid = rid.split('.', 1)
                        declared[mod].add(rid)
                    else:
                        declared[name].add(rid)

    for name, path in mods.items():
        for f in py_files(path):
            src = open(f, encoding='utf-8').read()
            for i, line in enumerate(src.splitlines(), 1):
                for mod, xmlid in REF_RE.findall(line):
                    if mod not in mods:          # módulo de Odoo o de Enterprise
                        continue
                    if xmlid not in declared.get(mod, ()):
                        error("%s:%d" % (f, i),
                              "referencia a '%s.%s', que no está declarado en ningún "
                              "archivo de datos de '%s'" % (mod, xmlid, mod))


# ---------------------------------------------------------------------------
# Check 5 — ir.model.access.csv apuntando a modelos que no existen
#   Se detecta al instalar, con un error poco claro.
# ---------------------------------------------------------------------------

def check_access_models(mods):
    defined = set()                      # {'sgi.area', ...}
    for name, path in mods.items():
        for f in py_files(path):
            src = open(f, encoding='utf-8').read()
            defined |= set(re.findall(r"""(?<![a-zA-Z0-9_])_name\s*=\s*['"]([a-z0-9_.]+)['"]""", src))
    own_prefixes = {m.split('.')[0] for m in defined}

    for name, path in mods.items():
        csv_path = os.path.join(path, 'security', 'ir.model.access.csv')
        if not os.path.exists(csv_path):
            continue
        for i, line in enumerate(open(csv_path, encoding='utf-8'), 1):
            parts = line.strip().split(',')
            if i == 1 or len(parts) < 3:
                continue
            ref = parts[2].strip()
            if '.' in ref:               # modelo de otro módulo: no verificable aquí
                continue
            if not ref.startswith('model_'):
                continue
            model = ref[len('model_'):].replace('_', '.')
            # Sólo se juzgan los modelos con prefijo propio del repo: los de Odoo
            # y Enterprise no están aquí y su ausencia no prueba nada.
            if model.split('.')[0] not in own_prefixes or model in defined:
                continue
            # `a_b_c` es ambiguo: puede ser a.b.c o a.b_c. Se acepta cualquiera.
            tokens = ref[len('model_'):].split('_')
            if any('.'.join(tokens[:i]) + '.' + '_'.join(tokens[i:]) in defined
                   for i in range(1, len(tokens))):
                continue
            error("%s:%d" % (csv_path, i),
                  "da permisos a '%s', que ningún modelo del repo define" % ref)



# ---------------------------------------------------------------------------
# Check 7 — cambios en un módulo sin subir la versión del manifest
#   Odoo.sh sólo corre `-u` cuando cambia la versión. Sin bump, el cambio queda
#   en el repo y NUNCA llega a la base: vistas viejas, datos sin sembrar,
#   restricciones sin crear, modelos borrados que siguen en ir_model.
#   qb_capacidad_costeo acumuló 43 commits así, y de ahí salieron las tablas
#   faltantes, el modelo huérfano de Supabase y las 7 constraints sin aplicar.
#   Exenciones deliberadas y con motivo escrito: tools/no_bump.txt
# ---------------------------------------------------------------------------

def load_no_bump(root='.'):
    path = os.path.join(root, 'tools', 'no_bump.txt')
    if not os.path.exists(path):
        return set()
    return {ln.strip() for ln in open(path, encoding='utf-8')
            if ln.strip() and not ln.startswith('#')}


def check_version_bump(mods, base_ref, exempt):
    changed = subprocess.run(
        ['git', 'diff', '--name-only', '%s...HEAD' % base_ref],
        capture_output=True, text=True).stdout.split()
    touched = defaultdict(list)
    for f in changed:
        for name, path in mods.items():
            rel = os.path.relpath(path, '.')
            if f.startswith(rel + '/'):
                touched[name].append(f)

    for name, files in sorted(touched.items()):
        path = mods[name]
        man_rel = os.path.join(os.path.relpath(path, '.'), '__manifest__.py')
        old_man = subprocess.run(['git', 'show', '%s:%s' % (base_ref, man_rel)],
                                 capture_output=True, text=True)
        if old_man.returncode:
            continue                     # módulo nuevo: se instala, no se actualiza
        old_v = re.search(r"""'version'\s*:\s*['"]([^'"]+)['"]""", old_man.stdout)
        new_v = re.search(r"""'version'\s*:\s*['"]([^'"]+)['"]""",
                          open(os.path.join(path, '__manifest__.py'), encoding='utf-8').read())
        if not (old_v and new_v) or old_v.group(1) != new_v.group(1):
            continue                     # subió versión: todo bien
        if name in exempt:
            warn(man_rel,
                 "cambia %d archivo(s) sin subir la versión (%s). Está exento en "
                 "tools/no_bump.txt, así que ACUÉRDATE de correr `odoo-update %s` "
                 "al desplegar: si no, el cambio no llega a la base."
                 % (len(files), new_v.group(1), name))
            continue
        error(man_rel,
              "cambia %d archivo(s) sin subir la versión (%s). Odoo.sh sólo corre "
              "`-u` al cambiar la versión: así, este cambio NO llega a la base de "
              "datos. Sube la versión, o agrega '%s' a tools/no_bump.txt con el "
              "motivo y despliégalo con `odoo-update %s`."
              % (len(files), new_v.group(1), name, name))


# ---------------------------------------------------------------------------
# Check 6 — modelos nuevos sin subir la versión del manifest
#   Odoo.sh sólo corre `-u` cuando cambia la versión. Un modelo nuevo sin bump
#   queda en el registry SIN TABLA, y sólo truena cuando alguien lo usa.
#   Es lo que pasó con qb.cotizacion.tramo y qb.producto.ficha.
#   Sólo corre cuando hay rama base contra la que comparar.
# ---------------------------------------------------------------------------

def check_new_models_need_bump(mods, base_ref):
    def at_base(path):
        try:
            return subprocess.run(['git', 'show', '%s:%s' % (base_ref, path)],
                                  capture_output=True, text=True, check=True).stdout
        except subprocess.CalledProcessError:
            return None

    for name, path in mods.items():
        rel = os.path.relpath(path, '.')
        new_models = set()
        for f in py_files(path):
            fr = os.path.relpath(f, '.')
            old = at_base(fr)
            now = re.findall(r"""(?<![a-zA-Z0-9_])_name\s*=\s*['"]([a-z0-9_.]+)['"]""",
                             open(f, encoding='utf-8').read())
            before = re.findall(r"""(?<![a-zA-Z0-9_])_name\s*=\s*['"]([a-z0-9_.]+)['"]""", old or '')
            new_models |= set(now) - set(before)
        if not new_models:
            continue
        man_rel = os.path.join(rel, '__manifest__.py')
        old_man = at_base(man_rel)
        if old_man is None:
            continue                     # módulo nuevo entero: se instala, no se actualiza
        old_v = re.search(r"""'version'\s*:\s*['"]([^'"]+)['"]""", old_man)
        new_v = re.search(r"""'version'\s*:\s*['"]([^'"]+)['"]""",
                          open(os.path.join(path, '__manifest__.py'), encoding='utf-8').read())
        if old_v and new_v and old_v.group(1) == new_v.group(1):
            error(man_rel,
                  "agrega modelo(s) nuevo(s) [%s] sin subir la versión (%s). Odoo.sh "
                  "sólo corre `-u` al cambiar la versión: sin bump, esos modelos quedan "
                  "en el registry SIN TABLA. Sube la versión o corre `odoo-update %s` a "
                  "mano al desplegar."
                  % (', '.join(sorted(new_models)), new_v.group(1), name))


# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--base-ref', default=os.environ.get('BASE_REF') or '')
    args = ap.parse_args()

    mods = find_modules('.')
    manifests = {n: read_manifest(p) for n, p in mods.items()}
    print("Revisando %d módulos: %s\n" % (len(mods), ', '.join(sorted(mods))))

    check_manifest_files(mods, manifests)
    check_sql_constraints(mods)
    check_config_param_dupes(mods)
    check_env_refs(mods)
    check_access_models(mods)
    if args.base_ref:
        check_new_models_need_bump(mods, args.base_ref)
        check_version_bump(mods, args.base_ref, load_no_bump('.'))
    else:
        print("(checks de versión omitidos: sin --base-ref)\n")

    errors = [p for p in PROBLEMS if p[0] == 'ERROR']
    warns = [p for p in PROBLEMS if p[0] == 'WARN']
    for sev, path, msg in errors + warns:
        print("%-5s %s\n      %s\n" % (sev, path, msg))
    print("%d error(es), %d advertencia(s)." % (len(errors), len(warns)))
    return 1 if errors else 0


if __name__ == '__main__':
    sys.exit(main())
