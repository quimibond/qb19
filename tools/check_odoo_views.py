#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Checks estáticos que reproducen lo que Odoo revisa al cargar un módulo y
que el CI no podía ver porque el SGI depende de Enterprise (no se instala en
la imagen community). Nacen de tres builds rotos de `main` el 2026-09-25:

1. **Vistas de búsqueda, gráfica, pivote, calendario y actividad** se validan
   contra los RNG oficiales de Odoo 19 (`tools/odoo_rng/`, copiados de
   `odoo/addons/base/rng/`). Un `<group expand="0">` o `<group string="…">`
   en un `<search>` rompe el update aunque el XML sea válido.
2. **Orden de importación de modelos**: un `from .modulo import x` a nivel de
   módulo, cuando `modulo` define modelos y va DESPUÉS en `models/__init__.py`,
   registra sus clases antes que las que heredan (`Model 'sgi.activity.role'
   does not exist in registry`). Ese import debe ser local (dentro de la
   función) o el módulo debe reordenarse.

Uso: `python3 tools/check_odoo_views.py [ruta/a/addons ...]` (sin argumentos
revisa `addons/` y los módulos de la raíz). Sale con 1 si hay errores.
"""
import glob
import os
import re
import sys

from lxml import etree

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RNG_DIR = os.path.join(ROOT, 'tools', 'odoo_rng')
RNG_BY_TAG = {
    'search': 'search_view.rng',
    'graph': 'graph_view.rng',
    'pivot': 'pivot_view.rng',
    'calendar': 'calendar_view.rng',
    'activity': 'activity_view.rng',
}
_MODEL_CLASS = re.compile(r"class \w+\(models\.(Model|AbstractModel|TransientModel)\)")
_MODEL_NAME = re.compile(r"^\s+_name = '([\w.]+)'", re.M)
_MODEL_INHERIT = re.compile(r"^\s+_inherit = (?:'([\w.]+)'|\[([^\]]*)\])", re.M)
_LOCAL_IMPORT = re.compile(r"^from \.(\w+) import ", re.M)
_INIT_IMPORT = re.compile(r"^from \. import (\w+)", re.M)


def _validators():
    validators = {}
    for tag, filename in RNG_BY_TAG.items():
        path = os.path.join(RNG_DIR, filename)
        if os.path.exists(path):
            validators[tag] = etree.RelaxNG(etree.parse(path))
    return validators


def _module_dirs(paths):
    for base in paths:
        for manifest in glob.glob(os.path.join(base, '**', '__manifest__.py'), recursive=True):
            yield os.path.dirname(manifest)


def check_views(module_dir, validators):
    errors = []
    for xml_path in glob.glob(os.path.join(module_dir, '**', '*.xml'), recursive=True):
        try:
            tree = etree.parse(xml_path)
        except etree.XMLSyntaxError as exc:
            errors.append("%s: XML inválido: %s" % (os.path.relpath(xml_path, ROOT), exc))
            continue
        for record in tree.iter('record'):
            if record.get('model') != 'ir.ui.view':
                continue
            arch = next((f for f in record.findall('field') if f.get('name') == 'arch'), None)
            if arch is None or len(arch) == 0:
                continue
            view = arch[0]
            validator = validators.get(view.tag)
            if validator is None:
                continue
            if not validator.validate(view):
                for err in validator.error_log:
                    errors.append("%s (vista %s): %s" % (
                        os.path.relpath(xml_path, ROOT), record.get('id'), err.message))
    return errors


def _inherited_models(source):
    names = set()
    for single, many in _MODEL_INHERIT.findall(source):
        if single:
            names.add(single)
        else:
            names.update(re.findall(r"'([\w.]+)'", many))
    return names


def check_model_imports(module_dir):
    """Un `from .b import x` a nivel de módulo en `a` carga `b` en ese momento.
    Si `b` hereda (`_inherit`) un modelo que se define en un módulo que va
    después de `a` en __init__, ese modelo aún no existe en el registro y
    Odoo revienta al cargar. Importar módulos que solo definen modelos nuevos
    o heredan modelos ya registrados es válido."""
    errors = []
    init_path = os.path.join(module_dir, 'models', '__init__.py')
    if not os.path.exists(init_path):
        return errors
    order = _INIT_IMPORT.findall(open(init_path, encoding='utf-8').read())
    index = {name: i for i, name in enumerate(order)}
    sources = {}
    defined_at = {}
    for name in order:
        path = os.path.join(module_dir, 'models', name + '.py')
        if not os.path.exists(path):
            continue
        sources[name] = open(path, encoding='utf-8').read()
        for model in _MODEL_NAME.findall(sources[name]):
            defined_at.setdefault(model, index[name])
    for name, source in sources.items():
        for target in _LOCAL_IMPORT.findall(source):
            if target not in sources or index[target] <= index[name]:
                continue
            late = sorted(m for m in _inherited_models(sources[target])
                          if defined_at.get(m, -1) > index[name])
            if late:
                errors.append(
                    "%s: importa .%s a nivel de módulo; %s hereda %s, que se define después en "
                    "models/__init__.py y aún no existe en el registro. Hazlo import local." % (
                        os.path.relpath(os.path.join(module_dir, 'models', name + '.py'), ROOT),
                        target, target, ", ".join(late)))
    return errors


def main(argv):
    paths = argv[1:] or [os.path.join(ROOT, 'addons'), ROOT]
    validators = _validators()
    if not validators:
        print("Sin RNG en tools/odoo_rng: no se validan vistas.")
    errors = []
    seen = set()
    for module_dir in _module_dirs(paths):
        if module_dir in seen or '/.git/' in module_dir:
            continue
        seen.add(module_dir)
        errors += check_views(module_dir, validators)
        errors += check_model_imports(module_dir)
    for err in errors:
        print("ERROR:", err)
    print("%d error(es) en vistas RNG e imports de modelos." % len(errors))
    return 1 if errors else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
