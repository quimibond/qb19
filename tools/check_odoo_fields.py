#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Campos de las vistas contra los modelos reales (repo + código de Odoo 19).

Lo que tumbó el build de producción el 2026-10-09: una columna
`lot_producing_id` en la lista de órdenes de muestra del proyecto (57.136.0).
En Odoo 19 ese campo se llama `lot_producing_ids`; el singular era de Odoo
17/18. El CI no lo vio porque el SGI depende de Enterprise y sus vistas
`form`/`list`/`kanban` solo se validan al cargar el módulo en Odoo.sh.

Este checker reproduce esa validación de forma estática:

1. Arma un índice de campos leyendo con `ast` todos los `models/`, `wizard/`
   y `report/` del repo y, si se le da `--odoo-path`, los de Odoo 19
   Community (la imagen `odoo:19.0` del CI, o un clon). Resuelve `_inherit`
   (extensiones y mixins como `mail.thread`) e `_inherits` (delegación,
   `product.product` → `product.template`).
2. Recorre cada `<record model="ir.ui.view">` con `arch` XML y comprueba que
   cada `<field name="…">` exista en el modelo que le toca: el de la vista,
   o el comodelo cuando está dentro de una lista/forma incrustada o de un
   `xpath` que cruza un `field[@name='…']` relacional.

Lo que no sabe: campos de módulos Enterprise sobre modelos Community
(`quality`, `planning`, `documents`…), campos creados por Studio (`x_…`, se
omiten) y campos que Odoo agrega en tiempo de ejecución. Para los primeros
está `tools/odoo_fields_allow.txt` (`modelo.campo` por renglón, cada uno
verificado contra producción con `get_fields` por MCP). Un modelo del que no
hay código (Enterprise) no se revisa.

Uso: `python3 tools/check_odoo_fields.py [--odoo-path /ruta/a/odoo] [--verbose] [ruta/a/addons ...]`
Sale con 1 si hay errores.
"""
import ast
import os
import re
import sys

from lxml import etree

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ALLOW_FILE = os.path.join(ROOT, 'tools', 'odoo_fields_allow.txt')
FIELD_TYPES = {
    'Boolean', 'Integer', 'Float', 'Monetary', 'Char', 'Text', 'Html', 'Date', 'Datetime', 'Binary',
    'Image', 'Selection', 'Reference', 'Many2one', 'One2many', 'Many2many', 'Many2oneReference',
    'Json', 'Properties', 'PropertiesDefinition', 'Id',
}
RELATIONAL = {'Many2one', 'One2many', 'Many2many'}
MAGIC = {'id', 'display_name', 'create_date', 'create_uid', 'write_date', 'write_uid', '__last_update'}
SOURCE_DIRS = {'models', 'wizard', 'wizards', 'report', 'reports', 'controllers'}
SKIP_DIRS = {'tests', 'static', 'i18n', 'node_modules', '.git', '__pycache__'}
# Etiquetas cuyo atributo `name` NO es un campo del modelo.
NOT_A_FIELD = {'filter', 'button', 'widget', 'page', 'group', 'div', 'span', 'separator', 'header', 'footer',
               'notebook', 'sheet', 'label', 'xpath', 'templates', 't', 'a', 'kanban', 'list', 'tree', 'form',
               'control', 'create', 'setting', 'block', 'app', 'searchpanel', 'properties', 'approve', 'img',
               'strong', 'em', 'i', 'b', 'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'li', 'ol', 'table',
               'tr', 'td', 'th', 'thead', 'tbody', 'small', 'br', 'hr', 'progressbar', 'aside', 'main',
               'section', 'article', 'nav', 'footer', 'chatter', 'cohort', 'graph', 'pivot', 'calendar',
               'activity', 'gantt', 'map', 'hierarchy', 'dashboard', 'view', 'field_group', 'record', 'odoo',
               'data', 'newline', 'attribute', 'button_box', 'o_stat_info', 'select', 'option', 'input',
               'textarea', 'style', 'script', 'svg', 'path', 'g', 'rect', 'circle', 'text', 'tspan', 'line'}
_XPATH_FIELD = re.compile(r"field\[@name=['\"]([\w.]+)['\"]\]")


# ----------------------------------------------------------------------------
# Índice de campos
# ----------------------------------------------------------------------------
def _const(node):
    return node.value if isinstance(node, ast.Constant) else None


def _names(node):
    """'a.b' → ['a.b']; ['a', 'b'] → ['a', 'b']; {'a': 'x'} → ['a']."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, (ast.List, ast.Tuple)):
        return [v for v in (_const(e) for e in node.elts) if isinstance(v, str)]
    if isinstance(node, ast.Dict):
        return [v for v in (_const(k) for k in node.keys) if isinstance(v, str)]
    return []


def _field_call(value):
    """(tipo, comodelo) si `value` es `fields.Tipo(...)`; si no, None."""
    if not isinstance(value, ast.Call):
        return None
    func = value.func
    if isinstance(func, ast.Attribute) and func.attr in FIELD_TYPES:
        base = func.value
        if not (isinstance(base, ast.Name) and base.id == 'fields') and not (
                isinstance(base, ast.Attribute) and base.attr == 'fields'):
            return None
        ftype = func.attr
    else:
        return None
    comodel = None
    if ftype in RELATIONAL:
        if value.args:
            comodel = _const(value.args[0]) if isinstance(_const(value.args[0]), str) else None
        for kw in value.keywords:
            if kw.arg == 'comodel_name' and isinstance(_const(kw.value), str):
                comodel = _const(kw.value)
    return ftype, comodel


class FieldIndex:
    def __init__(self):
        # modelo → {'fields': {nombre: (tipo, comodelo)}, 'mixins': set, 'delegates': set}
        self.models = {}
        self.defined = set()  # modelos con una clase que los define (`_name`); los solo extendidos no cuentan
        self.files = 0
        self._cache = {}

    def _model(self, name):
        return self.models.setdefault(name, {'fields': {}, 'mixins': set(), 'delegates': set()})

    def add_tree(self, root):
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
            parts = set(dirpath.split(os.sep))
            if not parts & SOURCE_DIRS:
                continue
            for fn in filenames:
                if fn.endswith('.py'):
                    self.add_file(os.path.join(dirpath, fn))

    def add_file(self, path):
        try:
            with open(path, encoding='utf-8') as fh:
                tree = ast.parse(fh.read(), path)
        except (SyntaxError, UnicodeDecodeError, OSError):
            return
        self.files += 1
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                self._add_class(node)

    def _add_class(self, cls):
        name = inherit = inherits = None
        fields = {}
        for stmt in cls.body:
            if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                target = stmt.targets[0].id
                if target == '_name':
                    name = _const(stmt.value) if isinstance(_const(stmt.value), str) else None
                elif target == '_inherit':
                    inherit = _names(stmt.value)
                elif target == '_inherits':
                    inherits = _names(stmt.value)
                else:
                    info = _field_call(stmt.value)
                    if info:
                        fields[target] = info
            elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name) and stmt.value is not None:
                info = _field_call(stmt.value)
                if info:
                    fields[stmt.target.id] = info
        if not name and not inherit:
            return
        model = name or inherit[0]
        if name and name not in (inherit or []):
            # `_name = 'x'` junto con `_inherit = 'x'` (o una lista que lo incluye) es una
            # extensión, no la definición: no vuelve «conocido» un modelo de Odoo o Enterprise.
            self.defined.add(name)
        rec = self._model(model)
        rec['fields'].update(fields)
        for parent in (inherit or []):
            if parent != model:
                rec['mixins'].add(parent)
        for parent in (inherits or []):
            rec['delegates'].add(parent)
        self._cache.clear()

    def known(self, model):
        """Solo un modelo cuya definición se leyó: uno que el repo únicamente extiende
        (Enterprise: documents.document, quality.alert…) no se puede revisar."""
        return model in self.defined

    def fields(self, model, _seen=None):
        """{nombre: (tipo, comodelo)} con mixins y delegación resueltos."""
        if model in self._cache:
            return self._cache[model]
        _seen = _seen or set()
        if model in _seen or model not in self.models:
            return {}
        _seen.add(model)
        rec = self.models[model]
        out = {}
        for parent in sorted(rec['mixins'] | rec['delegates']):
            out.update(self.fields(parent, _seen))
        out.update(rec['fields'])
        for magic in MAGIC:
            out.setdefault(magic, ('Magic', 'res.users' if magic.endswith('_uid') else None))
        self._cache[model] = out
        return out

    def complete(self, model, _seen=None):
        """True si el modelo y todo lo que hereda tienen código leído. Sin el código de
        Odoo, un modelo del repo con `mail.thread` queda incompleto y no se revisa."""
        _seen = _seen or set()
        if model in _seen:
            return True
        _seen.add(model)
        if model not in self.defined:
            return False
        rec = self.models[model]
        return all(self.complete(p, _seen) for p in rec['mixins'] | rec['delegates'])

    def comodel(self, model, fname):
        info = self.fields(model).get(fname)
        return info[1] if info and info[0] in RELATIONAL else None


# ----------------------------------------------------------------------------
# Vistas
# ----------------------------------------------------------------------------
def _manifest_files(module_dir):
    path = os.path.join(module_dir, '__manifest__.py')
    try:
        with open(path, encoding='utf-8') as fh:
            manifest = ast.literal_eval(fh.read())
    except (OSError, ValueError, SyntaxError):
        return []
    return list(manifest.get('data', []) or []) + list(manifest.get('demo', []) or [])


def _module_dirs(paths):
    out = []
    for path in paths:
        for dirpath, dirnames, filenames in os.walk(path):
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
            if '__manifest__.py' in filenames:
                out.append(dirpath)
    return sorted(set(out))


def _load_allow():
    allow = set()
    if os.path.exists(ALLOW_FILE):
        with open(ALLOW_FILE, encoding='utf-8') as fh:
            for line in fh:
                line = line.split('#', 1)[0].strip()
                if line:
                    allow.add(line)
    return allow


class ViewChecker:
    def __init__(self, index, allow, verbose=False):
        self.index = index
        self.allow = allow
        self.verbose = verbose
        self.errors = []
        self.unknown_models = {}
        self.checked = 0

    def check_module(self, module_dir):
        module = os.path.basename(module_dir)
        for rel in _manifest_files(module_dir):
            path = os.path.join(module_dir, rel)
            if not path.endswith('.xml') or not os.path.exists(path):
                continue
            try:
                tree = etree.parse(path)
            except etree.XMLSyntaxError:
                continue
            for record in tree.iter('record'):
                if record.get('model') != 'ir.ui.view':
                    continue
                model = arch = None
                for field in record.findall('field'):
                    if field.get('name') == 'model':
                        model = (field.text or '').strip()
                    elif field.get('name') == 'arch':
                        arch = field
                if not model or arch is None or not len(arch):
                    continue
                xmlid = '%s.%s' % (module, record.get('id') or '?')
                ctx = {'file': os.path.relpath(path, ROOT), 'xmlid': xmlid}
                for child in arch:
                    self._visit(child, model, ctx)

    def _report(self, ctx, elem, model, fname):
        self.errors.append("%s:%s: vista %s: el campo «%s» no existe en el modelo «%s»"
                           % (ctx['file'], elem.sourceline, ctx['xmlid'], fname, model))

    def _check(self, ctx, elem, model, fname):
        if not model or not fname or fname.startswith('x_'):
            return
        if not self.index.known(model) or not self.index.complete(model):
            self.unknown_models.setdefault(model, ctx['xmlid'])
            return
        self.checked += 1
        if fname in self.index.fields(model):
            return
        if '%s.%s' % (model, fname) in self.allow:
            return
        self._report(ctx, elem, model, fname)

    def _xpath_model(self, expr, position, model):
        """Modelo que aplica dentro de un <xpath>: cada field[@name=…] relacional
        de la expresión baja al comodelo, salvo el último si la posición es
        before/after/replace/attributes (los hermanos viven en el modelo del padre)."""
        names = _XPATH_FIELD.findall(expr or '')
        cur = model
        for i, fname in enumerate(names):
            last = i == len(names) - 1
            if last and position != 'inside':
                break
            if not cur:
                return None
            if not self.index.known(cur):
                return None
            if fname not in self.index.fields(cur):
                return None  # el ancla no existe: eso lo reporta el chequeo del campo, no este
            cm = self.index.comodel(cur, fname)
            if not cm:
                return None
            cur = cm
        return cur

    def _visit(self, elem, model, ctx):
        if not isinstance(elem.tag, str):
            return  # comentario
        tag = etree.QName(elem).localname
        if tag == 'xpath':
            sub = self._xpath_model(elem.get('expr'), elem.get('position') or 'inside', model)
            for child in elem:
                self._visit(child, sub, ctx)
            return
        if tag == 'field':
            fname = elem.get('name')
            self._check(ctx, elem, model, fname)
            if len(elem):
                position = elem.get('position')
                if position and position != 'inside':
                    sub = model  # herencia: los hijos son hermanos del ancla, mismo modelo
                else:
                    sub = self.index.comodel(model, fname) if (model and fname) else None
                for child in elem:
                    self._visit(child, sub, ctx)
            return
        if tag == 'groupby':
            self._check(ctx, elem, model, elem.get('name'))
        for child in elem:
            self._visit(child, model, ctx)


def main(argv):
    args = list(argv[1:])
    odoo_path = os.environ.get('ODOO_PATH')
    verbose = False
    if '--odoo-path' in args:
        i = args.index('--odoo-path')
        odoo_path = args[i + 1]
        del args[i:i + 2]
    if '--verbose' in args:
        verbose = True
        args.remove('--verbose')
    paths = args or [os.path.join(ROOT, 'addons'), ROOT]
    index = FieldIndex()
    module_dirs = [d for d in _module_dirs(paths) if '/.git/' not in d]
    for module_dir in module_dirs:
        index.add_tree(module_dir)
    repo_models = len(index.models)
    if odoo_path:
        if not os.path.isdir(odoo_path):
            print("ERROR: --odoo-path %s no existe." % odoo_path)
            return 1
        index.add_tree(odoo_path)
    checker = ViewChecker(index, _load_allow(), verbose)
    for module_dir in module_dirs:
        checker.check_module(module_dir)
    for err in checker.errors:
        print("ERROR:", err)
    if verbose and checker.unknown_models:
        print("Modelos sin código (no se revisan): %s" % ", ".join(
            "%s (%s)" % (m, x) for m, x in sorted(checker.unknown_models.items())))
    print("%d error(es) en campos de vistas: %d campos revisados contra %d modelos (%d del repo%s); "
          "%d modelos sin código%s." % (
              len(checker.errors), checker.checked, len(index.models), repo_models,
              " + Odoo en %s" % odoo_path if odoo_path else ", sin código de Odoo: solo los del repo",
              len(checker.unknown_models), "" if verbose else " (--verbose los lista)"))
    return 1 if checker.errors else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
