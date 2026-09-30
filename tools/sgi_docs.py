#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Genera la documentación técnica del SGI leyendo el código (sin Odoo).

Uso (desde la raíz del repo):

    python3 tools/sgi_docs.py           # regenera docs/sgi/tecnica/
    python3 tools/sgi_docs.py --check   # compara con lo comprometido

Lee con ``ast`` los modelos de ``addons/quimibond_sgi`` y de sus satélites
(``addons/quimibond_sgi_*``) y con ``xml.etree`` sus vistas, acciones, menús,
crons, grupos, reglas y el CSV de permisos. No importa Odoo, así que corre
igual en local que en el CI.

Salidas (todas en ``docs/sgi/tecnica/``, marcadas «generado»):

- ``modelo-de-datos.md``: un renglón por modelo (propio o extendido).
- ``diccionario/<modelo>.md``: campos con tipo, etiqueta, ``help``,
  relación, cálculo, grupos y archivo; métodos públicos con su docstring.
- ``vistas-y-acciones.md``: árbol de menús, acciones y vistas por modelo.
- ``seguridad.md``: grupos con lo que implican, permisos (CSV) y reglas.
- ``crons.md``: acciones planificadas con su método y su docstring.
- ``parametros.md``: ``sgi.config._SGI_DEFAULT_PARAMS`` con su comentario.
- ``integraciones.md``: quién fuera del SGI nombra modelos ``sgi.*``.

``--check`` regenera en memoria y sale con 1 si algún archivo difiere de lo
comprometido (hay que correr el script y comprometer el resultado). Además
avisa, sin fallar, de los campos **nuevos** (que no estaban en el
diccionario comprometido) que no traen ``help``.
"""
import argparse
import ast
import csv
import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
ADDONS = os.path.join(ROOT, 'addons')
OUT = os.path.join(ROOT, 'docs', 'sgi', 'tecnica')
CORE = 'quimibond_sgi'
HEADER = ('<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: '
          'correr `python3 tools/sgi_docs.py`. -->\n\n')

FIELD_TYPES = {'Char', 'Text', 'Html', 'Integer', 'Float', 'Monetary', 'Boolean', 'Date',
               'Datetime', 'Selection', 'Many2one', 'One2many', 'Many2many', 'Binary',
               'Image', 'Json', 'Reference', 'Many2oneReference', 'Properties'}
# Tipos cuyo primer argumento posicional es el modelo relacionado, no la etiqueta.
COMODEL_FIRST = {'Many2one', 'One2many', 'Many2many'}


# ----------------------------------------------------------------- utilidades
def rel(path):
    return os.path.relpath(path, ROOT).replace(os.sep, '/')


def one_line(text, limit=200):
    text = re.sub(r'\s+', ' ', str(text or '')).strip()
    return text if len(text) <= limit else text[:limit - 1] + '…'


def cell(text, limit=200):
    """Texto seguro para una celda de tabla Markdown."""
    return one_line(text, limit).replace('|', '\\|')


def first_doc_line(node):
    doc = ast.get_docstring(node) if node is not None else None
    if not doc:
        return ''
    return one_line(doc.strip().split('\n\n')[0], 240)


def lit(node):
    try:
        return ast.literal_eval(node)
    except (ValueError, TypeError, SyntaxError, MemoryError, RecursionError):
        return None


def sgi_addons():
    names = sorted(n for n in os.listdir(ADDONS)
                   if (n == CORE or n.startswith(CORE + '_'))
                   and os.path.isfile(os.path.join(ADDONS, n, '__manifest__.py')))
    return names


def manifest(addon):
    with open(os.path.join(ADDONS, addon, '__manifest__.py'), encoding='utf-8') as f:
        return ast.literal_eval(f.read())


def walk(addon, exts):
    base = os.path.join(ADDONS, addon)
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = sorted(d for d in dirnames if d not in ('tests', 'migrations', '__pycache__', 'static'))
        for fn in sorted(filenames):
            if fn.endswith(exts):
                yield os.path.join(dirpath, fn)


# ----------------------------------------------------------------- Python
class Model:
    def __init__(self, name):
        self.name = name
        self.description = ''
        self.doc = ''
        self.kind = ''
        self.own = False          # lo define (``_name``) un módulo del SGI
        self.inherits = set()
        self.files = []
        self.fields = {}          # nombre -> dict
        self.methods = {}         # nombre -> (docstring, archivo)
        self.order = ''


def _kind(cls):
    for base in cls.bases:
        text = ast.unparse(base)
        for k in ('TransientModel', 'AbstractModel', 'Model'):
            if text.endswith(k):
                return k
    return ''


def _field(call, attr_type, addon, path):
    kw = {k.arg: k.value for k in call.keywords if k.arg}
    args = call.args
    comodel = ''
    label = None
    if attr_type in COMODEL_FIRST:
        if args:
            comodel = lit(args[0]) or ''
        if len(args) > 1 and attr_type == 'Many2one':
            label = lit(args[1])
        if 'comodel_name' in kw:
            comodel = lit(kw['comodel_name']) or comodel
    elif attr_type == 'Selection':
        if len(args) > 1:
            label = lit(args[1])
    elif args:
        label = lit(args[0])
    if 'string' in kw:
        label = lit(kw['string'])

    def val(key):
        if key not in kw:
            return None
        v = lit(kw[key])
        return v if v is not None else ast.unparse(kw[key])

    compute = val('compute')
    related = val('related')
    store = val('store')
    calc = ''
    if related:
        calc = 'related `%s`' % related
    elif compute:
        calc = 'compute `%s`' % (compute if isinstance(compute, str) else 'lambda')
    if calc:
        calc += ', guardado' if store else ', sin guardar'
    return {
        'type': attr_type,
        'label': label if isinstance(label, str) else '',
        'help': val('help') if isinstance(val('help'), str) else '',
        'required': 'sí' if val('required') is True else '',
        'comodel': comodel if isinstance(comodel, str) else '',
        'calc': calc,
        'groups': val('groups') if isinstance(val('groups'), str) else '',
        'where': '%s:%d' % (rel(path), call.lineno),
        'addon': addon,
    }


def parse_models(addons):
    models = {}
    for addon in addons:
        for path in walk(addon, ('.py',)):
            with open(path, encoding='utf-8') as f:
                tree = ast.parse(f.read(), path)
            for cls in (n for n in tree.body if isinstance(n, ast.ClassDef)):
                attrs = {}
                for st in cls.body:
                    if (isinstance(st, ast.Assign) and len(st.targets) == 1
                            and isinstance(st.targets[0], ast.Name)
                            and st.targets[0].id.startswith('_')):
                        attrs[st.targets[0].id] = lit(st.value)
                name = attrs.get('_name')
                inherit = attrs.get('_inherit') or []
                if isinstance(inherit, str):
                    inherit = [inherit]
                if not isinstance(name, str):
                    name = inherit[0] if len(inherit) == 1 else None
                if not name:
                    continue
                m = models.setdefault(name, Model(name))
                where = rel(path)
                if attrs.get('_name') and name not in inherit:
                    # La clase que define el modelo (``_name`` sin heredarse a sí mismo).
                    m.own = True
                    m.kind = _kind(cls)
                    m.description = attrs.get('_description') or ''
                    m.doc = first_doc_line(cls)
                    m.order = attrs.get('_order') or m.order
                    m.inherits.update(inherit)
                    m.files.insert(0, where)
                else:
                    m.kind = m.kind or _kind(cls)
                    if not m.own and not m.doc:
                        m.doc = first_doc_line(cls)
                    if where not in m.files:
                        m.files.append(where)
                if m.own and m.files.count(where) > 1:
                    m.files = [f for i, f in enumerate(m.files) if f not in m.files[:i]]
                for st in cls.body:
                    if (isinstance(st, ast.Assign) and len(st.targets) == 1
                            and isinstance(st.targets[0], ast.Name)
                            and isinstance(st.value, ast.Call)):
                        func = st.value.func
                        ftype = func.attr if isinstance(func, ast.Attribute) else ''
                        if ftype in FIELD_TYPES and ast.unparse(func).startswith('fields.'):
                            m.fields[st.targets[0].id] = _field(st.value, ftype, addon, path)
                    elif isinstance(st, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        if first_doc_line(st) or st.name not in m.methods:
                            m.methods[st.name] = (first_doc_line(st), where)
    return models


def method_doc(models, model, method):
    m = models.get(model)
    if m and method in m.methods:
        return m.methods[method][0]
    return ''


def param_defaults(addon):
    """``_SGI_DEFAULT_PARAMS`` con el comentario que va justo arriba de cada clave."""
    path = os.path.join(ADDONS, addon, 'models', 'sgi_format_map.py')
    if not os.path.exists(path):
        return []
    with open(path, encoding='utf-8') as f:
        src = f.read()
    lines = src.split('\n')
    tree = ast.parse(src)
    rows = []
    for node in ast.walk(tree):
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == '_SGI_DEFAULT_PARAMS'
                and isinstance(node.value, ast.Dict)):
            for k, v in zip(node.value.keys, node.value.values):
                comment = []
                i = k.lineno - 2
                while i >= 0 and lines[i].strip().startswith('#'):
                    comment.insert(0, lines[i].strip().lstrip('#').strip())
                    i -= 1
                rows.append((lit(k), lit(v), ' '.join(comment)))
    return rows


# ----------------------------------------------------------------- XML
def xml_records(addons):
    """(addon, archivo, elemento) de cada ``<record>`` y ``<menuitem>``."""
    for addon in addons:
        for path in walk(addon, ('.xml',)):
            try:
                root = ET.parse(path).getroot()
            except ET.ParseError:
                continue
            for el in root.iter():
                if el.tag in ('record', 'menuitem'):
                    yield addon, path, el


def fval(rec, name):
    for f in rec.findall('field'):
        if f.get('name') == name:
            return f
    return None


def ftext(rec, name):
    f = fval(rec, name)
    if f is None:
        return ''
    return (f.get('ref') or f.get('eval') or (f.text or '')).strip()


def xmlid(addon, ident):
    return ident if '.' in (ident or '') else '%s.%s' % (addon, ident)


def model_from_ref(ref, models):
    """``model_sgi_cron`` → ``sgi.cron`` (buscando entre los modelos conocidos)."""
    key = ref.split('.')[-1]
    key = key[len('model_'):] if key.startswith('model_') else key
    for name in models:
        if name.replace('.', '_') == key:
            return name
    return key.replace('_', '.')


# ----------------------------------------------------------------- salidas
def out_models(models):
    own = sorted(n for n, m in models.items() if m.own)
    ext = sorted(n for n, m in models.items() if not m.own)
    lines = [HEADER, '# Modelo de datos del SGI\n\n',
             'Modelos que definen el núcleo y sus satélites (%d) y modelos de otras apps que '
             'extienden (%d). El detalle de cada uno está en `diccionario/`.\n\n' % (len(own), len(ext)),
             '## Modelos propios\n\n',
             '| Modelo | Descripción | Qué es (docstring) | Tipo | Campos | Archivo |\n',
             '|---|---|---|---|---:|---|\n']
    for n in own:
        m = models[n]
        lines.append('| [`%s`](diccionario/%s.md) | %s | %s | %s | %d | `%s` |\n' % (
            n, n, cell(m.description), cell(m.doc) or '—', m.kind, len(m.fields), m.files[0]))
    lines += ['\n## Modelos de otras apps que el SGI extiende\n\n',
              '| Modelo | Campos que agrega | Archivos |\n', '|---|---:|---|\n']
    for n in ext:
        m = models[n]
        lines.append('| [`%s`](diccionario/%s.md) | %d | %s |\n' % (
            n, n, len(m.fields), ', '.join('`%s`' % f for f in m.files)))
    no_doc = [n for n in own if not models[n].doc]
    lines.append('\nModelos propios sin docstring de clase: %d de %d.\n' % (len(no_doc), len(own)))
    return ''.join(lines)


def out_dictionary(m):
    lines = [HEADER, '# `%s`\n\n' % m.name]
    if m.own:
        lines.append('**%s** (%s).' % (cell(m.description) or '—', m.kind or 'Model'))
        if m.inherits:
            lines.append(' Hereda de: %s.' % ', '.join('`%s`' % i for i in sorted(m.inherits)))
        lines.append('\n\n')
    else:
        lines.append('Modelo de otra app que el SGI extiende.\n\n')
    if m.doc:
        lines.append('%s\n\n' % m.doc)
    if m.order:
        lines.append('Orden: `%s`.\n\n' % m.order)
    lines.append('Archivos: %s.\n\n' % ', '.join('`%s`' % f for f in m.files))
    if m.fields:
        lines += ['## Campos (%d)\n\n' % len(m.fields),
                  '| Campo | Tipo | Etiqueta | Ayuda | Req. | Relación | Cálculo | Grupos | Dónde |\n',
                  '|---|---|---|---|---|---|---|---|---|\n']
        for name in sorted(m.fields):
            f = m.fields[name]
            lines.append('| `%s` | %s | %s | %s | %s | %s | %s | %s | `%s` |\n' % (
                name, f['type'], cell(f['label']), cell(f['help'], 300), f['required'],
                '`%s`' % f['comodel'] if f['comodel'] else '', cell(f['calc']),
                cell(f['groups']), f['where']))
        lines.append('\n')
    public = sorted(k for k in m.methods if not k.startswith('_'))
    if public:
        lines += ['## Métodos públicos (%d)\n\n' % len(public), '| Método | Qué hace (docstring) |\n',
                  '|---|---|\n']
        for k in public:
            lines.append('| `%s` | %s |\n' % (k, cell(m.methods[k][0]) or '—'))
    return ''.join(lines)


def out_views(addons, models):
    menus, actions, views = [], {}, defaultdict(list)
    for addon, path, el in xml_records(addons):
        if el.tag == 'menuitem':
            menus.append({
                'id': xmlid(addon, el.get('id')), 'name': el.get('name') or '',
                'parent': xmlid(addon, el.get('parent')) if el.get('parent') else '',
                'action': el.get('action') or '', 'groups': el.get('groups') or '',
                'seq': int(el.get('sequence') or 10), 'file': rel(path), 'n': len(menus)})
        elif el.get('model') in ('ir.actions.act_window', 'ir.actions.server', 'ir.actions.client'):
            actions[xmlid(addon, el.get('id'))] = {
                'kind': el.get('model').split('.')[-1], 'name': ftext(el, 'name'),
                'model': ftext(el, 'res_model') or model_from_ref(ftext(el, 'model_id'), models)
                if ftext(el, 'res_model') or ftext(el, 'model_id') else '',
                'modes': ftext(el, 'view_mode'), 'help': 'sí' if fval(el, 'help') is not None else '',
                'file': rel(path)}
        elif el.get('model') == 'ir.ui.view':
            arch = fval(el, 'arch')
            vtype = arch[0].tag if arch is not None and len(arch) else ''
            inherit = ftext(el, 'inherit_id')
            views[ftext(el, 'model') or '?'].append({
                'id': xmlid(addon, el.get('id')), 'type': vtype if not inherit else 'herencia',
                'inherit': xmlid(addon, inherit) if inherit else '', 'file': rel(path)})
    lines = [HEADER, '# Vistas, acciones y menús del SGI\n\n', '## Árbol de menús\n\n',
             'Sacado de los `<menuitem>` del núcleo y los satélites, ordenado por secuencia '
             'dentro de cada padre. El árbol que valida `test_menu_tree` está en '
             '`addons/quimibond_sgi/tools/sgi_menu_tree.txt`.\n\n']
    by_parent = defaultdict(list)
    ids = {m['id'] for m in menus}
    for m in menus:
        by_parent[m['parent'] if m['parent'] in ids else ''].append(m)

    def tree(parent, depth):
        for m in sorted(by_parent.get(parent, []), key=lambda x: (x['seq'], x['n'])):
            extra = []
            if m['action']:
                act = actions.get(xmlid(m['id'].split('.')[0], m['action']))
                extra.append('`%s`' % (act['model'] if act and act['model'] else m['action']))
            if m['groups']:
                extra.append('grupos: %s' % m['groups'].replace(',', ', '))
            if not depth and m['parent']:
                extra.append('bajo `%s`' % m['parent'])
            lines.append('%s- **%s**%s\n' % ('  ' * depth, m['name'],
                                             (' — ' + '; '.join(extra)) if extra else ''))
            tree(m['id'], depth + 1)
    tree('', 0)
    lines += ['\n## Acciones (%d)\n\n' % len(actions), '| Acción | Tipo | Título | Modelo | Vistas | Ayuda de pantalla vacía | Archivo |\n',
              '|---|---|---|---|---|---|---|\n']
    for k in sorted(actions):
        a = actions[k]
        lines.append('| `%s` | %s | %s | %s | %s | %s | `%s` |\n' % (
            k, a['kind'], cell(a['name']), '`%s`' % a['model'] if a['model'] else '',
            a['modes'], a['help'], a['file']))
    total = sum(len(v) for v in views.values())
    inh = sum(1 for v in views.values() for x in v if x['inherit'])
    own_inh = sum(1 for v in views.values() for x in v
                  if x['inherit'] and x['inherit'].split('.')[0] == x['id'].split('.')[0])
    lines += ['\n## Vistas por modelo (%d; %d heredan de otra vista, %d de su propio módulo)\n\n' % (
        total, inh, own_inh), '| Modelo | Vista | Tipo | Hereda de | Archivo |\n', '|---|---|---|---|---|\n']
    for model in sorted(views):
        for v in sorted(views[model], key=lambda x: x['id']):
            lines.append('| `%s` | `%s` | %s | %s | `%s` |\n' % (
                model, v['id'], v['type'], '`%s`' % v['inherit'] if v['inherit'] else '', v['file']))
    return ''.join(lines)


def out_security(addons, models):
    groups, rules = [], []
    for addon, path, el in xml_records(addons):
        if el.tag != 'record':
            continue
        if el.get('model') == 'res.groups':
            implied = fval(el, 'implied_ids')
            imp = []
            if implied is not None:
                for code, ref in re.findall(r"\((\d)\s*,\s*ref\('([^']+)'\)", implied.get('eval') or ''):
                    imp.append(('+' if code == '4' else '−') + xmlid(addon, ref))
            groups.append((xmlid(addon, el.get('id')), ftext(el, 'name'), imp, rel(path)))
        elif el.get('model') == 'ir.rule':
            rules.append((xmlid(addon, el.get('id')), ftext(el, 'name'),
                          model_from_ref(ftext(el, 'model_id'), models),
                          one_line(ftext(el, 'domain_force'), 160),
                          (fval(el, 'groups').get('eval') if fval(el, 'groups') is not None else '') or '',
                          rel(path)))
    acl = []
    for addon in addons:
        path = os.path.join(ADDONS, addon, 'security', 'ir.model.access.csv')
        if os.path.exists(path):
            with open(path, encoding='utf-8') as f:
                for r in csv.DictReader(f):
                    acl.append((model_from_ref(r.get('model_id:id', ''), models),
                                r.get('group_id:id') or '(todos)',
                                ''.join(c for c, k in zip('lecb', ('perm_read', 'perm_write', 'perm_create',
                                                                   'perm_unlink')) if r.get(k) == '1'),
                                addon))
    lines = [HEADER, '# Seguridad del SGI\n\n', '## Grupos (%d)\n\n' % len(groups),
             '«+» = implica ese grupo; «−» = quita una implicación que ya estaba en la base.\n\n',
             '| Grupo | Nombre | Implica | Archivo |\n', '|---|---|---|---|\n']
    for g in groups:
        lines.append('| `%s` | %s | %s | `%s` |\n' % (g[0], cell(g[1]), ', '.join('`%s`' % i for i in g[2]), g[3]))
    lines += ['\n## Permisos por modelo (%d renglones del CSV)\n\n' % len(acl),
              'l = leer, e = escribir, c = crear, b = borrar. Es el CSV tal cual; el permiso '
              'efectivo suma lo que implican los grupos y lo que quitan las reglas.\n\n',
              '| Modelo | Grupo | Permisos | Módulo |\n', '|---|---|---|---|\n']
    for a in sorted(acl):
        lines.append('| `%s` | `%s` | %s | %s |\n' % a)
    lines += ['\n## Reglas de registro (%d)\n\n' % len(rules),
              '| Regla | Nombre | Modelo | Dominio | Grupos | Archivo |\n', '|---|---|---|---|---|---|\n']
    for r in sorted(rules):
        lines.append('| `%s` | %s | `%s` | `%s` | %s | `%s` |\n' % (
            r[0], cell(r[1]), r[2], r[3].replace('|', '\\|'), cell(r[4]), r[5]))
    return ''.join(lines)


def out_crons(addons, models):
    rows = []
    for addon, path, el in xml_records(addons):
        if el.tag == 'record' and el.get('model') == 'ir.cron':
            model = model_from_ref(ftext(el, 'model_id'), models)
            code = ftext(el, 'code')
            meth = re.findall(r'model\.(\w+)\(', code)
            doc = method_doc(models, model, meth[0]) if meth else ''
            rows.append((ftext(el, 'name'), '`%s`' % model, '`%s`' % one_line(code, 80),
                         '%s %s' % (ftext(el, 'interval_number'), ftext(el, 'interval_type')),
                         ftext(el, 'active') or 'True', doc, rel(path)))
    lines = [HEADER, '# Acciones planificadas (crons) del SGI\n\n',
             '%d crons. Viven en archivos `noupdate`: lo que diga la base puede no coincidir con '
             'el código (activo, siguiente corrida) y cambiarlos requiere migración. Horas de la '
             'siguiente corrida: en la base (Ajustes → Técnico → Acciones planificadas).\n\n' % len(rows),
             '| Nombre | Modelo | Código | Cada | Activo | Qué hace (docstring del método) | Archivo |\n',
             '|---|---|---|---|---|---|---|\n']
    for r in sorted(rows):
        lines.append('| %s | %s | %s | %s | %s | %s | `%s` |\n' % (
            cell(r[0]), r[1], r[2], r[3], r[4], cell(r[5]) or '—', r[6]))
    return ''.join(lines)


def out_params(rows):
    lines = [HEADER, '# Parámetros del SGI\n\n',
             'Valores de arranque de `sgi.config._SGI_DEFAULT_PARAMS` '
             '(`addons/quimibond_sgi/models/sgi_format_map.py`). Solo se crean si no existen: lo '
             'que se edite en Ajustes → Técnico → Parámetros del sistema nunca se pisa. Otros '
             'parámetros los siembran los satélites o se ponen a mano (p. ej. '
             '`quimibond_sgi.mast_user_id`, `quimibond_sgi.sgi_company_id`).\n\n',
             '| Clave | De fábrica | Comentario en el código |\n', '|---|---|---|\n']
    for k, v, c in rows:
        lines.append('| `%s` | `%s` | %s |\n' % (k, cell(v, 120), cell(c, 300)))
    return ''.join(lines)


def out_integrations(addons):
    rows = defaultdict(set)
    pat = re.compile(r"""['"](sgi\.[a-z_.]+)['"]""")
    for name in sorted(os.listdir(ADDONS)):
        if name in addons or not os.path.isdir(os.path.join(ADDONS, name)):
            continue
        for path in walk(name, ('.py', '.xml')):
            with open(path, encoding='utf-8', errors='replace') as f:
                for model in pat.findall(f.read()):
                    rows[rel(path)].add(model)
    lines = [HEADER, '# Quién más lee el SGI\n\n',
             'Archivos fuera del SGI y sus satélites que nombran modelos `sgi.*`. Al renombrar '
             'o sacar un modelo, revisarlos: las señales de `quimibond_intelligence` se '
             'protegen con `tiene_modelo()` y no truenan, pero **callan**.\n\n',
             '| Archivo | Modelos |\n', '|---|---|\n']
    for path in sorted(rows):
        lines.append('| `%s` | %s |\n' % (path, ', '.join('`%s`' % m for m in sorted(rows[path]))))
    if not rows:
        lines.append('| — | — |\n')
    return ''.join(lines)


def generate():
    addons = sgi_addons()
    for a in addons:
        manifest(a)  # falla temprano si un manifest no se puede leer
    models = parse_models(addons)
    files = {
        'modelo-de-datos.md': out_models(models),
        'vistas-y-acciones.md': out_views(addons, models),
        'seguridad.md': out_security(addons, models),
        'crons.md': out_crons(addons, models),
        'parametros.md': out_params(param_defaults(CORE)),
        'integraciones.md': out_integrations(addons),
    }
    for name, m in models.items():
        files['diccionario/%s.md' % name] = out_dictionary(m)
    return files, models


def committed_fields(path):
    """Campos que ya estaban documentados en un diccionario comprometido."""
    if not os.path.exists(path):
        return set()
    with open(path, encoding='utf-8') as f:
        return set(re.findall(r'^\| `([a-z_0-9]+)` \| [A-Z]', f.read(), re.M))


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--check', action='store_true',
                    help='no escribe; sale con 1 si lo generado difiere de lo comprometido')
    opts = ap.parse_args()
    files, models = generate()
    if not opts.check:
        dic = os.path.join(OUT, 'diccionario')
        os.makedirs(dic, exist_ok=True)
        for fn in os.listdir(dic):
            if fn.endswith('.md') and 'diccionario/' + fn not in files:
                os.remove(os.path.join(dic, fn))
        for name, text in files.items():
            with open(os.path.join(OUT, name), 'w', encoding='utf-8') as f:
                f.write(text)
        print('docs/sgi/tecnica: %d archivos generados.' % len(files))
        return 0
    stale = []
    warnings = []
    for name, text in sorted(files.items()):
        path = os.path.join(OUT, name)
        current = open(path, encoding='utf-8').read() if os.path.exists(path) else None
        if current != text:
            stale.append(name)
        if name.startswith('diccionario/'):
            model = models[name[len('diccionario/'):-3]]
            known = committed_fields(path)
            for fname, f in sorted(model.fields.items()):
                if fname not in known and not f['help'] and f['addon'].startswith(CORE):
                    warnings.append('%s: campo nuevo `%s.%s` sin help' % (f['where'], model.name, fname))
    dic = os.path.join(OUT, 'diccionario')
    if os.path.isdir(dic):
        stale += ['diccionario/%s (sobra)' % fn for fn in sorted(os.listdir(dic))
                  if fn.endswith('.md') and 'diccionario/' + fn not in files]
    for w in warnings:
        print('ADVERTENCIA %s' % w)
    for s in stale:
        print('DESACTUALIZADO docs/sgi/tecnica/%s' % s)
    if stale:
        print('%d archivo(s) desactualizado(s): correr `python3 tools/sgi_docs.py` y comprometer.'
              % len(stale))
        return 1
    print('docs/sgi/tecnica al día (%d archivos, %d advertencia(s)).' % (len(files), len(warnings)))
    return 0


if __name__ == '__main__':
    sys.exit(main())
