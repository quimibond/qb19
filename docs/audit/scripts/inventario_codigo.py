# -*- coding: utf-8 -*-
"""Inventario de la fase 0 de la auditoría del SGI, sacado SOLO del código.

Uso:  python3 docs/audit/scripts/inventario_codigo.py [addon ...]
Escribe CSV en docs/audit/inventario/. Solo lectura sobre addons/.
"""
import ast
import csv
import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
OUT = os.path.join(ROOT, 'docs', 'audit', 'inventario')
ADDONS = sys.argv[1:] or ['quimibond_sgi']


def rel(p):
    return os.path.relpath(p, ROOT)


def lit(node):
    try:
        return ast.literal_eval(node)
    except Exception:
        try:
            return ast.unparse(node)
        except Exception:
            return '?'


def one_line(s, n=160):
    s = re.sub(r'\s+', ' ', str(s or '')).strip()
    return s if len(s) <= n else s[:n - 1] + '…'


def write_csv(name, rows, cols):
    with open(os.path.join(OUT, name), 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction='ignore')
        w.writeheader()
        for r in rows:
            w.writerow(r)


# --------------------------------------------------------------- manifest
def manifest(addon_dir):
    with open(os.path.join(addon_dir, '__manifest__.py'), encoding='utf-8') as f:
        return ast.literal_eval(f.read())


# --------------------------------------------------------------- python
FIELD_TYPES = {'Char', 'Text', 'Html', 'Integer', 'Float', 'Monetary', 'Boolean', 'Date',
               'Datetime', 'Selection', 'Many2one', 'One2many', 'Many2many', 'Binary',
               'Image', 'Json', 'Reference', 'Many2oneReference', 'Properties',
               'PropertiesDefinition', 'Id'}
RELATIONAL = {'Many2one', 'One2many', 'Many2many'}


def parse_py(addon, path, models, fields, methods):
    src = open(path, encoding='utf-8').read()
    try:
        tree = ast.parse(src)
    except SyntaxError as e:
        models.append({'addon': addon, 'file': rel(path), 'line': e.lineno, 'class': 'SYNTAX ERROR'})
        return
    for cls in [n for n in tree.body if isinstance(n, ast.ClassDef)]:
        attrs = {}
        for st in cls.body:
            if isinstance(st, ast.Assign) and len(st.targets) == 1 and isinstance(st.targets[0], ast.Name):
                t = st.targets[0].id
                if t.startswith('_'):
                    attrs[t] = lit(st.value)
        name = attrs.get('_name')
        inherit = attrs.get('_inherit')
        if isinstance(inherit, str):
            inherit = [inherit]
        if not name and not inherit:
            continue
        bases = [ast.unparse(b) for b in cls.bases]
        kind = 'Model'
        if any('TransientModel' in b for b in bases):
            kind = 'TransientModel'
        elif any('AbstractModel' in b for b in bases):
            kind = 'AbstractModel'
        model = name or (inherit[0] if inherit else '?')
        own = bool(name) and (not inherit or name not in inherit)
        models.append({
            'addon': addon, 'model': model, 'class': cls.name, 'kind': kind,
            'propio_o_heredado': 'propio' if own else 'heredado',
            'inherit': ','.join(inherit or []), 'description': attrs.get('_description', ''),
            'order': attrs.get('_order', ''), 'rec_name': attrs.get('_rec_name', ''),
            'file': rel(path), 'line': cls.lineno,
        })
        for st in cls.body:
            if isinstance(st, ast.Assign) and len(st.targets) == 1 and isinstance(st.targets[0], ast.Name) \
                    and isinstance(st.value, ast.Call):
                fn = st.value.func
                ftype = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, 'id', '')
                if ftype not in FIELD_TYPES:
                    continue
                kw = {k.arg: k.value for k in st.value.keywords if k.arg}
                args = st.value.args
                comodel = string = selection = ''
                if ftype in RELATIONAL:
                    if args:
                        comodel = lit(args[0])
                    if ftype == 'Many2one' and len(args) > 1:
                        string = lit(args[1])
                    if ftype in ('One2many',) and len(args) > 2:
                        string = lit(args[2])
                    if ftype == 'Many2many' and len(args) > 4:
                        string = lit(args[4])
                    comodel = comodel or lit(kw.get('comodel_name')) if kw.get('comodel_name') else comodel
                elif ftype in ('Selection', 'Reference'):
                    if args:
                        selection = lit(args[0])
                    if len(args) > 1:
                        string = lit(args[1])
                elif args:
                    string = lit(args[0])
                if 'string' in kw:
                    string = lit(kw['string'])
                if 'selection' in kw:
                    selection = lit(kw['selection'])
                compute = lit(kw['compute']) if 'compute' in kw else ''
                related = lit(kw['related']) if 'related' in kw else ''
                if 'store' in kw:
                    store = lit(kw['store'])
                else:
                    store = not (compute or related) if ftype != 'One2many' else 'n/a'
                fields.append({
                    'addon': addon, 'model': model, 'field': st.targets[0].id, 'type': ftype,
                    'comodel': comodel if isinstance(comodel, str) else str(comodel),
                    'string': one_line(string, 80), 'store': store, 'compute': compute,
                    'related': related, 'required': lit(kw['required']) if 'required' in kw else '',
                    'index': lit(kw['index']) if 'index' in kw else '',
                    'readonly': lit(kw['readonly']) if 'readonly' in kw else '',
                    'groups': lit(kw['groups']) if 'groups' in kw else '',
                    'ondelete': lit(kw['ondelete']) if 'ondelete' in kw else '',
                    'tracking': lit(kw['tracking']) if 'tracking' in kw else '',
                    'default': 'sí' if 'default' in kw else '',
                    'inverse': lit(kw['inverse']) if 'inverse' in kw else '',
                    'search': lit(kw['search']) if 'search' in kw else '',
                    'company_dependent': lit(kw['company_dependent']) if 'company_dependent' in kw else '',
                    'selection': one_line(selection, 120),
                    'help': one_line(lit(kw['help']), 200) if 'help' in kw else '',
                    'tiene_help': 'sí' if 'help' in kw else 'no',
                    'file': rel(path), 'line': st.lineno,
                })
            elif isinstance(st, (ast.FunctionDef, ast.AsyncFunctionDef)):
                decos = [ast.unparse(d) for d in st.decorator_list]
                sudo = len(re.findall(r'\.sudo\(', ast.get_source_segment(src, st) or ''))
                excepts = sum(1 for n in ast.walk(st) if isinstance(n, ast.ExceptHandler))
                methods.append({
                    'addon': addon, 'model': model, 'method': st.name, 'decorators': ';'.join(decos),
                    'lines': (st.end_lineno or st.lineno) - st.lineno + 1, 'sudo_calls': sudo,
                    'except_handlers': excepts, 'file': rel(path), 'line': st.lineno,
                })


# --------------------------------------------------------------- xml
def line_of(path, token):
    try:
        for i, l in enumerate(open(path, encoding='utf-8'), 1):
            if token in l:
                return i
    except Exception:
        pass
    return ''


def fval(rec, name):
    for f in rec.findall('field'):
        if f.get('name') == name:
            if f.get('ref'):
                return 'ref:' + f.get('ref')
            if f.get('eval') is not None:
                return 'eval:' + f.get('eval')
            if f.get('search') is not None:
                return 'search:' + f.get('search')
            if len(f):
                return ET.tostring(f[0], encoding='unicode')[:0] or '<arch>'
            return (f.text or '').strip()
    return ''


def arch_root(rec):
    for f in rec.findall('field'):
        if f.get('name') == 'arch' and len(f):
            return f[0]
    return None


def parse_xml(addon, path, loaded, out):
    try:
        tree = ET.parse(path)
    except ET.ParseError as e:
        out['xml_errors'].append({'file': rel(path), 'error': str(e)})
        return
    root = tree.getroot()

    def walk(el, noupdate):
        for ch in el:
            nu = noupdate
            if ch.tag in ('data', 'odoo', 'openerp'):
                if ch.get('noupdate') is not None:
                    nu = ch.get('noupdate') in ('1', 'True', 'true')
                walk(ch, nu)
                continue
            handle(ch, nu)

    def handle(ch, nu):
        xid = ch.get('id', '')
        base = {'addon': addon, 'file': rel(path), 'line': line_of(path, 'id="%s"' % xid) if xid else '',
                'xml_id': xid, 'noupdate': 'sí' if nu else 'no', 'en_manifest': 'sí' if loaded else 'NO'}
        if ch.tag == 'menuitem':
            out['menus'].append({**base, 'name': ch.get('name', ''), 'parent': ch.get('parent', ''),
                                 'action': ch.get('action', ''), 'groups': ch.get('groups', ''),
                                 'sequence': ch.get('sequence', ''), 'active': ch.get('active', ''),
                                 'forma': 'menuitem'})
            for sub in ch.findall('menuitem'):  # anidados
                sub.set('parent', sub.get('parent') or xid)
                handle(sub, nu)
            return
        if ch.tag == 'template':
            out['qweb'].append({**base, 'name': ch.get('name', ''), 'inherit_id': ch.get('inherit_id', ''),
                                'priority': ch.get('priority', ''), 'forma': 'template'})
            return
        if ch.tag == 'function':
            out['functions'].append({**base, 'model': ch.get('model'), 'name': ch.get('name')})
            return
        if ch.tag == 'delete':
            out['deletes'].append({**base, 'model': ch.get('model'), 'xml_id': ch.get('id') or ch.get('search', '')})
            return
        if ch.tag != 'record':
            out['other_tags'].append({**base, 'tag': ch.tag})
            return
        model = ch.get('model')
        out['records'].append({**base, 'model': model, 'forceCreate': ch.get('forcecreate', '')})
        if model == 'ir.ui.view':
            a = arch_root(ch)
            vtype = a.tag if a is not None else ''
            if vtype in ('xpath', 'data') or fval(ch, 'inherit_id'):
                vtype_real = vtype
                # herencia: tipo desconocido sin el padre
            inherit = fval(ch, 'inherit_id')
            xpaths = []
            if a is not None and inherit:
                for x in a.iter('xpath'):
                    xpaths.append(x.get('expr', ''))
            positional = [x for x in xpaths if re.search(r'\[\d+\]', x)]
            out['views'].append({**base, 'model': fval(ch, 'model'), 'type': vtype if not inherit else 'herencia(' + vtype + ')',
                                 'inherit_id': inherit, 'priority': fval(ch, 'priority'), 'mode': fval(ch, 'mode'),
                                 'name': fval(ch, 'name'), 'groups': fval(ch, 'groups_id'),
                                 'hereda_de_otro_modulo': 'sí' if inherit and '.' in inherit.replace('ref:', '') and not inherit.replace('ref:', '').startswith(addon + '.') else ('no' if inherit else ''),
                                 'xpath_posicional': len(positional), 'n_xpath': len(xpaths)})
        elif model and model.startswith('ir.actions'):
            out['actions'].append({**base, 'model': model, 'name': fval(ch, 'name'),
                                   'res_model': fval(ch, 'res_model') or fval(ch, 'model_id') or fval(ch, 'model'),
                                   'view_mode': fval(ch, 'view_mode'), 'view_id': fval(ch, 'view_id'),
                                   'search_view_id': fval(ch, 'search_view_id'),
                                   'domain': one_line(fval(ch, 'domain'), 200), 'context': one_line(fval(ch, 'context'), 200),
                                   'groups': fval(ch, 'group_ids') or fval(ch, 'groups_id'),
                                   'state_or_tag': fval(ch, 'state') or fval(ch, 'tag') or fval(ch, 'report_type'),
                                   'report_name': fval(ch, 'report_name'),
                                   'binding': fval(ch, 'binding_model_id'),
                                   'code': one_line(fval(ch, 'code'), 160)})
        elif model == 'ir.ui.menu':
            out['menus'].append({**base, 'name': fval(ch, 'name'), 'parent': fval(ch, 'parent_id'),
                                 'action': fval(ch, 'action'), 'groups': fval(ch, 'group_ids') or fval(ch, 'groups_id'),
                                 'sequence': fval(ch, 'sequence'), 'active': fval(ch, 'active'), 'forma': 'record'})
        elif model == 'res.groups':
            out['groups'].append({**base, 'name': fval(ch, 'name'), 'implied': fval(ch, 'implied_ids'),
                                  'privilege_or_category': fval(ch, 'privilege_id') or fval(ch, 'category_id'),
                                  'users': fval(ch, 'user_ids') or fval(ch, 'users'), 'comment': one_line(fval(ch, 'comment'), 120)})
        elif model == 'ir.rule':
            out['rules'].append({**base, 'name': fval(ch, 'name'), 'model': fval(ch, 'model_id'),
                                 'groups': fval(ch, 'groups'), 'domain': one_line(fval(ch, 'domain_force'), 220),
                                 'r': fval(ch, 'perm_read'), 'w': fval(ch, 'perm_write'),
                                 'c': fval(ch, 'perm_create'), 'u': fval(ch, 'perm_unlink'), 'global': 'sí' if not fval(ch, 'groups') else 'no'})
        elif model == 'ir.cron':
            out['crons'].append({**base, 'name': fval(ch, 'name'), 'model': fval(ch, 'model_id'),
                                 'code': one_line(fval(ch, 'code'), 120), 'interval': fval(ch, 'interval_number') + ' ' + fval(ch, 'interval_type'),
                                 'active': fval(ch, 'active'), 'nextcall': fval(ch, 'nextcall'), 'user': fval(ch, 'user_id')})
        else:
            summ = {'name': fval(ch, 'name') or fval(ch, 'code') or fval(ch, 'key')}
            if model == 'ir.config_parameter':
                summ['name'] = fval(ch, 'key') + ' = ' + fval(ch, 'value')
            out['data'].append({**base, 'model': model, 'name': one_line(summ['name'], 120)})

    walk(root, root.get('noupdate') in ('1', 'True', 'true'))
    if root.tag not in ('odoo', 'openerp', 'data'):
        out['other_tags'].append({'addon': addon, 'file': rel(path), 'tag': 'root:' + root.tag})


# --------------------------------------------------------------- main
def main():
    os.makedirs(OUT, exist_ok=True)
    models, fields, methods = [], [], []
    out = defaultdict(list)
    files_rows = []
    access = []
    assets = []
    for addon in ADDONS:
        adir = os.path.join(ROOT, 'addons', addon)
        man = manifest(adir)
        loaded = set(man.get('data', [])) | set(man.get('demo', []))
        demo = set(man.get('demo', []))
        asset_globs = []
        for bundle, lst in (man.get('assets') or {}).items():
            for g in lst:
                if isinstance(g, str):
                    asset_globs.append((bundle, g))
        # python imports
        imported = set()
        for dp, dn, fn in os.walk(adir):
            if '__init__.py' in fn:
                txt = open(os.path.join(dp, '__init__.py'), encoding='utf-8').read()
                for m in re.findall(r'from \. import ([\w, ]+)', txt):
                    for x in m.split(','):
                        imported.add(os.path.join(dp, x.strip() + '.py'))
                        imported.add(os.path.join(dp, x.strip()))
        for dp, dn, fn in os.walk(adir):
            dn[:] = [d for d in dn if d != '__pycache__']
            for f in sorted(fn):
                p = os.path.join(dp, f)
                r = os.path.relpath(p, adir)
                ext = os.path.splitext(f)[1]
                estado = ''
                if r in loaded:
                    estado = 'manifest (demo)' if r in demo else 'manifest (data)'
                elif r in ('__manifest__.py', '__init__.py') or f == '__init__.py':
                    estado = 'arranque'
                elif r.startswith('tests/') and ext == '.py':
                    estado = ('helper de tests' if not f.startswith('test_') else 'test NO registrado en tests/__init__.py') if p not in imported else 'test registrado'
                elif ext == '.py' and (p in imported or os.path.dirname(p) in imported):
                    estado = 'importado'
                elif r.startswith('migrations/'):
                    estado = 'migración (Odoo la corre por versión)'
                elif r.startswith('static/description'):
                    estado = 'ícono/descr. de la app'
                elif r.startswith('static/'):
                    import fnmatch
                    hit = [b for b, g in asset_globs if fnmatch.fnmatch('%s/%s' % (addon, r), g) or fnmatch.fnmatch('%s/%s' % (addon, r), g.replace('/**/', '/'))]
                    estado = 'asset ' + ','.join(sorted(set(hit))) if hit else 'static NO registrado en assets'
                elif r.startswith('tools/'):
                    estado = 'script suelto (shell de Odoo.sh; fuera del manifest)'
                else:
                    estado = 'NO está en el manifest ni se importa'
                n = sum(1 for _ in open(p, 'rb'))
                files_rows.append({'addon': addon, 'file': r, 'ext': ext, 'lineas': n, 'estado': estado})
                if ext == '.py' and (estado in ('importado',) or r.startswith('controllers')):
                    parse_py(addon, p, models, fields, methods)
                if ext == '.xml' and not r.startswith('static/'):
                    parse_xml(addon, p, r in loaded, out)
                if ext == '.js':
                    txt = open(p, encoding='utf-8').read()
                    for m in re.finditer(r'registry\.category\(["\'](\w+)["\']\)\.add\(\s*["\']([\w.]+)["\']', txt):
                        assets.append({'addon': addon, 'file': r, 'categoria': m.group(1), 'clave': m.group(2)})
                    for m in re.finditer(r'static\s+template\s*=\s*["\']([\w.]+)["\']', txt):
                        assets.append({'addon': addon, 'file': r, 'categoria': 'owl_template', 'clave': m.group(1)})
                    for m in re.finditer(r'patch\(\s*([\w.]+)', txt):
                        assets.append({'addon': addon, 'file': r, 'categoria': 'patch', 'clave': m.group(1)})
        acc = os.path.join(adir, 'security', 'ir.model.access.csv')
        if os.path.exists(acc):
            for i, row in enumerate(csv.DictReader(open(acc, encoding='utf-8')), 2):
                row = {k.strip(): (v or '').strip() for k, v in row.items()}
                access.append({'addon': addon, 'line': i, **row})

    # uso de cada campo: menciones fuera de su definición
    corpus = defaultdict(int)
    tokre = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')
    for addon in ADDONS:
        adir = os.path.join(ROOT, 'addons', addon)
        for dp, dn, fn in os.walk(adir):
            if '__pycache__' in dp or '/migrations' in dp:
                continue
            for f in fn:
                if f.endswith(('.py', '.xml', '.js', '.csv')):
                    kind = 'test' if '/tests' in dp else ('xml' if f.endswith('.xml') else ('js' if f.endswith('.js') else 'py'))
                    for t in tokre.findall(open(os.path.join(dp, f), encoding='utf-8', errors='ignore').read()):
                        corpus[(kind, t)] += 1
    for fl in fields:
        n = fl['field']
        fl['usos_py'] = corpus[('py', n)] - 1
        fl['usos_xml'] = corpus[('xml', n)]
        fl['usos_js'] = corpus[('js', n)]
        fl['usos_tests'] = corpus[('test', n)]
    for m in methods:
        n = m['method']
        m['menciones_py'] = corpus[('py', n)] - 1
        m['menciones_xml'] = corpus[('xml', n)]
        m['menciones_tests'] = corpus[('test', n)]

    write_csv('modelos.csv', models, ['addon', 'model', 'class', 'kind', 'propio_o_heredado', 'inherit', 'description', 'order', 'rec_name', 'file', 'line'])
    write_csv('campos.csv', fields, ['addon', 'model', 'field', 'type', 'comodel', 'string', 'store', 'compute', 'related', 'required', 'index', 'readonly', 'groups', 'ondelete', 'tracking', 'default', 'inverse', 'search', 'company_dependent', 'selection', 'tiene_help', 'help', 'usos_py', 'usos_xml', 'usos_js', 'usos_tests', 'file', 'line'])
    write_csv('metodos.csv', methods, ['addon', 'model', 'method', 'decorators', 'lines', 'sudo_calls', 'except_handlers', 'menciones_py', 'menciones_xml', 'menciones_tests', 'file', 'line'])
    write_csv('vistas.csv', out['views'], ['addon', 'xml_id', 'model', 'type', 'inherit_id', 'hereda_de_otro_modulo', 'priority', 'mode', 'name', 'groups', 'n_xpath', 'xpath_posicional', 'noupdate', 'en_manifest', 'file', 'line'])
    write_csv('qweb.csv', out['qweb'], ['addon', 'xml_id', 'name', 'inherit_id', 'priority', 'noupdate', 'en_manifest', 'file', 'line'])
    write_csv('acciones.csv', out['actions'], ['addon', 'xml_id', 'model', 'name', 'res_model', 'view_mode', 'view_id', 'search_view_id', 'domain', 'context', 'groups', 'state_or_tag', 'report_name', 'binding', 'code', 'noupdate', 'en_manifest', 'file', 'line'])
    write_csv('menus.csv', out['menus'], ['addon', 'xml_id', 'name', 'parent', 'action', 'groups', 'sequence', 'active', 'forma', 'noupdate', 'en_manifest', 'file', 'line'])
    write_csv('grupos.csv', out['groups'], ['addon', 'xml_id', 'name', 'implied', 'privilege_or_category', 'users', 'comment', 'noupdate', 'file', 'line'])
    write_csv('reglas.csv', out['rules'], ['addon', 'xml_id', 'name', 'model', 'groups', 'global', 'domain', 'r', 'w', 'c', 'u', 'noupdate', 'file', 'line'])
    write_csv('accesos.csv', access, ['addon', 'line', 'id', 'name', 'model_id:id', 'group_id:id', 'perm_read', 'perm_write', 'perm_create', 'perm_unlink'])
    write_csv('crons.csv', out['crons'], ['addon', 'xml_id', 'name', 'model', 'code', 'interval', 'active', 'nextcall', 'user', 'noupdate', 'file', 'line'])
    write_csv('datos.csv', out['data'], ['addon', 'xml_id', 'model', 'name', 'noupdate', 'en_manifest', 'file', 'line'])
    write_csv('registros_xml.csv', out['records'], ['addon', 'xml_id', 'model', 'noupdate', 'forceCreate', 'en_manifest', 'file', 'line'])
    write_csv('funciones_y_deletes_xml.csv', out['functions'] + out['deletes'], ['addon', 'xml_id', 'model', 'name', 'noupdate', 'file', 'line'])
    write_csv('archivos.csv', files_rows, ['addon', 'file', 'ext', 'lineas', 'estado'])
    write_csv('assets_js.csv', assets, ['addon', 'file', 'categoria', 'clave'])
    write_csv('xml_raros.csv', out['other_tags'] + out['xml_errors'], ['addon', 'file', 'tag', 'error', 'xml_id'])

    # resumen en consola
    c = Counter
    print('modelos', c(m['propio_o_heredado'] + '/' + m['kind'] for m in models))
    print('modelos distintos', len({m['model'] for m in models}))
    print('campos', len(fields), 'sin help', sum(1 for f in fields if f['tiene_help'] == 'no'))
    print('campos por tipo', c(f['type'] for f in fields).most_common())
    print('metodos', len(methods))
    print('vistas', c(v['type'] for v in out['views']).most_common())
    print('qweb', len(out['qweb']))
    print('acciones', c(a['model'] for a in out['actions']))
    print('menus', len(out['menus']))
    print('grupos', len(out['groups']), 'reglas', len(out['rules']), 'accesos', len(access))
    print('crons', len(out['crons']))
    print('datos', c(d['model'] for d in out['data']).most_common())
    print('registros', len(out['records']), 'noupdate', sum(1 for r in out['records'] if r['noupdate'] == 'sí'))
    print('archivos', c(f['estado'] for f in files_rows).most_common())
    print('assets', len(assets))
    print('xml raros', len(out['other_tags']), 'errores', out['xml_errors'])


if __name__ == '__main__':
    main()
