"""
Root conftest.py for /Users/jj/addons.

Stubs out the `odoo` package so that addon modules can be imported without a
real Odoo installation, enabling unit tests that only exercise pure-Python code.
"""
import sys
import types


def _stub_odoo():
    """Insert minimal stub modules for every odoo.* name the addon imports."""
    top_names = [
        'odoo',
        'odoo.models',
        'odoo.fields',
        'odoo.api',
        'odoo.exceptions',
        'odoo.tools',
        'odoo.tools.safe_eval',
    ]
    for name in top_names:
        if name not in sys.modules:
            sys.modules[name] = types.ModuleType(name)

    odoo = sys.modules['odoo']
    for attr in ('models', 'fields', 'api', 'exceptions', 'tools'):
        mod = sys.modules.setdefault(f'odoo.{attr}', types.ModuleType(f'odoo.{attr}'))
        setattr(odoo, attr, mod)

    # odoo.models
    odoo_models = sys.modules['odoo.models']
    for cls in ('Model', 'TransientModel', 'AbstractModel'):
        if not hasattr(odoo_models, cls):
            setattr(odoo_models, cls, object)

    # odoo.fields — each field is a callable returning None
    odoo_fields = sys.modules['odoo.fields']
    for fname in ('Char', 'Integer', 'Float', 'Boolean', 'Text',
                  'Many2one', 'One2many', 'Many2many', 'Datetime',
                  'Date', 'Selection', 'Html', 'Binary', 'Monetary'):
        if not hasattr(odoo_fields, fname):
            setattr(odoo_fields, fname, lambda *a, **kw: None)

    # odoo.api — decorators that are transparent pass-throughs
    odoo_api = sys.modules['odoo.api']
    def _passthrough(f=None, *a, **kw):
        if callable(f):
            return f
        return lambda g: g
    for dname in ('model', 'depends', 'constrains', 'onchange', 'returns',
                  'multi', 'one', 'cr', 'model_cr', 'model_create_multi'):
        if not hasattr(odoo_api, dname):
            setattr(odoo_api, dname, _passthrough)

    # odoo.exceptions
    odoo_exc = sys.modules['odoo.exceptions']
    for exc in ('UserError', 'ValidationError', 'AccessError', 'MissingError'):
        if not hasattr(odoo_exc, exc):
            setattr(odoo_exc, exc, type(exc, (Exception,), {}))


_stub_odoo()
