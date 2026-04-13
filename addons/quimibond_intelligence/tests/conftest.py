"""
conftest.py — make IngestionCore importable without a full Odoo installation.

The test imports:
    from quimibond_intelligence.models.ingestion_core import IngestionCore

When pytest collects the test it tries to import the full
`quimibond_intelligence` package, which in turn imports `sync_log.py` (an
Odoo model that needs `from odoo import ...`).  To avoid that we register a
lightweight fake package in sys.modules *before* collection begins.
"""
import sys
import types
import importlib.util
from pathlib import Path


def _stub_odoo():
    """Insert stub modules so Odoo-dependent files don't crash on import."""
    for name in ('odoo', 'odoo.models', 'odoo.fields', 'odoo.api',
                 'odoo.exceptions', 'odoo.tools'):
        if name not in sys.modules:
            sys.modules[name] = types.ModuleType(name)

    odoo = sys.modules['odoo']
    # Minimal stubs expected by the addon models
    for attr in ('models', 'fields', 'api', 'exceptions', 'tools'):
        mod = sys.modules.get(f'odoo.{attr}', types.ModuleType(f'odoo.{attr}'))
        setattr(odoo, attr, mod)

    # odoo.models.Model base class
    odoo_models = sys.modules['odoo.models']
    if not hasattr(odoo_models, 'Model'):
        odoo_models.Model = object
    if not hasattr(odoo_models, 'TransientModel'):
        odoo_models.TransientModel = object

    # odoo.fields stubs
    odoo_fields = sys.modules['odoo.fields']
    for fname in ('Char', 'Integer', 'Float', 'Boolean', 'Text',
                  'Many2one', 'One2many', 'Many2many', 'Datetime',
                  'Date', 'Selection', 'Html'):
        if not hasattr(odoo_fields, fname):
            setattr(odoo_fields, fname, lambda *a, **kw: None)

    # odoo.api stubs
    odoo_api = sys.modules['odoo.api']
    for dname in ('model', 'depends', 'constrains', 'onchange', 'returns',
                  'multi', 'one', 'cr', 'model_cr'):
        if not hasattr(odoo_api, dname):
            setattr(odoo_api, dname, lambda f=None, *a, **kw: (f if f else lambda g: g))


_stub_odoo()


def _load_ingestion_core():
    """
    Directly load ingestion_core.py into a synthetic package so the test's
    import statement resolves without walking the full addon __init__.py.
    """
    models_dir = Path(__file__).parent.parent / 'models'
    spec = importlib.util.spec_from_file_location(
        'quimibond_intelligence.models.ingestion_core',
        models_dir / 'ingestion_core.py',
    )
    mod = importlib.util.module_from_spec(spec)

    # Ensure the parent packages exist in sys.modules
    pkg = sys.modules.setdefault('quimibond_intelligence', types.ModuleType('quimibond_intelligence'))
    models_pkg = sys.modules.setdefault('quimibond_intelligence.models', types.ModuleType('quimibond_intelligence.models'))
    pkg.models = models_pkg

    sys.modules['quimibond_intelligence.models.ingestion_core'] = mod
    models_pkg.ingestion_core = mod
    spec.loader.exec_module(mod)


_load_ingestion_core()
