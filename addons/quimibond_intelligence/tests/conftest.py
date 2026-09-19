"""
conftest.py — make SupabaseClient importable without a full Odoo installation.

The test imports:
    from quimibond_intelligence.models.supabase_client import SupabaseClient

When pytest collects the test it tries to import the full
`quimibond_intelligence` package, which in turn imports `sync_log.py` (an
Odoo model that needs `from odoo import ...`).  To avoid that we register a
lightweight fake package in sys.modules *before* collection begins and load
supabase_client.py straight from disk into it.
"""
import sys
import types
import importlib.util
from pathlib import Path


def _load_supabase_client():
    """
    Directly load supabase_client.py into a synthetic package so the test's
    import statement resolves without walking the full addon __init__.py.
    """
    models_dir = Path(__file__).parent.parent / 'models'
    spec = importlib.util.spec_from_file_location(
        'quimibond_intelligence.models.supabase_client',
        models_dir / 'supabase_client.py',
    )
    mod = importlib.util.module_from_spec(spec)

    # Ensure the parent packages exist in sys.modules
    pkg = sys.modules.setdefault('quimibond_intelligence', types.ModuleType('quimibond_intelligence'))
    models_pkg = sys.modules.setdefault('quimibond_intelligence.models', types.ModuleType('quimibond_intelligence.models'))
    pkg.models = models_pkg

    sys.modules['quimibond_intelligence.models.supabase_client'] = mod
    models_pkg.supabase_client = mod
    spec.loader.exec_module(mod)


_load_supabase_client()

# Los test_senales_*.py y test_push_senales.py son tests de Odoo (TransactionCase): los corre el CI
# dentro de Odoo, no pytest.
collect_ignore_glob = ['test_senales_*.py', 'test_push_senales.py', 'common.py']
