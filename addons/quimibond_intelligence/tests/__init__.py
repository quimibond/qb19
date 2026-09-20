# Tests de Odoo (TransactionCase): los corre el CI dentro de Odoo
# (--test-tags /quimibond_intelligence). Los archivos de pytest puro
# (conftest.py, test_supabase_client_details.py) no se importan aquí, y este
# paquete también lo importa pytest al recolectar: fuera de Odoo no existe
# `odoo.tests`, así que solo se salta ese caso (cualquier otro error sí sube).
try:
    from . import test_senales_base
    from . import test_senales_finanzas
    from . import test_senales_comercial
    from . import test_senales_operaciones
    from . import test_senales_compras
    from . import test_senales_calidad
    from . import test_senales_rh_sistemas
    from . import test_senales_direccion
    from . import test_push_senales
except ModuleNotFoundError as exc:  # pragma: no cover - solo fuera de Odoo (pytest)
    if not (exc.name or '').startswith('odoo'):
        raise
