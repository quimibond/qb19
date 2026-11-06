import inspect

from odoo.tests.common import BaseCase

from . import test_api
from . import test_api_key_scope
from . import test_authentication
from . import test_config_settings
from . import test_controller_utils
from . import test_custom_tools
from . import test_enabled_models
from . import test_error_sanitizer
from . import test_helpers
from . import test_logging
from . import test_main_controller
from . import test_mcp_protocol
from . import test_mcp_rate_limit
from . import test_mcp_read_tools
from . import test_mcp_tools_unit
from . import test_mcp_write_tools
from . import test_model_selection_ui
from . import test_oauth
from . import test_oauth_scopes
from . import test_rate_limiting
from . import test_security
from . import test_suite_tag
from . import test_user_context


def _quitar_standard(modulo):
    """Odoo.sh corre el tag `standard` sobre la copia de producción.

    Las pruebas del proveedor piden llaves de 30 días y una base vacía
    (más un login admin/admin). Ahí fallan en el setUp, antes de probar
    nada, y el build de cada rama de desarrollo sale en rojo. Decisión
    2026-10-22, docs/RUNBOOK_DESPLIEGUE.md. Siguen disponibles con
    ``--test-tags much_unit/mcp_server`` en una base limpia.
    La clase marcada ``mcp_suite`` se deja en `standard`: es la que
    vigila que el resto no vuelva a entrar.
    """
    for obj in vars(modulo).values():
        if not inspect.isclass(obj) or not issubclass(obj, BaseCase):
            continue
        # No tocar HttpCase/TransactionCase importados: viven en odoo.tests.
        if obj.__module__ != modulo.__name__:
            continue
        tags = getattr(obj, 'test_tags', None)
        if not tags or 'mcp_suite' in tags:
            continue
        obj.test_tags = set(tags) - {'standard'}


for _mod in list(vars().values()):
    if inspect.ismodule(_mod) and getattr(_mod, '__name__', '').startswith(__name__ + '.'):
        _quitar_standard(_mod)
