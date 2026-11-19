from . import controllers
from . import models


def post_init_hook(env):
    """Carga la lista inicial de armadoras en los sitios que no tienen una."""
    from .controllers.contenido import ARMADORAS
    env['website'].search([('qb_armadoras', '=', False)]).write({'qb_armadoras': '\n'.join(ARMADORAS)})
