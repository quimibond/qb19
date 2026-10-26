from . import ficha_tecnica_tejido
from . import ficha_tecnica_acabado
from . import product_template
from . import product_product
from . import ficha_tecnica_caracteristica
from . import maquina_config
from . import mrp_aviso_maquina  # 2.5.0: después de maquina_config (usa _ft_config_at)
from . import proceso  # 2.8.0: después de maquina_config (usa el mixin de valores y _ft_parametros)
from . import materia_prima  # 2.9.0: después de maquina_config (mismo motor de parámetros) y del tejido
from . import acabado_ruta  # 2.9.0: crudo y operaciones de la ficha de acabado (después de proceso)
from . import acabado_tintoreria  # 2.16.0: pestaña Tintorería (usa la cadena de acabado_ruta y los procesos)
from . import unidades  # 2.10.0: al final (extiende tejido, acabado, parámetros y características)
