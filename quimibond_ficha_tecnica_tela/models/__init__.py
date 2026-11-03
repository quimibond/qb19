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
from . import alternativas  # 2.20.0: después de acabado_ruta y mrp_aviso_maquina (extiende operación y orden de trabajo)
from . import acabado_tintoreria  # 2.16.0: pestaña Tintorería (usa la cadena de acabado_ruta y los procesos)
from . import unidades  # 2.10.0: al final (extiende tejido, acabado, parámetros y características)
from . import tiempo_ficha  # 2.22.0 (2.23.0: antes tiempo_rama.py): después de acabado_ruta (usa ft_proceso_id) y proceso
from . import capacidad_conversion  # 2.23.0: después de tiempo_ficha (usa _ft_producto_bom) y mrp_aviso_maquina
from . import revision_listas  # 2.23.0: después de tiempo_ficha y capacidad_conversion (usa sus campos)
from . import ficha_producto  # 2.23.0: al final (usa la revisión, la ficha del producto y los procesos)
from . import ruta_familia  # 2.23.0: después de la revisión y la ficha del producto (usa _ft_bom_de y action_ft_datos_ficha)
from . import rendimiento_real  # 2.24.0: al final (extiende la ficha de acabado, sus características y la revisión)
