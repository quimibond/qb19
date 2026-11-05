# -*- coding: utf-8 -*-
from . import calidad_defecto_tela
from . import mrp_inspeccion_restriccion
from . import mrp_tara_producto
from . import mrp_rollo_bitacora
from . import mrp_rollo_final  # 3.1.0: después de la bitácora (la extiende)
from . import hr_employee
from . import stock_lot
from . import stock_move  # 3.3.1: la orden de saldos no lanza abastecimiento
from . import mrp_production
from . import mrp_production_inspeccion  # 3.1.0: después de mrp_production (llama a su cierre)
from . import mrp_workorder
from . import res_config_settings
