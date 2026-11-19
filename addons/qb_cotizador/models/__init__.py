# -*- coding: utf-8 -*-
from . import motivo
from . import res_partner  # 1.6.0: antes de cotizacion (usa sus campos)
from . import aprobacion  # 1.8.0: ciclo compartido; antes de cotizacion y negociacion (lo heredan)
from . import cotizacion
from . import negociacion  # 1.8.0
from . import acuerdo  # 1.8.0: después de negociacion (usa descuento_tramos)
from . import account_move  # 1.8.0
from . import sale_order
from . import settings
