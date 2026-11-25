# -*- coding: utf-8 -*-
from . import res_company
from . import res_partner
# Antes de sat_compare: la vista SQL lee la columna sat_pago_terceros.
from . import product_template
from . import sat_sync_log
from . import syntage_client
from . import sat_cfdi
from . import sat_cfdi_factura
from . import sat_cfdi_folio
from . import sat_bill_rule
# Después de sat_cfdi_factura: hereda su _create_bill_from_xml.
from . import sat_cfdi_compra
from . import sat_cfdi_relaciones
from . import sat_cfdi_estado
from . import sat_syntage_file
from . import sat_webhook_event
from . import sat_compare
from . import sat_cfdi_pago
from . import sat_pago_compare
from . import sat_iva_month
from . import account_move
from . import account_move_semaforo
# Después de sat_pago_compare: lo extiende con los avisos de complementos.
from . import sat_rep_avisos
