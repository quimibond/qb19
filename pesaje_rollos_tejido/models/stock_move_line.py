import logging
import traceback
from odoo import models

_logger = logging.getLogger(__name__)


class StockMoveLine(models.Model):
    _inherit = 'stock.move.line'

    def unlink(self):
        for ml in self:
            if ml.move_id.production_id:
                _logger.warning(
                    "DIAGNOSTICO UNLINK: line %s (move %s, prod %s, qty %s, picked %s). Stack:\n%s",
                    ml.id, ml.move_id.id, ml.move_id.production_id.name,
                    ml.quantity, ml.picked,
                    ''.join(traceback.format_stack())
                )
        return super().unlink()