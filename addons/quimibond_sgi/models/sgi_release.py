# -*- coding: utf-8 -*-
"""P-7: no surtir lotes sin liberar (C6.10) y liga del traslado Embarcar con
su entrega (C2.21 / C2.26).

**Lote liberado por Calidad** es, en Quimibond, un lote que ya salió de las
ubicaciones de espera: la materia prima se libera al pasar de «6 Entrada MP»
a «1 Materia Prima» con sus controles aprobados (C6.05) y el producto
terminado al pasar de Inspección → Liberación → 14/15/16 con el control de
la operación Liberar aprobado (C4.18, C5.07, C6.16). Un lote con un control
de calidad fallido tampoco está liberado, esté donde esté.

Las requisiciones de producción (Requisición MP y Requisición PP y PT) no se
pueden validar con un lote así. El bloqueo es duro: lo que corresponde es
avisar a Control de producción y a Calidad (C6.10 «si falla»), no surtir.

Parámetros (Ajustes → Técnico → Parámetros del sistema):

- ``quimibond_sgi.release_block_enabled`` (True): apaga el bloqueo.
- ``quimibond_sgi.release_block_picking_type_ids`` (ids separados por coma):
  tipos de operación que se bloquean; en producción 113 (Requisición MP) y
  210 (Requisición PP y PT).
- ``quimibond_sgi.unreleased_location_ids`` (ids separados por coma):
  ubicaciones (y sus hijas) donde un lote todavía no está liberado; en
  producción 324 (6 Entrada MP), 44 (18 Cuarentena MP), 36 (10 Inspección),
  246 (Liberación) y 45 (19 Cuarentena PP y T).
"""
from datetime import datetime

from odoo import api, fields, models
from odoo.exceptions import UserError

RELEASE_BLOCK_ENABLED_PARAM = 'quimibond_sgi.release_block_enabled'
RELEASE_BLOCK_TYPES_PARAM = 'quimibond_sgi.release_block_picking_type_ids'
UNRELEASED_LOCATIONS_PARAM = 'quimibond_sgi.unreleased_location_ids'


def _param_ids(env, key):
    raw = env['ir.config_parameter'].sudo().get_param(key) or ''
    return [int(part) for part in raw.replace(';', ',').split(',') if part.strip().isdigit()]


class StockPicking(models.Model):
    _inherit = 'stock.picking'

    # ---- Liga del traslado interno con la entrega del mismo pedido ----
    # El traslado Embarcar (14 PT → Embarques) no encadena movimientos con la
    # orden de entrega; comparte el pedido. Con este campo C2.21 y C2.26 se
    # miden contra la fecha programada de la entrega (P-4: due_field).
    sgi_delivery_picking_id = fields.Many2one(
        'stock.picking', string="Entrega que surte", compute='_compute_sgi_delivery_picking',
        store=True, readonly=False, index=True, copy=False,
        help="Orden de entrega del mismo pedido que este traslado interno "
             "surte: la abierta con la fecha programada más próxima, o la "
             "última si todas están hechas. Se puede corregir a mano.")

    @api.depends('sale_id', 'picking_type_code', 'sale_id.picking_ids.state',
                 'sale_id.picking_ids.picking_type_code',
                 'sale_id.picking_ids.scheduled_date')
    def _compute_sgi_delivery_picking(self):
        far = datetime.max
        for picking in self:
            if picking.picking_type_code != 'internal' or not picking.sale_id:
                picking.sgi_delivery_picking_id = False
                continue
            deliveries = picking.sale_id.picking_ids.filtered(
                lambda p: p.picking_type_code == 'outgoing' and p.state != 'cancel'
                and p.id != picking.id)
            open_ = deliveries.filtered(lambda p: p.state != 'done')
            if open_:
                chosen = open_.sorted(lambda p: (p.scheduled_date or far, p.id))[:1]
            else:
                chosen = deliveries.sorted(
                    lambda p: (p.date_done or p.scheduled_date or far, p.id))[-1:]
            picking.sgi_delivery_picking_id = chosen

    # ---- P-7: no surtir lotes sin liberar ----
    @api.model
    def _sgi_release_block_enabled(self):
        raw = self.env['ir.config_parameter'].sudo().get_param(
            RELEASE_BLOCK_ENABLED_PARAM, 'True')
        return str(raw).strip().lower() in ('1', 'true', 'yes', 'si', 'sí')

    def _sgi_unreleased_lines(self):
        """Líneas de este surtido cuyo lote no está liberado: (línea, motivo)."""
        self.ensure_one()
        unreleased = self.env['stock.location'].browse(
            _param_ids(self.env, UNRELEASED_LOCATIONS_PARAM)).exists()
        out = []
        lines = self.move_line_ids.filtered(lambda l: l.quantity)
        failed_lots = self.env['stock.lot']
        lots = lines.lot_id
        if lots:
            checks = self.env['quality.check'].sudo().search(
                [('lot_ids', 'in', lots.ids), ('quality_state', '!=', 'none')],
                order='id asc')
            last = {}
            for check in checks:
                for lot in check.lot_ids:
                    last[lot.id] = check.quality_state
            failed_lots = lots.filtered(lambda l: last.get(l.id) == 'fail')
        for line in lines:
            location = line.location_id
            if unreleased and any(location.parent_path.startswith(u.parent_path)
                                  for u in unreleased):
                out.append((line, "en %s" % location.display_name))
            elif line.lot_id in failed_lots:
                out.append((line, "con control de calidad fallido"))
        return out

    def _sgi_check_release(self):
        if not self._sgi_release_block_enabled():
            return
        types = set(_param_ids(self.env, RELEASE_BLOCK_TYPES_PARAM))
        for picking in self.filtered(lambda p: p.picking_type_id.id in types):
            problems = picking._sgi_unreleased_lines()
            if not problems:
                continue
            detail = "\n".join(
                "• %s — %s (%s)" % (line.lot_id.name or line.product_id.display_name,
                                    line.product_id.display_name, why)
                for line, why in problems)
            raise UserError(
                "%s: no se puede surtir con lotes que Calidad no ha liberado:\n%s\n\n"
                "Avisa a Control de producción y a Calidad; surte solo lotes liberados "
                "(C6.10)." % (picking.name, detail))

    def button_validate(self):
        self._sgi_check_release()
        return super().button_validate()
