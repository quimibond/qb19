# -*- coding: utf-8 -*-
"""Cierre del baño con Inspección como estación única (diseño 2026-11-03, §3 y §4.5).

El supervisor cierra la orden en kg con el botón nativo «Marcar como hecho» (D9). El cierre de José se queda como está
(no cierra con rollos sin inspeccionar; iguala la orden a lo pesado en Rama y consume todo el crudo) y, en la misma
transacción, se registra la orden en metros que la orden en kg abastece (MTO):

- consume cada rollo de Rama completo (sus kg de Rama, §4.6);
- produce cada rollo final con su lote ``M…``: PQ y FE la tela en metros (14 y 15), Segunda y Desperdicio el saldo en
  kg (16 y 17), D5 y §4.3;
- la orden en metros se cierra con los metros reales de primera y FE y los kg reales de los saldos.

Si algo falla, no se cierra nada (caso 11). No hay cierre parcial (D7): los rollos finales no están en inventario
hasta que el baño cierra.
"""
from odoo import api, fields, models, _
from odoo.exceptions import UserError
from odoo.tools import float_compare, float_is_zero


class MrpProductionInspeccion(models.Model):
    _inherit = 'mrp.production'

    rollo_final_kg_ids = fields.One2many('mrp.rollo.final', 'production_id', 'Rollos finales (Inspección)')
    rollo_final_m_ids = fields.One2many('mrp.rollo.final', 'production_m_id', 'Rollos finales')
    inspeccion_completa_sin_cerrar = fields.Boolean(
        'Rollos inspeccionados, sin cerrar', compute='_compute_inspeccion_completa_sin_cerrar', store=True,
        help='Orden en kg abierta con todos sus rollos de Rama inspeccionados: el baño ya se puede cerrar. Sus rollos '
             'finales no están en inventario hasta que se cierre.')

    @api.depends('state', 'rollo_bitacora_ids.estado_inspeccion')
    def _compute_inspeccion_completa_sin_cerrar(self):
        for orden in self:
            rollos = orden.rollo_bitacora_ids
            orden.inspeccion_completa_sin_cerrar = bool(rollos) and orden.state not in ('done', 'cancel') \
                and all(r.estado_inspeccion == 'inspeccionado' for r in rollos)

    def _insp_orden_metros(self):
        """La orden en metros que consume el acabado en kg de esta orden (destino MTO de su terminado)."""
        self.ensure_one()
        terminado = self.move_finished_ids.filtered(lambda m: m.product_id == self.product_id)
        ordenes = terminado.move_dest_ids.raw_material_production_id.filtered(lambda p: p.state != 'cancel')
        return ordenes[:1]

    def button_mark_done(self):
        con_rollos_finales = self.filtered('rollo_final_kg_ids')
        res = super().button_mark_done()
        if isinstance(res, dict):
            # El core pidió confirmar algo (consumo, pendiente): el baño todavía no se cerró. Al confirmar, el asistente
            # del core vuelve a llamar a este método y entonces se registra.
            return res
        for orden in con_rollos_finales.filtered(lambda o: o.state == 'done'):
            orden._insp_registrar_metros()
        return res

    def _insp_registrar_metros(self):
        """Registra en la orden en metros los rollos finales del baño (§4.5). ``self`` es la orden en kg ya cerrada."""
        self.ensure_one()
        finales = self.rollo_final_kg_ids.filtered(lambda r: r.estado == 'inspeccionado')
        if not finales:
            return
        orden_m = finales.production_m_id
        if len(orden_m) != 1:
            raise UserError(_('Los rollos finales de %s pertenecen a más de una orden en metros.', self.name))
        if orden_m.state in ('done', 'cancel'):
            raise UserError(_('No se puede registrar el baño %(kg)s: la orden en metros %(m)s ya está %(e)s.',
                              kg=self.name, m=orden_m.name, e=dict(orden_m._fields['state'].selection)[orden_m.state]))
        if orden_m.state == 'draft':
            orden_m.action_confirm()
        Final = self.env['mrp.rollo.final']
        rama = finales.bitacora_ids
        primeras = finales.filtered(lambda r: r.clasificacion in ('PQ', 'FE'))
        saldos = finales - primeras
        uom_m = orden_m.product_id.uom_id
        total_m = uom_m.round(sum(primeras.mapped('metros')))
        if float_is_zero(total_m, precision_rounding=uom_m.rounding):
            raise UserError(_(
                'El baño %s no tiene rollos de primera ni FE: la orden en metros %s no puede cerrar en cero. Revisa la '
                'clasificación en Inspección.', self.name, orden_m.name))

        # 1. Cantidad de la orden en metros = metros reales de primera y FE (sin pendiente: D7).
        orden_m.write({'product_qty': total_m, 'qty_producing': total_m, 'consumption': 'flexible'})

        # 2. Consumo: cada rollo de Rama completo, de donde esté (10 INSPECCIÓN al cerrar la orden en kg).
        raw = orden_m._clasif_move_a()
        if not raw:
            raise UserError(_('La orden en metros %s no tiene consumo pendiente del acabado en kg.', orden_m.name))
        orden_m._clasif_asegurar_consumo(rama)
        (raw.move_line_ids - raw.move_line_ids.filtered(lambda ml: ml.lot_id in rama.lot_id)).unlink()
        raw.with_context(do_not_unreserve=True).write({'product_uom_qty': sum(rama.mapped('kg_rama'))})

        # 3. La tela en metros: un lote por rollo final, PQ a 14 y FE a 15.
        terminado = orden_m.move_finished_ids.filtered(
            lambda m: m.product_id == orden_m.product_id and m.state not in ('done', 'cancel'))[:1]
        if not terminado:
            raise UserError(_('La orden en metros %s no tiene movimiento de la tela pendiente.', orden_m.name))
        orden_m._rama_unlink_lines_without_lot(terminado)
        terminado.move_line_ids.unlink()
        terminado.with_context(do_not_unreserve=True).write({'product_uom_qty': total_m})
        for rollo in primeras.sorted('secuencia'):
            destino = Final._ubicacion_calidad(rollo.clasificacion)
            lote = self._insp_lote(orden_m.product_id, rollo.name, orden_m.company_id)
            self.env['stock.move.line'].create({
                'move_id': terminado.id, 'product_id': orden_m.product_id.id, 'lot_id': lote.id,
                'quantity': rollo.metros, 'location_id': terminado.location_id.id,
                'location_dest_id': destino.id, 'production_id': orden_m.id})
            rollo.write({'product_id': orden_m.product_id.id, 'lot_id': lote.id, 'location_dest_id': destino.id})

        # 4. Los saldos en kg (subproductos de la lista en metros, cantidad 0 en la lista; la real es el peso).
        for clasificacion in ('SEGUNDA', 'DESP'):
            grupo = saldos.filtered(lambda r: r.clasificacion == clasificacion)
            if not grupo:
                continue
            producto = Final._producto_saldo(clasificacion)
            movimiento = orden_m.move_finished_ids.filtered(
                lambda m: m.product_id == producto and m.state not in ('done', 'cancel'))[:1]
            if not movimiento:
                raise UserError(_(
                    'La lista en metros de %(m)s no tiene el subproducto %(s)s: agrégalo (pestaña Subproductos) o '
                    'genera las listas desde la ficha.', m=orden_m.product_id.display_name, s=producto.display_name))
            destino = Final._ubicacion_calidad(clasificacion)
            movimiento.move_line_ids.unlink()
            movimiento.with_context(do_not_unreserve=True).write({
                'product_uom_qty': producto.uom_id.round(sum(grupo.mapped('kg_neto'))),
                'location_dest_id': destino.id})
            for rollo in grupo.sorted('secuencia'):
                lote = self._insp_lote(producto, rollo.name, orden_m.company_id) if producto.tracking != 'none' \
                    else self.env['stock.lot']
                self.env['stock.move.line'].create({
                    'move_id': movimiento.id, 'product_id': producto.id, 'lot_id': lote.id or False,
                    'quantity': rollo.kg_neto, 'location_id': movimiento.location_id.id,
                    'location_dest_id': destino.id, 'production_id': orden_m.id})
                rollo.write({'product_id': producto.id, 'lot_id': lote.id or False, 'location_dest_id': destino.id})
            movimiento.picked = True

        # 5. Se repite el consumo (escribir qty_producing reescala líneas del core) y se cierra.
        orden_m._clasif_asegurar_consumo(rama)
        resultado = orden_m.button_mark_done()
        if orden_m.state != 'done':
            raise UserError(_(
                'No se pudo registrar la orden en metros %(m)s al cerrar el baño %(kg)s; no se cerró nada. Respuesta '
                'de Odoo: %(r)s', m=orden_m.name, kg=self.name,
                r=(resultado or {}).get('name') if isinstance(resultado, dict) else resultado))
        finales.write({'estado': 'registrado'})
        hechos = orden_m.move_finished_ids.filtered(lambda m: m.state == 'done')
        if float_compare(sum(hechos.filtered(lambda m: m.product_id == orden_m.product_id).mapped('quantity')),
                         total_m, precision_rounding=uom_m.rounding):
            raise UserError(_('La orden en metros %s no registró los metros de los rollos finales.', orden_m.name))
        self.message_post(body=_(
            'Baño cerrado: %(n)s rollos finales registrados en %(m)s (%(mts).2f m de primera y FE; %(kg).2f kg de '
            'saldos).', n=len(finales), m=orden_m.name, mts=total_m, kg=sum(saldos.mapped('kg_neto'))))

    @api.model
    def _insp_lote(self, producto, nombre, company):
        Lote = self.env['stock.lot']
        dominio = [('name', '=', nombre), ('product_id', '=', producto.id), ('company_id', '=', company.id)]
        if Lote.search_count(dominio):
            raise UserError(_('Ya existe el lote %(l)s para %(p)s.', l=nombre, p=producto.display_name))
        return Lote.create({'name': nombre, 'product_id': producto.id, 'company_id': company.id})
