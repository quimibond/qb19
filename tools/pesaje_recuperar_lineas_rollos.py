# -*- coding: utf-8 -*-
"""Recupera las líneas de movimiento de rollos que `_do_unreserve()` borró.

Contexto (2026-10-06/07): al editar la «Cantidad a producir» de una orden de
fabricación en progreso, el core de Odoo 19 vuelve a poner la demanda del
movimiento del terminado en `product_qty`; el siguiente pesaje la regresaba a 0
con `stock.move.write()` sin `do_not_unreserve`, y el core borraba todas las
líneas de rollo no marcadas como `picked`. Los lotes (`stock.lot`) y el
Historial de Pesaje (`mrp.weighing.log`) quedaron intactos, y `qty_producing`
y `qty_produced` de la orden de trabajo ya traían el peso de esos rollos: lo
único que falta es la línea `stock.move.line` de cada rollo. El parche
`pesaje_rollos_tejido` 19.0.1.4.2 evita que vuelva a pasar.

Uso, en la shell de Odoo.sh de la rama de producción (`odoo-shell`):

    >>> exec(open('/home/odoo/src/user/tools/pesaje_recuperar_lineas_rollos.py').read())
    >>> detectar(env)                                   # qué órdenes tienen rollos sin línea
    >>> recuperar(env, ['TL/OP-TEJ/H13915'])             # en seco: solo imprime
    >>> recuperar(env, ['TL/OP-TEJ/H13915'], aplicar=True)   # crea las líneas y hace commit
    >>> desarmar(env)                                   # demanda a 0 (con do_not_unreserve) en OFs en progreso
    >>> desarmar(env, aplicar=True)

Reglas:
- Solo se recrea la línea de un rollo cuyo lote existe y NO tiene ninguna
  línea en ningún movimiento (si el lote ya se movió a otro lado, se reporta
  y no se toca).
- No se modifica `qty_producing` ni `qty_produced`: ya incluyen esos rollos.
  Al final se imprime la diferencia entre la suma de líneas y `qty_producing`
  para comprobarlo.
- Sin `aplicar=True` se hace rollback al terminar.
"""
from datetime import datetime


def _movimiento_terminado(production):
    return production.move_finished_ids.filtered(
        lambda m: m.product_id == production.product_id and m.state not in ('done', 'cancel')
    )[:1]


def _rollos_sin_linea(env, production):
    """Devuelve [(log, lot, lineas_en_otro_lado)] por cada rollo del historial."""
    Lot = env['stock.lot']
    Line = env['stock.move.line']
    out = []
    for log in production.weighing_log_ids.sorted('roll_number'):
        lot = log.lot_id or Lot.search([
            ('name', '=', log.roll_number),
            ('product_id', '=', production.product_id.id),
            ('company_id', '=', production.company_id.id),
        ], limit=1)
        lineas = Line.search([('lot_id', '=', lot.id)]) if lot else Line
        out.append((log, lot, lineas))
    return out


def detectar(env, nombres=None):
    """Lista las órdenes en progreso con rollos pesados que no tienen línea."""
    dominio = [('state', 'in', ('progress', 'to_close')), ('roll_count', '>', 0)]
    if nombres:
        dominio.append(('name', 'in', nombres))
    afectadas = []
    for production in env['mrp.production'].search(dominio, order='name'):
        rollos = _rollos_sin_linea(env, production)
        faltan = [log.roll_number for log, lot, lineas in rollos if lot and not lineas]
        sin_lote = [log.roll_number for log, lot, lineas in rollos if not lot]
        move = _movimiento_terminado(production)
        if faltan or sin_lote or (move and move.product_uom_qty):
            afectadas.append(production.name)
            print("%s  rollos=%d  sin línea=%d %s  sin lote=%d  demanda=%s" % (
                production.name, production.roll_count, len(faltan), faltan,
                len(sin_lote), move.product_uom_qty if move else '-'))
    if not afectadas:
        print("Sin órdenes afectadas.")
    return afectadas


def recuperar(env, nombres, aplicar=False):
    """Recrea las líneas de los rollos sin línea en las órdenes indicadas."""
    Line = env['stock.move.line']
    creadas = 0
    for production in env['mrp.production'].search([('name', 'in', nombres)]):
        move = _movimiento_terminado(production)
        if not move:
            print("%s: sin movimiento de terminado abierto, se omite" % production.name)
            continue
        wo = production.workorder_ids.filtered(lambda w: w.state in ('ready', 'progress'))[:1]
        uom = production.product_id.uom_id
        print("== %s (%s) ==" % (production.name, production.product_id.display_name))
        for log, lot, lineas in _rollos_sin_linea(env, production):
            if not lot:
                print("  %s: SIN LOTE, no se recrea" % log.roll_number)
                continue
            if lineas:
                otros = lineas.filtered(lambda l: l.move_id != move)
                if otros:
                    print("  %s: el lote ya tiene línea en otro movimiento (%s), se omite" % (
                        log.roll_number, ', '.join(otros.mapped('move_id.reference'))))
                continue
            vals = {
                'move_id': move.id,
                'product_id': production.product_id.id,
                'lot_id': lot.id,
                'lot_name': lot.name,
                'quantity': uom.round(log.weight_neto),
                'location_id': move.location_id.id,
                'location_dest_id': move.location_dest_id.id,
                'workorder_id': wo.id if wo else False,
                'production_id': production.id,
                'date': log.date_time or datetime.now(),
            }
            print("  %s: recrear %.4f kg (pesado %s)" % (log.roll_number, vals['quantity'], log.date_time))
            if aplicar:
                Line.create(vals)
                if not log.lot_id:
                    log.lot_id = lot.id
            creadas += 1
        suma = sum(move.move_line_ids.mapped('quantity'))
        print("  suma de líneas %.2f  vs  qty_producing %.2f  (diferencia %.2f)" % (
            suma, production.qty_producing, production.qty_producing - suma))
    if aplicar:
        env.cr.commit()
        print("Listo: %d líneas creadas y confirmadas." % creadas)
    else:
        env.cr.rollback()
        print("En seco: %d líneas por crear. Repite con aplicar=True." % creadas)
    return creadas


def desarmar(env, aplicar=False):
    """Regresa a 0 la demanda del terminado en las OFs en progreso con rollos,
    con do_not_unreserve, para que el siguiente pesaje no borre nada aunque el
    módulo sin parche siga corriendo."""
    n = 0
    for production in env['mrp.production'].search([('state', 'in', ('progress', 'to_close')), ('roll_count', '>', 0)]):
        move = _movimiento_terminado(production)
        if move and move.product_uom_qty:
            print("%s: demanda %.2f -> 0 (líneas %d, %.2f kg)" % (
                production.name, move.product_uom_qty, len(move.move_line_ids), move.quantity))
            if aplicar:
                move.with_context(do_not_unreserve=True).write({'product_uom_qty': 0})
            n += 1
    if aplicar:
        env.cr.commit()
    else:
        env.cr.rollback()
    print("%d órdenes %s." % (n, 'actualizadas' if aplicar else 'por actualizar (en seco)'))
    return n
