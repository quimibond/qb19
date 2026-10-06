# -*- coding: utf-8 -*-
"""Traza de la conversión absorbida: de la orden que la capitalizó a la venta.

Desde el corte, Odoo abona a la cuenta de costos fabriles aplicados lo que
las máquinas del centro absorbido corrieron (horas × tarifa) y lo carga al
inventario. Esa conversión viaja dentro de los lotes: el tejido crudo entra a
tintorería, el teñido a acabado, el acabado a la entrega. Solo la parte que
llegó a una entrega a cliente está en el costo de ventas del mes; el resto
sigue en el almacén como tejido, teñido o producto terminado.

La capa de valoración del mes es 501.01.01 − MP vendida del modelo − ESTA
cifra: restar el abono completo (como sugiere la intuición) expensaría en el
mes la conversión de mercancía que no se ha vendido y dejaría la absorción
sin efecto en resultados. En septiembre de 2026 el abono fue $1,197,422 y lo
que llegó a ventas $344,667 (28.8%): los $852,755 restantes estaban en el
almacén (lotes H $399K, lotes I $40K, lotes J $413K).

El recorrido es por lotes, no por nombres: la salida terminada de cada
orden absorbida reparte su conversión entre sus lotes a prorrata de
cantidad; cada consumo de un lote por otra orden le pasa la fracción
consumida; la salida de esa orden la vuelve a repartir; y al final cada
lote carga a ventas la fracción entregada a cliente en el período. Las
órdenes consumidoras no tienen que estar absorbidas ni tener patrón de
nombre, y el lote de salida puede llamarse como quiera.

Alcance: TODAS las órdenes absorbidas desde el corte más antiguo hasta el fin
del período, porque un lote tejido en septiembre puede venderse en octubre
y su conversión es costo de ventas de octubre. Por eso el período también
reporta cuánto queda en inventario al cierre: es el saldo que la traza del
mes siguiente tiene que seguir cargando.
"""
import logging
from collections import defaultdict

from odoo import api, models

from .cuenta_map import mo_qty_sql

_logger = logging.getLogger(__name__)

# Etapas máximas de propagación (tejido → teñido → acabado → reproceso…).
# Un ciclo real de recetas no existe; el tope es un cinturón de seguridad.
MAX_ETAPAS = 8


class QbCostoAbsorcionTraza(models.AbstractModel):
    _name = 'qb.costo.absorcion.traza'
    _description = 'Traza de la conversión absorbida hasta la venta'

    @api.model
    def trazar(self, centros, desde, periodo_ini, periodo_fin):
        """Sigue la conversión capitalizada por los workcenters de `centros`.

        `desde` es el corte más antiguo (las órdenes absorbidas se toman
        desde ahí); `[periodo_ini, periodo_fin)` es el mes que se costea.

        Devuelve un dict con:
          vendida        conversión que llegó a entregas a cliente EN el período
          vendida_acum   ídem acumulada desde el corte hasta fin del período
          en_inventario  conversión que sigue en lotes (o en órdenes sin
                         terminar) al cierre del período
          sin_lote       conversión de órdenes cuya salida no lleva lote: no
                         se puede seguir y se reporta aparte
          bruta          Σ horas × tarifa de las órdenes consideradas
          ordenes, lotes conteos, para el panel
          vendida_por_producto  {product_id: conversión vendida en el
                         período}, por el producto del lote entregado. Es
                         lo que cada fila de `qb.costo.producto` carga como
                         conversión del período: suma exactamente `vendida`
        """
        res = {'vendida': 0.0, 'vendida_acum': 0.0, 'en_inventario': 0.0,
               'sin_lote': 0.0, 'bruta': 0.0, 'ordenes': 0, 'lotes': 0,
               'vendida_por_producto': {}}
        wc_ids = centros.mapped('workcenter_ids').ids
        if not wc_ids or not desde:
            return res
        cr = self.env.cr
        self.env.flush_all()   # el SQL crudo no ve el buffer del ORM
        # 1. Lo que cada orden absorbida capitalizó: Odoo abona
        #    duration/60 × costs_hour por orden de trabajo (mrp_account).
        cr.execute("""
            SELECT mp.id, SUM(wo.duration / 60.0 * wc.costs_hour)
            FROM mrp_workorder wo
            JOIN mrp_workcenter wc ON wc.id = wo.workcenter_id
            JOIN mrp_production mp ON mp.id = wo.production_id
            WHERE wo.workcenter_id = ANY(%s)
              AND wc.costs_hour > 0 AND wo.duration > 0
              AND mp.state = 'done' AND mp.company_id = %s
              AND mp.date_finished >= %s AND mp.date_finished < %s
            GROUP BY mp.id
        """, (list(wc_ids), self.env.company.id, desde, periodo_fin))
        conv_mo = {mo_id: float(c or 0.0) for mo_id, c in cr.fetchall()}
        res['ordenes'] = len(conv_mo)
        res['bruta'] = sum(conv_mo.values())
        if not conv_mo:
            return res

        lotes = {}            # lot_id -> [conversión acumulada, cantidad producida]
        wip = defaultdict(float)   # orden consumidora sin salida terminada
        sin_lote = [0.0]

        def repartir(mo_id, conv, terminados, frontera):
            """La salida terminada de la orden se lleva la conversión a
            prorrata de cantidad. Sin salida (orden abierta) se queda en
            proceso; sin lote, se reporta como no trazable."""
            lineas = terminados.get(mo_id) or []
            total = sum(q for _lot, q in lineas)
            if total <= 0:
                wip[mo_id] += conv
                return
            for lot, q in lineas:
                parte = conv * q / total
                if not lot:
                    sin_lote[0] += parte
                    continue
                rec = lotes.get(lot)
                if rec is None:
                    rec = lotes[lot] = [0.0, 0.0]
                    rec[1] = q
                elif mo_id not in cantidad_vista[lot]:
                    rec[1] += q
                cantidad_vista[lot].add(mo_id)
                rec[0] += parte
                frontera[lot] += parte

        cantidad_vista = defaultdict(set)   # lot -> órdenes que ya sumaron cantidad
        frontera = defaultdict(float)
        terminados = self._salidas_terminadas(list(conv_mo))
        for mo_id, conv in conv_mo.items():
            repartir(mo_id, conv, terminados, frontera)

        # 2. Propagación por etapas: lo consumido de un lote pasa a la orden
        #    que lo consumió, y de ahí a sus lotes de salida. Se propagan
        #    DELTAS, así una orden que recibe conversión por dos caminos en
        #    etapas distintas reparte cada tramo al llegar.
        consumos = {}   # lot -> [(mo_id, fracción consumida)]
        consumido_qty = defaultdict(float)
        for _etapa in range(MAX_ETAPAS):
            if not frontera:
                break
            nuevos = [lot for lot in frontera if lot not in consumos]
            if nuevos:
                for lot, filas in self._consumos_por_lote(
                        nuevos, periodo_fin).items():
                    qty = lotes[lot][1]
                    consumos[lot] = []
                    for mo_id, c in filas:
                        consumido_qty[lot] += c
                        if qty > 0:
                            consumos[lot].append((mo_id, min(c / qty, 1.0)))
                for lot in nuevos:
                    consumos.setdefault(lot, [])
            pendiente = defaultdict(float)
            for lot, delta in frontera.items():
                for mo_id, frac in consumos.get(lot, ()):
                    pendiente[mo_id] += delta * frac
            frontera = defaultdict(float)
            if not pendiente:
                break
            terminados = self._salidas_terminadas(list(pendiente))
            for mo_id, conv in pendiente.items():
                repartir(mo_id, conv, terminados, frontera)
        else:
            _logger.warning(
                'qb.costo.absorcion.traza: la propagación llegó a %s etapas '
                'y aún quedaba conversión por repartir ($%.0f) — ¿recetas '
                'cíclicas?', MAX_ETAPAS, sum(frontera.values()))

        # 3. Entregas a cliente: la fracción entregada de cada lote es la que
        #    está en el costo de ventas. Lo demás sigue en inventario.
        entregas = self._entregas_por_lote(list(lotes), periodo_ini,
                                           periodo_fin)
        producto_lote = self._producto_por_lote(list(lotes))
        por_producto = defaultdict(float)
        vendida = vendida_acum = inventario = 0.0
        for lot, (conv, qty) in lotes.items():
            if qty <= 0:
                inventario += conv
                continue
            unit = conv / qty
            ent_per, ent_acum = entregas.get(lot, (0.0, 0.0))
            parte = unit * min(max(ent_per, 0.0), qty)
            vendida += parte
            if parte:
                por_producto[producto_lote.get(lot)] += parte
            vendida_acum += unit * min(max(ent_acum, 0.0), qty)
            fuera = consumido_qty.get(lot, 0.0) + max(ent_acum, 0.0)
            inventario += max(conv - unit * min(fuera, qty), 0.0)
        res.update({
            'vendida': vendida,
            'vendida_acum': vendida_acum,
            'en_inventario': inventario + sum(wip.values()),
            'sin_lote': sin_lote[0],
            'lotes': len(lotes),
            'vendida_por_producto': dict(por_producto),
        })
        return res

    # ------------------------------------------------------------------
    # Tarifa de conversión por artículo
    # ------------------------------------------------------------------
    @api.model
    def conversion_por_orden(self, centros, desde, hasta):
        """{mo_id: (product_id, conversión, cantidad en la UdM del
        producto)} de las órdenes terminadas en `[desde, hasta)` que
        corrieron en los workcenters de `centros`.

        La conversión es la misma que Odoo abona a la cuenta de costos
        fabriles aplicados: duración real de cada orden de trabajo × tarifa
        de su máquina. La cantidad es la de la orden COMPLETA (no la de la
        orden de trabajo): el kilo tejido carga toda la conversión de su
        orden."""
        wc_ids = centros.mapped('workcenter_ids').ids
        if not wc_ids:
            return {}
        cr = self.env.cr
        self.env.flush_all()
        cr.execute("""
            SELECT mp.id, mp.product_id, mp.product_uom_id, %s,
                   SUM(wo.duration / 60.0 * wc.costs_hour)
            FROM mrp_workorder wo
            JOIN mrp_workcenter wc ON wc.id = wo.workcenter_id
            JOIN mrp_production mp ON mp.id = wo.production_id
            WHERE wo.workcenter_id = ANY(%%s)
              AND wc.costs_hour > 0 AND wo.duration > 0
              AND mp.state = 'done' AND mp.company_id = %%s
              AND mp.date_finished >= %%s AND mp.date_finished < %%s
            GROUP BY mp.id
        """ % mo_qty_sql(self.env, 'mp'),
            (list(wc_ids), self.env.company.id, desde, hasta))
        filas = cr.fetchall()
        Product = self.env['product.product']
        Uom = self.env['uom.uom']
        out = {}
        for mo_id, pid, uom_id, qty, conv in filas:
            product = Product.browse(pid)
            qty = float(qty or 0.0)
            if uom_id and uom_id != product.uom_id.id:
                qty = Uom.browse(uom_id)._compute_quantity(
                    qty, product.uom_id, round=False,
                    raise_if_failure=False)
            out[mo_id] = (pid, float(conv or 0.0), qty)
        return out

    @api.model
    def productos_con_conversion(self, centros):
        """Productos que SE FABRICAN en las máquinas de `centros` aunque no
        tengan órdenes en el período: alguna vez corrieron ahí, su receta
        trae una operación ahí o una familia de máquinas del centro los
        declara. Son los que, sin historial reciente, toman la tarifa de su
        familia o la del centro en vez de explotar su receta hasta el hilo
        (y perder la conversión). También los que salieron de una orden
        cuyo nombre sigue el patrón del centro (`mo_name_pattern`)."""
        wc_ids = centros.mapped('workcenter_ids').ids
        ids = set()
        if wc_ids:
            self.env.flush_all()
            self.env.cr.execute("""
                SELECT DISTINCT mp.product_id
                FROM mrp_workorder wo
                JOIN mrp_production mp ON mp.id = wo.production_id
                WHERE wo.workcenter_id = ANY(%s)
                  AND mp.company_id = %s
                UNION
                SELECT DISTINCT COALESCE(b.product_id, pp.id)
                FROM mrp_routing_workcenter op
                JOIN mrp_bom b ON b.id = op.bom_id AND b.active
                JOIN product_product pp
                  ON pp.product_tmpl_id = b.product_tmpl_id
                WHERE op.workcenter_id = ANY(%s)
                  AND (b.product_id IS NULL OR b.product_id = pp.id)
            """, (list(wc_ids), self.env.company.id, list(wc_ids)))
            ids.update(r[0] for r in self.env.cr.fetchall() if r[0])
        # Los crudos tejidos ANTES de que las máquinas tuvieran tarifa (o en
        # órdenes sin orden de trabajo) solo se reconocen por el nombre de su
        # orden: el mismo patrón con el que el centro mide su producción.
        # Sin esto, el crudo del WK135B66JNG165 (dos órdenes de 2025, sin
        # workorder) explotaba hasta el hilo y la tela salía sin tejido.
        patrones = [p.strip() for c in centros
                    for p in (c.mo_name_pattern or '').split(',') if p.strip()]
        if patrones:
            self.env.flush_all()
            self.env.cr.execute("""
                SELECT DISTINCT product_id FROM mrp_production
                WHERE state = 'done' AND company_id = %s
                  AND name LIKE ANY(%s)
            """, (self.env.company.id, patrones))
            ids.update(r[0] for r in self.env.cr.fetchall() if r[0])
        codigos = self.env['qb.familia.producto'].search([
            ('familia_id.centro_id', 'in', centros.ids),
            ('familia_id.active', '=', True),
        ]).mapped('product_code')
        if codigos:
            ids.update(self.env['product.product'].search(
                [('default_code', 'in', codigos)]).ids)
        return ids

    @api.model
    def _producto_por_lote(self, lot_ids):
        if not lot_ids:
            return {}
        self.env.flush_all()
        self.env.cr.execute(
            'SELECT id, product_id FROM stock_lot WHERE id = ANY(%s)',
            (list(lot_ids),))
        return dict(self.env.cr.fetchall())

    # ------------------------------------------------------------------
    @api.model
    def _salidas_terminadas(self, mo_ids):
        """{mo_id: [(lot_id | None, cantidad)]} de las líneas terminadas
        hechas de cada orden, sin subproductos (el saldo y el desperdicio
        no cargan conversión: Odoo tampoco se la asigna salvo cost_share)."""
        if not mo_ids:
            return {}
        self.env.flush_all()
        self.env.cr.execute("""
            SELECT sm.production_id, sml.lot_id, SUM(sml.quantity)
            FROM stock_move_line sml
            JOIN stock_move sm ON sm.id = sml.move_id
            WHERE sm.production_id = ANY(%s)
              AND sml.state = 'done' AND sm.byproduct_id IS NULL
            GROUP BY 1, 2
        """, (list(mo_ids),))
        out = defaultdict(list)
        for mo_id, lot, q in self.env.cr.fetchall():
            out[mo_id].append((lot, float(q or 0.0)))
        return out

    @api.model
    def _consumos_por_lote(self, lot_ids, hasta):
        """{lot_id: [(orden consumidora, cantidad)]} de lo consumido como
        componente hasta `hasta` (exclusivo)."""
        if not lot_ids:
            return {}
        self.env.flush_all()
        self.env.cr.execute("""
            SELECT sml.lot_id, sm.raw_material_production_id,
                   SUM(sml.quantity)
            FROM stock_move_line sml
            JOIN stock_move sm ON sm.id = sml.move_id
            WHERE sml.lot_id = ANY(%s)
              AND sml.state = 'done'
              AND sm.raw_material_production_id IS NOT NULL
              AND sml.date < %s
            GROUP BY 1, 2
        """, (list(lot_ids), hasta))
        out = defaultdict(list)
        for lot, mo_id, q in self.env.cr.fetchall():
            out[lot].append((mo_id, float(q or 0.0)))
        return out

    @api.model
    def _entregas_por_lote(self, lot_ids, periodo_ini, periodo_fin):
        """{lot_id: (entregado neto en el período, entregado neto acumulado)}
        a ubicaciones de cliente; una devolución (cliente → planta) resta."""
        if not lot_ids:
            return {}
        self.env.flush_all()
        self.env.cr.execute("""
            SELECT sml.lot_id,
                   COALESCE(SUM(CASE
                       WHEN ld.usage = 'customer' THEN sml.quantity
                       ELSE -sml.quantity END)
                     FILTER (WHERE sml.date >= %s), 0),
                   COALESCE(SUM(CASE
                       WHEN ld.usage = 'customer' THEN sml.quantity
                       ELSE -sml.quantity END), 0)
            FROM stock_move_line sml
            JOIN stock_location ls ON ls.id = sml.location_id
            JOIN stock_location ld ON ld.id = sml.location_dest_id
            WHERE sml.lot_id = ANY(%s)
              AND sml.state = 'done' AND sml.date < %s
              AND ((ld.usage = 'customer' AND ls.usage <> 'customer')
                   OR (ls.usage = 'customer' AND ld.usage <> 'customer'))
            GROUP BY sml.lot_id
        """, (periodo_ini, list(lot_ids), periodo_fin))
        return {lot: (float(p or 0.0), float(a or 0.0))
                for lot, p, a in self.env.cr.fetchall()}
