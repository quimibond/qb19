# -*- coding: utf-8 -*-
"""Rendimiento vendible por producto: de cada 100 unidades que salen de
producción, cuántas llegan a primera calidad.

Reglas (heredadas del módulo anterior, memo del 31-ago-2026):

- Se lee `stock.move.line` (el `stock.move` apunta a la ubicación padre y
  dejaba millones de metros sin clasificar).
- Vendible = lo que entra a PT primera calidad. Merma = FE + segundas +
  desperdicio. El FE cuenta como merma: no tiene canal de venta directo.
- Se excluyen los tipos de operación de conversión de unidades y cambio de
  artículo (reetiquetan material ya clasificado).
- Solo entradas desde producción, inspección o liberación: los reingresos
  ya se clasificaron antes.
- Ventana de 12 meses; por producto si clasificó al menos el umbral de
  unidades; abajo del umbral, tasa de planta.
- Lo capturado a mano manda mientras esté vigente (un lote de desarrollo
  no es la producción normal).
- 1.7.0, flujo en dos niveles: de las entradas que salen de una orden de
  fabricación solo cuentan las de la clasificación final (tipos
  `tipos_clasificacion`, 409/411) y las de la ruta vieja; las de la orden en
  kg (tipos `tipos_kg`, 410) no, y la tela en proceso en kg («I» + código)
  nunca, aunque entre a una ubicación de primera o de merma: es un
  semielaborado. La merma de proceso (crudo → tela en kg) ya está en el
  factor del crudo de la lista A; aquí solo va lo que la clasificación
  manda a segunda, FE o desperdicio.
- 1.17.0, primera para planear sin lotes accidentados: los lotes de una tela
  salen casi todos entre 85 y 98 % de primera y unos cuantos se caen a 0-50 %
  (casi todo a FE). El percentil 10 caía en esos lotes y cada orden se
  inflaba como si fuera a salir mal (WC090Q11JNT168: 0.40 con mediana 0.93;
  NN040Q66JNT163: 0, que se volvía 1.0). Ahora un lote con menos de
  `rendimiento_plan_accidente` (0.75) × la mediana de su tela es accidentado:
  no entra al percentil, se cuenta y, si pasan de
  `rendimiento_plan_accidentes_max` (10 %) de los lotes, la tela queda en
  «Revisar» para Ingeniería. Lo capturado para planear va aparte de lo
  capturado para costear.
"""
from datetime import date

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

from .horas import codigo_metros, es_proceso_kg

FUENTES = [
    ('manual', 'Capturado con motivo'),
    ('producto', 'Historia propia'),
    ('planta', 'Promedio de planta'),
    ('ninguna', 'Sin dato'),
]


class QbProductoRendimiento(models.Model):
    _name = 'qb.producto.rendimiento'
    _description = 'Rendimiento vendible por producto'
    _order = 'product_id'
    _rec_name = 'product_id'

    product_id = fields.Many2one('product.product', required=True,
                                 index=True, ondelete='cascade')
    default_code = fields.Char(related='product_id.default_code', store=True)
    company_id = fields.Many2one(
        'res.company', required=True,
        default=lambda self: self.env.company)
    rendimiento = fields.Float(
        digits=(6, 4), string='Rendimiento vigente',
        help='Fracción de primera calidad que usa el costeo (0.88 = 88 %).')
    fuente = fields.Selection(FUENTES, default='ninguna', required=True)
    rendimiento_calculado = fields.Float(
        digits=(6, 4), help='El que sale de los movimientos de almacén, '
        'aunque mande el manual.')
    unidades_clasificadas = fields.Float(
        digits=(16, 2), help='Unidades que entraron a primera o merma en la '
        'ventana; abajo del umbral se usa la tasa de planta.')
    unidades_vendibles = fields.Float(digits=(16, 2))
    # 1.9.0: para planear. El costeo usa el rendimiento del total de metros (lo
    # esperado); para que la venta llegue completa, la clasificación final pide
    # los kg con el rendimiento que cubre la mayoría de los lotes de la tela.
    rendimiento_plan = fields.Float(
        digits=(6, 4), string='Primera para planear',
        help='Fracción de primera que alcanzan o superan los lotes normales de '
             'la tela según la cobertura (parámetro rendimiento_plan_cobertura, '
             '0.90 = 9 de cada 10; los accidentados no cuentan). La lista de '
             'clasificación final pide los kg '
             'con este valor para que la venta llegue completa. Lotes = '
             'lotes de teñido (o la orden de cada rollo en el flujo en kg).')
    plan_fuente = fields.Selection(FUENTES, default='ninguna', required=True,
                                   string='Fuente para planear')
    plan_lotes = fields.Integer(string='Lotes medidos',
                                help='Lotes de la ventana con al menos el mínimo '
                                     'de unidades, accidentados incluidos.')
    # 1.17.0
    plan_mediana = fields.Float(
        digits=(6, 4), string='Mediana de los lotes',
        help='Fracción de primera del lote de en medio. Los lotes por debajo '
             'de esta mediana × rendimiento_plan_accidente son accidentados.')
    plan_accidentes = fields.Integer(
        string='Lotes accidentados',
        help='Lotes que salieron muy abajo de la mediana de la tela (casi todo '
             'a FE, Segunda o Desperdicio). No entran a la primera para '
             'planear: se cubren con existencia o con un baño complementario.')
    plan_revisar = fields.Boolean(
        string='Revisar primera para planear',
        help='Más lotes accidentados que rendimiento_plan_accidentes_max: la '
             'tela tiene un problema de proceso que el número no arregla. '
             'Ingeniería corrige el proceso o captura la primera para planear.')
    calculado_el = fields.Datetime(readonly=True)
    # Captura manual
    rendimiento_manual = fields.Float(
        digits=(6, 4), string='Rendimiento capturado',
        help='Manda sobre el calculado del costeo mientras esté vigente. Desde '
             '1.17.0 no cambia la primera para planear: esa tiene su captura.')
    manual_motivo = fields.Char(string='Motivo')
    manual_vigente_hasta = fields.Date(
        string='Vigente hasta',
        help='Vacío = sin caducidad. Al vencer, el recálculo vuelve al '
             'calculado.')
    # 1.17.0: captura para planear, aparte de la del costeo.
    plan_manual = fields.Float(
        digits=(6, 4), string='Primera para planear capturada',
        help='Manda sobre la primera para planear calculada mientras esté '
             'vigente. No cambia el costeo.')
    plan_manual_motivo = fields.Char(string='Motivo para planear')
    plan_manual_hasta = fields.Date(
        string='Planear vigente hasta',
        help='Vacío = sin caducidad. Al vencer, el recálculo vuelve a la '
             'primera para planear calculada.')

    _product_uniq = models.Constraint(
        'unique(product_id, company_id)',
        'Ya hay un rendimiento para ese producto.')
    _rango = models.Constraint(
        'CHECK(rendimiento_manual >= 0 AND rendimiento_manual <= 1)',
        'El rendimiento capturado es una fracción entre 0 y 1.')
    _rango_plan = models.Constraint(
        'CHECK(plan_manual >= 0 AND plan_manual <= 1)',
        'La primera para planear capturada es una fracción entre 0 y 1.')

    @api.constrains('rendimiento_manual', 'manual_motivo',
                    'plan_manual', 'plan_manual_motivo')
    def _check_manual(self):
        for rec in self:
            if rec.rendimiento_manual and not (rec.manual_motivo or '').strip():
                raise UserError('Escriba el motivo del rendimiento capturado.')
            if rec.plan_manual and not (rec.plan_manual_motivo or '').strip():
                raise UserError('Escriba el motivo de la primera para planear '
                                'capturada.')

    # ------------------------------------------------------------------
    @api.model
    def _ids_param(self, key, default):
        return [int(x) for x in self.env['qb.parametro'].get_list(key, default)
                if x.strip().isdigit()]

    @api.model
    def _saldo_ids(self):
        """1.10.0: productos de saldo en kg (segundas y desperdicio) que salen
        como subproducto de la clasificación final. [0] si no hay, para SQL."""
        codigos = [c.strip() for c in self.env['qb.parametro'].get_list(
            'saldos_clasificacion', 'SALDO DE TELA TEJIDO,SALDO TEJIDO D') if c.strip()]
        ids = self.env['product.product'].with_context(active_test=False).search(
            [('default_code', 'in', codigos)]).ids if codigos else []
        return ids or [0]

    @api.model
    def _saldos_clasificacion(self, desde, hasta, por_lote=False):
        """1.10.0: {(tela, lote): unidades} de merma de los saldos en kg que
        salieron de la clasificación final de cada tela, convertidos a la
        unidad de la tela con la ficha de acabado (kg por unidad). Sin ficha no
        se convierte (no se inventa una conversión). `lote` es None si no se
        pide por lote; si no, el lote del saldo sin su número de rollo, igual
        que en `medir_lotes`."""
        merma = self._ids_param('calidad_locs_merma', '41,42,43')
        origen = self._ids_param('calidad_locs_origen', '15,36,246')
        clasif = self._ids_param('tipos_clasificacion', '409,411') or [0]
        if not (merma and origen):
            return {}
        saldos = tuple(self._saldo_ids())
        # 1.10.1: un baño sin primera ni FE sale en una orden de saldos
        # (quimibond_rollos_bitacora) cuyo producto es el propio saldo; la
        # tela es la del acabado en kg que consume esa orden (E3 de la
        # prueba en main, 2026-11-05).
        self.env.cr.execute("""
            SELECT CASE WHEN mo.product_id IN %s THEN COALESCE((
                       SELECT raw.product_id FROM stock_move raw
                        WHERE raw.raw_material_production_id = mo.id
                          AND raw.state = 'done'
                        ORDER BY raw.id LIMIT 1), mo.product_id)
                   ELSE mo.product_id END AS pid,
                   regexp_replace(COALESCE(lot.name, ''), '-[0-9]{3,}$', '') AS lote,
                   SUM(sml.quantity) AS kg
              FROM stock_move_line sml
              JOIN stock_move sm ON sm.id = sml.move_id
              JOIN mrp_production mo ON mo.id = sm.production_id
              LEFT JOIN stock_lot lot ON lot.id = sml.lot_id
             WHERE sml.state = 'done'
               AND sml.date >= %s AND sml.date < %s
               AND sml.location_dest_id IN %s
               AND sml.location_id IN %s
               AND sml.product_id IN %s
               AND mo.picking_type_id IN %s
               AND sm.company_id = %s
             GROUP BY 1, 2
        """, (saldos, desde, hasta, tuple(merma), tuple(origen), saldos,
              tuple(clasif), self.env.company.id))
        Horas = self.env['qb.producto.horas']
        Prod = self.env['product.product']
        kg_u = {}
        out = {}
        telas = {}
        for pid, lote, kg in self.env.cr.fetchall():
            if pid not in telas:
                # Orden de saldos: del acabado en kg («I» + tela) a la tela
                # en metros. Sin tela, no se cuenta.
                prod = Prod.browse(pid)
                tela = prod
                if es_proceso_kg(prod):
                    tela = Prod.search([('default_code', '=', codigo_metros(
                        prod.default_code))], limit=1)
                telas[pid] = tela.id
            pid = telas[pid]
            if not pid:
                continue
            if pid not in kg_u:
                kg_u[pid] = Horas.kg_por_unidad_ficha(Prod.browse(pid))[0]
            if not kg_u[pid] or not kg:
                continue
            clave = (pid, lote if por_lote else None)
            out[clave] = out.get(clave, 0.0) + kg / kg_u[pid]
        return out

    @api.model
    def medir(self, hasta=None, meses=None):
        """{product_id: (vendible, total)} en la ventana que termina en
        `hasta` (exclusivo), más la tasa de planta."""
        P = self.env['qb.parametro']
        vendible = self._ids_param('calidad_locs_vendible', '40')
        merma = self._ids_param('calidad_locs_merma', '41,42,43')
        origen = self._ids_param('calidad_locs_origen', '15,36,246')
        excluir = self._ids_param('calidad_picking_excluir', '77,147') or [0]
        tipos_kg = self._ids_param('tipos_kg', '410') or [0]
        if not (vendible and merma and origen):
            return {}, 1.0
        meses = meses or int(P.get_float('horas_historia_meses', 12)) or 12
        hasta = hasta or (date.today() + relativedelta(days=1))
        desde = hasta - relativedelta(months=meses)
        self.env.flush_all()
        self.env.cr.execute("""
            SELECT sml.product_id,
                   SUM(CASE WHEN sml.location_dest_id IN %s
                            THEN sml.quantity ELSE 0 END) AS vend,
                   SUM(sml.quantity) AS total
              FROM stock_move_line sml
              JOIN stock_move sm ON sm.id = sml.move_id
              LEFT JOIN mrp_production mo ON mo.id = sm.production_id
             WHERE sml.state = 'done'
               AND sml.date >= %s AND sml.date < %s
               AND sml.location_dest_id IN %s
               AND sml.location_id IN %s
               AND (sm.picking_type_id IS NULL
                    OR sm.picking_type_id NOT IN %s)
               AND (mo.id IS NULL OR mo.picking_type_id IS NULL
                    OR mo.picking_type_id NOT IN %s)
               AND NOT (sml.product_id IN %s
                        AND COALESCE(mo.picking_type_id, 0) IN %s)
               AND sm.company_id = %s
             GROUP BY sml.product_id
        """, (tuple(vendible), desde, hasta, tuple(vendible + merma),
              tuple(origen), tuple(excluir), tuple(tipos_kg), tuple(self._saldo_ids()),
              tuple(self._ids_param('tipos_clasificacion', '409,411') or [0]),
              self.env.company.id))
        filas = self.env.cr.fetchall()
        Prod = self.env['product.product']
        datos = {pid: (v or 0.0, t or 0.0) for pid, v, t in filas
                 if not es_proceso_kg(Prod.browse(pid))}
        # 1.10.0: segundas y desperdicio salen de la clasificación como saldo
        # en kg; cuentan como merma de su tela, en la unidad de la tela.
        for (pid, _lote), unidades in self._saldos_clasificacion(desde, hasta).items():
            v, t = datos.get(pid, (0.0, 0.0))
            datos[pid] = (v, t + unidades)
        tot_v = sum(v for v, _t in datos.values())
        tot_t = sum(t for _v, t in datos.values())
        return datos, (tot_v / tot_t if tot_t else 1.0)

    @api.model
    def medir_lotes(self, hasta=None, meses=None):
        """1.9.0: {product_id: [fracción de primera de cada lote]} en la
        ventana. Mismos filtros que `medir`; el lote es el lote de producción:
        el de teñido en la ruta vieja («20361 J2») o, en el flujo en kg, la
        orden de cada rollo (el lote «<orden>-0001» sin su número de rollo).
        Lotes con menos de `rendimiento_plan_min_lote` unidades no cuentan
        (retazos y reprocesos)."""
        P = self.env['qb.parametro']
        vendible = self._ids_param('calidad_locs_vendible', '40')
        merma = self._ids_param('calidad_locs_merma', '41,42,43')
        origen = self._ids_param('calidad_locs_origen', '15,36,246')
        excluir = self._ids_param('calidad_picking_excluir', '77,147') or [0]
        tipos_kg = self._ids_param('tipos_kg', '410') or [0]
        if not (vendible and merma and origen):
            return {}
        min_lote = P.get_float('rendimiento_plan_min_lote', 500.0)
        meses = meses or int(P.get_float('horas_historia_meses', 12)) or 12
        hasta = hasta or (date.today() + relativedelta(days=1))
        desde = hasta - relativedelta(months=meses)
        self.env.flush_all()
        self.env.cr.execute("""
            SELECT sml.product_id,
                   regexp_replace(COALESCE(lot.name, ''), '-[0-9]{3,}$', '') AS lote,
                   SUM(CASE WHEN sml.location_dest_id IN %s
                            THEN sml.quantity ELSE 0 END) AS vend,
                   SUM(sml.quantity) AS total
              FROM stock_move_line sml
              JOIN stock_move sm ON sm.id = sml.move_id
              JOIN stock_lot lot ON lot.id = sml.lot_id
              LEFT JOIN mrp_production mo ON mo.id = sm.production_id
             WHERE sml.state = 'done'
               AND sml.date >= %s AND sml.date < %s
               AND sml.location_dest_id IN %s
               AND sml.location_id IN %s
               AND (sm.picking_type_id IS NULL
                    OR sm.picking_type_id NOT IN %s)
               AND (mo.id IS NULL OR mo.picking_type_id IS NULL
                    OR mo.picking_type_id NOT IN %s)
               AND NOT (sml.product_id IN %s
                        AND COALESCE(mo.picking_type_id, 0) IN %s)
               AND sm.company_id = %s
             GROUP BY 1, 2
        """, (tuple(vendible), desde, hasta, tuple(vendible + merma),
              tuple(origen), tuple(excluir), tuple(tipos_kg), tuple(self._saldo_ids()),
              tuple(self._ids_param('tipos_clasificacion', '409,411') or [0]),
              self.env.company.id))
        filas = self.env.cr.fetchall()
        Prod = self.env['product.product']
        por_lote = {(pid, lote): [v or 0.0, t or 0.0] for pid, lote, v, t in filas}
        for clave, unidades in self._saldos_clasificacion(desde, hasta, por_lote=True).items():
            por_lote.setdefault(clave, [0.0, 0.0])[1] += unidades
        out = {}
        for (pid, _lote), (v, t) in por_lote.items():
            if not t or t < min_lote:
                continue
            out.setdefault(pid, []).append(v / t)
        return {pid: lotes for pid, lotes in out.items()
                if not es_proceso_kg(Prod.browse(pid))}

    @api.model
    def _cubre(self, fracciones, cobertura):
        """El valor que alcanzan o superan `cobertura` de los lotes: con 0.90,
        el que dejan abajo solo 1 de cada 10 (percentil 10, sin interpolar)."""
        orden = sorted(fracciones)
        if not orden:
            return 0.0
        # 1e-9: (1 - 0.9) × 10 da 0.999… en punto flotante.
        idx = int((1.0 - cobertura) * len(orden) + 1e-9)
        return orden[min(max(idx, 0), len(orden) - 1)]

    @api.model
    def _separar_accidentes(self, fracciones, umbral):
        """1.17.0: (normales, accidentados, mediana). Accidentado = lote con
        menos de `umbral` × la mediana de la tela."""
        if not fracciones:
            return [], [], 0.0
        orden = sorted(fracciones)
        n = len(orden)
        mediana = (orden[n // 2] if n % 2 else
                   (orden[n // 2 - 1] + orden[n // 2]) / 2.0)
        # 1e-9: 0.75 × 0.8 da 0.6000…01 en punto flotante.
        limite = umbral * mediana - 1e-9
        normales = [f for f in fracciones if f >= limite]
        accidentes = [f for f in fracciones if f < limite]
        return normales, accidentes, mediana

    @api.model
    def recalcular(self, products=None, hasta=None):
        """Recalcula el rendimiento de `products` (default: los que tienen
        historia o captura manual). Devuelve las filas."""
        P = self.env['qb.parametro']
        company = self.env.company
        min_u = P.get_float('rendimiento_min_unidades', 10000.0)
        datos, planta = self.medir(hasta)
        lotes = self.medir_lotes(hasta)
        cobertura = min(max(P.get_float('rendimiento_plan_cobertura', 0.90), 0.5), 1.0)
        min_lotes = int(P.get_float('rendimiento_plan_min_lotes', 8)) or 1
        umbral = min(max(P.get_float('rendimiento_plan_accidente', 0.75), 0.0), 1.0)
        acc_max = P.get_float('rendimiento_plan_accidentes_max', 0.10)
        separados = {pid: self._separar_accidentes(fs, umbral)
                     for pid, fs in lotes.items()}
        planta_plan = self._cubre(
            [f for norm, _a, _m in separados.values() for f in norm],
            cobertura) or planta
        hoy = fields.Date.today()
        existentes = {r.product_id.id: r for r in self.search(
            [('company_id', '=', company.id)])}
        if products is None:
            ids = set(datos) | set(existentes) | set(lotes)
        else:
            ids = set(products.ids)
        ahora = fields.Datetime.now()
        filas = self.browse()
        for pid in ids:
            rec = existentes.get(pid)
            v, t = datos.get(pid, (0.0, 0.0))
            if t >= min_u:
                calc, fuente = (v / t if t else 1.0), 'producto'
            elif t > 0:
                calc, fuente = planta, 'planta'
            else:
                calc, fuente = planta, 'planta'
            manual = rec and rec.rendimiento_manual and (
                not rec.manual_vigente_hasta or rec.manual_vigente_hasta >= hoy)
            fs = lotes.get(pid, [])
            normales, accidentes, mediana = separados.get(pid, ([], [], 0.0))
            plan_manual = rec and rec.plan_manual and (
                not rec.plan_manual_hasta or rec.plan_manual_hasta >= hoy)
            propio = self._cubre(normales, cobertura) if len(normales) >= min_lotes else 0.0
            if plan_manual:
                plan, plan_fuente = rec.plan_manual, 'manual'
            elif propio:
                plan, plan_fuente = propio, 'producto'
            else:
                # Pocos lotes, o una tela cuyos lotes salen todos en 0: el
                # valor de planta, nunca 1.0 por un cálculo en cero.
                plan, plan_fuente = planta_plan, 'planta'
            revisar = bool(len(fs) >= min_lotes and
                           len(accidentes) > acc_max * len(fs))
            vals = {
                'rendimiento_calculado': calc,
                'rendimiento': rec.rendimiento_manual if manual else calc,
                'fuente': 'manual' if manual else fuente,
                'rendimiento_plan': plan or 1.0, 'plan_fuente': plan_fuente,
                'plan_lotes': len(fs),
                'plan_mediana': mediana, 'plan_accidentes': len(accidentes),
                'plan_revisar': revisar,
                'unidades_clasificadas': t, 'unidades_vendibles': v,
                'calculado_el': ahora,
            }
            if rec:
                rec.write(vals)
            else:
                rec = self.create(dict(vals, product_id=pid, company_id=company.id))
            filas |= rec
        return filas

    @api.model
    def planta(self, hasta=None):
        return self.medir(hasta)[1]

    @api.model
    def para(self, product, hasta=None):
        """Rendimiento de un producto para costear: la fila vigente; sin
        fila, la tasa de planta. Devuelve (fracción, fuente)."""
        rec = self.search([('product_id', '=', product.id),
                           ('company_id', '=', self.env.company.id)], limit=1)
        if rec and rec.fuente != 'ninguna':
            return rec.rendimiento or 1.0, rec.fuente
        return self.planta(hasta), 'planta'

    @api.model
    def plan_para(self, product):
        """1.9.0: (fracción de primera para planear, fuente). Sin fila, 1.0 y
        'ninguna': sin dato no se infla la orden."""
        rec = self.search([('product_id', '=', product.id),
                           ('company_id', '=', self.env.company.id)], limit=1)
        if rec and rec.rendimiento_plan and rec.plan_fuente != 'ninguna':
            return rec.rendimiento_plan, rec.plan_fuente
        return 1.0, 'ninguna'

    @api.model
    def cron_recalcular(self):
        for company in self.env['res.company'].search([]):
            self.with_company(company).recalcular()

    def action_recalcular_todo(self):
        self.recalcular()
        return {
            'type': 'ir.actions.act_window',
            'name': 'Rendimiento vendible',
            'res_model': 'qb.producto.rendimiento',
            'view_mode': 'list,form',
        }
