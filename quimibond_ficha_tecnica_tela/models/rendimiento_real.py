# -*- coding: utf-8 -*-
"""2.24.0 (Dirección General 2026-11-03): un solo rendimiento de tela acabada, calculado con lo fabricado.

Rendimiento = metros de la tela acabada fabricados ÷ kilos de tela consumidos (crudo, teñido o acabado en kg; sin
engomado ni químicos) en sus órdenes terminadas de los últimos 12 meses. Ese número es el
``rendimiento_tela_acabada`` de la ficha: lo usan la conversión kg → m de la lista en metros y los tiempos de rama e
inspección. Sin órdenes, se queda el que tiene la ficha. Se recalcula cada noche antes de la revisión de listas; nunca
toca una lista de materiales.

Los saldos en kg que salen de esas órdenes (segundas y desperdicio, flujo nuevo de inspección) se restan de los kilos:
son tela que no se volvió metros de la tela y ya cuentan como merma en la primera para planear del costeo.

2.27.0 (Dirección General 2026-11-05): **por kg de tela terminada.** Todos los que usan el rendimiento lo necesitan por
kg de la tela acabada en kg (el producto «I»): la conversión kg → m de la lista en metros y del costeo, los tiempos de
rama e inspección (la lista en kg es por kg de tela acabada) y la alerta de metros de Inspección. 2.24.0 lo dividía
entre los kilos de tela que **entran** a la orden (crudo o teñido), que pesan más: salía ≈10 % abajo (WJ038: 14.19
m/kg contra 15.79 de las listas de empaque de 2026, 1,800 rollos pesados). Ahora, en orden:

1. **Tela terminada** (``terminado``): metros ÷ kg de las conversiones de metros a kg (órdenes que consumen la tela en
   metros y producen su acabado en kg; el kg es el pesado de la lista de empaque) y de los rollos finales registrados
   de Inspección (``mrp.rollo.final``, metros y kg netos de cada rollo de primera o FE, si está instalado).
2. **Estimado** (``estimado``): sin esos datos, metros por kg que entró (lo de 2.24.0) × el consumo de crudo de la
   ficha (kg que entran por kg de tela acabada).
3. **Ficha** (``ficha``): sin nada, se queda el de la ficha.

2.27.1: una conversión o rollo a más de 15 % de la mediana de su tela no cuenta, y lo medido se usa con al menos 3
muestras buenas; con menos, el estimado (y si tampoco hay, el de la ficha).

2.27.2: **los rollos de Inspección le ganan a las conversiones** (son la fuente que escala: cada rollo pesado neto en la
estación; las conversiones son el puente mientras no hay rollos, con el kg de la lista de empaque) y **el rendimiento
que Ingeniería escribe a mano queda fijo** («Rendimiento forzado», como el de la ficha de tejido): el recálculo no lo
pisa con conversiones ni con el estimado, solo con 3 o más rollos de Inspección, y entonces avisa si cambió más que la
alerta. Orden: rollos → forzado → conversiones → estimado → ficha.

Revisión 2026-11-06 (en 2.27.1): el estimado cuenta los kg de reproceso (códigos con punto al final) y no multiplica por el
consumo de crudo los kg de la tela acabada en kg; con menos de 3 muestras y sin estimado se queda el de la ficha.
"""
from datetime import timedelta

from odoo import api, fields, models

DIAS_REAL = 365
# 2.27.1: una conversión o un rollo que se aleja más que esto de la mediana de la tela no cuenta (conversiones que juntan
# kg de otra fuente: XJ140Q21JNT165 tenía 5 entre 4.35 y 4.48 m/kg y dos de 0.81 y 2.07), y hacen falta al menos
# MIN_MUESTRAS buenas para usar lo medido; con menos, el estimado.
DESVIACION_MAX = 0.15
MIN_MUESTRAS = 3
# 2.27.2: el recálculo escribe con este contexto; cualquier otra escritura del rendimiento (Ingeniería en la ficha) lo
# deja forzado.
CTX_CALCULADO = 'ft_rendimiento_calculado'
# 2.26.0: el cambio de rendimiento que pide revisión (%), y la constante si el parámetro no está.
PARAM_ALERTA_RENDIMIENTO_PCT = 'quimibond_ficha_tecnica_tela.alerta_rendimiento_pct'
# 2.27.1: los rollos de Inspección cuentan desde esta fecha (AAAA-MM-DD; vacío = los últimos 12 meses). Es el arranque
# del flujo nuevo en esa base: en main, los rollos de las pruebas de punta a punta (5-nov) no son producción.
PARAM_ROLLOS_DESDE = 'quimibond_ficha_tecnica_tela.rendimiento_rollos_desde'
ALERTA_RENDIMIENTO_PCT = 5.0
POSICION_PROCESO = ('H', 'I', 'J')  # crudo, teñido, acabado (DAT P-D02-01, posición 9)


def tipo_tela(code):
    """Revisión 2026-11-06: «terminada» si es la tela acabada en kg (su código con «I» al frente), «entrada» si es crudo, teñido o
    tela (H, I o J en la posición 9), o None si no es tela (engomado, colorantes, auxiliares). Quita la marca de
    reproceso del final («WD038Q46ING163.»): esos kg son tela y antes se perdían (WD038Q46JNG166 salía a 32.1 m/kg)."""
    code = (code or '').strip().upper().rstrip('.-_ ')
    if len(code) in (15, 17) and code.startswith('I') and code[9:10] in POSICION_PROCESO:
        return 'terminada'
    if len(code) in (14, 16) and code[8:9] in POSICION_PROCESO:
        return 'entrada'
    return None


def es_tela_kg(code):
    """Crudo, teñido o tela (H, I o J en la posición 9), o su código en kg con «I» al frente. Lo demás
    (engomado, colorantes, auxiliares) no es tela."""
    return tipo_tela(code) is not None


def ancho_en_m(ancho):
    """El ancho de la ficha en metros (se captura en cm: 160; si ya viene en m: 1.6)."""
    return ancho / 100.0 if ancho > 10 else ancho


class FichaTecnicaAcabadoRendimientoReal(models.Model):
    _inherit = 'ficha.tecnica.acabado'

    rendimiento_real_ordenes = fields.Integer(
        string='Órdenes del rendimiento', readonly=True,
        help='Conversiones a kg y rollos de Inspección (o, estimado, órdenes terminadas) de los últimos 12 meses con '
             'los que se calculó el rendimiento.')
    rendimiento_fuente = fields.Selection(
        [('rollos', 'Rollos de Inspección (metros y kg netos de cada rollo)'),
         ('terminado', 'Conversiones a kg (kg de la lista de empaque)'),
         ('estimado', 'Estimado (metros por kg que entró × consumo de crudo)'),
         ('ficha', 'Capturado en la ficha')],
        string='De dónde sale el rendimiento', readonly=True, copy=False,
        help='El rendimiento es por kg de tela terminada. «Rollos de Inspección» es el dato medido en la estación y gana '
             'a todo; «Conversiones a kg» se usa mientras la tela no tiene 3 rollos; «Estimado» cuando tampoco tiene '
             'conversiones.')
    rendimiento_manual = fields.Boolean(
        string='Rendimiento forzado', copy=False,
        help='Ingeniería escribió el rendimiento a mano: el recálculo nocturno no lo cambia con conversiones ni con el '
             'estimado, solo con 3 o más rollos de Inspección (y entonces avisa). Desmárquelo para volver al cálculo.')
    # 2.26.0: aviso cuando el recálculo nocturno mueve el rendimiento más que la alerta (propuesta de Consolti,
    # 2026-11-05: que las telas del piloto no cambien sin aviso). El número se escribe igual; el aviso queda como
    # pendiente de Ingeniería en la revisión de listas hasta que alguien lo marca revisado.
    rendimiento_por_revisar = fields.Boolean(string='Rendimiento por revisar', readonly=True, copy=False)
    rendimiento_anterior = fields.Float(string='Rendimiento anterior (m/kg)', digits=(12, 4), readonly=True,
                                        copy=False)
    rendimiento_cambio_fecha = fields.Datetime(string='Rendimiento cambió el', readonly=True, copy=False)
    peso_real = fields.Float(
        string='Peso real (g/m²)', digits=(12, 1), compute='_compute_peso_real',
        help='El peso que da lo fabricado al ancho de venta: 1000 ÷ (rendimiento × ancho de venta en m).')

    @api.depends('rendimiento_tela_acabada', 'rendimiento_real_ordenes', 'ancho_acabado')
    def _compute_peso_real(self):
        for ficha in self:
            ancho = ancho_en_m(ficha.ancho_acabado)
            rend = ficha.rendimiento_tela_acabada if ficha.rendimiento_real_ordenes else 0.0
            ficha.peso_real = 1000.0 / (rend * ancho) if rend and ancho else 0.0

    @api.model
    def _ft_fabricado(self, products, desde):
        """{producto: (metros, kg de tela que entró, kg de tela acabada en kg, kg de saldos, órdenes)} de las órdenes
        terminadas desde ``desde`` que fabricaron esos productos. Revisión 2026-11-06: los kg de la tela acabada en kg (el «I…» que
        consume la orden en metros del flujo nuevo) van aparte de los de crudo o teñido, porque ya son kg de tela
        terminada y no se multiplican por el consumo de crudo; los saldos también van aparte. Solo cuentan las órdenes
        que consumieron tela en kg."""
        Move = self.env['stock.move'].sudo()
        metro = self.env.ref('uom.product_uom_meter', raise_if_not_found=False)
        kg = self.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        if not products or not metro or not kg:
            return {}
        # Por orden: solo cuentan las órdenes que consumieron tela en kg. Las que fabrican la tela con otra tela ya en
        # metros (conversión de artículo TL/CONV-ART: WC090Q11JNT162 desde WC090Q11JNT165) suman metros sin kilos y
        # el rendimiento salía al triple (36.7 m/kg en main, revisión 2026-11-06).
        metros_orden, producto_orden = {}, {}
        for orden, product, uom, qty in Move._read_group(
                [('state', '=', 'done'), ('product_id', 'in', products.ids), ('production_id.state', '=', 'done'),
                 ('production_id.date_finished', '>=', desde)],
                ['production_id', 'product_id', 'product_uom'], ['quantity:sum']):
            if uom == metro or uom._has_common_reference(metro):
                metros_orden[orden] = metros_orden.get(orden, 0.0) + uom._compute_quantity(qty, metro, round=False)
                producto_orden[orden] = product
        kilos_orden, terminada_orden = {}, {}
        for orden, componente, uom, qty in Move._read_group(
                [('state', '=', 'done'), ('raw_material_production_id.product_id', 'in', products.ids),
                 ('raw_material_production_id.state', '=', 'done'),
                 ('raw_material_production_id.date_finished', '>=', desde)],
                ['raw_material_production_id', 'product_id', 'product_uom'], ['quantity:sum']):
            tipo = tipo_tela(componente.default_code)
            if not tipo or not (uom == kg or uom._has_common_reference(kg)):
                continue
            destino = terminada_orden if tipo == 'terminada' else kilos_orden
            destino[orden] = destino.get(orden, 0.0) + uom._compute_quantity(qty, kg, round=False)
        # Los saldos en kg (segundas y desperdicio) que salen de esas órdenes son tela que no se volvió metros de la
        # tela: se restan de los kilos. Así el rendimiento es el de la tela que sí se vuelve metros, y la primera para
        # planear del costeo (que cuenta los saldos como merma) no descuenta la misma merma dos veces.
        saldos_orden = {}
        for orden, uom, qty in Move._read_group(
                [('state', '=', 'done'), ('product_id', 'in', self._ft_saldo_products().ids),
                 ('production_id.product_id', 'in', products.ids), ('production_id.state', '=', 'done'),
                 ('production_id.date_finished', '>=', desde)],
                ['production_id', 'product_uom'], ['quantity:sum']):
            if uom == kg or uom._has_common_reference(kg):
                saldos_orden[orden] = saldos_orden.get(orden, 0.0) + uom._compute_quantity(qty, kg, round=False)
        res = {}
        for orden, m in metros_orden.items():
            k_ent, k_term = kilos_orden.get(orden, 0.0), terminada_orden.get(orden, 0.0)
            if not (k_ent or k_term):
                continue
            product = producto_orden[orden]
            m0, e0, t0, s0, n0 = res.get(product, (0.0, 0.0, 0.0, 0.0, 0))
            res[product] = (m0 + m, e0 + k_ent, t0 + k_term, s0 + saldos_orden.get(orden, 0.0), n0 + 1)
        return res

    @api.model
    def _ft_conversiones(self, fichas, desde):
        """2.27.0: {ficha: (metros, kg de tela terminada, conversiones)} desde ``desde``: órdenes terminadas que producen
        el acabado en kg de la ficha consumiendo su tela en metros (el kg es el de la lista de empaque)."""
        Move = self.env['stock.move'].sudo()
        metro = self.env.ref('uom.product_uom_meter', raise_if_not_found=False)
        kg = self.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        fichas = fichas.filtered(lambda f: f.product_acabado_id and f.product_proceso_id)
        if not fichas or not metro or not kg:
            return {}
        por_par = {(f.product_proceso_id, f.product_acabado_id): f for f in fichas}
        muestras = {}  # ficha → [(metros, kg)], una por conversión
        # Metros de la tela consumidos por orden, y kg del acabado en kg producidos por esa misma orden.
        metros_orden = {}
        for orden, producto, uom, qty in Move._read_group(
                [('state', '=', 'done'), ('product_id', 'in', fichas.product_acabado_id.ids),
                 ('raw_material_production_id.product_id', 'in', fichas.product_proceso_id.ids),
                 ('raw_material_production_id.state', '=', 'done'),
                 ('raw_material_production_id.date_finished', '>=', desde)],
                ['raw_material_production_id', 'product_id', 'product_uom'], ['quantity:sum']):
            ficha = por_par.get((orden.product_id, producto))
            if ficha and (uom == metro or uom._has_common_reference(metro)):
                metros_orden[orden] = (ficha, metros_orden.get(orden, (ficha, 0.0))[1]
                                       + uom._compute_quantity(qty, metro, round=False))
        if metros_orden:
            for orden, uom, qty in Move._read_group(
                    [('state', '=', 'done'), ('production_id', 'in', [o.id for o in metros_orden]),
                     ('product_id', 'in', fichas.product_proceso_id.ids)],
                    ['production_id', 'product_uom'], ['quantity:sum']):
                if not (uom == kg or uom._has_common_reference(kg)):
                    continue
                ficha, m = metros_orden[orden]
                k = uom._compute_quantity(qty, kg, round=False)
                if m and k:
                    muestras.setdefault(ficha, []).append((m, k))
        return self._ft_sin_raras(muestras)

    @api.model
    def _ft_rollos(self, fichas, desde):
        """2.27.2: {ficha: (metros, kg netos, rollos)} de los rollos finales registrados de Inspección
        (``mrp.rollo.final``, primera y FE) desde ``desde``, si el módulo de rollos está instalado."""
        Final = self.env.get('mrp.rollo.final')
        fichas = fichas.filtered(lambda f: f.product_acabado_id and f.product_proceso_id)
        if Final is None or not fichas or not {'metros', 'kg_neto', 'estado', 'clasificacion'} <= set(Final._fields):
            return {}
        por_kg = {f.product_proceso_id: f for f in fichas}
        por_m = {f.product_acabado_id: f for f in fichas}
        arranque = fields.Date.to_date(self.env['ir.config_parameter'].sudo().get_param(PARAM_ROLLOS_DESDE) or None)
        desde = max(fields.Date.to_date(desde), arranque) if arranque else desde
        muestras = {}  # ficha → [(metros, kg)], una por rollo
        for rollo in Final.sudo().search([('estado', '=', 'registrado'), ('clasificacion', 'in', ('PQ', 'FE')),
                                          ('fecha', '>=', desde), ('metros', '>', 0), ('kg_neto', '>', 0)]):
            ficha = por_kg.get(rollo.production_id.product_id) or por_m.get(rollo.production_m_id.product_id)
            if ficha:
                muestras.setdefault(ficha, []).append((rollo.metros, rollo.kg_neto))
        return self._ft_sin_raras(muestras)

    @api.model
    def _ft_sin_raras(self, muestras):
        """2.27.1: {ficha: [(metros, kg)]} → {ficha: (metros, kg, n)} sin las muestras que se alejan más de
        DESVIACION_MAX de la mediana de la tela."""
        res = {}
        for ficha, lista in muestras.items():
            ratios = sorted(m / k for m, k in lista)
            mitad = len(ratios) // 2
            mediana = ratios[mitad] if len(ratios) % 2 else (ratios[mitad - 1] + ratios[mitad]) / 2.0
            buenas = [(m, k) for m, k in lista if abs(m / k - mediana) <= mediana * DESVIACION_MAX]
            if buenas:
                res[ficha] = (sum(m for m, _k in buenas), sum(k for _m, k in buenas), len(buenas))
        return res

    @api.model
    def _ft_saldo_products(self):
        """Los productos de saldo de la lista en metros (los mismos parámetros que «Generar listas»)."""
        from .ruta_familia import SALDOS  # local: orden de carga de models/
        params = self.env['ir.config_parameter'].sudo()
        codigos = [(params.get_param(clave) or defecto).strip() for clave, defecto in SALDOS]
        return self.env['product.product'].with_context(active_test=False).search([('default_code', 'in', codigos)])

    def _ft_actualizar_rendimiento_real(self):
        """El rendimiento de estas fichas (todas si vacío) con lo fabricado; sin datos se queda el de la ficha. Orden
        (2.27.2): rollos de Inspección → forzado por Ingeniería → conversiones → estimado → ficha (sin «pocas
        muestras» desde la revisión del 2026-11-06)."""
        fichas = self or self.search([('product_acabado_id', '!=', False)])
        desde = fields.Date.context_today(self) - timedelta(days=DIAS_REAL)
        rollos = self._ft_rollos(fichas, desde)
        conversiones = self._ft_conversiones(fichas, desde)
        datos = self._ft_fabricado(fichas.product_acabado_id, desde)
        limite = self._ft_alerta_rendimiento_pct()
        for ficha in fichas.with_context(**{CTX_CALCULADO: True}):
            nuevo, n, fuente = 0.0, 0, 'ficha'
            m_r, kg_r, n_r = rollos.get(ficha, (0.0, 0.0, 0))
            m_c, kg_c, n_c = conversiones.get(ficha, (0.0, 0.0, 0))
            m, kg_entrada, kg_terminada, kg_saldos, n_e = datos.get(ficha.product_acabado_id, (0.0, 0.0, 0.0, 0.0, 0))
            consumo = ficha.consumo_crudo if 'consumo_crudo' in ficha._fields else 0.0
            # Revisión 2026-11-06: kg de tela terminada que salieron de esas órdenes: la tela acabada en kg tal cual, el crudo o
            # teñido entre el consumo de crudo (kg que entran por kg de tela acabada) y menos los saldos. Con crudo o
            # teñido y sin consumo capturado no se puede estimar.
            kg_estimado = 0.0
            if m and (kg_terminada or kg_entrada) and (consumo or not kg_entrada):
                kg_estimado = kg_terminada + (kg_entrada / consumo if kg_entrada else 0.0) - kg_saldos
            if m_r and kg_r and n_r >= MIN_MUESTRAS:
                nuevo, n, fuente = round(m_r / kg_r, 4), n_r, 'rollos'
            elif ficha.rendimiento_manual:
                # Lo escribió Ingeniería: solo los rollos de Inspección lo cambian.
                if ficha.rendimiento_fuente != 'ficha' or ficha.rendimiento_real_ordenes:
                    ficha.write({'rendimiento_real_ordenes': 0, 'rendimiento_fuente': 'ficha'})
                continue
            elif m_c and kg_c and n_c >= MIN_MUESTRAS:
                nuevo, n, fuente = round(m_c / kg_c, 4), n_c, 'terminado'
            elif kg_estimado > 0:
                nuevo, n, fuente = round(m / kg_estimado, 4), n_e, 'estimado'
            # Revisión 2026-11-06: con menos de MIN_MUESTRAS rollos o conversiones y sin estimado se queda el de la ficha: una o dos
            # muestras no se pueden filtrar (WJ060Q21JNT165: una conversión de 12.05 contra 9.77 real).
            if nuevo:
                vals = {'rendimiento_tela_acabada': nuevo, 'rendimiento_real_ordenes': n, 'rendimiento_fuente': fuente,
                        'rendimiento_manual': False}
                antes = ficha.rendimiento_tela_acabada
                if antes and abs(nuevo - antes) / antes * 100.0 > limite:
                    vals.update(rendimiento_por_revisar=True, rendimiento_anterior=antes,
                                rendimiento_cambio_fecha=fields.Datetime.now())
                ficha.write(vals)
            elif ficha.rendimiento_real_ordenes or ficha.rendimiento_fuente != 'ficha':
                ficha.write({'rendimiento_real_ordenes': 0, 'rendimiento_fuente': 'ficha'})
        return True

    def write(self, vals):
        """2.27.2: el rendimiento escrito a mano (Ingeniería en la ficha) queda forzado; el recálculo escribe con
        CTX_CALCULADO. Desmarcar «Rendimiento forzado» vuelve a calcularlo en ese momento."""
        if self.env.context.get(CTX_CALCULADO):
            return super().write(vals)
        recalcular = vals.get('rendimiento_manual') is False
        if 'rendimiento_tela_acabada' in vals and 'rendimiento_manual' not in vals:
            # Cuenta como revisado: quita el aviso de cambio.
            vals = dict(vals, rendimiento_manual=bool(vals['rendimiento_tela_acabada']), rendimiento_fuente='ficha',
                        rendimiento_real_ordenes=0, rendimiento_por_revisar=False)
        res = super().write(vals)
        if recalcular:
            self._ft_actualizar_rendimiento_real()
        return res

    @api.model
    def _ft_alerta_rendimiento_pct(self):
        valor = self.env['ir.config_parameter'].sudo().get_param(PARAM_ALERTA_RENDIMIENTO_PCT)
        try:
            return float(valor) if valor not in (None, False, '') else ALERTA_RENDIMIENTO_PCT
        except ValueError:
            return ALERTA_RENDIMIENTO_PCT

    def action_ft_rendimiento_revisado(self):
        """Ingeniería revisó el cambio de rendimiento: se quita el aviso (el número no cambia)."""
        self.write({'rendimiento_por_revisar': False})
        return True


class FichaTecnicaRevisionRendimiento(models.Model):
    _inherit = 'ficha.tecnica.revision'

    @api.model
    def _cron_ft_actualizar(self):
        # 2.24.0: primero el rendimiento (la revisión compara las listas contra él).
        self.env['ficha.tecnica.acabado']._ft_actualizar_rendimiento_real()
        return super()._cron_ft_actualizar()
