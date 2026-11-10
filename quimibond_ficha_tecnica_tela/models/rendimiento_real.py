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

2.28.0 (Dirección, 2026-11-06): **el recálculo nocturno propone, no escribe.** Lo medido queda en
``rendimiento_medido`` con su fuente y estado; la ficha (lista en metros, tiempos, costeo) cambia solo con «Usar el medido»
de Ingeniería. Lo que se aleja más de 25 % del teórico (1000 ÷ peso × ancho) se marca «fuera del teórico» y no se
propone. El «Rendimiento forzado» ya no hace falta para protegerlo: queda como marca de que Ingeniería lo escribió a mano.

Revisión 2026-11-06 (en 2.27.1): el estimado cuenta los kg de reproceso (códigos con punto al final) y no multiplica por el
consumo de crudo los kg de la tela acabada en kg; con menos de 3 muestras y sin estimado se queda el de la ficha.
"""
from datetime import timedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

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
# 2.28.0: lo medido que se aleja más que esto del teórico (1000 ÷ peso × ancho) no se propone: es un dato malo.
# Revisión 2026-11-10: el ancho del teórico es el que baja de rama (el de venta solo si falta).
PARAM_TOPE_TEORICO_PCT = 'quimibond_ficha_tecnica_tela.tope_teorico_pct'
TOPE_TEORICO_PCT = 25.0
UNIDADES_GRAMAJE = ('', 'g/m2', 'gr/m2', 'gsm', 'g', 'gr')
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
        help='Muestras con que se midió el rendimiento que tiene la ficha (rollos de Inspección, conversiones a kg u '
             'órdenes del estimado, de los últimos 12 meses). Se copia de «Muestras de lo medido» al dar «Usar el '
             'medido» y queda en 0 si Ingeniería escribe el rendimiento a mano o viene del Excel.')
    rendimiento_fuente = fields.Selection(
        [('rollos', 'Rollos de Inspección (metros y kg netos de cada rollo)'),
         ('terminado', 'Conversiones a kg (kg de la lista de empaque)'),
         ('estimado', 'Estimado (metros por kg que entró × consumo de crudo)'),
         ('ficha', 'Capturado en la ficha'),
         ('produccion', 'Peso y ancho de rama con merma (Ingeniería, 10-nov-2026)')],
        string='De dónde sale el rendimiento', readonly=True, copy=False,
        help='De dónde salió el rendimiento de tela terminada que tiene la ficha. Se llena solo: con la fuente de lo '
             'medido al dar «Usar el medido», o «Capturado en la ficha» si lo escribió Ingeniería o lo trajo el '
             'importador de Excel.')
    rendimiento_manual = fields.Boolean(
        string='Rendimiento forzado', copy=False,
        help='Marca de que Ingeniería escribió el rendimiento a mano en la ficha → «Tela acabada» → Real: se pone sola '
             'al escribirlo y «Usar el medido» la quita. Desde 2.28.0 no protege nada, porque el recálculo nocturno '
             'nunca cambia la ficha (solo propone en «Rendimiento medido»); desmarcarla vuelve a medir en ese momento.')
    # 2.26.0: aviso cuando el recálculo nocturno mueve el rendimiento más que la alerta (propuesta de Consolti,
    # 2026-11-05: que las telas del piloto no cambien sin aviso). El número se escribe igual; el aviso queda como
    # pendiente de Ingeniería en la revisión de listas hasta que alguien lo marca revisado.
    rendimiento_por_revisar = fields.Boolean(
        string='Rendimiento por revisar', readonly=True, copy=False,
        help='Se calcula cada noche: marcado cuando lo medido se aleja de la ficha más que la alerta (5 %, parámetro '
             '«alerta_rendimiento_pct») y deja un pendiente de Ingeniería en la revisión de listas. Se quita con «Usar '
             'el medido», «Marcar revisado» o escribiendo el rendimiento a mano.')
    rendimiento_anterior = fields.Float(
        string='Rendimiento anterior (m/kg)', digits=(12, 4), readonly=True, copy=False,
        help='El rendimiento de tela terminada que tenía la ficha antes del último cambio («Usar el medido» o escrito '
             'a mano). Se llena solo y se ve en la ficha → «Tela acabada» → Real.')
    rendimiento_cambio_fecha = fields.Datetime(
        string='Rendimiento cambió el', readonly=True, copy=False,
        help='Se llena solo: cuándo el recálculo nocturno propuso el rendimiento medido que está por aprobar o '
             'cuándo Ingeniería lo aplicó con «Usar el medido».')
    # 2.28.0 (Dirección, 2026-11-06): el recálculo nocturno PROPONE; el rendimiento de la ficha (el que usan la lista
    # en metros, los tiempos y el costeo) solo cambia cuando Ingeniería lo aprueba con «Usar el medido».
    rendimiento_medido = fields.Float(
        string='Rendimiento medido (m/kg)', digits=(12, 4), readonly=True, copy=False,
        help='Rendimiento de tela terminada que da lo fabricado en 12 meses: rollos de primera y FE de Inspección (3 o '
             'más), si no conversiones a kg (3 o más), si no el estimado; no cuentan los que se alejan más de 15 % de '
             'la mediana. Lo calcula el recálculo nocturno y solo propone: la ficha cambia con «Usar el medido».')
    rendimiento_medido_fuente = fields.Selection(
        [('rollos', 'Rollos de Inspección'), ('terminado', 'Conversiones a kg'), ('estimado', 'Estimado')],
        string='De dónde sale lo medido', readonly=True, copy=False,
        help='Se calcula cada noche: de qué datos salió el rendimiento medido (rollos de Inspección, conversiones a kg '
             'con el kg de la lista de empaque, o estimado con el consumo de crudo de la ficha).')
    rendimiento_medido_ordenes = fields.Integer(
        string='Muestras de lo medido', readonly=True, copy=False,
        help='Se calcula cada noche: cuántos rollos, conversiones u órdenes (estimado) entraron al rendimiento '
             'medido.')
    rendimiento_medido_estado = fields.Selection(
        [('igual', 'Igual al de la ficha'), ('propuesto', 'Por aprobar'), ('fuera', 'Fuera del teórico')],
        string='Lo medido contra la ficha', readonly=True, copy=False,
        help='Se calcula cada noche. «Igual»: se aleja de la ficha menos que la alerta (5 %). «Por aprobar»: se aleja '
             'más; Ingeniería lo usa o lo marca revisado. «Fuera del teórico»: se aleja más que el tope del teórico '
             '(25 %, peso y ancho): es un dato malo y no se propone.')
    # Revisión 2026-11-10 (Ingeniería, 6-nov: «el cálculo del rendimiento para el acabado sale del peso de la tela y
    # el ancho rollo de la rama; el código solo es la venta»): el teórico y el peso real usan el ancho bajando de rama
    # del artículo cuando está capturado; el ancho de venta solo si falta. Con el de venta el teórico salía 2 a 6 %
    # arriba de lo medido (WJ045: 13.89 contra 13.19; con 1.67 m de rama da 13.31).
    rendimiento_teorico = fields.Float(
        string='Rendimiento teórico (m/kg)', digits=(12, 4), compute='_compute_rendimiento_teorico',
        help='Se calcula: 1000 ÷ (peso de la ficha en g/m² × ancho en m). El ancho es el que baja de rama (parámetro '
             'del proceso del artículo en las ramas, promedio); si no está capturado, el ancho de venta. Solo es '
             'control: lo medido que se aleja más de 25 % no se propone.')
    rendimiento_teorico_ancho = fields.Char(
        string='Ancho del teórico', compute='_compute_rendimiento_teorico',
        help='Se calcula: el ancho en metros con que salieron el rendimiento teórico y el peso real, y de dónde sale '
             '(bajando de rama o, si falta, de venta).')
    peso_real = fields.Float(
        string='Peso real (g/m²)', digits=(12, 1), compute='_compute_peso_real',
        help='Se calcula: el peso que da lo fabricado, 1000 ÷ (rendimiento de la ficha × ancho en m), con el mismo '
             'ancho que el teórico. Solo si el rendimiento de la ficha salió de lo medido («Usar el medido»).')

    def _ft_ancho_rama(self):
        """{ficha: ancho bajando de rama en m} con el promedio de lo capturado en los procesos del artículo (uno por
        rama; suelen ser iguales). Sin ancho capturado la ficha no aparece: un vacío no es un cero."""
        products = self.product_proceso_id | self.product_acabado_id
        if not products:
            return {}
        anchos = {}
        for proceso in self.env['ficha.tecnica.proceso'].sudo().search([('product_id', 'in', products.ids)]):
            ancho = proceso._ft_value('ancho_rama')
            if ancho:
                anchos.setdefault(proceso.product_id.id, []).append(ancho_en_m(ancho))
        res = {}
        for ficha in self:
            valores = anchos.get(ficha.product_proceso_id.id, []) + anchos.get(ficha.product_acabado_id.id, [])
            if valores:
                res[ficha] = sum(valores) / len(valores)
        return res

    rendimiento_produccion = fields.Float(
        string='Rendimiento de producción calculado (m/kg)', digits=(12, 4), compute='_compute_rendimiento_produccion',
        help='Se calcula: 1000 ÷ (peso de la ficha en g/m² × ancho bajando de rama en m) × (1 − merma de la rama). '
             'La merma (orilla, muestras y merma natural, 10 % por lo general) y el ancho salen de Máquinas → '
             'Procesos por artículo → la tela en cada rama (promedio si corre en varias). Es el rendimiento que usa '
             'todo lo de producción (Ingeniería, 10-nov-2026): la noche lo pasa a la ficha y a sus listas.')
    rendimiento_produccion_calculo = fields.Char(
        string='Cálculo del rendimiento de producción', compute='_compute_rendimiento_produccion',
        help='La cuenta del rendimiento de producción con sus datos, o el dato que falta para calcularlo.')

    def _ft_merma_rama(self):
        """{ficha: merma en %} con el promedio de lo capturado en los procesos de la tela (uno por rama). Sin merma
        capturada la ficha no aparece."""
        products = self.product_proceso_id | self.product_acabado_id
        if not products:
            return {}
        mermas = {}
        for proceso in self.env['ficha.tecnica.proceso'].sudo().search([('product_id', 'in', products.ids)]):
            if proceso.valor_ids.filtered(lambda v: v.code == 'merma' and v.valor):
                mermas.setdefault(proceso.product_id.id, []).append(proceso._ft_value('merma'))
        res = {}
        for ficha in self:
            valores = mermas.get(ficha.product_proceso_id.id, []) + mermas.get(ficha.product_acabado_id.id, [])
            if valores:
                res[ficha] = sum(valores) / len(valores)
        return res

    def _ft_rendimiento_produccion(self):
        """2.30.0 (Ingeniería, Jessica Francisco, 10-nov-2026): {ficha: (rendimiento, cálculo o lo que falta)}. El
        rendimiento de producción sale del peso de la tela y el ancho del rollo de rama, con la merma (orilla,
        muestras y merma natural) sumada: 1000 ÷ (g/m² × ancho de rama m) × (1 − merma). Es por kg que entra a
        acabado, como se capturan las órdenes (los kg de tintorería consumidos traen la orilla)."""
        from .unidades import _norm  # local: orden de carga de models/
        ramas, mermas = self._ft_ancho_rama(), self._ft_merma_rama()
        res = {}
        for ficha in self:
            gramaje = ficha.peso_acabado if _norm(ficha.peso_acabado_tol_unit) in UNIDADES_GRAMAJE else 0.0
            ancho, merma = ramas.get(ficha), mermas.get(ficha)
            faltan = [nombre for nombre, valor in (('el peso en g/m²', gramaje), ('el ancho bajando de rama', ancho),
                                                   ('la merma de la rama', merma)) if not valor]
            if faltan:
                res[ficha] = (0.0, 'Falta %s (Procesos por artículo → la tela en la rama).' % ' y '.join(faltan)
                              if faltan != ['el peso en g/m²'] else 'Falta el peso en g/m² de la ficha.')
                continue
            valor = round(1000.0 / (gramaje * ancho) * (1 - merma / 100.0), 4)
            res[ficha] = (valor, '1000 ÷ (%s g/m² × %s m) × (1 − %s %%) = %s m/kg' % (
                ('%.2f' % gramaje).rstrip('0').rstrip('.'), ('%.3f' % ancho).rstrip('0').rstrip('.'),
                ('%.2f' % merma).rstrip('0').rstrip('.'), valor))
        return res

    @api.depends('peso_acabado', 'peso_acabado_tol_unit', 'product_proceso_id', 'product_acabado_id')
    def _compute_rendimiento_produccion(self):
        calculo = self._ft_rendimiento_produccion()
        for ficha in self:
            ficha.rendimiento_produccion, ficha.rendimiento_produccion_calculo = calculo.get(ficha, (0.0, False))

    def _ft_aplicar_rendimiento_produccion(self):
        """2.30.0: pasa el rendimiento de producción a la ficha (y a la conversión de sus listas en metros) donde se
        puede calcular y es otro. Devuelve las fichas que cambiaron."""
        cambiadas = self.browse()
        calculo = self._ft_rendimiento_produccion()
        for ficha in self:
            valor, texto = calculo.get(ficha, (0.0, False))
            if not valor or abs(valor - (ficha.rendimiento_tela_acabada or 0.0)) < 1e-4:
                continue
            ficha.with_context(**{CTX_CALCULADO: True}).write({
                'rendimiento_anterior': ficha.rendimiento_tela_acabada,
                'rendimiento_tela_acabada': valor,
                'rendimiento_fuente': 'produccion',
                'rendimiento_real_ordenes': 0,
                'rendimiento_manual': False,
                'rendimiento_por_revisar': False,
                'rendimiento_cambio_fecha': fields.Datetime.now(),
            })
            if hasattr(ficha, 'message_post'):  # el SGI le agrega el chatter a la ficha
                ficha.message_post(body='Rendimiento de producción: %s (antes %s).' % (texto, ficha.rendimiento_anterior))
            ficha._ft_pasar_conversion_a_listas()
            cambiadas |= ficha
        return cambiadas

    def action_ft_calcular_rendimiento_produccion(self):
        """Botón de la ficha: aplica ya el rendimiento de producción (la noche lo hace sola)."""
        self._ft_aplicar_rendimiento_produccion()
        return True

    def _ft_ancho_venta_m(self):
        """2.30.0: el ancho de venta en metros con su unidad del catálogo (cm, m, in); sin unidad, la regla de
        siempre (más de 10 es cm)."""
        self.ensure_one()
        if not self.ancho_acabado:
            return 0.0
        unidad = self.ancho_acabado_tol_unit_id
        metro = self.env.ref('uom.product_uom_meter', raise_if_not_found=False)
        factor = unidad._ft_factor_a(metro) if unidad and metro else 0.0
        return self.ancho_acabado * factor if factor else ancho_en_m(self.ancho_acabado)

    def _ft_ancho_teorico(self):
        """{ficha: (ancho en m, True si es el de rama)}: el ancho bajando de rama o, si falta, el de venta."""
        rama = self._ft_ancho_rama()
        return {ficha: (rama[ficha], True) if ficha in rama else (ficha._ft_ancho_venta_m(), False)
                for ficha in self}

    @api.depends('peso_acabado', 'peso_acabado_tol_unit', 'ancho_acabado', 'ancho_acabado_tol_unit_id',
                 'product_proceso_id', 'product_acabado_id')
    def _compute_rendimiento_teorico(self):
        from .unidades import _norm  # local: orden de carga de models/
        anchos = self._ft_ancho_teorico()
        for ficha in self:
            ancho, de_rama = anchos[ficha]
            gramaje = ficha.peso_acabado if _norm(ficha.peso_acabado_tol_unit) in UNIDADES_GRAMAJE else 0.0
            ficha.rendimiento_teorico = round(1000.0 / (gramaje * ancho), 4) if gramaje and ancho else 0.0
            ficha.rendimiento_teorico_ancho = '%s m (%s)' % (
                ('%.3f' % ancho).rstrip('0').rstrip('.'),
                'bajando de rama' if de_rama else 'de venta: falta el ancho bajando de rama') if ancho else False

    @api.depends('rendimiento_tela_acabada', 'rendimiento_real_ordenes', 'ancho_acabado', 'ancho_acabado_tol_unit_id',
                 'product_proceso_id', 'product_acabado_id')
    def _compute_peso_real(self):
        anchos = self._ft_ancho_teorico()
        for ficha in self:
            ancho = anchos[ficha][0]
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
        tope = self._ft_param_pct(PARAM_TOPE_TEORICO_PCT, TOPE_TEORICO_PCT)
        produccion = fichas._ft_rendimiento_produccion()
        for ficha in fichas.with_context(**{CTX_CALCULADO: True}):
            nuevo, n, fuente = 0.0, 0, False
            m_r, kg_r, n_r = rollos.get(ficha, (0.0, 0.0, 0))
            m_c, kg_c, n_c = conversiones.get(ficha, (0.0, 0.0, 0))
            m, kg_entrada, kg_terminada, kg_saldos, n_e = datos.get(ficha.product_acabado_id, (0.0, 0.0, 0.0, 0.0, 0))
            consumo = ficha.consumo_crudo if 'consumo_crudo' in ficha._fields else 0.0
            # Revisión 2026-11-06: kg de tela terminada que salieron de esas órdenes: la tela acabada en kg tal cual, el
            # crudo o teñido entre el consumo de crudo (kg que entran por kg de tela acabada) y menos los saldos. Con
            # crudo o teñido y sin consumo capturado no se puede estimar.
            kg_estimado = 0.0
            if m and (kg_terminada or kg_entrada) and (consumo or not kg_entrada):
                kg_estimado = kg_terminada + (kg_entrada / consumo if kg_entrada else 0.0) - kg_saldos
            if m_r and kg_r and n_r >= MIN_MUESTRAS:
                nuevo, n, fuente = round(m_r / kg_r, 4), n_r, 'rollos'
            elif m_c and kg_c and n_c >= MIN_MUESTRAS:
                nuevo, n, fuente = round(m_c / kg_c, 4), n_c, 'terminado'
            elif kg_estimado > 0:
                nuevo, n, fuente = round(m / kg_estimado, 4), n_e, 'estimado'
            # Con menos de MIN_MUESTRAS rollos o conversiones y sin estimado no hay medido: una o dos muestras no se
            # pueden filtrar (WJ060Q21JNT165: una conversión de 12.05 contra 9.77 real).
            vals = {'rendimiento_medido': nuevo, 'rendimiento_medido_fuente': fuente,
                    'rendimiento_medido_ordenes': n, 'rendimiento_medido_estado': False, 'rendimiento_por_revisar': False}
            if not ficha.rendimiento_fuente:
                vals['rendimiento_fuente'] = 'ficha'  # el rendimiento de la ficha viene de lo capturado
            if nuevo:
                # 2.28.0: lo medido se propone; la ficha no cambia sola.
                teorico, antes = ficha.rendimiento_teorico, ficha.rendimiento_tela_acabada
                # 2.30.0: con rendimiento de producción (peso, ancho de rama y merma) lo medido es solo control: es por
                # kg de tela terminada, sin merma, así que se compara contra el teórico sin merma (avisa si el peso o
                # el ancho de la ficha no cuadran con lo fabricado), no contra la ficha.
                if produccion.get(ficha, (0.0,))[0] and teorico:
                    antes = teorico
                if teorico and abs(nuevo - teorico) / teorico * 100.0 > tope:
                    vals['rendimiento_medido_estado'] = 'fuera'
                elif not antes or abs(nuevo - antes) / antes * 100.0 > limite:
                    vals.update(rendimiento_medido_estado='propuesto', rendimiento_por_revisar=True)
                    if not ficha.rendimiento_por_revisar or ficha.rendimiento_medido != nuevo:
                        vals['rendimiento_cambio_fecha'] = fields.Datetime.now()
                else:
                    vals['rendimiento_medido_estado'] = 'igual'
            if any(ficha[k] != v for k, v in vals.items() if k != 'rendimiento_cambio_fecha') \
                    or 'rendimiento_cambio_fecha' in vals:
                ficha.write(vals)
        return True

    def action_ft_usar_rendimiento_medido(self):
        """2.28.0: Ingeniería aprueba lo medido: pasa a ser el rendimiento de la ficha (lista en metros, tiempos, costeo).
        Lo «fuera del teórico» no se aplica con este botón: hay que capturarlo a mano después de revisar los datos."""
        con_produccion = [f.articulo for f, (valor, _t) in self._ft_rendimiento_produccion().items() if valor]
        if con_produccion:
            raise UserError('El rendimiento de %s sale del peso, el ancho de rama y la merma (Ingeniería, 10-nov-2026); '
                            'lo medido es solo control. Si no cuadra, corrija el peso de la ficha o el ancho y la '
                            'merma de la tela en la rama.' % ', '.join(con_produccion))
        for ficha in self.filtered(lambda f: f.rendimiento_medido and f.rendimiento_medido_estado != 'fuera'):
            ficha.with_context(**{CTX_CALCULADO: True}).write({
                'rendimiento_anterior': ficha.rendimiento_tela_acabada,
                'rendimiento_tela_acabada': ficha.rendimiento_medido,
                'rendimiento_fuente': ficha.rendimiento_medido_fuente,
                'rendimiento_real_ordenes': ficha.rendimiento_medido_ordenes,
                'rendimiento_manual': False,
                'rendimiento_por_revisar': False,
                'rendimiento_medido_estado': 'igual',
                'rendimiento_cambio_fecha': fields.Datetime.now(),
            })
            ficha._ft_pasar_conversion_a_listas()
        return True

    def _ft_pasar_conversion_a_listas(self):
        """Revisión 2026-11-10 (en 2.28.0): el rendimiento de la ficha cambió por decisión de Ingeniería («Usar el
        medido» o escrito a mano) y la conversión kg → m de las listas en metros de la tela se pone al día en ese
        momento, también en las archivadas; en una lista activa solo cambia las órdenes nuevas. Antes la lista se
        quedaba con el valor anterior hasta que alguien oprimía «Usar los datos de la ficha»: en main, 40 de las 45
        listas del flujo estaban desfasadas el 10-nov, cinco días después de regenerarlas. Deja nota en cada lista
        que cambia. La primera para planear del costeo no entra aquí: cuando cambia, la lista queda como pendiente de
        Ingeniería en la revisión, igual que antes. Devuelve las listas que cambiaron."""
        Bom = self.env['mrp.bom'].with_context(active_test=False)
        cambiadas = Bom
        for ficha in self:
            tela = ficha.product_acabado_id
            if not tela or not ficha.rendimiento_tela_acabada:
                continue
            boms = Bom.search([('type', '=', 'normal'), '|', ('product_id', '=', tela.id),
                               '&', ('product_id', '=', False), ('product_tmpl_id', '=', tela.product_tmpl_id.id)])
            # El aviso de la lista no depende del rendimiento de la ficha: sin esto se lee el de antes del cambio.
            boms.invalidate_recordset(['ft_conversion_kg', 'ft_conversion_calculo', 'ft_conversion_difiere'])
            for bom in boms.filtered('ft_conversion_difiere'):
                line, _kg, calculo = bom._ft_conversion()
                antes = line.product_qty
                bom.action_ft_usar_conversion_ficha()
                bom.message_post(body='Conversión a metros puesta al día con el rendimiento de la ficha %s: de %s a '
                                 '%s %s por %s (%s).' % (
                                     ficha.display_name, antes, line.product_qty, line.product_uom_id.name,
                                     bom.product_uom_id.name, calculo))
                cambiadas |= bom
        return cambiadas

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
        if 'rendimiento_tela_acabada' in vals:
            # 2.30.0: el rendimiento escrito a mano también guarda el anterior, como «Usar el medido».
            for ficha in self.filtered(lambda f: f.rendimiento_tela_acabada
                                       and abs(f.rendimiento_tela_acabada - (vals['rendimiento_tela_acabada'] or 0.0))
                                       > 1e-9):
                super(FichaTecnicaAcabadoRendimientoReal, ficha).write(
                    {'rendimiento_anterior': ficha.rendimiento_tela_acabada})
        res = super().write(vals)
        if recalcular:
            self._ft_actualizar_rendimiento_real()
        if 'rendimiento_tela_acabada' in vals:
            self._ft_pasar_conversion_a_listas()
        return res

    @api.model
    def _ft_alerta_rendimiento_pct(self):
        return self._ft_param_pct(PARAM_ALERTA_RENDIMIENTO_PCT, ALERTA_RENDIMIENTO_PCT)

    @api.model
    def _ft_param_pct(self, clave, defecto):
        valor = self.env['ir.config_parameter'].sudo().get_param(clave)
        try:
            return float(valor) if valor not in (None, False, '') else defecto
        except ValueError:
            return defecto

    def action_ft_rendimiento_revisado(self):
        """Ingeniería revisó lo medido y se queda con el rendimiento de la ficha: se quita el aviso (el número no
        cambia). Vuelve a avisar si lo medido cambia."""
        self.write({'rendimiento_por_revisar': False})
        return True


class FichaTecnicaRevisionRendimiento(models.Model):
    _inherit = 'ficha.tecnica.revision'

    @api.model
    def _cron_ft_actualizar(self):
        # 2.24.0: primero el rendimiento (la revisión compara las listas contra él).
        Acabado = self.env['ficha.tecnica.acabado']
        Acabado._ft_actualizar_rendimiento_real()
        # 2.30.0: el rendimiento de producción (peso, ancho de rama y merma) pasa a la ficha y a sus listas.
        Acabado.search([('product_acabado_id', '!=', False)])._ft_aplicar_rendimiento_produccion()
        return super()._cron_ft_actualizar()
