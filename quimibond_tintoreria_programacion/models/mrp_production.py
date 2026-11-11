# -*- coding: utf-8 -*-
import math
from collections import defaultdict

from odoo import Command, _, api, fields, models
from odoo.exceptions import UserError
from odoo.tools import float_compare, float_round

from odoo.addons.quimibond_ficha_tecnica_tela.models.mrp_aviso_maquina import GRUPOS


def banda_rendimiento(rendimiento):
    """Banda A/B/C del rendimiento de tejido (m/kg), con los mismos grupos que las fichas técnicas
    (GRUPOS): el límite inferior pertenece a la banda y el superior a la siguiente.
    Fuera de todas las bandas devuelve False."""
    rendimiento = rendimiento or 0.0
    for _codigo, grupo, desde, hasta in GRUPOS:
        if desde <= rendimiento < hasta:
            return grupo
    return False


def clave_rollo(nombre):
    """(lote del baño de tejido, número del rollo) de un lote de crudo: «H13850-0007» → («H13850», 7). Sin guion o sin
    número al final, el nombre completo y 0."""
    nombre = nombre or ''
    partes = nombre.rsplit('-', 1)
    if len(partes) == 2 and partes[1].isdigit():
        return partes[0], int(partes[1])
    return nombre, 0


def rango_rollos(nombres):
    """Texto de los rollos de un baño: «H13850 rollos 1-6»; con rollos salteados, «H13850 rollos 1, 3-4». Varios lotes de
    tejido se separan con « / »."""
    por_lote = defaultdict(list)
    for nombre in nombres:
        lote, numero = clave_rollo(nombre)
        por_lote[lote].append((numero, nombre))
    textos = []
    for lote, rollos in por_lote.items():
        rollos.sort()
        if not rollos[0][0]:
            textos.append(', '.join(nombre for _n, nombre in rollos))
            continue
        tramos, inicio, anterior = [], rollos[0][0], rollos[0][0]
        for numero, _nombre in rollos[1:]:
            if numero == anterior + 1:
                anterior = numero
                continue
            tramos.append((inicio, anterior))
            inicio = anterior = numero
        tramos.append((inicio, anterior))
        etiqueta = ', '.join('%s' % a if a == b else '%s-%s' % (a, b) for a, b in tramos)
        textos.append('%s rollos %s' % (lote, etiqueta))
    return ' / '.join(textos)


def repartir_rollos(kgs, capacidad):
    """1.9.0: reparte rollos completos y consecutivos en baños sin pasar la capacidad del jet y con los kg lo más parejos
    posible. ``kgs`` son los kg de crudo de cada rollo en orden de número. Empieza con el mínimo de baños (kg totales ÷
    capacidad) y sube uno si con rollos completos no cabe. Devuelve la lista de ``(inicio, fin)`` (fin excluido) de cada
    baño. Criterio: primero el baño más cargado lo menor posible y, a igualdad, la menor suma de cuadrados."""
    if not kgs:
        raise UserError(_("No hay rollos que repartir."))
    if capacidad <= 0:
        raise UserError(_("La capacidad del jet debe ser mayor que cero."))
    mayor = max(kgs)
    if float_compare(mayor, capacidad, precision_digits=4) > 0:
        raise UserError(_(
            "Un rollo de crudo pesa %(kg)s kg y la capacidad del jet es de %(cap)s kg: los rollos no se cortan, así que "
            "no cabe en ningún baño de ese jet.", kg=round(mayor, 3), cap=round(capacidad, 3)))
    acumulado = [0.0]
    for kg in kgs:
        acumulado.append(acumulado[-1] + kg)
    total, rollos = acumulado[-1], len(kgs)
    n_min = max(1, math.ceil(round(total / capacidad, 6)))
    for n in range(n_min, rollos + 1):
        mejor = [[None] * (rollos + 1) for _i in range(n + 1)]
        mejor[0][0] = (0.0, 0.0, None)
        for baño in range(1, n + 1):
            for fin in range(baño, rollos + 1):
                candidato = None
                for inicio in range(baño - 1, fin):
                    previo = mejor[baño - 1][inicio]
                    if previo is None:
                        continue
                    carga = acumulado[fin] - acumulado[inicio]
                    valor = (round(max(previo[0], carga), 6), round(previo[1] + carga * carga, 6), inicio)
                    if candidato is None or valor[:2] < candidato[:2]:
                        candidato = valor
                mejor[baño][fin] = candidato
        final = mejor[n][rollos]
        if final is None or float_compare(final[0], capacidad, precision_digits=4) > 0:
            continue
        cortes, fin = [], rollos
        for baño in range(n, 0, -1):
            inicio = mejor[baño][fin][2]
            cortes.append((inicio, fin))
            fin = inicio
        return cortes[::-1]
    raise UserError(_(
        "Con rollos completos no cabe el crudo (%(kg)s kg en %(r)s rollos) en baños de %(cap)s kg.",
        kg=round(total, 3), r=rollos, cap=round(capacidad, 3)))


class MrpProduction(models.Model):
    _inherit = 'mrp.production'

    tintoreria_programacion_id = fields.Many2one(
        'tintoreria.programacion', 'Programación de Tintorería', copy=False, readonly=True,
        index=True, ondelete='set null')
    tintoreria_programada = fields.Boolean(
        'Tintorería programada', copy=False, readonly=True, tracking=True,
        help="Tintorería ya asignó el HTJ de esta orden y se calculó el tamaño de baño.")
    requiere_programacion_tintoreria = fields.Boolean(
        'Requiere programación de Tintorería', compute='_compute_requiere_programacion_tintoreria',
        store=True,
        help="Producto en kg (ligado como «Acabado en proceso» en una ficha técnica de acabado) "
             "y una operación de Tintorería en su BoM.")
    tintoreria_pendiente = fields.Boolean(
        'Pendiente de programar Tintorería', compute='_compute_tintoreria_pendiente',
        store=True, index=True)

    # ------------------------------------------------------------------
    # Campos calculados
    # ------------------------------------------------------------------
    def _es_producto_acabado_kg(self):
        """True si el producto es el «Acabado en proceso» (kg) de alguna ficha técnica de acabado.
        El vínculo kg <-> metros vive en la ficha (product_proceso_id), no en el código del artículo."""
        self.ensure_one()
        if not self.product_id:
            return False
        return bool(self.env['ficha.tecnica.acabado'].search_count(
            [('product_proceso_id', '=', self.product_id.id)], limit=1))

    @api.depends('product_id', 'bom_id', 'bom_id.operation_ids.workcenter_id',
                 'bom_id.operation_ids.name')
    def _compute_requiere_programacion_tintoreria(self):
        # Nota: se calcula al crear la orden; si la ficha de acabado se liga después, las órdenes ya
        # creadas no se recalculan solas.
        for orden in self:
            orden.requiere_programacion_tintoreria = bool(
                orden.product_id and orden._es_producto_acabado_kg()
                and orden.bom_id and orden.bom_id._operacion_tintoreria())

    @api.depends('requiere_programacion_tintoreria', 'tintoreria_programada', 'state')
    def _compute_tintoreria_pendiente(self):
        for orden in self:
            orden.tintoreria_pendiente = (
                orden.requiere_programacion_tintoreria
                and not orden.tintoreria_programada
                and orden.state not in ('done', 'cancel'))

    # ------------------------------------------------------------------
    # Datos para el cálculo
    # ------------------------------------------------------------------
    def _workorder_tintoreria(self):
        self.ensure_one()
        operacion = self.bom_id._operacion_tintoreria()
        return self.workorder_ids.filtered(lambda wo: wo.operation_id == operacion)[:1]

    def _rendimiento_tejido(self):
        """Rendimiento de tejido (m/kg) del producto en kg: el que fija la banda y la carga del jet."""
        self.ensure_one()
        rendimiento, _origen = self.product_id._ft_rendimiento()
        if not rendimiento:
            raise UserError(_(
                "No se encontró el rendimiento de tejido de %s. Revisa que tenga ficha de tejido con peso "
                "y ancho, o que su ficha de acabado esté ligada a ella como «Acabado en proceso».",
                self.product_id.display_name))
        return rendimiento

    def _tintoreria_movs_crudo(self):
        """Movimientos de consumo del crudo (componentes en kg de una categoría de tejido) de la orden."""
        self.ensure_one()
        kg = self._kg_uom()
        return self.move_raw_ids.filtered(
            lambda m: m.state != 'cancel' and m.product_uom._has_common_reference(kg) and self._es_tejido(m.product_id))

    def _tintoreria_rollos_crudo(self):
        """1.9.0 (Dirección, 2026-11-11): los rollos de crudo que ya se surtieron a la Ubic. 2 para esta orden, en orden de
        lote y número de rollo: lista de ``{'lote', 'kg', 'move', 'ubicacion'}``. Son la base de la programación: los baños
        se arman con rollos completos y los kg de cada baño son la suma de los suyos. Sin componente de tejido devuelve
        una lista vacía (la programación sigue con los kg de la orden, como antes). Si el crudo todavía no se surte (la
        requisición, REQ-MP, no está validada) o no trae lotes, bloquea."""
        self.ensure_one()
        kg = self._kg_uom()
        rollos = []
        for move in self._tintoreria_movs_crudo():
            pendientes = move.move_orig_ids.filtered(lambda m: m.state not in ('done', 'cancel'))
            if pendientes:
                raise UserError(_(
                    "Falta surtir el crudo %(p)s de la orden %(o)s: valida primero la requisición %(r)s (de la Ubic. 5 a "
                    "la Ubic. 2) con los rollos del baño. La programación se arma con los rollos surtidos.",
                    p=move.product_id.display_name, o=self.name,
                    r=', '.join(pendientes.picking_id.mapped('name')) or _('(sin requisición)')))
            # Los rollos son los que la requisición surtió completos a la Ubic. 2, no lo que la orden alcanzó a reservar:
            # si la requisición trajo el lote entero (p. ej. 400.76 kg) y la orden pidió 381 kg, Odoo reserva 381 y recorta
            # el último rollo; programar con eso partiría un rollo. Sin requisición (existencia cargada directo) se usan las
            # líneas de la propia orden.
            surtidas = move.move_orig_ids.filtered(lambda m: m.state == 'done').move_line_ids.filtered(
                lambda ml: ml.lot_id and ml.quantity > 0)
            lineas = surtidas or move.move_line_ids.filtered(lambda ml: ml.lot_id and ml.quantity > 0)
            if not lineas:
                raise UserError(_(
                    "El crudo %(p)s de la orden %(o)s no tiene rollos (lotes) surtidos en %(u)s: surte los rollos del "
                    "baño antes de programar.", p=move.product_id.display_name, o=self.name,
                    u=move.location_id.display_name))
            por_lote = {}
            for linea in lineas:
                datos = por_lote.setdefault(linea.lot_id, {
                    'lote': linea.lot_id, 'kg': 0.0, 'move': move,
                    'ubicacion': move.location_id if surtidas else linea.location_id})
                datos['kg'] += linea.product_uom_id._compute_quantity(linea.quantity, kg, round=False)
            rollos.extend(por_lote.values())
        rollos.sort(key=lambda r: (r['move'].product_id.id,) + clave_rollo(r['lote'].name) + (r['lote'].name,))
        return rollos

    def _capacidad_jet(self, workcenter, rendimiento):
        """(capacidad por baño en kg, banda, aviso) del jet para este producto.

        Fuente única: la configuración vigente del jet en las fichas técnicas.
        * Si el artículo tiene «kg por carga» en este jet, ese manda.
        * Jet con bandas capturadas: bloquea si la banda es N/A o el rendimiento cae fuera de las bandas.
        * Jet sin ninguna banda capturada (p. ej. HTJ5): su capacidad general, con aviso.
        """
        self.ensure_one()
        banda = banda_rendimiento(rendimiento)
        cap_articulo, motivo = workcenter._ft_capacidad_carga(rendimiento, product=self.product_id)
        if cap_articulo and 'artículo' in (motivo or ''):
            return cap_articulo, banda, False
        config = workcenter._ft_config_at()
        if not config:
            raise UserError(_(
                "El centro de trabajo %s no tiene configuración vigente en las fichas técnicas de máquina.",
                workcenter.display_name))
        general = config._ft_value('capacidad') or 0.0
        capacidades = {grupo: config._ft_value(codigo) or 0.0 for codigo, grupo, _d, _h in GRUPOS}
        if not any(capacidades.values()):
            if general <= 0:
                raise UserError(_(
                    "%s no tiene capacidad capturada (ni general ni por banda).", workcenter.display_name))
            aviso = _(
                "%(wc)s no tiene bandas de rendimiento capturadas: se usó su capacidad general de %(cap)s kg.",
                wc=workcenter.display_name, cap=general)
            return general, banda, aviso
        if not banda:
            raise UserError(_(
                "El rendimiento de tejido de %(prod)s (%(r)s m/kg) está fuera de las bandas de rendimiento "
                "de %(wc)s. Corrige el rendimiento en la ficha técnica.",
                prod=self.product_id.display_name, r=rendimiento, wc=workcenter.display_name))
        capacidad = capacidades[banda]
        if capacidad <= 0:
            raise UserError(_(
                "%(wc)s no puede procesar este artículo: la banda %(b)s (rendimiento de %(r)s m/kg) "
                "es N/A en ese jet.", wc=workcenter.display_name, b=banda, r=rendimiento))
        if general > 0:
            capacidad = min(capacidad, general)
        return capacidad, banda, False

    def _minutos_tintoreria(self, workcenter, kg_bano=None):
        """(minutos por baño, origen, aviso) de la operación de Tintorería en el HTJ dado.

        El tiempo de la ficha y el de la fórmula son por ORDEN (un solo ciclo), no por kg. Orden de
        prioridad (diseño §3.1):
        1. «Tiempo de ciclo» del artículo en ese jet (proceso del artículo en la ficha técnica; la
           ficha lo guarda en horas).
        2. Minutos de la fórmula de la BoM en ese HTJ (``tintoreria.formula.tiempo``).
        3. La operación de Tintorería de la BoM (con aviso, que la programación deja en el chatter de
           cada orden). 1.5.0: su tiempo es el de Odoo, por unidad de la BoM (en las listas en kg,
           minutos por kg), así que se multiplica por los kg del baño (``kg_bano``; si no se da, los de
           la orden), como lo calcula Odoo. 1.6.0: el tiempo de la operación como lo da Odoo
           (``time_cycle``), sea manual o calculado por historial.
        Si no existe ninguno la programación se bloquea (no se asume un valor).
        """
        self.ensure_one()
        productos = self.product_id
        acabados = self.env['ficha.tecnica.acabado'].search([('product_proceso_id', '=', self.product_id.id)])
        productos |= acabados.mapped('product_acabado_id')
        codigos = [c for c in productos.mapped('default_code') if c]
        valor = self.env['ficha.tecnica.proceso.valor'].search([
            ('code', '=', 'tiempo_ciclo'),
            ('workcenter_id', '=', workcenter.id),
            ('valor', '>', 0),
            '|', ('proceso_id.product_id', 'in', productos.ids), ('proceso_id.articulo', 'in', codigos),
        ], limit=1)
        if valor:
            minutos = valor.valor * (1.0 if (valor.unit or 'h').lower() in ('min', 'mins', 'minutos') else 60.0)
            return minutos, 'ficha', False
        formula = self.bom_id.formula_tintoreria_id
        if formula:
            tiempo = self.env['tintoreria.formula.tiempo'].search([
                ('formula_id', '=', formula.id), ('workcenter_id', '=', workcenter.id)], limit=1)
            if tiempo:
                return tiempo.minutos, 'formula', False
        operacion = self.bom_id._operacion_tintoreria()
        if operacion and operacion.time_cycle > 0:
            kg = self.product_qty if kg_bano is None else kg_bano
            minutos = self._minutos_operacion_bom(operacion, workcenter, kg)
            aviso = _(
                "No hay tiempo de ciclo del artículo en %(wc)s ni tiempo de la fórmula %(f)s en ese HTJ: "
                "se usó la operación de Tintorería de la BoM, %(m)s min por baño (cantidad base de la lista: "
                "%(q)s %(u)s; %(kg)s kg por baño).",
                wc=workcenter.display_name, f=formula.display_name or '-', m=round(minutos, 1),
                q=operacion.bom_id.product_qty or 1, u=operacion.bom_id.product_uom_id.name, kg=kg)
            return minutos, 'bom', aviso
        raise UserError(_(
            "No hay tiempo de Tintorería para %(prod)s en %(wc)s: captura el tiempo de ciclo del artículo "
            "en el jet (ficha técnica) o el tiempo de su fórmula en ese HTJ.",
            prod=self.product_id.display_name, wc=workcenter.display_name))

    def _minutos_operacion_bom(self, operacion, workcenter, kg):
        """1.5.0: minutos de la operación de la BoM para ``kg`` en el HTJ, con la misma cuenta que Odoo
        (``mrp.workorder._get_duration_expected``): preparación + limpieza + ciclos × tiempo del ciclo,
        entre la eficiencia de la máquina; un ciclo es la cantidad de la BoM (o la capacidad del HTJ
        para el producto, si está capturada)."""
        self.ensure_one()
        bom = operacion.bom_id
        capacidad, preparacion, limpieza = workcenter._get_capacity(
            self.product_id, bom.product_uom_id, bom.product_qty or 1)
        cantidad = self.product_uom_id._compute_quantity(kg, bom.product_uom_id)
        ciclos = float_round(cantidad / (capacidad or 1), precision_digits=0, rounding_method='UP')
        return preparacion + limpieza + ciclos * operacion.time_cycle * 100.0 / (
            workcenter.time_efficiency or 100.0)

    @api.model
    def _check_jet_operando(self, workcenter):
        """1.7.1: no se programa en un jet que no está operando (en montaje, en pruebas o fuera de servicio). El jet
        programado queda fijo y el planificador no lo cambia por otro (``_ft_maquina_fija``), así que la orden se
        quedaría esperando en una máquina que no trabaja. Ronda 10 en main (2026-11-06): el asistente dejaba
        programar en TINTORERIA 5, en montaje, sin aviso."""
        if workcenter.ft_estado != 'operando':
            estado = dict(workcenter._fields['ft_estado']._description_selection(self.env)).get(workcenter.ft_estado)
            raise UserError(_(
                "%(wc)s está %(estado)s: no se programa en un jet que no está operando. Escoge otro jet o cambia su "
                "estado en el centro de trabajo.", wc=workcenter.display_name, estado=(estado or '').lower()))

    def _calcular_plan_tintoreria(self, workcenter):
        """Capacidad por baño, número de baños y kg por baño (reparto simétrico) en el HTJ dado. 1.7.0: el número de
        baños sale de los kg de crudo que entran al jet; la orden se reparte en kg de tela en la misma proporción."""
        self.ensure_one()
        self._check_jet_operando(workcenter)
        rendimiento = self._rendimiento_tejido()
        capacidad, banda, aviso = self._capacidad_jet(workcenter, rendimiento)
        kg_total = self.product_qty
        uom = self.product_uom_id
        rollos = self._tintoreria_rollos_crudo()
        banos = []
        if rollos:
            # 1.9.0: el baño se arma con rollos completos y consecutivos; sus kg son la suma de los suyos.
            kgs = [r['kg'] for r in rollos]
            for inicio, fin in repartir_rollos(kgs, capacidad):
                grupo = rollos[inicio:fin]
                kg_bano_real = uom.round(sum(r['kg'] for r in grupo))
                banos.append({
                    'rollos': grupo, 'n_rollos': len(grupo), 'kg': kg_bano_real,
                    'texto': rango_rollos([r['lote'].name for r in grupo])})
            n_banos = len(banos)
            montos = [b['kg'] for b in banos]
            kg_total = sum(montos)
            kg_carga = kg_total
            crudo = max(rollos, key=lambda r: r['kg'])['move'].product_id
            kg_bano = uom.round(kg_total / n_banos)
        else:
            kg_carga, crudo = self._kg_carga_jet()  # quimibond_tintoreria_dosificacion 1.4.0
            n_banos = max(1, math.ceil(round(kg_carga / capacidad, 6)))
            kg_bano = uom.round(kg_total / n_banos)
            montos = [kg_bano] * (n_banos - 1)
            montos.append(uom.round(kg_total - sum(montos)))  # el redondeo se absorbe en el último baño
        # 1.5.0: después de repartir, porque el tiempo de la operación de la BoM va por kg del baño.
        minutos, origen_tiempo, aviso_tiempo = self._minutos_tintoreria(workcenter, kg_bano=max(montos))
        return {
            'rendimiento': rendimiento,
            'banda': banda,
            'capacidad_kg': capacidad,
            'n_banos': n_banos,
            'kg_bano': kg_bano,
            'kg_total': kg_total,
            'montos': montos,
            'banos': banos,
            'lote_crudo': ' / '.join(dict.fromkeys(clave_rollo(r['lote'].name)[0] for r in rollos)),
            'n_rollos': len(rollos),
            'kg_crudo': kg_carga,
            'kg_crudo_bano': float_round(kg_carga / n_banos, precision_digits=3),
            'crudo_id': crudo.id if crudo else False,
            'aviso_crudo': False if crudo else _(
                "La orden %s no tiene componente de tejido: los baños se calcularon con los kg de la orden.",
                self.name),
            'aviso': aviso,
            'minutos': minutos,
            'origen_tiempo': origen_tiempo,
            'aviso_tiempo': aviso_tiempo,
        }

    # ------------------------------------------------------------------
    # Programación
    # ------------------------------------------------------------------
    def _check_programable(self):
        self.ensure_one()
        if not self.requiere_programacion_tintoreria:
            raise UserError(_(
                "La orden %s no requiere programación de Tintorería (el producto no está ligado como "
                "«Acabado en proceso» en una ficha de acabado, o la BoM no tiene operación de Tintorería).",
                self.name))
        if self.tintoreria_programada:
            raise UserError(_("La orden %s ya está programada.", self.name))
        if self.state not in ('draft', 'confirmed'):
            raise UserError(_("La orden %s ya inició o está cerrada; no se puede programar.", self.name))
        # Odoo 19: estados de la workorder = blocked / ready / progress / done / cancel
        if any(wo.state not in ('blocked', 'ready') for wo in self.workorder_ids) \
                or any(self.move_raw_ids.mapped('picked')):
            raise UserError(_("La orden %s ya tiene operaciones iniciadas; no se puede programar.", self.name))
        if not self._workorder_tintoreria():
            raise UserError(_("La orden %s no tiene operación de Tintorería.", self.name))

    def _tintoreria_dividir_orden_metros(self):
        """1.8.0 (Dirección, 2026-11-06): un baño = una orden en kg = una orden en metros. Al dividir la orden en kg en
        baños se divide también, en la misma proporción, la orden en metros que la consume (MTO), y cada parte queda ligada
        a su baño. Así cada baño cierra su propia orden en metros con sus metros reales: sin pendiente calculado con el
        factor de la lista y sin repartir la demanda de kg entre baños (ronda 12 en main: la orden de 9,040 m cerró en
        10,763 y el reparto hizo nacer una orden en kg que nadie pidió).

        Solo divide si la orden en metros se abastece únicamente de estos baños y no ha empezado; si no, la deja como
        estaba (varios baños para una orden en metros, rollos 3.3.0). Devuelve las partes en el orden de los baños."""
        banos = self
        nada = self.env['mrp.production']
        if len(banos) < 2 or 'mrp.rollo.final' not in self.env:
            # Sin quimibond_rollos_bitacora no hay cierre que registre cada orden en metros con sus metros reales:
            # dividirla solo dejaría N órdenes para cerrar a mano (revisión 2026-11-06).
            return nada
        producto = banos[0].product_id
        terminados = banos.move_finished_ids.filtered(
            lambda m: m.product_id == producto and m.state not in ('done', 'cancel'))
        consumos = terminados.move_dest_ids.filtered(lambda m: m.state not in ('done', 'cancel'))
        orden_m = consumos.raw_material_production_id
        if not orden_m:
            return nada  # venta en kg o inventario en kg: no hay orden en metros que dividir
        # 1.8.2 (revisión 2026-11-08): en estos casos no se divide y antes no se avisaba; los baños registran en la misma
        # orden en metros con pendiente (rollos 3.3.0).
        if len(orden_m) != 1:
            return banos._tintoreria_avisar_sin_dividir(orden_m, _('la abastecen varias órdenes en metros'))
        if orden_m.state not in ('draft', 'confirmed'):
            return banos._tintoreria_avisar_sin_dividir(orden_m, _('la orden en metros ya no está confirmada'))
        if any(wo.state not in ('blocked', 'ready') for wo in orden_m.workorder_ids) \
                or any(orden_m.move_raw_ids.mapped('picked')):
            return banos._tintoreria_avisar_sin_dividir(orden_m, _('la orden en metros ya empezó'))
        if consumos.move_orig_ids.production_id - banos:
            return banos._tintoreria_avisar_sin_dividir(
                orden_m, _('también la abastece otra orden en kg (%s)',
                           ', '.join((consumos.move_orig_ids.production_id - banos).mapped('name'))))
        kg_total = sum(banos.mapped('product_qty'))
        if not kg_total:
            return nada
        uom = orden_m.product_uom_id
        cantidad = orden_m.product_qty
        montos = [uom.round(cantidad * b.product_qty / kg_total) for b in banos[:-1]]
        montos.append(uom.round(cantidad - sum(montos)))
        partes = orden_m._split_productions({orden_m: montos})
        if len(partes) != len(banos):
            raise UserError(_(
                "La orden en metros %(m)s se dividió en %(p)s partes para %(b)s baños; no se programó nada. Revisa las "
                "cantidades de la orden en metros.", m=orden_m.name, p=len(partes), b=len(banos)))
        consumos_partes = partes.move_raw_ids.filtered(lambda m: m.product_id == producto)
        for bano, parte in zip(banos, partes):
            consumo = parte.move_raw_ids.filtered(lambda m: m.product_id == producto and m.state not in ('done', 'cancel'))
            # Todos los terminados del baño, no solo el del acabado: los subproductos (el desperdicio de Rama de la
            # lista en kg) también quedan ligados al consumo de la orden en metros, y si apuntan a las otras partes la
            # orden en metros cree que otro baño la abastece y al cerrar deja un pendiente (ronda 13 en main:
            # TL/OP-M/00035-003).
            for terminado in bano.move_finished_ids.filtered(lambda m: m.move_dest_ids & consumos_partes):
                otros = terminado.move_dest_ids - consumos_partes
                terminado.move_dest_ids = [Command.set((otros | consumo).ids)]
            parte.message_post(body=_(
                "Orden en metros del baño %(b)s: %(m)s %(u)s planeados para %(kg)s kg de tela. Al cerrar el baño se "
                "registra con sus metros reales.", b=bano.name, m=parte.product_qty, u=uom.name, kg=bano.product_qty))
        return partes

    def _tintoreria_avisar_sin_dividir(self, ordenes_m, motivo):
        """1.8.2: nota en los baños y en la orden en metros cuando no se divide la orden en metros (un baño = una orden
        en metros no se cumple): cada baño registra sus metros y la orden en metros deja pendiente para los que siguen."""
        mensaje = _('No se dividió la orden en metros %(m)s entre los baños %(b)s: %(r)s. Cada baño registra sus metros en '
                    'ella y deja pendiente para los que siguen.', m=', '.join(ordenes_m.mapped('name')),
                    b=', '.join(self.mapped('name')), r=motivo)
        for orden in self | ordenes_m:
            orden.message_post(body=mensaje)
        return self.env['mrp.production']

    def _tintoreria_orden_metros_propia(self):
        """Pares (baño, orden en metros) donde la orden en metros la abastece solo ese baño y es una parte de la división
        de 1.8.0 (su grupo tiene otras partes) y no ha empezado."""
        pares = []
        for orden in self.filtered(lambda o: o.state not in ('done', 'cancel') and o.tintoreria_programada):
            terminado = orden.move_finished_ids.filtered(lambda m: m.product_id == orden.product_id)
            orden_m = terminado.move_dest_ids.raw_material_production_id.filtered(
                lambda p: p.state in ('draft', 'confirmed'))
            if len(orden_m) != 1:
                continue
            consumo = orden_m.move_raw_ids.filtered(lambda m: m.product_id == orden.product_id)
            # Solo los terminados del acabado (no el desperdicio de Rama, subproducto en cero) dicen quién la abastece.
            abastecen = consumo.move_orig_ids.filtered(lambda m: m.product_id == orden.product_id and m.state != 'cancel')
            if abastecen.production_id != orden:
                continue
            if len(orden_m.production_group_id.production_ids) < 2 or orden_m.qty_produced \
                    or any(wo.state not in ('blocked', 'ready') for wo in orden_m.workorder_ids):
                continue
            pares.append((orden, orden_m))
        return pares

    def _tintoreria_ajustar_cantidad(self, plan):
        """1.9.0: la orden pasa de lo pedido a los kg de los rollos que se surtieron (394.70 en vez de 381). Los demás
        componentes y los terminados se escalan con la misma proporción; el crudo no se escala: sus kg son los de los
        rollos (``_tintoreria_asignar_rollos``). Los componentes dosificados los recalcula la dosificación al asignar el
        HTJ con los kg de crudo reales. Lo que se surte de otra ubicación (engomado) sí propaga la diferencia a su
        requisición, como cualquier cambio de cantidad de Odoo; los terminados no tocan inventario."""
        self.ensure_one()
        uom = self.product_uom_id
        nuevo, anterior = plan['kg_total'], self.product_qty
        if not anterior or not float_compare(nuevo, anterior, precision_rounding=uom.rounding):
            return
        factor = nuevo / anterior
        crudos = self._tintoreria_movs_crudo()
        for move in self.move_raw_ids.filtered(
                lambda m: m.state not in ('done', 'cancel') and m not in crudos and not m.bom_line_id.dosificado):
            move.write({'product_uom_qty': move.product_uom.round(move.product_uom_qty * factor)})
        for move in self.move_finished_ids.filtered(lambda m: m.state not in ('done', 'cancel')):
            move.with_context(do_not_unreserve=True, no_procurement=True).write({
                'product_uom_qty': move.product_uom.round(move.product_uom_qty * factor)})
        self.with_context(skip_compute_move_raw_ids=True).product_qty = nuevo
        self.message_post(body=_(
            "Cantidad ajustada a los rollos de crudo surtidos: %(a)s → %(n)s %(u)s.", a=anterior, n=nuevo, u=uom.name))

    def _tintoreria_asignar_rollos(self, plan):
        """1.9.0 (Dirección, 2026-11-11): ``self`` son las órdenes de los baños, en el orden del plan. A cada una le deja
        exactamente los rollos completos de su baño: su movimiento de crudo pide la suma de sus kg y trae solo sus lotes
        (la división nativa reparte las líneas reservadas por cantidad y partía un rollo entre dos baños)."""
        crudos = self.env['stock.move']
        for orden in self:
            crudos |= orden._tintoreria_movs_crudo()
        crudos.move_line_ids.unlink()
        for orden, bano in zip(self, plan['banos']):
            for move in orden._tintoreria_movs_crudo():
                rollos = [r for r in bano['rollos'] if r['move'].product_id == move.product_id]
                kg = sum(r['kg'] for r in rollos)
                move.with_context(do_not_unreserve=True, no_procurement=True).write({
                    'product_uom_qty': move.product_uom._compute_quantity(kg, move.product_uom)})
                for rollo in rollos:
                    self.env['stock.move.line'].create({
                        'move_id': move.id, 'product_id': move.product_id.id, 'lot_id': rollo['lote'].id,
                        'quantity': move.product_uom._compute_quantity(rollo['kg'], move.product_uom),
                        'product_uom_id': move.product_uom.id, 'location_id': rollo['ubicacion'].id,
                        'location_dest_id': move.location_dest_id.id})
                move._recompute_state()

    def _programar_tintoreria(self, workcenter):
        """Asigna el HTJ, reparte los rollos de crudo surtidos en baños de rollos completos (una orden por baño) y marca
        las órdenes como programadas. Devuelve el registro tintoreria.programacion. Sin componente de tejido, divide la
        MO en baños simétricos de kg, como antes (1.8.x)."""
        self.ensure_one()
        self._check_programable()
        plan = self._calcular_plan_tintoreria(workcenter)
        nombre_origen = self.name
        kg_pedido = self.product_qty
        kg_total = plan['kg_total']
        producto = self.product_id

        if plan['banos']:
            self._tintoreria_ajustar_cantidad(plan)
        ordenes = self
        if plan['n_banos'] > 1:
            # División nativa de Odoo 19: conserva los enlaces MTO (move_dest_ids) hacia la MO de B
            # y reparte los consumos de manera proporcional.
            ordenes = self._split_productions({self: plan['montos']})
            ordenes._tintoreria_dividir_orden_metros()  # 1.8.0: un baño = una orden en metros
        if plan['banos']:
            ordenes._tintoreria_asignar_rollos(plan)

        programacion = self.env['tintoreria.programacion'].create({
            'name': nombre_origen,
            'product_id': producto.id,
            'workcenter_id': workcenter.id,
            'kg_total': kg_total,
            'kg_pedido': kg_pedido,
            'lote_crudo': plan['lote_crudo'],
            'n_rollos': plan['n_rollos'],
            'rendimiento': plan['rendimiento'],
            'banda': plan['banda'],
            'capacidad_kg': plan['capacidad_kg'],
            'n_banos': plan['n_banos'],
            'kg_bano': plan['kg_bano'],
            'kg_crudo': plan['kg_crudo'],
            'kg_crudo_bano': plan['kg_crudo_bano'],
            'minutos_bano': plan['minutos'],
            'origen_tiempo': plan['origen_tiempo'],
        })
        for numero, bano in enumerate(plan['banos'], 1):
            self.env['tintoreria.programacion.bano'].create({
                'programacion_id': programacion.id, 'production_id': ordenes[numero - 1].id, 'secuencia': numero,
                'n_rollos': bano['n_rollos'], 'kg_crudo': bano['kg'], 'rollos': bano['texto']})
        for orden in ordenes:
            # Asignar el HTJ dispara el recálculo de la dosificación (quimibond_tintoreria_dosificacion).
            workorder = orden._workorder_tintoreria()
            workorder.workcenter_id = workcenter
            # Tintorería es tiempo por baño (cada orden es un baño). Se escribe después de asignar el
            # HTJ para que ese cambio no lo reemplace.
            workorder.duration_expected = plan['minutos']
            orden.write({
                'tintoreria_programada': True,
                'tintoreria_programacion_id': programacion.id,
            })
            orden.message_post(body=_(
                "Tintorería programada en %(wc)s: %(n)s baño(s) de %(kg)s kg de tela, %(crudo)s kg de crudo por baño "
                "(capacidad %(cap)s kg; orden de origen %(o)s).",
                wc=workcenter.display_name, n=plan['n_banos'], kg=orden.product_qty, crudo=plan['kg_crudo_bano'],
                cap=plan['capacidad_kg'], o=nombre_origen))
            if plan['banos']:
                bano = plan['banos'][list(ordenes).index(orden)]
                orden.message_post(body=_(
                    "Rollos de crudo de este baño: %(r)s (%(n)s rollos, %(kg)s kg), baño %(i)s de %(t)s.",
                    r=bano['texto'], n=bano['n_rollos'], kg=bano['kg'], i=list(ordenes).index(orden) + 1,
                    t=plan['n_banos']))
            if plan['aviso_crudo']:
                orden.message_post(body=plan['aviso_crudo'])
            if plan['aviso']:
                orden.message_post(body=plan['aviso'])
            if plan['aviso_tiempo']:
                orden.message_post(body=plan['aviso_tiempo'])
        return programacion

    # ------------------------------------------------------------------
    # Validaciones de arranque
    # ------------------------------------------------------------------
    def _check_tintoreria_programada(self):
        for orden in self:
            if orden.requiere_programacion_tintoreria and not orden.tintoreria_programada:
                raise UserError(_(
                    "La orden %s no tiene la Programación de Tintorería confirmada. Tintorería debe asignar "
                    "el HTJ antes de iniciar cualquier operación.", orden.name))

    def action_start(self):
        self._check_tintoreria_programada()
        return super().action_start()

    def button_mark_done(self):
        self._check_tintoreria_programada()
        return super().button_mark_done()

    def action_cancel(self):
        """1.6.2 (ronda 5 de pruebas en main, 2026-11-05): al cancelar órdenes de una programación, la programación lo
        anota; si ya no le queda ninguna orden viva, pasa a «Cancelada» (antes se quedaba «Programada»)."""
        programaciones = self.filtered(lambda o: o.state not in ('done', 'cancel')).tintoreria_programacion_id
        partes_m = self._tintoreria_orden_metros_propia()
        res = super().action_cancel()
        # 1.8.0 (revisión 2026-11-06): un baño cancelado se lleva su orden en metros (un baño = una orden en metros); si
        # no, quedaba «Confirmada» con sus metros planeados y nadie que la abastezca. La de los otros baños no cambia.
        for orden, orden_m in partes_m:
            if orden.state == 'cancel' and orden_m.state not in ('done', 'cancel'):
                orden_m.action_cancel()
                orden_m.message_post(body=_("Cancelada junto con su baño %s.", orden.name))
        for programacion in programaciones:
            canceladas = self.filtered(
                lambda o: o.tintoreria_programacion_id == programacion and o.state == 'cancel')
            if not canceladas:
                continue
            nombres = ', '.join(canceladas.mapped('name'))
            if all(o.state == 'cancel' for o in programacion.production_ids):
                programacion.state = 'cancelada'
                programacion.message_post(body=_(
                    "Cancelada: se cancelaron todas sus órdenes (%s).", nombres))
            else:
                programacion.message_post(body=_(
                    "Se canceló %s; las demás órdenes siguen programadas.", nombres))
        return res
