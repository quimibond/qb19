# -*- coding: utf-8 -*-
import logging
from collections import defaultdict

from markupsafe import Markup

from odoo import api, Command, fields, models, _
from odoo.exceptions import UserError
from odoo.tools import float_compare

_logger = logging.getLogger(__name__)

PARAM_CRUDO_MINIMO_PCT = 'quimibond_rollos_bitacora.crudo_minimo_pct'
PARAM_USUARIO_FALTANTES = 'quimibond_rollos_bitacora.usuario_faltantes'
# 3.7.0 (Dirección, 2026-11-06): el número de baño sale del nombre de la orden en kg («TL/OP-KG/J00001» → «J00001»;
# una parte de la división en baños, «J00001-001»). La «J» viene del prefijo de la secuencia del tipo de operación
# (hooks.configurar_secuencias_j). Antes era una secuencia aparte (3.5.0).
# 3.6.1: código de secuencia del tipo «Fabricación Tela en kg» (fichas, ruta_familia.TIPO_OPERACION['kg']).
TIPO_FLUJO_KG = 'OP-KG'
# 3.8.0: id de la ubicación «Químicos-Colorantes Piso» (de donde la orden en kg consume engomado, químicos y colorantes).
PARAM_UBICACION_PISO = 'quimibond_rollos_bitacora.ubicacion_piso'


class MrpProduction(models.Model):
    _inherit = 'mrp.production'

    es_desarrollo = fields.Boolean(
        'Orden de Desarrollo', compute='_compute_es_desarrollo', store=True, readonly=False, copy=True,
        tracking=True, recursive=True,
        help='Marca la orden en metros como de Desarrollo: sus rollos terminados van al Almacén 31 en vez '
             'de los almacenes 14/15/16/17. La orden en kg que la abastece (MTO) la hereda.')
    rollo_bitacora_ids = fields.One2many('mrp.rollo.bitacora', 'production_id', 'Rollos')
    es_clasificacion_final = fields.Boolean(
        'Orden de Clasificación final', compute='_compute_es_clasificacion_final',
        help='La orden tiene operación de Clasificación final (centro Empaque): su terminado son metros.')

    @api.depends('workorder_ids.es_clasificacion')
    def _compute_es_clasificacion_final(self):
        for orden in self:
            orden.es_clasificacion_final = any(orden.workorder_ids.mapped('es_clasificacion'))
    desperdicio_kg = fields.Float('Desperdicio Tejido pesado (kg)', digits=(12, 3), readonly=True, copy=False)
    desperdicio_operador = fields.Char('Operador del desperdicio', readonly=True, copy=False)
    desperdicio_fecha = fields.Datetime('Desperdicio pesado el', readonly=True, copy=False)
    desperdicio_manual = fields.Boolean('Desperdicio capturado manualmente', readonly=True, copy=False)
    desperdicio_motivo = fields.Char('Motivo de la captura manual del desperdicio', readonly=True, copy=False)
    desperdicio_lote_id = fields.Many2one('stock.lot', 'Lote del desperdicio de Rama', readonly=True, copy=False)
    numero_bano = fields.Char(
        'No. de baño', readonly=True, copy=False, index=True, tracking=True,
        help='Nombre de la orden en kg sin el prefijo del tipo de operación («J00001», o «J00001-001» si la programación '
             'de tintorería la dividió en baños). Se fija al empezar su primera operación o al abrir el pesaje de Rama o '
             'Inspección. Da nombre a los lotes de Rama y de Inspección: «<baño>-0001».')
    _unique_numero_bano = models.Constraint('UNIQUE(numero_bano)', 'Ese número de baño ya lo tiene otra orden.')
    es_pesaje_rollos = fields.Boolean(
        'Pesa por rollo en Rama', compute='_compute_es_pesaje_rollos',
        help='La orden tiene operación de Rama: el lote del terminado es el de cada rollo, no un lote '
             'único de la orden (se oculta el campo Número de serie/lote).')

    @api.depends('workorder_ids.workcenter_id', 'workorder_ids.es_rama', 'workorder_ids.es_clasificacion')
    def _compute_es_pesaje_rollos(self):
        for orden in self:
            orden.es_pesaje_rollos = any(orden.workorder_ids.mapped('es_rama')) \
                or any(orden.workorder_ids.mapped('es_clasificacion'))

    @api.depends('move_dest_ids.raw_material_production_id.es_desarrollo')
    def _compute_es_desarrollo(self):
        for orden in self:
            destino = orden.move_dest_ids.raw_material_production_id
            # Con orden destino manda la de metros; sin ella, el valor capturado se queda.
            orden.es_desarrollo = any(destino.mapped('es_desarrollo')) if destino else orden.es_desarrollo

    def _asignar_numero_bano(self):
        """Da el siguiente número de baño a las órdenes que aún no lo tienen (3.5.0). Se llama cuando el baño ya es
        definitivo (la programación de tintorería divide la orden después de confirmarla)."""
        for orden in self.filtered(lambda o: not o.numero_bano and o.state not in ('done', 'cancel')):
            orden.numero_bano = orden._numero_bano_del_nombre()
        return self

    # ------------------------------------------------------------------
    # 3.8.0 — Ubicación «Químicos-Colorantes Piso»
    # ------------------------------------------------------------------
    def _ubicacion_piso(self):
        """La ubicación «Químicos-Colorantes Piso» (parámetro ``ubicacion_piso``, que pone la migración 3.8.0), o vacío."""
        valor = self.env['ir.config_parameter'].sudo().get_param(PARAM_UBICACION_PISO)
        piso = self.env['stock.location'].browse(int(valor)).exists() if valor and valor.isdigit() else \
            self.env['stock.location']
        return piso if piso.active else self.env['stock.location']

    def _surte_desde_piso(self, product, piso):
        """3.8.0: el producto va por el Piso si tiene (por producto o por categoría) una ruta con una regla que sale del
        Piso, que es como la migración deja «Surtido Engomado» y «Surtido Químicos y Colorantes». El engomado lleva la
        ruta en el producto y los químicos y colorantes en su categoría, así que no se decide por categoría."""
        rutas = self.env['stock.rule'].sudo().search([('location_src_id', '=', piso.id)]).route_id
        return bool(rutas and (product.route_ids | product.categ_id.total_route_ids) & rutas)

    def _get_move_raw_values(self, product, product_uom_qty, product_uom, operation_id=False, bom_line=False):
        datos = super()._get_move_raw_values(product, product_uom_qty, product_uom, operation_id, bom_line)
        if self.picking_type_id.sequence_code == TIPO_FLUJO_KG:
            piso = self._ubicacion_piso()
            if piso and self._surte_desde_piso(product, piso):
                datos['location_id'] = piso.id
        return datos

    # ------------------------------------------------------------------
    # 3.9.0 — El sobrante del Piso regresa a su ubicación de origen
    # ------------------------------------------------------------------
    def _sobrante_piso(self, move):
        """Lo que se surtió al Piso para este componente y la orden no consumió, por lote y ubicación de origen.

        Surtido = lo hecho en los movimientos que alimentan el movimiento de consumo (la requisición); consumido = lo
        que la orden descontó, por lote. Regresa una lista de ``(lote, origen, cantidad)`` en la unidad del movimiento y
        sin pasar de lo que hoy hay en el Piso (la devolución no deja el Piso en negativo)."""
        surtido = defaultdict(float)
        for linea in move.move_orig_ids.filtered(lambda m: m.state == 'done').move_line_ids:
            cantidad = linea.product_uom_id._compute_quantity(linea.quantity, move.product_uom, round=False)
            surtido[(linea.lot_id, linea.location_id)] += cantidad
        consumido = defaultdict(float)
        for linea in move.move_line_ids:
            consumido[linea.lot_id] += linea.product_uom_id._compute_quantity(
                linea.quantity, move.product_uom, round=False)
        piso = move.location_id
        resultado = []
        for lote in {lote for lote, _origen in surtido}:
            sobra = sum(c for (l, _o), c in surtido.items() if l == lote) - consumido.get(lote, 0.0)
            disponible = move.product_id.uom_id._compute_quantity(
                self.env['stock.quant']._get_available_quantity(move.product_id, piso, lot_id=lote or None, strict=True),
                move.product_uom, round=False)
            sobra = min(sobra, disponible)
            for (l, origen), cantidad in surtido.items():
                if l != lote or move.product_uom.compare(sobra, 0) <= 0:
                    continue
                parte = min(sobra, cantidad)
                resultado.append((lote, origen, parte))
                sobra -= parte
        return resultado

    def _devolver_sobrante_piso(self):
        """3.9.0 (Dirección, 2026-11-07): al cerrar la orden en kg, lo que se surtió al Piso y no se consumió regresa a
        la ubicación de donde salió, con una transferencia interna ligada a la orden (lote por lote). El scrap sigue
        siendo el botón nativo de Odoo para lo que ya no se puede reutilizar. Si algo falla no se deshace el cierre: se
        deja la nota para hacer la devolución a mano."""
        piso = self._ubicacion_piso()
        if not piso:
            return
        for orden in self.filtered(lambda o: o.picking_type_id.sequence_code == TIPO_FLUJO_KG):
            try:
                with self.env.cr.savepoint():
                    orden._devolver_sobrante_piso_orden(piso)
            except Exception:  # no se detiene el cierre por una devolución: queda la nota para Almacén
                _logger.exception('rollos 3.9.0: no se pudo devolver el sobrante del Piso de %s', orden.name)
                orden.message_post(body=_(
                    'No se pudo devolver el sobrante de químicos y colorantes al almacén. Hacer la transferencia '
                    'interna a mano desde %s.', piso.display_name))

    def _devolver_sobrante_piso_orden(self, piso):
        self.ensure_one()
        por_origen = defaultdict(list)
        for move in self.move_raw_ids.filtered(lambda m: m.state == 'done' and m.location_id == piso):
            for lote, origen, cantidad in self._sobrante_piso(move):
                por_origen[origen].append((move, lote, cantidad))
        if not por_origen:
            return self.env['stock.picking']
        tipo = self.picking_type_id.warehouse_id.int_type_id
        if not tipo:
            raise UserError(_('El almacén de %s no tiene tipo de operación de transferencia interna.', self.name))
        transferencias = self.env['stock.picking']
        lineas_nota = []
        for origen, partidas in por_origen.items():
            transferencia = self.env['stock.picking'].create({
                'picking_type_id': tipo.id, 'location_id': piso.id, 'location_dest_id': origen.id,
                'origin': _('Sobrante de %s', self.name), 'company_id': self.company_id.id,
                'move_ids': [Command.create({
                    'product_id': move.product_id.id, 'product_uom': move.product_uom.id,
                    'product_uom_qty': cantidad, 'location_id': piso.id, 'location_dest_id': origen.id,
                    'company_id': self.company_id.id, 'picked': True,
                    'move_line_ids': [Command.create({
                        'product_id': move.product_id.id, 'product_uom_id': move.product_uom.id,
                        'quantity': cantidad, 'lot_id': lote.id, 'location_id': piso.id,
                        'location_dest_id': origen.id, 'company_id': self.company_id.id})],
                }) for move, lote, cantidad in partidas]})
            transferencia.action_confirm()
            transferencia.with_context(skip_backorder=True, skip_sanity_check=True, skip_immediate=True).button_validate()
            transferencias |= transferencia
            for move, lote, cantidad in partidas:
                lineas_nota.append(_('%(p)s: %(c)s %(u)s a %(o)s', p=move.product_id.display_name, c=round(cantidad, 4),
                                     u=move.product_uom.name, o=origen.display_name))
        self.message_post(body=Markup('<p>%s</p><ul>%s</ul>') % (
            _('Sobrante del Piso devuelto a su ubicación de origen (%s):', ', '.join(transferencias.mapped('name'))),
            Markup('').join(Markup('<li>%s</li>') % linea for linea in lineas_nota)))
        return transferencias

    def _numero_bano_del_nombre(self):
        """3.7.0: el último tramo del nombre de la orden («TL/OP-KG/J00001» → «J00001»). Si otra orden ya lo tiene (una
        orden vieja numerada con la secuencia de 3.5.0), se usa el nombre completo con guiones."""
        self.ensure_one()
        nombre = self.name or ''
        numero = nombre.rsplit('/', 1)[-1]
        if self.search_count([('numero_bano', '=', numero), ('id', '!=', self.id)]):
            numero = nombre.replace('/', '-')
        return numero

    def action_asignar_numero_bano(self):
        self._asignar_numero_bano()
        return True

    # ------------------------------------------------------------------ cancelar (3.5.2)
    def action_cancel(self):
        """3.5.2 (ronda 5 de pruebas en main, 2026-11-05): una orden en kg con rollos o desperdicio pesados en Rama no se
        cancela: la tela ya existe y cancelar la saca del inventario, deja la bitácora pendiente y regresar a medio baño
        descuadra el inventario (plan de regreso). Se termina el baño; lo que no sirva sale como Segunda o Desperdicio.
        Solo un administrador de Fabricación la cancela (rollos pesados en la orden equivocada), y queda la nota. Al
        cancelarse un baño que abastecía una orden en metros, esa orden recibe la nota de que se quedó sin abasto."""
        abiertas = self.filtered(lambda o: o.state not in ('done', 'cancel'))
        con_rama = abiertas.filtered(lambda o: o.rollo_bitacora_ids or o.desperdicio_kg)
        if con_rama and not self.env.user.has_group('mrp.group_mrp_manager'):
            orden = con_rama[:1]
            raise UserError(_(
                'La orden %(o)s ya tiene %(n)s rollos pesados en Rama (%(kg).2f kg) y no se cancela: la tela ya existe. '
                'Termine el baño (Inspección y Marcar como hecho); lo que no sirva sale como Segunda o Desperdicio. Si '
                'los rollos se pesaron en la orden equivocada, la cancela un administrador de Fabricación.',
                o=orden.name, n=len(orden.rollo_bitacora_ids), kg=sum(orden.rollo_bitacora_ids.mapped('kg_rama'))))
        abasto = {orden: (orden._insp_orden_metros(), orden.product_qty) for orden in abiertas}
        res = super().action_cancel()
        for orden in con_rama.filtered(lambda o: o.state == 'cancel'):
            rollos = orden.rollo_bitacora_ids
            orden.message_post(body=_(
                'Cancelada por %(u)s con %(n)s rollos pesados en Rama (%(kg).2f kg; lotes %(l)s) y %(d).2f kg de '
                'desperdicio: esos lotes no entraron al inventario.',
                u=self.env.user.name, n=len(rollos), kg=sum(rollos.mapped('kg_rama')),
                l=', '.join(rollos.lot_id.mapped('name')) or '-', d=orden.desperdicio_kg))
        for orden, (orden_m, kg) in abasto.items():
            if orden.state == 'cancel' and orden_m and orden_m.state not in ('done', 'cancel'):
                orden_m.message_post(body=_(
                    'El baño %(b)s se canceló: esta orden se quedó sin %(kg).2f kg de abasto. Cree otra orden en kg o '
                    'vuelva a correr el reabastecimiento.', b=orden.name, kg=kg))
        return res

    # ------------------------------------------------------------------ Rama
    # El botón «Pesar rollo» vive en la lista de operaciones (mrp.workorder), no en la orden.

    def _rama_unlink_lines_without_lot(self, finished_move):
        """El core crea al confirmar una línea del terminado sin lote con toda la cantidad planeada;
        con pesaje por rollo sobra (mismo criterio que pesaje_rollos_tejido 1.4.3)."""
        lines = finished_move.move_line_ids.filtered(
            lambda ml: not ml.lot_id and not ml.lot_name and ml.state not in ('done', 'cancel'))
        lines.unlink()

    def _rama_registrar_movimiento(self, lote, kg_neto, workorder):
        return self._registrar_rollo_terminado(lote, kg_neto, workorder)

    def _registrar_rollo_terminado(self, lote, cantidad, workorder):
        """Registra el rollo en el terminado de la MO: línea con lote y cantidad, SIN picked
        (Rama en kg y Clasificación final en metros).

        Porta el patrón de pesaje_rollos_tejido.action_register_roll_with_weight, que ya está probado
        contra el core de Odoo 19 (demanda en 0 con do_not_unreserve; línea no picked, porque el core
        cuenta un terminado picked como ya producido y _post_inventory borraría los rollos).
        """
        self.ensure_one()
        finished_move = self.move_finished_ids.filtered(
            lambda m: m.product_id == self.product_id and m.state not in ('done', 'cancel'))[:1]
        if not finished_move:
            raise UserError(_('La orden %s no tiene movimiento de producto terminado pendiente.', self.name))
        if finished_move.product_uom_qty != 0:
            finished_move.with_context(do_not_unreserve=True).write({'product_uom_qty': 0})
        self._rama_unlink_lines_without_lot(finished_move)
        self.env['stock.move.line'].create({
            'move_id': finished_move.id,
            'product_id': self.product_id.id,
            'lot_id': lote.id,
            'lot_name': lote.name,
            'quantity': cantidad,
            'location_id': finished_move.location_id.id,
            'location_dest_id': finished_move.location_dest_id.id,
            'workorder_id': workorder.id,
            'production_id': self.id,
        })
        # qty_producing arranca en la cantidad planeada, así que no se acumula: se iguala al total de
        # kg realmente pesados (si no, la MO llega al cierre con planeado + pesado y las operaciones
        # posteriores heredan esa cantidad).
        lineas_rollo = finished_move.move_line_ids.filtered(lambda ml: ml.lot_id or ml.lot_name)
        self.qty_producing = self.product_id.uom_id.round(sum(lineas_rollo.mapped('quantity')))
        workorder.qty_produced += cantidad

    # ------------------------------------------------------ Clasificación final (MO de B)
    def _clasif_move_a(self):
        """Movimiento de consumo del Producto A (kg) en la MO de B: el componente con el código de B
        más la «I» al frente; si no, el primer componente con seguimiento por lote."""
        self.ensure_one()
        pendientes = self.move_raw_ids.filtered(lambda m: m.state not in ('done', 'cancel'))
        codigo_a = 'I' + (self.product_id.default_code or '')
        return pendientes.filtered(lambda m: m.product_id.default_code == codigo_a)[:1] \
            or pendientes.filtered(lambda m: m.product_id.tracking == 'lot')[:1]

    def _clasif_ubicacion_rollo(self, rollo, raw):
        """Ubicación interna donde está el lote de A (Ubic. 10 al cerrar la MO de A). Se prefiere la de
        origen del consumo; si el movimiento nació con otro origen (p. ej. 2 PRODUCCIÓN), se usa la real."""
        quants = self.env['stock.quant'].sudo().search([
            ('product_id', '=', raw.product_id.id), ('lot_id', '=', rollo.lot_id.id),
            ('location_id.usage', '=', 'internal'), ('quantity', '>', 0)])
        en_origen = quants.filtered(lambda q: q.location_id.parent_path.startswith(raw.location_id.parent_path))
        return (en_origen or quants)[:1].location_id

    def _clasif_asegurar_consumo(self, rollos):
        """Línea de consumo con el lote de A y su kg real (neto de Rama), marcada como picked. Se deja
        explícito (y se repite al cerrar) porque escribir qty_producing hace que el core reescale líneas."""
        self.ensure_one()
        raw = self._clasif_move_a()
        if not raw:
            return
        for rollo in rollos:
            linea = raw.move_line_ids.filtered(lambda ml: ml.lot_id == rollo.lot_id)[:1]
            valores = {'quantity': rollo.kg_rama, 'picked': True}
            if linea:
                linea.write(valores)
            else:
                self.env['stock.move.line'].create(dict(
                    valores,
                    move_id=raw.id,
                    product_id=raw.product_id.id,
                    lot_id=rollo.lot_id.id,
                    location_id=(self._clasif_ubicacion_rollo(rollo, raw) or raw.location_id).id,
                    location_dest_id=raw.location_dest_id.id,
                ))

    # ---------------------------------------------------------- Inspección y desperdicio
    def _es_bano_flujo_kg(self):
        """3.6.1: baño del flujo nuevo: orden del tipo «Fabricación Tela en kg» (código de secuencia OP-KG, el que
        pone «Generar listas» de fichas) con operación de Rama. Las órdenes del flujo viejo en metros y las de otros tipos
        con Rama no entran."""
        self.ensure_one()
        return self.picking_type_id.sequence_code == TIPO_FLUJO_KG and any(self.workorder_ids.mapped('es_rama'))

    def _validar_rollos_pesados(self):
        """3.6.1 (prueba de Dirección en main, 2026-11-06): un baño del flujo nuevo sin rollos pesados en Rama no
        termina Rama ni cierra. Las revisiones de rollos e Inspección solo corrían con rollos: con las 5 operaciones en
        «Hecho» y sin pesar nada, la orden cerraba con los kg planeados, sin pesaje ni clasificación. Si el baño no llegó
        a Rama, la orden se cancela (sin rollos la cancela cualquiera, 3.5.2)."""
        for orden in self.filtered(lambda o: o.state not in ('done', 'cancel') and not o.rollo_bitacora_ids):
            if orden._es_bano_flujo_kg():
                raise UserError(_(
                    'La orden %s no tiene rollos pesados en Rama. Pese cada rollo con «PESAR ROLLO» antes de terminar '
                    'Rama o cerrar el baño; si el baño no llegó a Rama, cancele la orden.', orden.name))

    def _validar_rollos_inspeccionados(self):
        """Inspección no libera parcialmente: todos los rollos de Rama deben estar inspeccionados."""
        for orden in self:
            pendientes = orden.rollo_bitacora_ids.filtered(lambda r: r.estado_inspeccion != 'inspeccionado')
            if pendientes:
                raise UserError(_(
                    'La orden %(orden)s tiene %(n)s rollo(s) sin inspeccionar: %(rollos)s.',
                    orden=orden.name, n=len(pendientes),
                    rollos=', '.join(pendientes.sorted('secuencia').mapped('name'))))

    def _moves_crudo(self):
        """3.3.2: consumo de la tela cruda de la orden en kg: el componente en peso con más kg planeados (el crudo va a
        1.1 kg por kg; químicos y engomado son gramos o pocos kg por baño). 3.5.1: entre los componentes de una categoría
        de tejido, si los hay: en IWJ032 el LAVADO001 (detergente en la abridora, «Aux. acabado») va a 2.2 kg por kg y
        se tomaba como crudo, así que la orden no cerraba (Ingeniería, 2026-11-05). 3.14.1: todos los de tejido, no solo
        el de más kg."""
        self.ensure_one()
        kg = self.env.ref('uom.product_uom_kgm')
        pesos = self.move_raw_ids.filtered(
            lambda m: m.state != 'cancel' and m.product_uom._has_common_reference(kg))
        if not pesos:
            return pesos
        tela = pesos.filtered(lambda m: 'TEJIDO' in (m.product_id.categ_id.complete_name or '').upper())
        if tela:
            # 3.14.1 (revisión de código, 2026-11-06): todos los componentes de tejido son crudo. Con dos crudos en el baño
            # contaba solo el de más kg contra todo lo pesado en Rama (cierre bloqueado sin razón) y el otro se trataba
            # como químico (sus reservas sin lote se borraban).
            return tela
        planeado = {}
        for move in pesos:
            planeado[move.product_id] = planeado.get(move.product_id, 0.0) + move.product_uom._compute_quantity(
                move.product_uom_qty, kg, round=False)
        crudo = max(planeado, key=planeado.get)
        return pesos.filtered(lambda m: m.product_id == crudo)

    def _validar_consumo_crudo(self):
        """3.3.2 (Dirección, 2026-11-05): la orden en kg no cierra si consume menos crudo que los kg que pesó Rama: la
        tela teñida no puede pesar más que el crudo que entró. Pasaba con el consumo flexible cuando no había crudo en
        existencia (en la prueba de main cerraron baños con 19 y 0 kg de crudo y el kg salió a 15 y 1.92 $/kg).
        Con el parámetro ``crudo_minimo_pct`` capturado, además pide ese porcentaje del crudo planeado."""
        kg = self.env.ref('uom.product_uom_kgm')
        minimo = (self.env['ir.config_parameter'].sudo().get_param(PARAM_CRUDO_MINIMO_PCT) or '').strip()
        try:
            minimo = float(minimo) if minimo else 0.0
        except ValueError:
            minimo = 0.0
        for orden in self.filtered('rollo_bitacora_ids'):
            moves = orden._moves_crudo()
            if not moves:
                continue
            consumido = sum(m.product_uom._compute_quantity(m.quantity, kg, round=False)
                            for m in moves if m.state == 'done' or m.picked)
            planeado = sum(m.product_uom._compute_quantity(m.product_uom_qty, kg, round=False) for m in moves)
            pesado = sum(orden.rollo_bitacora_ids.mapped('kg_rama'))
            producto = moves[:1].product_id.display_name
            if float_compare(consumido, pesado, precision_digits=2) < 0:
                raise UserError(_(
                    'La orden %(o)s consume %(c).2f kg de crudo (%(p)s) y Rama pesó %(r).2f kg: la tela teñida no puede '
                    'pesar más que el crudo. Captura el consumo real del crudo (con su lote) antes de cerrar; si no hay '
                    'existencia, primero la requisición.', o=orden.name, c=consumido, p=producto, r=pesado))
            if minimo and planeado and consumido / planeado * 100.0 < minimo:
                raise UserError(_(
                    'La orden %(o)s consume %(c).2f kg de crudo, el %(pct).1f %% de los %(pl).2f kg planeados; el mínimo es '
                    '%(m)s %%. Captura el consumo real del crudo antes de cerrar.', o=orden.name, c=consumido,
                    pct=consumido / planeado * 100.0, pl=planeado, m=minimo))

    def _avisar_componentes_faltantes(self):
        """3.3.4 (Dirección, 2026-11-05): químicos, colorantes, engomado y agua se descuentan con la cantidad teórica y su
        existencia en Odoo es una estimación (el agua no se mide), así que un faltante no detiene el cierre: la orden en kg
        cierra con lo que hay y deja una nota en la orden y una actividad para Almacén (requisición o ajuste); el conteo
        periódico corrige la diferencia. Solo el crudo bloquea (``_validar_consumo_crudo``)."""
        usuario_param = (self.env['ir.config_parameter'].sudo().get_param(PARAM_USUARIO_FALTANTES) or '').strip()
        usuario_avisos = self.env['res.users'].search([('login', '=', usuario_param)], limit=1) if usuario_param else False
        for orden in self.filtered('rollo_bitacora_ids'):
            crudo = orden._moves_crudo()
            faltan = []
            for move in orden.move_raw_ids - crudo:
                if move.state in ('done', 'cancel') or not move.product_id.is_storable:
                    continue
                # 3.3.6: una reserva sin lote de un componente con lote (en 2 PRODUCCIÓN quedan registros sin lote y sin
                # existencia) detenía el cierre con «Debe proporcionar un número de lote» (prueba en main, 2026-11-05).
                # Esa parte no existe: se quita y queda como faltante.
                if move.product_id.tracking != 'none':
                    sin_lote = move.move_line_ids.filtered(
                        lambda ml: not ml.lot_id and not ml.lot_name and ml.state not in ('done', 'cancel'))
                    if sin_lote:
                        sin_lote.unlink()
                # Lo que sí está apartado se consume: sin «picked», Odoo cancelaría el consumo parcial al cerrar.
                if not move.picked and move.product_uom.compare(move.quantity, 0) > 0:
                    move.picked = True
                consumido = move.quantity if move.picked else 0.0
                if move.product_uom.compare(consumido, move.product_uom_qty) < 0:
                    faltan.append(_('%(p)s: faltaron %(f)s %(u)s (se consumió %(c)s de %(d)s)',
                                    p=move.product_id.display_name, u=move.product_uom.name,
                                    f=round(move.product_uom_qty - consumido, 4), c=round(consumido, 4),
                                    d=round(move.product_uom_qty, 4)))
            if not faltan:
                continue
            titulo = _('Faltante de componentes al cerrar %s', orden.name)
            nota = Markup('<p>%s</p><ul>%s</ul><p>%s</p>') % (
                _('La orden cerró con lo que había en existencia en %s; el costo de esta orden queda corto en estos '
                  'componentes:', orden.location_src_id.display_name),
                Markup('').join(Markup('<li>%s</li>') % linea for linea in faltan),
                _('Hacer la requisición o el ajuste de inventario que corresponda.'))
            orden.message_post(body=nota, subject=titulo)
            usuario = usuario_avisos or orden.user_id or self.env.user
            orden.activity_schedule('mail.mail_activity_data_todo', summary=titulo, note=nota, user_id=usuario.id)

    def _post_inventory(self, cancel_backorder=False):
        self._validar_consumo_crudo()
        self._avisar_componentes_faltantes()
        return super()._post_inventory(cancel_backorder=cancel_backorder)

    def _lote_desperdicio(self, producto):
        """Lote del subproducto de desperdicio (3.17.0, Dirección 2026-11-10): el número de la orden y el mes, como
        «J00002-11-2026» (antes solo «11-2026»). Un lote por orden y mes; se crea si todavía no existe."""
        mes = fields.Datetime.context_timestamp(self, fields.Datetime.now()).strftime('%m-%Y')
        orden = self.numero_bano or (self.name or '').rsplit('/', 1)[-1]
        nombre = '%s-%s' % (orden, mes) if orden else mes
        Lote = self.env['stock.lot']
        dominio = [('name', '=', nombre), ('product_id', '=', producto.id), ('company_id', '=', self.company_id.id)]
        return Lote.search(dominio, limit=1) or Lote.create({
            'name': nombre, 'product_id': producto.id, 'company_id': self.company_id.id})

    def _historial_pesaje(self, workcenter, roll_number, bruto, tara, neto, pesador, lot=False, fecha=False):
        """3.3.8 (Dirección, 2026-11-05): copia de consulta de cada pesada de Rama e Inspección en el «Historial de
        Pesaje» de la orden (``mrp.weighing.log`` de ``pesaje_rollos_tejido``), para ver en un solo lugar lo pesado
        en Tejido, Rama e Inspección. La fuente sigue siendo la pestaña Rollos (bitácora y rollos finales); el
        historial no mueve inventario ni el contador de rollos de Tejido (``roll_count``), así que no cambia el
        cierre. Sin ese módulo instalado no hace nada."""
        self.ensure_one()
        if 'mrp.weighing.log' not in self.env:
            return False
        return self.env['mrp.weighing.log'].sudo().create({
            'production_id': self.id,
            'date_time': fecha or fields.Datetime.now(),
            'roll_number': roll_number,
            'lot_id': lot.id if lot else False,
            'pesador': pesador or False,
            'weight_bruto': bruto,
            'tara': tara,
            'weight_neto': neto,
            'workcenter_id': workcenter.id if workcenter else False,
        })

    def _historial_pesaje_borrar(self, roll_numbers):
        """3.3.8: quita del historial las pesadas de capturas anuladas (los usuarios de Manufactura no borran
        registros del historial, por eso con sudo)."""
        if 'mrp.weighing.log' not in self.env or not self or not roll_numbers:
            return
        self.env['mrp.weighing.log'].sudo().search([
            ('production_id', 'in', self.ids), ('roll_number', 'in', list(roll_numbers))]).unlink()

    def _rama_registrar_desperdicio(self, workorder, kg, operador, manual):
        """Registra el peso real del subproducto Desperdicio Tejido (orilla) en su movimiento de la MO.

        Un solo pesaje por orden. El movimiento del subproducto nace con demanda 0 (la cantidad no es
        estándar, §16): se iguala demanda y cantidad al peso y se marca como picked para que el cierre
        de la MO lo haga y no lo cancele.
        """
        self.ensure_one()
        if self.desperdicio_kg:
            raise UserError(_('La orden %s ya tiene el desperdicio pesado (%.3f kg).', self.name, self.desperdicio_kg))
        candidatos = self.move_finished_ids.filtered(
            lambda m: m.product_id != self.product_id and m.state not in ('done', 'cancel'))
        movimiento = candidatos.filtered(
            lambda m: m.byproduct_id.operation_id and m.byproduct_id.operation_id == workorder.operation_id)[:1] \
            or candidatos[:1]
        if not movimiento:
            raise UserError(_(
                'La orden %s no tiene subproducto de desperdicio. Agrégalo en la lista de materiales del '
                'artículo (pestaña Subproductos, operación Rama).', self.name))
        producto = movimiento.product_id
        if producto.tracking == 'serial':
            raise UserError(_('El subproducto %s no puede tener seguimiento por número de serie.',
                              producto.display_name))
        lote = self._lote_desperdicio(producto) if producto.tracking == 'lot' else self.env['stock.lot']
        # El subproducto hereda el destino del terminado (Ubic. 10); el desperdicio va al Almacén 17 (§16).
        destino = self.env['stock.location'].search(
            [('complete_name', 'ilike', '17 PRODUCTO TERMINADO DESPERDICIO'), ('usage', '=', 'internal')], limit=1)
        movimiento.with_context(do_not_unreserve=True).write(dict(
            {'product_uom_qty': kg}, **({'location_dest_id': destino.id} if destino else {})))
        lineas = movimiento.move_line_ids
        valores = {'quantity': kg, 'lot_id': lote.id or False}
        if destino:
            valores['location_dest_id'] = destino.id
        if lineas:
            lineas[:1].write(valores)
            (lineas - lineas[:1]).unlink()
        else:
            self.env['stock.move.line'].create(dict(
                valores,
                move_id=movimiento.id,
                product_id=producto.id,
                location_id=movimiento.location_id.id,
                location_dest_id=movimiento.location_dest_id.id,
                production_id=self.id,
            ))
        movimiento.picked = True
        self.write({
            'desperdicio_kg': kg,
            'desperdicio_operador': operador,
            'desperdicio_fecha': fields.Datetime.now(),
            'desperdicio_manual': manual,
            'desperdicio_lote_id': lote.id or False,
        })

    # ------------------------------------------------------------ planeación
    def _ordenes_previas(self):
        """MOs que abastecen a esta por la cadena MTO (la MO en kg que produce el Producto A de la MO en m)."""
        self.ensure_one()
        # 3.6.4: solo los movimientos que de verdad abastecen el consumo (mismo producto, no cancelados). Los
        # subproductos del baño (desperdicio de Rama, cancelado en cero) también quedan ligados y hacían creer que otro
        # baño abastecía la orden en metros (ronda 13 en main: pendiente TL/OP-M/00035-003).
        origenes = self.move_raw_ids.mapped(lambda raw: raw.move_orig_ids.filtered(
            lambda m, raw=raw: m.product_id == raw.product_id and m.state != 'cancel'))
        return origenes.production_id - self

    def _plan_workorders(self, replan=False):
        """La MO en metros no puede empezar antes de que termine la MO en kg que la abastece.

        El core planea cada MO desde su propia fecha de inicio y no mira la cadena MTO entre MOs: la de
        metros quedaba a la misma hora que la de kg. Aquí se planea primero la MO previa y la fecha de inicio
        de esta se recorre al fin de la previa (el core arranca el primer paso desde esa fecha)."""
        resultado = None
        for orden in self:
            previas = orden._ordenes_previas().filtered(lambda p: p.state not in ('done', 'cancel'))
            for previa in previas.filtered(lambda p: p.workorder_ids and (replan or not p.is_planned)):
                previa._plan_workorders(replan)
            fin_previas = max(previas.filtered('date_finished').mapped('date_finished'), default=False)
            if fin_previas and (not orden.date_start or orden.date_start < fin_previas):
                orden.with_context(force_date=True).write({'date_start': fin_previas})
            resultado = super(MrpProduction, orden)._plan_workorders(replan)
        return resultado

    def button_mark_done(self):
        self._validar_rollos_pesados()
        self._validar_rollos_inspeccionados()
        """Cierre de la MO en kg con rollos de la bitácora.

        Igual que en pesaje_rollos_tejido: la demanda del terminado se dejó en 0 al pesar; antes del
        cierre se iguala la cantidad de la MO al kg realmente pesado para que el core no genere un
        rollo de ajuste ni cancele el movimiento. A diferencia de tejido, NO se tocan las demandas de
        los componentes: la tela cruda se consume completa y la diferencia contra los kg netos es merma.
        """
        for orden in self.filtered('rollo_bitacora_ids'):
            finished_move = orden.move_finished_ids.filtered(
                lambda m: m.product_id == orden.product_id and m.state not in ('done', 'cancel'))[:1]
            if not finished_move:
                continue
            orden._rama_unlink_lines_without_lot(finished_move)
            lineas_rollo = finished_move.move_line_ids.filtered(lambda ml: ml.lot_id or ml.lot_name)
            lineas_rollo.filtered('picked').write({'picked': False})
            total = orden.product_id.uom_id.round(sum(lineas_rollo.mapped('quantity')))
            if float_compare(total, 0, precision_rounding=orden.product_id.uom_id.rounding) > 0:
                orden.write({'product_qty': total, 'qty_producing': total})
                finished_move.with_context(do_not_unreserve=True).write({'product_uom_qty': total})
        res = super().button_mark_done()
        # 3.9.0: el sobrante del Piso regresa a su origen cuando la orden en kg ya cerró (el core puede devolver una
        # acción y dejarla abierta; en ese caso se hará al cerrarla).
        self.filtered(lambda o: o.state == 'done')._devolver_sobrante_piso()
        return res
