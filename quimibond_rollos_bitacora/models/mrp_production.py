# -*- coding: utf-8 -*-
from markupsafe import Markup

from odoo import api, fields, models, _
from odoo.exceptions import UserError
from odoo.tools import float_compare

PARAM_CRUDO_MINIMO_PCT = 'quimibond_rollos_bitacora.crudo_minimo_pct'
PARAM_USUARIO_FALTANTES = 'quimibond_rollos_bitacora.usuario_faltantes'


class MrpProduction(models.Model):
    _inherit = 'mrp.production'

    es_desarrollo = fields.Boolean(
        'Orden de Desarrollo', compute='_compute_es_desarrollo', store=True, readonly=False, copy=True,
        tracking=True, recursive=True,
        help='Marca la orden en metros como de Desarrollo: sus rollos terminados van al Almacén 31 en vez '
             'de los almacenes 14/15/16/17. La orden en kg que la abastece (MTO) la hereda.')
    rollo_bitacora_ids = fields.One2many('mrp.rollo.bitacora', 'production_id', 'Rollos')
    rollo_final_ids = fields.One2many('mrp.rollo.bitacora', 'production_b_id', 'Rollos clasificados (m)')
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

    def _clasif_validar_rollo_disponible(self, rollo):
        """El rollo de A debe estar físicamente en la ubicación de origen del consumo (Ubic. 10)."""
        self.ensure_one()
        raw = self._clasif_move_a()
        if not raw:
            raise UserError(_('La orden %s no tiene consumo pendiente del Producto A.', self.name))
        if rollo.product_id != raw.product_id:
            raise UserError(_('El rollo %(rollo)s es de %(a)s, no de %(b)s, que es el componente de esta orden.',
                              rollo=rollo.name, a=rollo.product_id.display_name, b=raw.product_id.display_name))
        if not self._clasif_ubicacion_rollo(rollo, raw):
            raise UserError(_(
                'El rollo %(rollo)s no tiene existencia en inventario. Verifica que la orden %(of)s ya esté '
                'cerrada y que el rollo no se haya clasificado antes.',
                rollo=rollo.name, of=rollo.production_id.name))

    def _clasif_ubicacion_rollo(self, rollo, raw):
        """Ubicación interna donde está el lote de A (Ubic. 10 al cerrar la MO de A). Se prefiere la de
        origen del consumo; si el movimiento nació con otro origen (p. ej. 2 PRODUCCIÓN), se usa la real."""
        quants = self.env['stock.quant'].sudo().search([
            ('product_id', '=', raw.product_id.id), ('lot_id', '=', rollo.lot_id.id),
            ('location_id.usage', '=', 'internal'), ('quantity', '>', 0)])
        en_origen = quants.filtered(lambda q: q.location_id.parent_path.startswith(raw.location_id.parent_path))
        return (en_origen or quants)[:1].location_id

    def _clasif_registrar_rollo(self, rollo, lote_b, workorder):
        """Registra el rollo clasificado: terminado B (lote B, metros finales) y consumo del kg real
        del lote de A. La demanda de la lista de materiales es teórica; la diferencia es variación."""
        self.ensure_one()
        self._registrar_rollo_terminado(lote_b, rollo.metros_final, workorder)
        self._clasif_asegurar_consumo(rollo)

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
        1.1 kg por kg; químicos y engomado son gramos o pocos kg por baño)."""
        self.ensure_one()
        kg = self.env.ref('uom.product_uom_kgm')
        pesos = self.move_raw_ids.filtered(
            lambda m: m.state != 'cancel' and m.product_uom._has_common_reference(kg))
        if not pesos:
            return pesos
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
        """Lote del Desperdicio Tejido: el que ya se usa en la planta, uno por mes («MM-AAAA», ej. 10-2026),
        que se crea si todavía no existe."""
        nombre = fields.Datetime.context_timestamp(self, fields.Datetime.now()).strftime('%m-%Y')
        Lote = self.env['stock.lot']
        dominio = [('name', '=', nombre), ('product_id', '=', producto.id), ('company_id', '=', self.company_id.id)]
        return Lote.search(dominio, limit=1) or Lote.create({
            'name': nombre, 'product_id': producto.id, 'company_id': self.company_id.id})

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
        })

    # ------------------------------------------------------------ planeación
    def _ordenes_previas(self):
        """MOs que abastecen a esta por la cadena MTO (la MO en kg que produce el Producto A de la MO en m)."""
        self.ensure_one()
        return self.move_raw_ids.move_orig_ids.production_id - self

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
        for orden in self.filtered('rollo_final_ids'):
            orden._clasif_cierre_metros()
        return super().button_mark_done()

    def _clasif_cierre_metros(self):
        """Cierre de la MO en metros: igual que en kg, la cantidad de la MO es la suma de los rollos
        clasificados, y el consumo de A es el kg real de cada lote (la demanda teórica se iguala a él)."""
        self.ensure_one()
        finished_move = self.move_finished_ids.filtered(
            lambda m: m.product_id == self.product_id and m.state not in ('done', 'cancel'))[:1]
        raw = self._clasif_move_a()
        rollos = self.rollo_final_ids
        if raw:
            self._clasif_asegurar_consumo(rollos)
            (raw.move_line_ids - raw.move_line_ids.filtered(lambda ml: ml.lot_id in rollos.lot_id)).unlink()
            total_kg = sum(rollos.mapped('kg_rama'))
            raw.with_context(do_not_unreserve=True).write({'product_uom_qty': total_kg})
        if finished_move:
            self._rama_unlink_lines_without_lot(finished_move)
            lineas_rollo = finished_move.move_line_ids.filtered(lambda ml: ml.lot_id or ml.lot_name)
            lineas_rollo.filtered('picked').write({'picked': False})
            total = self.product_id.uom_id.round(sum(lineas_rollo.mapped('quantity')))
            if float_compare(total, 0, precision_rounding=self.product_id.uom_id.rounding) > 0:
                self.write({'product_qty': total, 'qty_producing': total})
                finished_move.with_context(do_not_unreserve=True).write({'product_uom_qty': total})
                # write de qty_producing reescala líneas del core: se repite el consumo explícito.
                self._clasif_asegurar_consumo(rollos)
