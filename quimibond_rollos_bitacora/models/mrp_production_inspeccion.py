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

3.3.0 (revisión del flujo, 2026-11-05):

- **Varios baños para una orden en metros** (la programación divide la orden en kg; v5.4 §17.3): cada baño registra su
  parte en la orden en metros abierta y deja pendiente lo de los baños que siguen (pendiente nativo de Odoo,
  ``TL/OP-M/…-002``); el último baño la cierra con sus metros reales. Antes el segundo baño ya no podía cerrar.
- El registro se decide por el **estado** de la orden en kg después del cierre nativo, no por lo que devuelve: el cierre
  desde el piso de producción devuelve una acción y la orden ya quedó hecha.
- Un baño sin primera ni FE ya no cancela la orden en metros mientras otro baño la siga abasteciendo.
- Los lotes de los rollos finales se reutilizan si el cierre se repite (Odoo pidió confirmar algo y se vuelve a llamar).
- Aviso en el chatter con los kg pesados contra lo planeado; con el parámetro
  ``quimibond_rollos_bitacora.cierre_minimo_pct`` capturado, no cierra un baño con menos de ese porcentaje.
"""
from odoo import api, fields, models, _
from odoo.exceptions import UserError
from odoo.tools import float_compare, float_is_zero

from .mrp_rollo_bitacora import CALIDADES_EN_METROS, CLASIFICACIONES, PARAM_PRELIM_ALTO, PARAM_PRELIM_ANCHO

PARAM_CIERRE_MINIMO_PCT = 'quimibond_rollos_bitacora.cierre_minimo_pct'
# 3.11.0 (A3.2): con el parámetro en «1», el cierre de la orden en kg ya no registra ni cierra la orden en metros: los rollos
# finales pasan a ella clasificados por Inspección (preliminar) y se registran al cerrar la orden en metros, ya con la
# clasificación definitiva de Empaque. Sin el parámetro (por defecto) todo funciona como en 3.10.0.
# Rollos finales que aún no se registran en inventario: recién inspeccionados o ya clasificados en Empaque.
ESTADOS_SIN_REGISTRAR = ('inspeccionado', 'clasificado')


class MrpProductionInspeccion(models.Model):
    _inherit = 'mrp.production'

    rollo_final_kg_ids = fields.One2many('mrp.rollo.final', 'production_id', 'Rollos finales (Inspección)')
    rollo_final_m_ids = fields.One2many('mrp.rollo.final', 'production_m_id', 'Rollos finales')
    # 3.3.8 (Dirección, 2026-11-05): resumen de la pestaña Rollos (sin guardar).
    rollos_rama_count = fields.Integer('Rollos pesados en Rama', compute='_compute_resumen_rollos')
    rollos_rama_kg = fields.Float('Kg pesados en Rama', digits=(12, 2), compute='_compute_resumen_rollos')
    rollos_rama_metros = fields.Float('Metros en Rama (cuentametros)', digits=(12, 2),
                                      compute='_compute_resumen_rollos')
    rollos_rama_pct = fields.Float('Pesado en Rama contra lo planeado (%)', digits=(12, 1),
                                   compute='_compute_resumen_rollos')
    rollos_pendientes_count = fields.Integer('Rollos de Rama por inspeccionar', compute='_compute_resumen_rollos')
    rollos_final_count = fields.Integer('No. de rollos finales', compute='_compute_resumen_rollos')
    rollos_final_kg = fields.Float('Kg de rollos finales', digits=(12, 2), compute='_compute_resumen_rollos')
    rollos_final_metros = fields.Float('Metros de rollos finales', digits=(12, 2), compute='_compute_resumen_rollos')
    rollos_merma_kg = fields.Float('Merma de Inspección (kg)', digits=(12, 2), compute='_compute_resumen_rollos',
                                   help='Kg de Rama de los rollos ya inspeccionados menos los kg de sus rollos finales.')
    rollos_merma_pct = fields.Float('Merma de Inspección (%)', digits=(12, 1), compute='_compute_resumen_rollos')
    rollos_por_calidad = fields.Char('Kg por calidad', compute='_compute_resumen_rollos')
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
        """La orden en metros abierta que consume el acabado en kg de esta orden (destino MTO de su terminado). 3.3.0:
        si un baño anterior ya registró su parte, es el pendiente (``…-002``), no la orden que ya quedó hecha."""
        self.ensure_one()
        terminado = self.move_finished_ids.filtered(lambda m: m.product_id == self.product_id)
        ordenes = terminado.move_dest_ids.raw_material_production_id
        return ordenes.filtered(lambda p: p.state not in ('done', 'cancel'))[:1]

    @api.depends('product_qty', 'rollo_bitacora_ids.kg_rama', 'rollo_bitacora_ids.metros_rama',
                 'rollo_bitacora_ids.estado_inspeccion',
                 'rollo_final_kg_ids.kg_neto', 'rollo_final_kg_ids.metros', 'rollo_final_kg_ids.clasificacion',
                 'rollo_final_kg_ids.bitacora_ids')
    def _compute_resumen_rollos(self):
        """3.3.8: lo que la pestaña Rollos resume arriba. La merma se mide sobre los rollos de Rama ya inspeccionados y
        los rollos finales que salen solo de ellos, para no contar un rollo cortado a la mitad."""
        etiquetas = dict(CLASIFICACIONES)
        for orden in self:
            rama = orden.rollo_bitacora_ids
            finales = orden.rollo_final_kg_ids
            orden.rollos_rama_count = len(rama)
            orden.rollos_rama_kg = sum(rama.mapped('kg_rama'))
            orden.rollos_rama_metros = sum(rama.mapped('metros_rama'))
            orden.rollos_rama_pct = orden.rollos_rama_kg / orden.product_qty * 100.0 if orden.product_qty else 0.0
            orden.rollos_pendientes_count = len(rama.filtered(lambda r: r.estado_inspeccion == 'pendiente'))
            orden.rollos_final_count = len(finales)
            orden.rollos_final_kg = sum(finales.mapped('kg_neto'))
            orden.rollos_final_metros = sum(finales.mapped('metros'))
            inspeccionados = rama.filtered(lambda r: r.estado_inspeccion == 'inspeccionado')
            cerrados = finales.filtered(lambda f: f.bitacora_ids and not (f.bitacora_ids - inspeccionados))
            base = sum(inspeccionados.mapped('kg_rama'))
            orden.rollos_merma_kg = base - sum(cerrados.mapped('kg_neto')) if base else 0.0
            orden.rollos_merma_pct = orden.rollos_merma_kg / base * 100.0 if base else 0.0
            partes = []
            for clave, _etiqueta in CLASIFICACIONES:
                del_tipo = finales.filtered(lambda f, c=clave: f.clasificacion == c)
                if del_tipo:
                    partes.append('%s %s: %.2f kg' % (len(del_tipo), etiquetas[clave], sum(del_tipo.mapped('kg_neto'))))
            orden.rollos_por_calidad = ' · '.join(partes)

    def _insp_otros_banos(self, orden_m):
        """3.3.0: las otras órdenes en kg (baños) que siguen abasteciendo a la orden en metros y no han cerrado."""
        self.ensure_one()
        return orden_m._ordenes_previas().filtered(lambda p: p.state not in ('done', 'cancel')) - self

    def _insp_aviso_cierre(self, planeado):
        """3.3.0: kg pesados en Rama (más el desperdicio) contra los kg planeados del baño. Con el parámetro
        ``cierre_minimo_pct`` capturado no deja cerrar abajo de ese porcentaje (José cerró una prueba con 2 de ~7 rollos y
        el 73 % del crudo quedó como merma sin aviso). Devuelve el texto para el chatter."""
        self.ensure_one()
        pesado = sum(self.rollo_bitacora_ids.mapped('kg_rama'))
        if not planeado:
            return False
        pct = pesado / planeado * 100.0
        minimo = (self.env['ir.config_parameter'].sudo().get_param(PARAM_CIERRE_MINIMO_PCT) or '').strip()
        if minimo:
            try:
                minimo = float(minimo)
            except ValueError:
                minimo = 0.0
            if minimo and pct < minimo:
                raise UserError(_(
                    'El baño %(o)s tiene %(p).2f kg pesados en Rama de %(pl).2f kg planeados (%(pct).1f %%), abajo del '
                    'mínimo de %(m).1f %% para cerrar. Si el baño de verdad terminó, revisa que se hayan pesado todos '
                    'los rollos; el crudo se consume completo al cerrar.',
                    o=self.name, p=pesado, pl=planeado, pct=pct, m=minimo))
        return _('Baño cerrado con %(p).2f kg pesados en Rama de %(pl).2f kg planeados (%(pct).1f %%); desperdicio de '
                 'Rama %(d).2f kg.', p=pesado, pl=planeado, pct=pct, d=self.desperdicio_kg or 0.0)

    def _insp_modo(self):
        """Cómo se registra el baño al cerrarlo (3.2.0):

        - ``metros``: abastece una orden en metros y tiene primera o FE (diseño 2026-11-03, §4.5);
        - ``kg``: no abastece orden en metros (venta en kg o inventario en kg) y tiene primera o FE: los rollos finales
          salen de la misma orden en kg, el acabado en kg con su peso real;
        - ``saldos``: no tiene primera ni FE: la orden en metros (si hay) se cancela y los saldos salen de una orden de
          saldos aparte (Odoo no cierra una orden con cero de su producto).
        """
        self.ensure_one()
        finales = self.rollo_final_kg_ids.filtered(lambda r: r.estado in ESTADOS_SIN_REGISTRAR)
        if not finales:
            return False
        # 3.15.0: con orden en metros, la Segunda también es tela en metros; el baño para venta en kg (sin orden en metros)
        # sigue sacándola como saldo en kg, porque no hay producto en metros donde registrarla.
        calidades = CALIDADES_EN_METROS if finales.production_m_id else ('PQ', 'FE')
        if not finales.filtered(lambda r: r.clasificacion_registro in calidades):
            return 'saldos'
        return 'metros' if finales.production_m_id else 'kg'

    def button_mark_done(self):
        # 3.11.0: con la Clasificación final en Empaque activa, cerrar la orden en metros es lo que registra sus rollos
        # finales (con la calidad definitiva); el cierre de la orden en kg solo se los pasa.
        if not self.env.context.get('qb_insp_desde_empaque'):
            en_empaque = self.filtered(lambda o: o._insp_orden_m_en_empaque())
            if en_empaque:
                res = en_empaque._insp_cerrar_desde_empaque()
                resto = self - en_empaque
                return resto.button_mark_done() if resto else res
        modos, avisos = {}, {}
        for orden in self.filtered('rollo_final_kg_ids'):
            modo = orden._insp_modo()
            if not modo:
                continue
            # Con orden en metros (aunque se haya cancelado: 3.14.1 crea otra), los rollos pasan a Empaque y la orden en
            # metros los registra al cerrar; sin ella (baño para venta en kg) el baño se registra solo.
            if modo in ('metros', 'saldos') and orden.rollo_final_kg_ids.production_m_id:
                modo = 'empaque'
            orden._validar_rollos_inspeccionados()
            avisos[orden.id] = orden._insp_aviso_cierre(orden.product_qty)
            modos[orden.id] = modo
            if modo == 'kg':
                orden._insp_preparar_kg()
        res = super().button_mark_done()
        # 3.3.0: se decide por el estado. El core devuelve una acción tanto cuando pide confirmar algo (la orden sigue
        # abierta; al confirmar vuelve a llamar a este método) como cuando ya cerró (piso de producción, reporte).
        for orden in self.browse(list(modos)).filtered(lambda o: o.state == 'done'):
            modo = modos[orden.id]
            if modo == 'metros':
                orden._insp_registrar_metros()
            elif modo == 'empaque':
                orden._insp_pasar_a_orden_m()
            elif modo == 'kg':
                orden._insp_registrar_kg()
            else:
                orden._insp_registrar_saldos()
            if avisos.get(orden.id):
                orden.message_post(body=avisos[orden.id])
        return res

    # ------------------------------------------------------------ Clasificación final en Empaque (3.11.0, A3.2)
    def _insp_pasar_a_orden_m(self):
        """Cierre de la orden en kg con el flujo de Empaque: los rollos finales (clasificados por Inspección) quedan ligados a
        la orden en metros, sin movimientos de inventario; ahí Empaque los confirma o cambia y la orden los registra al
        cerrar."""
        self.ensure_one()
        finales = self.rollo_final_kg_ids.filtered(lambda r: r.estado in ESTADOS_SIN_REGISTRAR)
        orden_m = self._insp_orden_metros() or self._insp_orden_metros_reemplazo(finales)
        if not orden_m:
            raise UserError(_('No se puede cerrar el baño %s: no hay orden en metros abierta a la que pasar sus rollos.',
                              self.name))
        if orden_m.state == 'draft':
            orden_m.action_confirm()
        finales.filtered(lambda r: r.production_m_id != orden_m).write({'production_m_id': orden_m.id})
        self.message_post(body=_(
            'Baño cerrado: %(n)s rollos finales (%(kg).2f kg) pasan a %(m)s, pendientes de Clasificación final en '
            'Empaque.', n=len(finales), kg=sum(finales.mapped('kg_neto')), m=orden_m.name))
        orden_m.message_post(body=_(
            'Recibe %(n)s rollos finales del baño %(b)s, clasificados por Inspección. Se registran en inventario al cerrar '
            'esta orden.', n=len(finales), b=self.name))

    # 3.13.0 (A3.3b): desperdicio de la orden en metros (SALDO TEJIDO D), pesado en Empaque (diseño §16). Campos propios: los
    # de Rama (`desperdicio_kg`) son del subproducto de la orden en kg y bloquean su cancelación.
    desperdicio_empaque_kg = fields.Float('Desperdicio pesado en Empaque (kg)', digits=(12, 3), readonly=True, copy=False)
    desperdicio_empaque_operador = fields.Char('Operador del desperdicio de Empaque', readonly=True, copy=False)
    desperdicio_empaque_fecha = fields.Datetime('Desperdicio de Empaque pesado el', readonly=True, copy=False)
    desperdicio_empaque_manual = fields.Boolean('Desperdicio de Empaque capturado a mano', readonly=True, copy=False)
    desperdicio_empaque_motivo = fields.Char('Motivo de la captura manual (desperdicio de Empaque)', readonly=True,
                                             copy=False)

    clasificacion_empaque_pendiente = fields.Boolean(
        'Con rollos por clasificar en Empaque', compute='_compute_clasificacion_empaque_pendiente')

    def _compute_clasificacion_empaque_pendiente(self):
        for orden in self:
            orden.clasificacion_empaque_pendiente = orden._insp_orden_m_en_empaque()

    def _insp_extra_desperdicio(self):
        """Kg de desperdicio que Empaque pesó en esta orden en metros: se suman al saldo Desperdicio de los rollos que
        Inspección clasificó como tal (mismo producto, ubicación 17), al registrar."""
        self.ensure_one()
        return self.desperdicio_empaque_kg or 0.0

    def _zpl_etiqueta_desperdicio_empaque(self):
        """Etiqueta del desperdicio pesado en Empaque (3.15.0, José 2026-11-08): fecha, producto (código), nombre, número de
        lote, cantidad con su unidad de medida y el código de barras del número de lote. El producto es el subproducto de
        desperdicio (SALDO TEJIDO D) y el lote el del mes, el mismo con que se registra al cerrar. Tamaño: el de la
        preliminar (68 x 38 mm, ajustable en Ajustes de la etiqueta preliminar)."""
        self.ensure_one()
        if not self.desperdicio_empaque_kg:
            raise UserError(_('La orden %s todavía no tiene desperdicio pesado en Empaque.', self.name))
        producto = self.env['mrp.rollo.final']._producto_saldo('DESP')
        lote = self._lote_desperdicio(producto)
        fecha = fields.Datetime.context_timestamp(self, self.desperdicio_empaque_fecha or fields.Datetime.now())
        return self.env['mrp.rollo.bitacora']._zpl_preliminar_acomodo(
            PARAM_PRELIM_ANCHO, PARAM_PRELIM_ALTO, [
                'PRODUCTO: %s' % (producto.default_code or ''),
                'NOMBRE: %s' % producto.name,
                'FECHA: %s' % fecha.strftime('%d/%m/%Y %H:%M'),
                'LOTE: %s' % lote.name,
            ], ['CANTIDAD: %.2f %s' % (self.desperdicio_empaque_kg, producto.uom_id.name)], lote.name)

    def action_imprimir_etiqueta_desperdicio_empaque(self):
        self.ensure_one()
        accion = self.env.ref('quimibond_rollos_bitacora.action_report_etiqueta_desperdicio_empaque').report_action(self)
        accion.update({'close_on_report_download': True})
        return accion

    def action_pesar_desperdicio_empaque(self):
        """Abre el pesaje del desperdicio de Empaque de esta orden en metros (3.13.0, A3.3b)."""
        self.ensure_one()
        if not self._insp_orden_m_en_empaque():
            raise UserError(_('La orden %s no está en Clasificación final: el desperdicio se pesa mientras Empaque '
                              'clasifica los rollos y la orden sigue abierta.', self.name))
        return {
            'type': 'ir.actions.act_window',
            'name': _('Pesar desperdicio (Empaque)'),
            'res_model': 'rollo.empaque.desperdicio.wizard',
            'view_mode': 'form',
            'target': 'new',
            'context': {'default_production_id': self.id, **(
                {'default_workorder_id': self.env.context['default_workorder_id']}
                if self.env.context.get('default_workorder_id') else {})},
        }

    def action_clasificar_empaque(self):
        """Abre la estación de Clasificación final (Empaque) de esta orden en metros (3.12.0, A3.3)."""
        self.ensure_one()
        if self.state in ('done', 'cancel'):
            raise UserError(_('La orden %s ya está cerrada o cancelada.', self.name))
        if not self._insp_orden_m_en_empaque():
            raise UserError(_('La orden %s no tiene rollos por clasificar: los baños pasan sus rollos a la orden en '
                              'metros al cerrar la orden en kg.', self.name))
        return {
            'type': 'ir.actions.act_window',
            'name': _('Clasificar en Empaque'),
            'res_model': 'rollo.empaque.clasificacion.wizard',
            'view_mode': 'form',
            'target': 'new',
            'context': {'default_production_id': self.id, **(
                {'default_workorder_id': self.env.context['default_workorder_id']}
                if self.env.context.get('default_workorder_id') else {})},
        }

    def _insp_orden_m_en_empaque(self):
        """Orden en metros abierta con rollos finales que le pasó un baño y todavía no se registran."""
        self.ensure_one()
        if self.state in ('done', 'cancel'):
            return False
        return bool(self.rollo_final_m_ids.filtered(lambda r: r.estado in ESTADOS_SIN_REGISTRAR))

    def _insp_cerrar_desde_empaque(self):
        """Cierre de la orden en metros con rollos pendientes: registra el baño con la calidad, el peso y los metros de
        registro de cada rollo (los de Empaque si ya clasificó, si no los de Inspección). Con primera o FE registra la tela
        y cierra la orden; sin ellas (todo saldos) cancela la orden en metros y saca los saldos en una orden aparte, la
        misma lógica de 3.2.0. Un baño por orden en metros (programación 1.8.0)."""
        for orden_m in self:
            finales = orden_m.rollo_final_m_ids.filtered(lambda r: r.estado in ESTADOS_SIN_REGISTRAR)
            banos = finales.production_id
            if len(banos) != 1:
                raise UserError(_(
                    'La orden en metros %(m)s recibe rollos de %(n)s baños (%(b)s). Con Clasificación final en Empaque cada '
                    'baño tiene su orden en metros (programación 1.8.0).',
                    m=orden_m.name, n=len(banos), b=', '.join(banos.mapped('name'))))
            sin_clasificar = finales.filtered(lambda r: r.estado != 'clasificado')
            if sin_clasificar:
                # 3.12.0 (Dirección, 2026-11-07): Empaque confirma o cambia la calidad de cada rollo antes de cerrar.
                nombres = ', '.join(sin_clasificar.sorted('secuencia').mapped('name')[:10])
                raise UserError(_(
                    'Faltan %(n)s rollos por clasificar en Empaque en %(m)s: %(r)s%(mas)s. Confirma o cambia la calidad '
                    'de cada uno en «Clasificar en Empaque» antes de cerrar.', n=len(sin_clasificar), m=orden_m.name,
                    r=nombres, mas='…' if len(sin_clasificar) > 10 else ''))
            if banos.state != 'done':
                raise UserError(_('El baño %(b)s sigue abierto: cierra primero la orden en kg antes de cerrar %(m)s.',
                                  b=banos.name, m=orden_m.name))
            registrar = banos.with_context(qb_insp_desde_empaque=True)
            if finales.filtered(lambda r: r.clasificacion_registro in CALIDADES_EN_METROS):
                registrar._insp_registrar_metros()
            else:
                registrar._insp_registrar_saldos()
        return True

    # ------------------------------------------------------------ baño en kg (3.2.0)
    def _insp_preparar_kg(self):
        """Antes del cierre de un baño en kg: el terminado de la orden en kg son los rollos finales (no los de Rama),
        el acabado en kg con su peso real y su lote K…, PQ a 14 y FE a 15; los saldos, movimientos de subproducto a 16 y
        17. El cierre de José iguala la orden a la suma de esas líneas."""
        self.ensure_one()
        Final = self.env['mrp.rollo.final']
        finales = self.rollo_final_kg_ids.filtered(lambda r: r.estado in ESTADOS_SIN_REGISTRAR)
        terminado = self.move_finished_ids.filtered(
            lambda m: m.product_id == self.product_id and m.state not in ('done', 'cancel'))[:1]
        if not terminado:
            raise UserError(_('La orden %s no tiene movimiento del acabado en kg pendiente.', self.name))
        terminado.move_line_ids.unlink()
        for rollo in finales.filtered(lambda r: r.clasificacion_registro in ('PQ', 'FE')).sorted('secuencia'):
            destino = Final._ubicacion_calidad(rollo.clasificacion_registro, desarrollo=self.es_desarrollo)
            lote = self._insp_lote(self.product_id, rollo.name, self.company_id)
            self.env['stock.move.line'].create({
                'move_id': terminado.id, 'product_id': self.product_id.id, 'lot_id': lote.id, 'lot_name': lote.name,
                'quantity': rollo.kg_registro, 'location_id': terminado.location_id.id, 'location_dest_id': destino.id,
                'production_id': self.id})
            rollo.write({'product_id': self.product_id.id, 'lot_id': lote.id, 'location_dest_id': destino.id})
        for clasificacion in ('SEGUNDA', 'DESP'):
            grupo = finales.filtered(lambda r: r.clasificacion_registro == clasificacion)
            if grupo:
                movimiento = self._insp_movimiento_saldo(clasificacion, terminado)
                self._insp_lineas_saldo(movimiento, grupo)

    def _insp_movimiento_saldo(self, clasificacion, base):
        """Movimiento del saldo en esta orden: el subproducto de la lista si lo trae; si no, se agrega (cantidad real,
        sin costo, a la ubicación de la calidad)."""
        Final = self.env['mrp.rollo.final']
        producto = Final._producto_saldo(clasificacion)
        destino = Final._ubicacion_calidad(clasificacion)
        movimiento = self.move_finished_ids.filtered(
            lambda m: m.product_id == producto and m.state not in ('done', 'cancel'))[:1]
        if not movimiento:
            movimiento = self.env['stock.move'].create({
                'product_id': producto.id, 'product_uom': producto.uom_id.id, 'product_uom_qty': 0.0,
                'location_id': base.location_id.id, 'location_dest_id': destino.id, 'production_id': self.id,
                'company_id': self.company_id.id, 'origin': self.name, 'cost_share': 0.0})
            movimiento._action_confirm()
        return movimiento

    @api.model
    def _insp_limpiar_saldos(self, movimientos):
        """3.3.1: deja en cero los saldos pendientes, también sus líneas: la división nativa copia al pendiente la línea
        sin lote del saldo del baño anterior y el siguiente cierre pedía lote (E1 de la prueba en main, 2026-11-05)."""
        movimientos = movimientos.filtered(lambda m: m.state not in ('done', 'cancel'))
        movimientos.move_line_ids.unlink()
        movimientos.with_context(do_not_unreserve=True).write({'product_uom_qty': 0.0, 'picked': False})

    def _insp_lineas_saldo(self, movimiento, grupo, extra_kg=0.0, clasificacion=False):
        """Líneas del saldo: una por rollo final del grupo y, con ``extra_kg`` (3.13.0: el desperdicio que Empaque pesó
        en la orden en metros), una más con el lote del mes, como el desperdicio de Rama."""
        producto = movimiento.product_id
        destino = self.env['mrp.rollo.final']._ubicacion_calidad(grupo[:1].clasificacion_registro or clasificacion)
        movimiento.move_line_ids.unlink()
        movimiento.with_context(do_not_unreserve=True).write({
            'product_uom_qty': producto.uom_id.round(sum(grupo.mapped('kg_registro')) + extra_kg),
            'location_dest_id': destino.id})
        if extra_kg:
            lote_extra = self._lote_desperdicio(producto) if producto.tracking != 'none' else self.env['stock.lot']
            self.env['stock.move.line'].create({
                'move_id': movimiento.id, 'product_id': producto.id, 'lot_id': lote_extra.id or False,
                'quantity': extra_kg, 'location_id': movimiento.location_id.id, 'location_dest_id': destino.id,
                'production_id': movimiento.production_id.id})
        for rollo in grupo.sorted('secuencia'):
            lote = self._insp_lote(producto, rollo.name, self.company_id) if producto.tracking != 'none' \
                else self.env['stock.lot']
            self.env['stock.move.line'].create({
                'move_id': movimiento.id, 'product_id': producto.id, 'lot_id': lote.id or False,
                'quantity': rollo.kg_registro, 'location_id': movimiento.location_id.id, 'location_dest_id': destino.id,
                'production_id': movimiento.production_id.id})
            rollo.write({'product_id': producto.id, 'lot_id': lote.id or False, 'location_dest_id': destino.id})
        movimiento.picked = True

    def _insp_registrar_kg(self):
        self.ensure_one()
        finales = self.rollo_final_kg_ids.filtered(lambda r: r.estado in ESTADOS_SIN_REGISTRAR)
        faltan = finales.filtered(lambda r: not r.lot_id)
        if faltan:
            raise UserError(_('No se registraron los rollos finales %s del baño %s.',
                              ', '.join(faltan.mapped('name')), self.name))
        finales.write({'estado': 'registrado'})
        self.message_post(body=_(
            'Baño cerrado en kg: %(n)s rollos finales con su peso real (%(kg).2f kg de primera y FE; %(s).2f kg de '
            'saldos).', n=len(finales), kg=sum(finales.filtered(lambda r: r.clasificacion_registro in ('PQ', 'FE')).mapped('kg_neto')),
            s=sum(finales.filtered(lambda r: r.clasificacion_registro in ('SEGUNDA', 'DESP')).mapped('kg_neto'))))

    # ------------------------------------------------------- baño sin primera ni FE (3.2.0)
    def _insp_registrar_saldos(self):
        """Después del cierre (los rollos de Rama ya están en inventario): una orden de saldos consume los rollos de Rama
        completos y saca cada rollo final como saldo en kg, con su lote, a 16 o 17."""
        self.ensure_one()
        finales = self.rollo_final_kg_ids.filtered(lambda r: r.estado in ESTADOS_SIN_REGISTRAR)
        rama = finales.bitacora_ids
        Final = self.env['mrp.rollo.final']
        # 3.3.0: la orden en metros (si hay) ya no se cancela antes del cierre: al cerrar, el MTO le reservó estos rollos;
        # se le quitan para que la orden de saldos los consuma, y se cancela solo si ningún otro baño la abastece.
        orden_m = self._insp_orden_metros() or finales.production_m_id.filtered(
            lambda p: p.state not in ('done', 'cancel'))[:1]
        if orden_m:
            orden_m.move_raw_ids.move_line_ids.filtered(
                lambda ml: ml.lot_id in rama.lot_id and ml.state not in ('done', 'cancel')).unlink()
        por_saldo = {c: finales.filtered(lambda r, c=c: r.clasificacion_registro == c) for c in ('SEGUNDA', 'DESP')}
        # 3.13.0: el desperdicio que Empaque pesó en la orden en metros (que aquí se cancela) sale en esta orden de saldos.
        extra_desp = orden_m._insp_extra_desperdicio() if orden_m else 0.0
        extra = {'SEGUNDA': 0.0, 'DESP': extra_desp}
        principal_c = max(por_saldo, key=lambda c: sum(por_saldo[c].mapped('kg_registro')) + extra[c])
        principal = Final._producto_saldo(principal_c)
        kg_principal = principal.uom_id.round(sum(por_saldo[principal_c].mapped('kg_registro')) + extra[principal_c])
        Tipo = self.env['stock.picking.type']
        tipo = Tipo.search([('code', '=', 'mrp_operation'), ('sequence_code', '=', 'OP-M'),
                            ('company_id', 'in', [self.company_id.id, False])], limit=1) or self.picking_type_id
        quant = self.env['stock.quant'].sudo().search([
            ('lot_id', 'in', rama.lot_id.ids), ('location_id.usage', '=', 'internal'), ('quantity', '>', 0)], limit=1)
        origen = quant.location_id or self.location_dest_id
        orden_s = self.create({
            'product_id': principal.id, 'product_qty': kg_principal, 'product_uom_id': principal.uom_id.id,
            'picking_type_id': tipo.id, 'location_src_id': origen.id,
            'location_dest_id': Final._ubicacion_calidad(principal_c).id, 'origin': self.name,
            'consumption': 'flexible', 'company_id': self.company_id.id,
            'move_raw_ids': [(0, 0, {
                'product_id': self.product_id.id, 'product_uom': self.product_id.uom_id.id,
                'product_uom_qty': sum(rama.mapped('kg_rama')), 'location_id': origen.id,
                'location_dest_id': self.product_id.with_company(self.company_id).property_stock_production.id,
                'company_id': self.company_id.id})]})
        # 3.3.1: el consumo de la orden de saldos sale de existencias (los rollos de Rama ya están en inventario); sin el
        # contexto, la regla bajo pedido de la ruta del producto en kg creaba una orden en kg que nadie pidió (E2).
        orden_s.with_context(qb_insp_sin_abastecer=True).action_confirm()
        orden_s.write({'qty_producing': kg_principal})
        terminado = orden_s.move_finished_ids.filtered(lambda m: m.product_id == principal)[:1]
        orden_s._insp_lineas_saldo(terminado, por_saldo[principal_c], extra_kg=extra[principal_c],
                                   clasificacion=principal_c)
        terminado.picked = False  # el terminado principal no va «picked» (patrón de José)
        otra = 'DESP' if principal_c == 'SEGUNDA' else 'SEGUNDA'
        if por_saldo[otra] or extra[otra]:
            orden_s._insp_lineas_saldo(orden_s._insp_movimiento_saldo(otra, terminado), por_saldo[otra],
                                       extra_kg=extra[otra], clasificacion=otra)
        orden_s._clasif_asegurar_consumo_saldos(rama)
        # 3.3.1 (Dirección, 2026-11-05): los saldos entran a costo cero; sin tela que lo absorba, el costo del baño se
        # queda en la cuenta de la ubicación de producción como merma del periodo (no como inventario de saldo).
        resultado = orden_s.with_context(qb_insp_saldos_sin_costo=True).button_mark_done()
        if orden_s.state != 'done':
            raise UserError(_(
                'No se pudo registrar la orden de saldos del baño %(kg)s; no se cerró nada. Respuesta de Odoo: %(r)s',
                kg=self.name, r=(resultado or {}).get('name') if isinstance(resultado, dict) else resultado))
        finales.write({'estado': 'registrado', 'production_saldo_id': orden_s.id})
        self.message_post(body=_(
            'Baño cerrado sin primera ni FE: los saldos salen en kg en %(o)s.', o=orden_s.name))
        if orden_m and not self._insp_otros_banos(orden_m):
            orden_m.action_cancel()
            orden_m.message_post(body=_(
                'Cancelada al cerrar el baño %s: no salió primera ni FE y ningún otro baño la abastece; los saldos salen '
                'en kg.', self.name))

    def _cal_price(self, consumed_moves):
        """3.3.1: en la orden de saldos de un baño sin primera ni FE, Segunda y Desperdicio entran a costo cero: se venden
        a 1–2 $/kg (facturas del último año) y con el costo del baño quedaban en inventario a más de 100 $/kg."""
        res = super()._cal_price(consumed_moves)
        if self.env.context.get('qb_insp_saldos_sin_costo'):
            self.move_finished_ids.filtered(lambda m: m.state not in ('done', 'cancel')).write({'price_unit': 0.0})
        return res

    def _clasif_asegurar_consumo_saldos(self, rama):
        """Consumo de la orden de saldos: cada rollo de Rama completo, con su lote (picked)."""
        self.ensure_one()
        raw = self.move_raw_ids.filtered(lambda m: m.state not in ('done', 'cancel'))[:1]
        raw.move_line_ids.unlink()
        for rollo in rama:
            quant = self.env['stock.quant'].sudo().search([
                ('lot_id', '=', rollo.lot_id.id), ('location_id.usage', '=', 'internal'), ('quantity', '>', 0)], limit=1)
            self.env['stock.move.line'].create({
                'move_id': raw.id, 'product_id': raw.product_id.id, 'lot_id': rollo.lot_id.id,
                'quantity': rollo.kg_rama, 'picked': True,
                'location_id': (quant.location_id or raw.location_id).id, 'location_dest_id': raw.location_dest_id.id})
        raw.picked = True

    def _insp_orden_metros_reemplazo(self, finales):
        """3.14.1 (revisión de código, 2026-11-06): si la orden en metros de los rollos finales se canceló o se cerró por
        fuera después de inspeccionar (pedido cancelado), el baño no podía cerrar nunca: «no hay orden en metros abierta».
        Ahora se crea otra orden en metros de la misma tela y lista de materiales, sin abastecer (los rollos de Rama ya
        están en inventario), y el baño se registra en ella como siempre: la tela queda en metros, no en kg. Sin tela en
        metros a la cual registrar, no hace nada (el llamador avisa)."""
        self.ensure_one()
        anterior = finales.production_m_id.sorted('id')[-1:]
        producto = anterior.product_id
        if not producto:
            return self.env['mrp.production']
        rama = finales.bitacora_ids
        primeras = finales.filtered(lambda r: r.clasificacion_registro in CALIDADES_EN_METROS)
        metros = producto.uom_id.round(sum(primeras.mapped('metros_registro'))) or anterior.product_qty
        quant = self.env['stock.quant'].sudo().search([
            ('lot_id', 'in', rama.lot_id.ids), ('location_id.usage', '=', 'internal'), ('quantity', '>', 0)], limit=1)
        origen = quant.location_id or self.location_dest_id
        nueva = self.create({
            'product_id': producto.id, 'product_qty': metros, 'product_uom_id': producto.uom_id.id,
            'bom_id': anterior.bom_id.id, 'picking_type_id': anterior.picking_type_id.id,
            'location_src_id': origen.id, 'origin': self.name, 'company_id': self.company_id.id,
            'es_desarrollo': anterior.es_desarrollo or self.es_desarrollo})
        # Sin abastecer: el consumo del acabado en kg sale de los rollos de Rama ya registrados (como la orden de saldos).
        nueva.with_context(qb_insp_sin_abastecer=True).action_confirm()
        finales.write({'production_m_id': nueva.id})
        estado = _('cancelada') if anterior.state == 'cancel' else _('cerrada')
        mensaje = _('La orden en metros %(a)s está %(e)s; el baño %(b)s se registra en la orden en metros nueva %(n)s.',
                    a=anterior.name, e=estado, b=self.name, n=nueva.name)
        self.message_post(body=mensaje)
        nueva.message_post(body=mensaje)
        return nueva

    def _insp_registrar_metros(self):
        """Registra en la orden en metros los rollos finales del baño (§4.5). ``self`` es la orden en kg ya cerrada.

        3.3.0: si otros baños siguen abasteciendo la misma orden en metros (la programación dividió la orden en kg), este
        baño produce solo sus metros y la orden en metros deja pendiente (backorder nativo) lo planeado de esos baños; el
        último baño la cierra con sus metros reales, sin pendiente (D7)."""
        self.ensure_one()
        finales = self.rollo_final_kg_ids.filtered(lambda r: r.estado in ESTADOS_SIN_REGISTRAR)
        if not finales:
            return
        orden_m = self._insp_orden_metros() or finales.production_m_id.filtered(
            lambda p: p.state not in ('done', 'cancel'))[:1]
        if not orden_m:
            orden_m = self._insp_orden_metros_reemplazo(finales)
        if not orden_m:
            raise UserError(_('No se puede registrar el baño %(kg)s: no hay orden en metros abierta que abastezca '
                              '(%(m)s).', kg=self.name, m=', '.join(finales.production_m_id.mapped('name')) or '—'))
        if orden_m.state == 'draft':
            orden_m.action_confirm()
        finales.filtered(lambda r: r.production_m_id != orden_m).write({'production_m_id': orden_m.id})
        Final = self.env['mrp.rollo.final']
        rama = finales.bitacora_ids
        primeras = finales.filtered(lambda r: r.clasificacion_registro in CALIDADES_EN_METROS)  # 3.15.0: con Segunda
        saldos = finales - primeras
        uom_m = orden_m.product_id.uom_id
        total_m = uom_m.round(sum(primeras.mapped('metros_registro')))
        if float_is_zero(total_m, precision_rounding=uom_m.rounding):
            raise UserError(_(
                'El baño %s no tiene rollos de primera, FE ni Segunda: la orden en metros %s no puede cerrar en cero. Revisa la '
                'clasificación en Inspección.', self.name, orden_m.name))
        raw = orden_m._clasif_move_a()
        if not raw:
            raise UserError(_('La orden en metros %s no tiene consumo pendiente del acabado en kg.', orden_m.name))

        # 1. Cantidad de la orden en metros. Último baño: los metros reales (sin pendiente). Con otros baños abiertos: sus
        #    metros más lo planeado de los otros, que queda de pendiente.
        otros = self._insp_otros_banos(orden_m)
        pendiente_m = 0.0
        if otros:
            kg_por_m = orden_m._insp_kg_por_metro(raw)
            kg_otros = sum(otros.mapped('product_qty'))
            kg_este = sum(rama.mapped('kg_rama'))
            if kg_por_m > 0:
                pendiente_m = kg_otros / kg_por_m
            elif kg_este:
                pendiente_m = kg_otros * total_m / kg_este
            pendiente_m = uom_m.round(pendiente_m)
            if float_is_zero(pendiente_m, precision_rounding=uom_m.rounding):
                pendiente_m = uom_m.rounding
        cantidad = total_m + pendiente_m
        orden_m.write({'product_qty': cantidad, 'qty_producing': total_m, 'consumption': 'flexible'})

        # 2. Consumo: cada rollo de Rama completo, de donde esté (10 INSPECCIÓN al cerrar la orden en kg). Los rollos de
        #    los otros baños no se tocan: siguen reservados y pasan al pendiente.
        orden_m._clasif_asegurar_consumo(rama)
        (raw.move_line_ids - raw.move_line_ids.filtered(lambda ml: ml.lot_id in rama.lot_id)).unlink()
        # Demanda: lo que consume este baño, más lo planeado de los otros cuando los hay (Odoo pasa al pendiente la parte
        # proporcional).
        kg_demanda = sum(rama.mapped('kg_rama')) + (sum(otros.mapped('product_qty')) if otros else 0.0)
        # 3.6.2: sin abastecer. La demanda solo se iguala a lo pesado; los baños que la cubren ya existen. Sin
        # no_procurement, cuando el último baño pesa más que la parte que Odoo le dejó al pendiente, Odoo abastecía la
        # diferencia con una orden en kg nueva (ronda 12 en main, TL/OP-KG/00066).
        raw.with_context(do_not_unreserve=True, no_procurement=True).write({'product_uom_qty': kg_demanda})

        # 3. La tela en metros: un lote por rollo final, PQ a 14, FE a 15 y Segunda a 16 (3.15.0).
        terminado = orden_m.move_finished_ids.filtered(
            lambda m: m.product_id == orden_m.product_id and m.state not in ('done', 'cancel'))[:1]
        if not terminado:
            raise UserError(_('La orden en metros %s no tiene movimiento de la tela pendiente.', orden_m.name))
        orden_m._rama_unlink_lines_without_lot(terminado)
        terminado.move_line_ids.unlink()
        terminado.with_context(do_not_unreserve=True).write({'product_uom_qty': cantidad})
        desarrollo = orden_m.es_desarrollo or self.es_desarrollo
        for rollo in primeras.sorted('secuencia'):
            destino = Final._ubicacion_calidad(rollo.clasificacion_registro, desarrollo=desarrollo)
            lote = self._insp_lote(orden_m.product_id, rollo.name, orden_m.company_id)
            self.env['stock.move.line'].create({
                'move_id': terminado.id, 'product_id': orden_m.product_id.id, 'lot_id': lote.id,
                'quantity': rollo.metros_registro, 'location_id': terminado.location_id.id,
                'location_dest_id': destino.id, 'production_id': orden_m.id})
            rollo.write({'product_id': orden_m.product_id.id, 'lot_id': lote.id, 'location_dest_id': destino.id})

        # 4. El desperdicio en kg (subproductos de la lista en metros, cantidad 0 en la lista; la real es el peso). Si la
        #    lista no los trae, se agregan a la orden (3.2.0).
        extra_desp = orden_m._insp_extra_desperdicio()
        for clasificacion in ('SEGUNDA', 'DESP'):
            grupo = saldos.filtered(lambda r: r.clasificacion_registro == clasificacion)
            extra = extra_desp if clasificacion == 'DESP' else 0.0
            if grupo or extra:
                orden_m._insp_lineas_saldo(orden_m._insp_movimiento_saldo(clasificacion, terminado), grupo,
                                           extra_kg=extra, clasificacion=clasificacion)
            else:
                # 3.3.1: un pendiente trae el saldo que heredó de un baño anterior; sin rollos de esa calidad, va en cero.
                producto = Final._producto_saldo(clasificacion)
                orden_m._insp_limpiar_saldos(orden_m.move_finished_ids.filtered(lambda m: m.product_id == producto))

        # 5. Se repite el consumo (escribir qty_producing reescala líneas del core) y se cierra; con otros baños, con
        #    pendiente nativo para lo que falta.
        orden_m._clasif_asegurar_consumo(rama)
        if otros:
            orden_cierre = orden_m.with_context(skip_backorder=True, mo_ids_to_backorder=orden_m.ids)
        else:
            orden_cierre = orden_m
        resultado = orden_cierre.button_mark_done()
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
        pendiente = self.env['mrp.production']
        if otros:
            pendiente = orden_m.production_group_id.production_ids.filtered(
                lambda p: p.state not in ('done', 'cancel') and p != orden_m)
            if not pendiente:
                raise UserError(_('La orden en metros %s no dejó pendiente para los baños %s.',
                                  orden_m.name, ', '.join(otros.mapped('name'))))
            # El pendiente hereda en proporción la demanda de los saldos de este baño: los saldos no se planean.
            saldos_prod = Final._productos_saldo()
            pendiente._insp_limpiar_saldos(pendiente.move_finished_ids.filtered(lambda m: m.product_id in saldos_prod))
        self.message_post(body=_(
            'Baño cerrado: %(n)s rollos finales registrados en %(m)s (%(mts).2f m de primera, FE y Segunda; %(kg).2f kg '
            'de desperdicio).%(p)s', n=len(finales), m=orden_m.name, mts=total_m, kg=sum(saldos.mapped('kg_registro')),
            p=(_(' Queda pendiente %s para los baños %s.', pendiente[:1].name, ', '.join(otros.mapped('name')))
               if pendiente else '')))

    def _insp_kg_por_metro(self, raw):
        """3.3.0: kg del acabado por metro de la orden en metros, de su lista de materiales (la demanda del movimiento
        ya cambió con baños anteriores); si no hay lista, la proporción de la demanda."""
        self.ensure_one()
        bom = self.bom_id
        lineas = bom.bom_line_ids.filtered(lambda l: l.product_id == raw.product_id)
        if bom and lineas and bom.product_qty:
            kg = sum(l.product_uom_id._compute_quantity(l.product_qty, raw.product_uom, round=False) for l in lineas)
            metros = bom.product_uom_id._compute_quantity(bom.product_qty, self.product_uom_id, round=False)
            if metros:
                return kg / metros
        return raw.product_uom_qty / self.product_qty if self.product_qty else 0.0

    @api.model
    def _insp_lote(self, producto, nombre, company):
        """Lote del rollo final. 3.3.0: si ya existe sin existencia ni movimientos hechos (el cierre se repitió porque
        Odoo pidió confirmar algo), se reutiliza; si ya tiene inventario, es otro rollo y es error."""
        Lote = self.env['stock.lot']
        dominio = [('name', '=', nombre), ('product_id', '=', producto.id), ('company_id', '=', company.id)]
        lote = Lote.search(dominio, limit=1)
        if lote:
            usado = self.env['stock.move.line'].sudo().search_count([('lot_id', '=', lote.id), ('state', '=', 'done')])
            if usado:
                raise UserError(_('Ya existe el lote %(l)s para %(p)s.', l=nombre, p=producto.display_name))
            return lote
        return Lote.create({'name': nombre, 'product_id': producto.id, 'company_id': company.id})
