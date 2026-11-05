# -*- coding: utf-8 -*-
"""Rollo final: lo que sale de la revisadora (diseño 2026-11-03, Dirección General, §4.1).

Inspección es la única estación (D8): corta o junta rollos de Rama del mismo baño (D3, D6), pesa cada rollo final con
la tara de Inspección, le pone metros, calidad y defectos, le asigna su lote (``M<orden en metros>-NNNN``, D4) e
imprime la etiqueta final. No mueve inventario: los rollos finales quedan «inspeccionados» y se registran en la orden
en metros cuando el supervisor cierra la orden en kg con el botón nativo (D7, D9; ``mrp_production_inspeccion``).
"""
from odoo import api, fields, models, _
from odoo.exceptions import UserError

from .mrp_rollo_bitacora import (
    CALIDAD_CORTA, CLASIFICACIONES, COLORES, LIBRAS_POR_KG, PARAM_INSPECCION_IMPRIME, PARAM_PRELIM_ALTO,
    PARAM_PRELIM_ANCHO, SUFIJO_CODIGO, YARDAS_POR_METRO)

# Calidad → (parámetro de la ubicación, nombre por defecto, unidad en que sale). Primera y FE en metros (la tela);
# segundas y desperdicio en kg (los saldos), D5 y §4.3.
DESTINO_CALIDAD = {
    'PQ': ('quimibond_rollos_bitacora.ubicacion_pq', '14 PRODUCTO TERMINADO PQ', 'm'),
    'FE': ('quimibond_rollos_bitacora.ubicacion_fe', '15 PRODUCTO TERMINADO FE', 'm'),
    'SEGUNDA': ('quimibond_rollos_bitacora.ubicacion_segunda', '16 PRODUCTO TERMINADO SEGUNDAS', 'kg'),
    'DESP': ('quimibond_rollos_bitacora.ubicacion_desperdicio', '17 PRODUCTO TERMINADO DESPERDICIO', 'kg'),
}
# Saldos en kg: los mismos parámetros que usan las fichas para ponerlos de subproducto en la lista en metros.
SALDO_CALIDAD = {
    'SEGUNDA': ('quimibond_ficha_tecnica_tela.saldo_segunda', 'SALDO DE TELA TEJIDO'),
    'DESP': ('quimibond_ficha_tecnica_tela.saldo_desperdicio', 'SALDO TEJIDO D'),
}
PARAM_ALERTA_MERMA_PCT = 'quimibond_rollos_bitacora.alerta_merma_inspeccion_pct'
# 3.3.5: la primera y la FE de una orden de desarrollo van al almacén de desarrollos (los saldos siguen a 16 y 17).
DESTINO_DESARROLLO = ('quimibond_rollos_bitacora.ubicacion_desarrollo', '31 DESARROLLOS TOLUCA')


class MrpRolloFinal(models.Model):
    _name = 'mrp.rollo.final'
    _description = 'Rollo final (Inspección)'
    _order = 'production_id desc, secuencia'
    _rec_name = 'name'

    name = fields.Char('Rollo final (lote)', required=True, readonly=True, index=True, copy=False)
    secuencia = fields.Integer('No. de rollo', required=True, readonly=True)
    production_id = fields.Many2one('mrp.production', 'Orden (kg)', required=True, readonly=True, index=True,
                                    ondelete='cascade', help='El baño: solo se juntan rollos de la misma orden en kg.')
    production_m_id = fields.Many2one('mrp.production', 'Orden (m)', readonly=True, index=True,
                                      help='La orden en metros que abastece la orden en kg (MTO); da el nombre del lote '
                                           'y registra el rollo al cerrar el baño. Vacía en un baño para venta en kg '
                                           '(3.2.0): el rollo queda como el acabado en kg, con lote K<orden en kg>.')
    production_saldo_id = fields.Many2one('mrp.production', 'Orden de saldos', readonly=True, index=True,
                                          help='3.2.0: baño sin primera ni FE; sus saldos salen de esta orden.')
    bitacora_ids = fields.Many2many('mrp.rollo.bitacora', 'mrp_rollo_final_bitacora_rel', 'final_id', 'bitacora_id',
                                    'Rollos de Rama', readonly=True)
    company_id = fields.Many2one(related='production_id.company_id', store=True)

    kg_bruto = fields.Float('Peso bruto (kg)', digits=(12, 4), readonly=True)
    tara = fields.Float('Tara de Inspección (kg)', digits=(12, 4), readonly=True)
    kg_neto = fields.Float('Kg netos', digits=(12, 4), readonly=True)
    captura_manual = fields.Boolean('Captura manual', readonly=True)
    motivo_manual = fields.Char('Motivo de captura manual', readonly=True)
    metros = fields.Float('Metros', digits=(12, 2), readonly=True)
    no_empalmes = fields.Integer('No. de empalmes', readonly=True)
    clasificacion = fields.Selection(CLASIFICACIONES, 'Calidad', required=True, readonly=True)
    defecto_ids = fields.Many2many('calidad.defecto.tela', 'mrp_rollo_final_defecto_rel', 'final_id', 'defecto_id',
                                   'Defectos', readonly=True)
    siglas_defectos = fields.Char('Siglas de defectos', compute='_compute_siglas_defectos')
    workcenter_id = fields.Many2one('mrp.workcenter', 'Revisadora', readonly=True)
    operador = fields.Char('Operador de Inspección', readonly=True)
    siglas_operador = fields.Char('Siglas del operador', readonly=True)
    fecha = fields.Datetime('Inspeccionado el', readonly=True)
    zpl_etiqueta = fields.Text('ZPL etiqueta final', readonly=True, copy=False)
    zpl_etiqueta_preliminar = fields.Text('ZPL etiqueta preliminar', readonly=True, copy=False)

    estado = fields.Selection([('inspeccionado', 'Inspeccionado, sin registrar'), ('registrado', 'Registrado')],
                              'Estado', default='inspeccionado', required=True, readonly=True)
    product_id = fields.Many2one('product.product', 'Producto registrado', readonly=True,
                                 help='La tela (PQ y FE, en metros) o el saldo (Segunda y Desperdicio, en kg).')
    lot_id = fields.Many2one('stock.lot', 'Lote registrado', readonly=True, copy=False)
    location_dest_id = fields.Many2one('stock.location', 'Ubicación', readonly=True)

    # 3.3.0: alerta 2 del v5.4 §9.1 (metros contra kg × rendimiento de acabado de la ficha), guardada al capturar.
    metros_teoricos = fields.Float('Metros teóricos', digits=(12, 2), readonly=True,
                                   help='Kg netos × rendimiento de tela acabada de la ficha, al capturar el rollo.')
    alerta_metros = fields.Boolean('Alerta de metros', readonly=True,
                                   help='Los metros capturados se apartan de los teóricos más que la alerta 2 de Ajustes '
                                        'de Manufactura (%).')

    _unique_nombre = models.Constraint('unique(name, company_id)', 'Ese rollo final ya existe.')
    _check_kg = models.Constraint('CHECK(kg_neto > 0)', 'Los kg netos del rollo final deben ser mayores a cero.')
    _check_metros = models.Constraint('CHECK(metros > 0)', 'Los metros del rollo final deben ser mayores a cero.')

    @api.depends('defecto_ids.clave')
    def _compute_siglas_defectos(self):
        for rollo in self:
            rollo.siglas_defectos = ' '.join(rollo.defecto_ids.sorted('clave').mapped('clave'))

    # ------------------------------------------------------------------ reglas
    @api.model
    def _ubicacion_calidad(self, clasificacion, desarrollo=False):
        """Ubicación interna de la calidad (§4.3), por parámetro; se busca por nombre y gana la más corta (en producción
        hay ubicaciones con el mismo prefijo, «15 PRODUCTO TERMINADO FE TOLUCA15»).

        3.3.5: con ``desarrollo`` (la orden en metros o la orden en kg marcada «Orden de Desarrollo»), la primera y la FE
        van a «31 DESARROLLOS TOLUCA» (parámetro ``ubicacion_desarrollo``); los saldos en kg siguen a 16 y 17. Antes el
        campo existía pero ningún ruteo lo leía y los desarrollos entraban a 14 (prueba 5 en main, 2026-11-05)."""
        clave, defecto, _unidad = DESTINO_CALIDAD[clasificacion]
        if desarrollo and clasificacion in ('PQ', 'FE'):
            clave, defecto = DESTINO_DESARROLLO
        nombre = (self.env['ir.config_parameter'].sudo().get_param(clave) or defecto).strip()
        ubicaciones = self.env['stock.location'].search([('name', '=ilike', nombre + '%'), ('usage', '=', 'internal')])
        if not ubicaciones:
            raise UserError(_('No existe la ubicación «%(u)s» para la calidad %(c)s (parámetro %(p)s).',
                              u=nombre, c=clasificacion, p=clave))
        return ubicaciones.sorted(lambda u: (len(u.complete_name), u.id))[:1]

    @api.model
    def _producto_saldo(self, clasificacion):
        clave, defecto = SALDO_CALIDAD[clasificacion]
        codigo = (self.env['ir.config_parameter'].sudo().get_param(clave) or defecto).strip()
        producto = self.env['product.product'].search([('default_code', '=', codigo)], limit=1)
        if not producto:
            raise UserError(_('No existe el saldo «%(s)s» para la calidad %(c)s (parámetro %(p)s).',
                              s=codigo, c=clasificacion, p=clave))
        return producto

    @api.model
    def _productos_saldo(self):
        """3.3.0: los productos de saldo que existan (Segunda y Desperdicio)."""
        productos = self.env['product.product']
        for clasificacion in SALDO_CALIDAD:
            clave, defecto = SALDO_CALIDAD[clasificacion]
            codigo = (self.env['ir.config_parameter'].sudo().get_param(clave) or defecto).strip()
            productos |= productos.search([('default_code', '=', codigo)], limit=1)
        return productos

    @api.model
    def _ordenes_m_grupo(self, orden_m):
        """3.3.0: la orden en metros y sus pendientes (``…-001``, ``…-002``): comparten el consecutivo de los lotes."""
        return orden_m.production_group_id.production_ids | orden_m if orden_m.production_group_id else orden_m

    @api.model
    def _raiz_orden(self, orden):
        """Último segmento del nombre sin el sufijo de pendiente: ``TL/OP-M/00008-002`` → ``00008``."""
        segmento = orden.name.split('/')[-1]
        if orden.backorder_sequence and '-' in segmento:
            segmento = segmento.rsplit('-', 1)[0]
        return segmento

    @api.model
    def _siguiente_secuencia(self, orden_m, orden_kg=None):
        """Consecutivo por orden en metros (lote M…, 3.3.0: con sus pendientes) o, sin ella, por orden en kg (lote K…,
        3.2.0)."""
        dominio = [('production_m_id', 'in', self._ordenes_m_grupo(orden_m).ids)] if orden_m else \
            [('production_id', '=', orden_kg.id), ('production_m_id', '=', False)]
        # 3.3.5: sin huecos. Al anular un rollo de en medio su número queda libre y el siguiente lo toma; un rollo anulado
        # no tiene lote (solo se registra al cerrar el baño), así que el nombre no choca.
        usados = set(self.search(dominio).mapped('secuencia'))
        secuencia = 1
        while secuencia in usados:
            secuencia += 1
        return secuencia

    @api.model
    def _nombre_lote(self, orden_m, secuencia, orden_kg=None):
        """``M<último segmento de la orden en metros>-NNNN`` (§4.2): la «M» lo distingue del rollo de Rama. Sin orden en
        metros (venta en kg, 3.2.0): ``K<último segmento de la orden en kg>-NNNN``. 3.3.0: sin el sufijo de pendiente,
        así los baños de una misma orden siguen un solo consecutivo."""
        if orden_m:
            return 'M%s-%04d' % (self._raiz_orden(orden_m), secuencia)
        return 'K%s-%04d' % (orden_kg.name.split('/')[-1], secuencia)

    # ------------------------------------------------------------------ anular (3.3.0)
    def _revertir_rama(self):
        """Deja pendientes de inspección los rollos de Rama de estos rollos finales, quitando lo que la estación les
        copió."""
        for rama in self.bitacora_ids:
            rama.write({'estado_inspeccion': 'pendiente', 'clasificacion_inicial': False, 'metros_insp': 0.0,
                        'no_empalmes': 0, 'defecto_ids': [(5, 0, 0)], 'workcenter_insp_id': False,
                        'operador_insp': False, 'fecha_registro_insp': False})

    def action_anular(self):
        """3.3.0: corrige una captura de Inspección antes de cerrar el baño. Borra el rollo final y deja pendientes sus
        rollos de Rama (con sus otros pedazos, si los hay) para volver a inspeccionarlos. Solo el responsable de
        Manufactura; un rollo ya registrado no se anula (ya está en inventario)."""
        if not self.env.user.has_group('mrp.group_mrp_manager'):
            raise UserError(_('Solo el responsable de Manufactura anula un rollo final.'))
        registrados = self.filtered(lambda r: r.estado == 'registrado')
        if registrados:
            raise UserError(_('El rollo final %s ya está registrado en inventario; no se anula.',
                              ', '.join(registrados.mapped('name'))))
        cerradas = self.production_id.filtered(lambda p: p.state in ('done', 'cancel'))
        if cerradas:
            raise UserError(_('El baño %s ya cerró; no se anulan sus rollos finales.', ', '.join(cerradas.mapped('name'))))
        for rollo in self:
            rollo.production_id.message_post(body=_(
                'Rollo final %(r)s anulado por %(u)s (%(c)s, %(kg).2f kg, %(m).2f m); sus rollos de Rama %(rama)s quedan '
                'pendientes de inspección.', r=rollo.name, u=self.env.user.name, c=rollo.clasificacion,
                kg=rollo.kg_neto, m=rollo.metros, rama=', '.join(rollo.bitacora_ids.mapped('name'))))
        # Al anular un pedazo, el rollo de Rama queda abierto y sus otros pedazos siguen capturados.
        self.unlink()
        return {'type': 'ir.actions.act_window_close'}

    def unlink(self):
        """3.3.0: un rollo final registrado no se borra; uno sin registrar deja pendientes sus rollos de Rama."""
        if self.filtered(lambda r: r.estado == 'registrado'):
            raise UserError(_('Un rollo final registrado en inventario no se borra.'))
        self._revertir_rama()
        for orden in self.production_id:
            orden._historial_pesaje_borrar(self.filtered(lambda r: r.production_id == orden).mapped('name'))
        return super().unlink()

    # ------------------------------------------------------------------ etiqueta
    def _datos_etiqueta(self):
        self.ensure_one()
        producto = self.production_m_id.product_id or self.production_id.product_id
        codigo = producto.default_code or ''
        Ficha = self.env.get('ficha.tecnica.acabado')
        ficha = None
        if Ficha is not None:
            ficha = Ficha.search(['|', ('product_acabado_id', '=', producto.id), ('product_proceso_id', '=', producto.id)],
                                 order='id desc', limit=1)
        if codigo.upper().startswith('I'):
            codigo_tela = codigo[1:]  # posiciones del código sin la «I» (DAT P-D02-01)
        else:
            codigo_tela = codigo
        ancho = ficha.ancho_acabado if ficha and 'ancho_acabado' in ficha._fields else 0
        if not ancho and codigo_tela[11:14].isdigit():
            ancho = int(codigo_tela[11:14])
        return {'codigo': codigo, 'color': COLORES.get(codigo_tela[9:11].upper(), codigo_tela[9:11]), 'ancho': ancho}

    def _zpl_etiqueta(self):
        """Etiqueta final de José (§12.3) con los datos del rollo final: PQ con neto/bruto; FE, Segunda y Desperdicio
        (PNC) con kilos y libras, siglas de defectos y el segundo código. Código de barras: metros;kg;orden;rollo.
        Acomodo y tamaño (76 x 102 mm) en `mrp.rollo.bitacora._zpl_final_acomodo`."""
        self.ensure_one()
        datos = self._datos_etiqueta()
        clasif = self.clasificacion
        pnc = clasif != 'PQ'
        fecha = fields.Datetime.context_timestamp(self, self.fecha or fields.Datetime.now())
        of = self.name.rsplit('-', 1)[0]
        renglones = [
            'CÓDIGO: %s%s' % (datos['codigo'], SUFIJO_CODIGO.get(clasif, '')),
            'COLOR: %s' % datos['color'],
            'ANCHO (cm): %g' % datos['ancho'],
            ('METROS: %.2f' % self.metros, 'YARDAS: %.2f' % (self.metros * YARDAS_POR_METRO)),
            ('KILOS: %.2f' % self.kg_neto, 'LIBRAS: %.2f' % (self.kg_neto * LIBRAS_POR_KG)) if pnc
            else ('PESO NETO: %.2f' % self.kg_neto, 'PESO BRUTO: %.2f' % self.kg_bruto),
            'LOTE DE PRODUCCIÓN: %s' % of,
            'NO. DE ROLLO: %s' % self.secuencia,
            'OPERADOR INSPECCIÓN: %s' % (self.siglas_operador or ''),
            'FECHA: %s' % fecha.strftime('%d/%m/%Y'),
        ]
        codigos = ['%.2f;%.3f;%s;%s' % (self.metros, self.kg_neto, of, self.secuencia)]
        if pnc:
            renglones.append('DEFECTOS: %s' % (self.siglas_defectos or ''))
            codigos.append(('%s %s' % (self.secuencia, self.siglas_defectos or '')).strip())
        return self.env['mrp.rollo.bitacora']._zpl_final_acomodo(clasif, renglones, codigos)

    def _zpl_etiqueta_preliminar(self):
        """Etiqueta preliminar de Inspección (68 x 38 mm, 3.4.0): artículo, orden de trabajo, fecha, revisadora, kg,
        metros, la calidad en un recuadro y el código de barras del lote (que ya lleva el número de rollo)."""
        self.ensure_one()
        fecha = fields.Datetime.context_timestamp(self, self.fecha or fields.Datetime.now())
        return self.env['mrp.rollo.bitacora']._zpl_preliminar_acomodo(PARAM_PRELIM_ANCHO, PARAM_PRELIM_ALTO, [
            'ARTÍCULO: %s' % self._datos_etiqueta()['codigo'],
            'ORDEN: %s' % (self.production_id.name or ''),
            'FECHA: %s' % fecha.strftime('%d/%m/%Y %H:%M'),
            'CENTRO: %s' % (self.workcenter_id.name or ''),
        ], ['KG: %.2f' % self.kg_neto, 'M: %.2f' % self.metros], self.name, CALIDAD_CORTA.get(self.clasificacion, ''))

    @api.model
    def _accion_al_confirmar(self):
        """Qué etiqueta sale al confirmar un rollo en la revisadora: la preliminar de 68 x 38 mm (por defecto) o la final
        de 76 x 102 mm (parámetro `inspeccion_imprime` = `final`). La otra se imprime con su botón o desde Imprimir."""
        return self.env['ir.config_parameter'].sudo().get_param(PARAM_INSPECCION_IMPRIME, 'preliminar')

    def action_imprimir_etiqueta(self):
        if not all(self.mapped('zpl_etiqueta')):
            raise UserError(_('Hay rollos sin etiqueta final.'))
        return self.env.ref('quimibond_rollos_bitacora.action_report_etiqueta_rollo_final').report_action(self)

    def action_imprimir_etiqueta_preliminar(self):
        # Los rollos de antes de 3.4.0 no la guardaron: el reporte la arma al vuelo.
        return self.env.ref('quimibond_rollos_bitacora.action_report_etiqueta_rollo_preliminar').report_action(self)


class MrpRolloBitacoraFinal(models.Model):
    _inherit = 'mrp.rollo.bitacora'

    rollo_final_insp_ids = fields.Many2many('mrp.rollo.final', 'mrp_rollo_final_bitacora_rel', 'bitacora_id',
                                            'final_id', 'Rollos finales', readonly=True)
