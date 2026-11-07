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
    clasificacion = fields.Selection(CLASIFICACIONES, 'Calidad (Inspección, preliminar)', required=True, readonly=True,
                                     help='La calidad con la que Inspección clasifica el rollo. La definitiva la confirma o '
                                          'la cambia Clasificación final en Empaque (3.10.0).')
    defecto_ids = fields.Many2many('calidad.defecto.tela', 'mrp_rollo_final_defecto_rel', 'final_id', 'defecto_id',
                                   'Defectos', readonly=True)
    siglas_defectos = fields.Char('Siglas de defectos', compute='_compute_siglas_defectos')
    workcenter_id = fields.Many2one('mrp.workcenter', 'Revisadora', readonly=True)
    operador = fields.Char('Operador de Inspección', readonly=True)
    siglas_operador = fields.Char('Siglas del operador', readonly=True)
    fecha = fields.Datetime('Inspeccionado el', readonly=True)
    zpl_etiqueta = fields.Text('ZPL etiqueta final', readonly=True, copy=False)
    zpl_etiqueta_preliminar = fields.Text('ZPL etiqueta preliminar', readonly=True, copy=False)

    estado = fields.Selection([('inspeccionado', 'Inspeccionado, sin registrar'),
                               ('clasificado', 'Clasificado en Empaque'), ('registrado', 'Registrado')],
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

    # ------------------------------------------------------------------ 3.10.0: Clasificación final (Empaque)
    # Inspección clasifica de forma preliminar en la orden en kg; Empaque, ya en la orden en metros, confirma o cambia
    # la calidad según Calidad (Dirección, 2026-11-07). Estos campos los llena la estación de Empaque (A3.3); la
    # orden en metros registra con los valores «de registro»: los de Empaque si los hay, si no los de Inspección.
    clasificacion_final = fields.Selection(
        CLASIFICACIONES, 'Calidad definitiva (Empaque)', readonly=True, copy=False, index=True,
        help='La calidad que confirma o cambia Clasificación final. Vacía mientras Empaque no clasifique el rollo.')
    kg_bruto_final = fields.Float('Peso bruto en Empaque (kg)', digits=(12, 4), readonly=True, copy=False)
    tara_final = fields.Float('Tara en Empaque (kg)', digits=(12, 4), readonly=True, copy=False)
    kg_neto_final = fields.Float('Kg netos en Empaque', digits=(12, 4), readonly=True, copy=False,
                                 help='Si Empaque pesa el rollo, este peso manda; si no, el de Inspección.')
    metros_final = fields.Float('Metros definitivos (Empaque)', digits=(12, 2), readonly=True, copy=False)
    motivo_ajuste_final = fields.Char('Motivo del cambio en Empaque', readonly=True, copy=False)
    defecto_final_ids = fields.Many2many(
        'calidad.defecto.tela', 'mrp_rollo_final_defecto_final_rel', 'final_id', 'defecto_id',
        'Defectos definitivos (Empaque)', readonly=True, copy=False)
    siglas_defectos_final = fields.Char('Siglas de defectos definitivos', compute='_compute_siglas_defectos_final')
    workcenter_final_id = fields.Many2one('mrp.workcenter', 'Centro de Clasificación final', readonly=True, copy=False)
    operador_final = fields.Char('Operador de Clasificación final', readonly=True, copy=False)
    siglas_operador_final = fields.Char('Siglas del operador de Empaque', readonly=True, copy=False)
    fecha_final = fields.Datetime('Clasificado en Empaque el', readonly=True, copy=False)
    reetiquetado = fields.Boolean(
        'Reetiquetado', compute='_compute_valores_registro', store=True,
        help='Empaque cambió la calidad que Inspección había puesto: hay que reimprimir la etiqueta final.')
    clasificacion_registro = fields.Selection(
        CLASIFICACIONES, 'Calidad de registro', compute='_compute_valores_registro', store=True,
        help='Con la que se registra en inventario: la definitiva de Empaque si existe, si no la de Inspección.')
    kg_registro = fields.Float('Kg de registro', digits=(12, 4), compute='_compute_valores_registro', store=True,
                               help='Los kg de Empaque si pesó el rollo; si no, los de Inspección.')
    metros_registro = fields.Float('Metros de registro', digits=(12, 2), compute='_compute_valores_registro',
                                   store=True, help='Los metros de Empaque si ya clasificó el rollo; si no, los de Inspección.')

    _unique_nombre = models.Constraint('unique(name, company_id)', 'Ese rollo final ya existe.')
    _check_kg = models.Constraint('CHECK(kg_neto > 0)', 'Los kg netos del rollo final deben ser mayores a cero.')
    # 3.6.2: el Desperdicio sale en kg y puede no tener metros (pedazos, orillas); Primera, FE y Segunda sí los llevan.
    _check_metros = models.Constraint("CHECK(metros > 0 OR (clasificacion = 'DESP' AND metros >= 0))",
                                      'Los metros del rollo final deben ser mayores a cero (salvo en Desperdicio).')

    @api.depends('defecto_ids.clave')
    def _compute_siglas_defectos(self):
        for rollo in self:
            rollo.siglas_defectos = ' '.join(rollo.defecto_ids.sorted('clave').mapped('clave'))

    @api.depends('defecto_final_ids.clave')
    def _compute_siglas_defectos_final(self):
        for rollo in self:
            rollo.siglas_defectos_final = ' '.join(rollo.defecto_final_ids.sorted('clave').mapped('clave'))

    @api.depends('clasificacion', 'kg_neto', 'metros', 'clasificacion_final', 'kg_neto_final', 'metros_final')
    def _compute_valores_registro(self):
        """3.10.0: lo que manda al registrar es lo de Empaque si ya clasificó el rollo y, si no, lo de Inspección. El peso
        de Empaque solo cuenta si Empaque pesó (mayor a cero)."""
        for rollo in self:
            clasificado = bool(rollo.clasificacion_final)
            rollo.reetiquetado = clasificado and rollo.clasificacion_final != rollo.clasificacion
            rollo.clasificacion_registro = rollo.clasificacion_final or rollo.clasificacion
            rollo.kg_registro = rollo.kg_neto_final if (clasificado and rollo.kg_neto_final > 0) else rollo.kg_neto
            rollo.metros_registro = rollo.metros_final if clasificado else rollo.metros

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
    @api.model
    def _siguiente_secuencia(self, orden_m, orden_kg=None):
        """Consecutivo por baño (orden en kg), 3.5.0: el lote lleva el número de baño, así que cada baño tiene su serie
        aunque varios abastezcan una orden en metros. `orden_m` queda por compatibilidad."""
        # 3.3.5: sin huecos. Al anular un rollo de en medio su número queda libre y el siguiente lo toma; un rollo anulado
        # no tiene lote (solo se registra al cerrar el baño), así que el nombre no choca.
        usados = set(self.search([('production_id', '=', orden_kg.id)]).mapped('secuencia')) if orden_kg else set()
        secuencia = 1
        while secuencia in usados:
            secuencia += 1
        return secuencia

    @api.model
    def _nombre_lote(self, orden_m, secuencia, orden_kg=None):
        """Lote del rollo final = número de baño + No. de rollo: «00001-0001» (3.5.0; antes M<orden en metros>-NNNN o
        K<orden en kg>-NNNN). Es el que queda en inventario y va en la etiqueta final."""
        return '%s-%04d' % ((orden_kg.numero_bano if orden_kg else False) or _('(baño)'), secuencia)

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
        # 3.12.0: la etiqueta final lleva lo definitivo: lo que Empaque confirmó o cambió; sin clasificar, lo de Inspección.
        clasificado = bool(self.clasificacion_final)
        clasif = self.clasificacion_registro or self.clasificacion
        metros, kg_neto = self.metros_registro, self.kg_registro
        kg_bruto = self.kg_bruto_final if (clasificado and self.kg_neto_final > 0) else self.kg_bruto
        siglas_defectos = self.siglas_defectos_final if clasificado else self.siglas_defectos
        pnc = clasif != 'PQ'
        fecha = fields.Datetime.context_timestamp(self, self.fecha or fields.Datetime.now())
        of = self.name.rsplit('-', 1)[0]
        renglones = [
            'CÓDIGO: %s%s' % (datos['codigo'], SUFIJO_CODIGO.get(clasif, '')),
            'COLOR: %s' % datos['color'],
            'ANCHO (cm): %g' % datos['ancho'],
            ('METROS: %.2f' % metros, 'YARDAS: %.2f' % (metros * YARDAS_POR_METRO)),
            ('KILOS: %.2f' % kg_neto, 'LIBRAS: %.2f' % (kg_neto * LIBRAS_POR_KG)) if pnc
            else ('PESO NETO: %.2f' % kg_neto, 'PESO BRUTO: %.2f' % kg_bruto),
            'LOTE DE PRODUCCIÓN: %s' % of,
            'NO. DE ROLLO: %s' % self.secuencia,
            'OPERADOR INSPECCIÓN: %s' % (self.siglas_operador or ''),
            'FECHA: %s' % fecha.strftime('%d/%m/%Y'),
        ]
        codigos = ['%.2f;%.3f;%s;%s' % (metros, kg_neto, of, self.secuencia)]
        if pnc:
            renglones.append('DEFECTOS: %s' % (siglas_defectos or ''))
            codigos.append(('%s %s' % (self.secuencia, siglas_defectos or '')).strip())
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
