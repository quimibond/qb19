# -*- coding: utf-8 -*-
from odoo import api, fields, models, _
from odoo.exceptions import UserError

from odoo.addons.quimibond_rollos_bitacora.models.mrp_rollo_bitacora import (
    CLASIFICACIONES, PARAM_ALERTA_DIFERENCIA_PCT, PARAM_ALERTA_METROS_INSP)


class RolloClasificacionWizard(models.TransientModel):
    """Clasificación final de un rollo en la MO de B, centro Empaque (diseño §3 punto 12, §9, §12.3).

    El rollo de A se escanea (lote-serie). Segundo pesaje con la tara de Clasificación final (kg finales,
    referencia), metros finales (los de Inspección confirmados o remedidos), clasificación definitiva y
    defectos. Al registrar: crea el lote de B (mismo nombre que el de A, cantidad = metros finales), consume
    el kg real del lote de A y manda la etiqueta final PQ o PNC.
    """
    _name = 'rollo.clasificacion.wizard'
    _inherit = ['scale.wizard.mixin']
    _description = 'Clasificación final de rollo'

    production_id = fields.Many2one('mrp.production', 'Orden (m)', required=True, readonly=True)
    workorder_id = fields.Many2one('mrp.workorder', 'Operación de Clasificación final', required=True, readonly=True)
    workcenter_id = fields.Many2one(related='workorder_id.workcenter_id', string='Centro')
    product_id = fields.Many2one(related='production_id.product_id', string='Artículo')

    empleado_numero = fields.Char('Número de empleado', required=True)
    empleado_nombre = fields.Char('Operador', compute='_compute_empleado_nombre')

    rollo_codigo = fields.Char('Escanear rollo')
    rollo_id = fields.Many2one('mrp.rollo.bitacora', 'Rollo', compute='_compute_rollo')
    rollo_estado = fields.Selection(
        [('ok', 'Listo'), ('no_encontrado', 'No encontrado'), ('sin_inspeccion', 'Sin inspeccionar'),
         ('clasificado', 'Ya clasificado')], compute='_compute_rollo')
    kg_rama = fields.Float('Kg netos (Rama)', related='rollo_id.kg_rama', digits=(12, 4))
    metros_insp = fields.Float('Metros (Inspección)', related='rollo_id.metros_insp', digits=(12, 2))
    clasificacion_inicial = fields.Selection(related='rollo_id.clasificacion_inicial')

    kg_bruto = fields.Float('Peso bruto (kg)', digits=(12, 4))
    tara_kg = fields.Float('Tara de Clasificación final (kg)', digits=(12, 4), compute='_compute_pesos')
    tara_faltante = fields.Boolean(compute='_compute_pesos')
    kg_neto = fields.Float('Peso neto (kg)', digits=(12, 4), compute='_compute_pesos')

    metros = fields.Float('Metros finales', digits=(12, 2))
    motivo_ajuste_metros = fields.Char('Motivo del ajuste de metros')
    clasificacion = fields.Selection(CLASIFICACIONES, 'Clasificación definitiva')
    defecto_ids = fields.Many2many(
        'calidad.defecto.tela', 'rollo_clasificacion_wizard_defecto_rel', 'wizard_id', 'defecto_id', 'Defectos',
        domain="[('tipo_tela', '=', 'tejido_acabado')]")
    alertas = fields.Text('Alertas', compute='_compute_alertas')

    @api.depends('empleado_numero')
    def _compute_empleado_nombre(self):
        Empleado = self.env['hr.employee']
        for wiz in self:
            nombre = False
            if wiz.empleado_numero:
                emp = Empleado._qb_operador(wiz.empleado_numero)  # 3.5.1: número = referencia S-<número>
                nombre = emp.name if emp else _('EMPLEADO NO ENCONTRADO')
            wiz.empleado_nombre = nombre

    @api.depends('rollo_codigo', 'production_id')
    def _compute_rollo(self):
        Bitacora = self.env['mrp.rollo.bitacora']
        for wiz in self:
            codigo = (wiz.rollo_codigo or '').strip()
            rollo = Bitacora
            if codigo and wiz.production_id:
                producto_a = wiz.production_id._clasif_move_a().product_id
                rollo = Bitacora.search([('name', '=', codigo), ('product_id', '=', producto_a.id)], limit=1)
            wiz.rollo_id = rollo
            if not codigo:
                wiz.rollo_estado = False
            elif not rollo:
                wiz.rollo_estado = 'no_encontrado'
            elif rollo.estado_inspeccion != 'inspeccionado':
                wiz.rollo_estado = 'sin_inspeccion'
            elif rollo.production_b_id:
                wiz.rollo_estado = 'clasificado'
            else:
                wiz.rollo_estado = 'ok'

    @api.depends('product_id', 'kg_bruto')
    def _compute_pesos(self):
        Tara = self.env['mrp.tara.producto']
        for wiz in self:
            tara = Tara._buscar_tara(wiz.product_id, 'clasificacion_final') if wiz.product_id else Tara
            wiz.tara_faltante = bool(wiz.product_id) and not tara
            wiz.tara_kg = tara.tara_kg
            wiz.kg_neto = wiz.kg_bruto - tara.tara_kg if tara else 0.0

    @api.depends('metros', 'metros_insp', 'kg_neto', 'rollo_id')
    def _compute_alertas(self):
        """Mismas alertas que la bitácora (§9.1), mostradas antes de registrar."""
        params = self.env['ir.config_parameter'].sudo()
        limite_metros = float(params.get_param(PARAM_ALERTA_METROS_INSP, 1.0))
        limite_pct = float(params.get_param(PARAM_ALERTA_DIFERENCIA_PCT, 3.0))
        for wiz in self:
            avisos = []
            if wiz.rollo_id and wiz.metros:
                dif = wiz.metros - wiz.metros_insp
                if abs(dif) > limite_metros:
                    avisos.append(_('Los metros finales difieren %.2f m de los de Inspección (límite %.2f m).',
                                    dif, limite_metros))
                teorico = wiz.rollo_id._rendimiento_acabado()
                esperado = wiz.kg_neto * teorico
                if esperado and abs(wiz.metros - esperado) > esperado * limite_pct / 100.0:
                    avisos.append(_('Los metros finales difieren %.2f m de los teóricos (%.2f m, límite %.1f %%).',
                                    wiz.metros - esperado, esperado, limite_pct))
            wiz.alertas = '\n'.join(avisos)

    @api.onchange('rollo_id')
    def _onchange_rollo_id(self):
        rollo = self.rollo_id
        if rollo:
            self.metros = rollo.metros_insp
            self.clasificacion = rollo.clasificacion_inicial
            self.defecto_ids = rollo.defecto_ids
            self.motivo_ajuste_metros = False

    def _siglas_operador_insp(self, rollo):
        """Siglas del operador que inspeccionó el rollo (el número de empleado va antes del « - »)."""
        numero = (rollo.operador_insp or '').split(' - ')[0].strip()
        emp = self.env['hr.employee']._qb_operador(numero)
        if not emp.siglas:
            raise UserError(_(
                'El operador de Inspección del rollo (%s) no tiene siglas. Captúralas en el empleado '
                '(campo Siglas) antes de imprimir la etiqueta.', rollo.operador_insp or _('sin operador')))
        return emp.siglas

    def action_confirmar(self):
        self.ensure_one()
        rollo = self.rollo_id
        produccion = self.production_id
        mensajes = {
            'no_encontrado': _('Ese código no corresponde a un rollo del Producto A de esta orden.'),
            'sin_inspeccion': _('El rollo %s no ha sido inspeccionado.', rollo.name),
            'clasificado': _('El rollo %s ya fue clasificado.', rollo.name),
        }
        if self.rollo_estado in mensajes:
            raise UserError(mensajes[self.rollo_estado])
        if not rollo:
            raise UserError(_('Escanea el código de barras del rollo.'))
        if self.tara_faltante:
            raise UserError(_(
                'No hay tara de Clasificación final para el artículo %s. Captúrala en Manufactura → '
                'Configuración → Taras por producto; el pesaje no asume tara 0.', self.product_id.display_name))
        if self.kg_bruto <= 0:
            raise UserError(_('Captura el peso bruto del rollo.'))
        if self.kg_neto <= 0:
            raise UserError(_('El peso bruto (%.4f kg) no supera la tara (%.4f kg).', self.kg_bruto, self.tara_kg))
        if self.metros <= 0:
            raise UserError(_('Captura los metros finales del rollo.'))
        if not self.clasificacion:
            raise UserError(_('Elige la clasificación definitiva del rollo.'))
        remedido = abs(self.metros - rollo.metros_insp) > 0.005
        if remedido and not (self.motivo_ajuste_metros or '').strip():
            raise UserError(_('Los metros finales difieren de los de Inspección: captura el motivo del ajuste.'))
        if produccion.product_id.tracking != 'lot':
            raise UserError(_('El artículo %s debe tener seguimiento por lote.', produccion.product_id.display_name))
        siglas = self._siglas_operador_insp(rollo)
        produccion._clasif_validar_rollo_disponible(rollo)

        lote_name = rollo.lot_id.name
        Lote = self.env['stock.lot']
        if Lote.search_count([('name', '=', lote_name), ('product_id', '=', produccion.product_id.id),
                              ('company_id', '=', produccion.company_id.id)]):
            raise UserError(_('Ya existe el lote %s para el artículo %s.', lote_name,
                              produccion.product_id.display_name))
        lote_b = Lote.create({'name': lote_name, 'product_id': produccion.product_id.id,
                              'company_id': produccion.company_id.id})
        ahora = fields.Datetime.now()
        rollo.write({
            'production_b_id': produccion.id,
            'lot_b_id': lote_b.id,
            'workcenter_final_id': self.workcenter_id.id,
            'operador_final': ' - '.join(filter(None, [self.empleado_numero, self.empleado_nombre])),
            'siglas_operador_final': siglas,
            'fecha_registro_final': ahora,
            'fecha_etiqueta_final': ahora,
            'kg_bruto_final': self.kg_bruto,
            'tara_final': self.tara_kg,
            'kg_final': self.kg_neto,
            'captura_manual_final': self.weighing_mode == 'manual',
            'metros_final': self.metros,
            'remedido': remedido,
            'motivo_ajuste_metros': (self.motivo_ajuste_metros or False) if remedido else False,
            'clasificacion_final': self.clasificacion,
            'defecto_ids': [(6, 0, self.defecto_ids.ids)],
        })
        rollo.zpl_etiqueta_final = rollo._zpl_etiqueta_final()
        produccion._clasif_registrar_rollo(rollo, lote_b, self.workorder_id)
        accion = rollo.action_imprimir_etiqueta_final()
        accion.update({'close_on_report_download': True})
        return accion
