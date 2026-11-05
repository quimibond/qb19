# -*- coding: utf-8 -*-
from odoo import api, fields, models, _
from odoo.exceptions import UserError

from .pesaje_manual import modo_por_defecto, motivo_por_defecto


class RolloPesajeRamaWizard(models.TransientModel):
    """Pesaje y registro de un rollo en Rama (diseño §8, puntos 8 y 9; §12.1; §13).

    Único pesaje del rollo en la orden en kg: peso bruto (báscula IoT o captura manual con motivo)
    menos la tara de Rama = kg netos = cantidad del lote de A. Genera el lote-serie, la bitácora,
    el movimiento de la MO y la etiqueta pequeña con solo el código de barras.
    """
    _name = 'rollo.pesaje.rama.wizard'
    _inherit = ['scale.wizard.mixin']
    _description = 'Pesaje de rollo en Rama'

    production_id = fields.Many2one('mrp.production', 'Orden (kg)', required=True, readonly=True)
    workorder_id = fields.Many2one('mrp.workorder', 'Operación de Rama', required=True, readonly=True)
    workcenter_id = fields.Many2one(related='workorder_id.workcenter_id', string='Rama')
    product_id = fields.Many2one(related='production_id.product_id', string='Artículo')
    lote_origen_id = fields.Many2one('stock.lot', 'Lote de tela cruda (opcional)')

    empleado_numero = fields.Char('Número de empleado', required=True)
    empleado_nombre = fields.Char('Operador', compute='_compute_empleado_nombre')

    secuencia = fields.Integer('No. de rollo', compute='_compute_secuencia')
    lote_name = fields.Char('Lote a generar', compute='_compute_secuencia')

    kg_bruto = fields.Float('Peso bruto (kg)', digits=(12, 4))
    tara_kg = fields.Float('Tara de Rama (kg)', digits=(12, 4), compute='_compute_pesos')
    tara_faltante = fields.Boolean(compute='_compute_pesos')
    kg_neto = fields.Float('Peso neto (kg)', digits=(12, 4), compute='_compute_pesos')
    motivo_manual = fields.Char('Motivo de la captura manual',
                                help='3.3.0: obligatorio cuando el peso se captura a mano (v5.4 §8.8 y §13).',
                                default=lambda self: motivo_por_defecto(self))
    weighing_mode = fields.Selection(default=lambda self: modo_por_defecto(self))  # 3.3.9

    @api.depends('empleado_numero')
    def _compute_empleado_nombre(self):
        Empleado = self.env['hr.employee']
        campo = 'x_studio_nmero_de_trabajador'
        for wiz in self:
            nombre = False
            if wiz.empleado_numero and campo in Empleado._fields:
                emp = Empleado.search([(campo, '=', wiz.empleado_numero)], limit=1)
                nombre = emp.name if emp else _('EMPLEADO NO ENCONTRADO')
            wiz.empleado_nombre = nombre

    def _siguiente_secuencia(self):
        self.ensure_one()
        ultimo = self.env['mrp.rollo.bitacora'].search(
            [('production_id', '=', self.production_id.id)], order='secuencia desc', limit=1)
        return (ultimo.secuencia or 0) + 1

    def _nombre_lote(self, secuencia):
        """Lote del rollo de Rama = número de baño + «J» (acabado) + No. de rollo: «00001-J0001» (3.5.0). La «J» lo
        distingue del rollo final de Inspección («00001-0001»). Sin número todavía, solo la vista previa."""
        self.ensure_one()
        return '%s-J%04d' % (self.production_id.numero_bano or _('(baño)'), secuencia)

    @api.depends('production_id')
    def _compute_secuencia(self):
        for wiz in self:
            if wiz.production_id:
                wiz.secuencia = wiz._siguiente_secuencia()
                wiz.lote_name = wiz._nombre_lote(wiz.secuencia)
            else:
                wiz.secuencia = 0
                wiz.lote_name = False

    @api.depends('product_id', 'kg_bruto')
    def _compute_pesos(self):
        Tara = self.env['mrp.tara.producto']
        for wiz in self:
            tara = Tara._buscar_tara(wiz.product_id, 'rama') if wiz.product_id else Tara
            wiz.tara_faltante = bool(wiz.product_id) and not tara
            wiz.tara_kg = tara.tara_kg
            wiz.kg_neto = wiz.kg_bruto - tara.tara_kg if tara else 0.0

    def action_confirmar(self):
        self.ensure_one()
        produccion = self.production_id
        if produccion.state in ('done', 'cancel'):
            # 3.3.5: un asistente abierto no registra rollos en una orden cerrada o cancelada.
            raise UserError(_('La orden %s ya está cerrada o cancelada: no se pesan rollos.', produccion.name))
        if self.kg_bruto <= 0:
            raise UserError(_('Captura el peso bruto del rollo.'))
        if self.tara_faltante:
            raise UserError(_(
                'No hay tara de Rama para el artículo %s. Captúrala en Manufactura → Configuración → Taras '
                'por producto; el pesaje no asume tara 0.', self.product_id.display_name))
        if self.kg_neto <= 0:
            raise UserError(_('El peso bruto (%.4f kg) no supera la tara (%.4f kg).', self.kg_bruto, self.tara_kg))
        manual = self.weighing_mode == 'manual'
        if manual and not (self.motivo_manual or '').strip():
            raise UserError(_('Captura el motivo de la captura manual del peso.'))
        if produccion.product_id.tracking != 'lot':
            raise UserError(_(
                'El artículo %s debe tener seguimiento por lote para registrar rollos.',
                produccion.product_id.display_name))

        produccion._asignar_numero_bano()
        secuencia = self._siguiente_secuencia()
        lote_name = self._nombre_lote(secuencia)
        if self.env['stock.lot'].search_count([
                ('name', '=', lote_name), ('product_id', '=', produccion.product_id.id),
                ('company_id', '=', produccion.company_id.id)]):
            raise UserError(_('Ya existe el lote %s para este artículo.', lote_name))

        kg_neto = produccion.product_id.uom_id.round(self.kg_neto)
        lote = self.env['stock.lot'].create({
            'name': lote_name,
            'product_id': produccion.product_id.id,
            'company_id': produccion.company_id.id,
        })
        ahora = fields.Datetime.now()
        rollo = self.env['mrp.rollo.bitacora'].create({
            'production_id': produccion.id,
            'secuencia': secuencia,
            'lote_origen_id': self.lote_origen_id.id,
            'lot_id': lote.id,
            'workcenter_rama_id': self.workcenter_id.id,
            'kg_bruto_rama': self.kg_bruto,
            'tara_rama': self.tara_kg,
            'kg_rama': kg_neto,
            'captura_manual_rama': manual,
            'motivo_manual_rama': (self.motivo_manual or '').strip() or False if manual else False,
            'operador_rama': ' - '.join(filter(None, [self.empleado_numero, self.empleado_nombre])),
            'fecha_registro_rama': ahora,
            'fecha_etiqueta_rama': ahora,
        })
        rollo.zpl_etiqueta_rama = rollo._zpl_etiqueta_rama()
        produccion._rama_registrar_movimiento(lote, kg_neto, self.workorder_id)
        produccion._historial_pesaje(self.workcenter_id, lote_name, self.kg_bruto, self.tara_kg, kg_neto,
                                     rollo.operador_rama, lot=lote, fecha=ahora)
        # Cierra la ventana de pesaje al descargar/imprimir la etiqueta: los rollos salen de Rama
        # espaciados en el tiempo, cada uno se pesa abriendo de nuevo PESAR ROLLO.
        accion = rollo.action_imprimir_etiqueta_rama()
        accion.update({'close_on_report_download': True})
        return accion
