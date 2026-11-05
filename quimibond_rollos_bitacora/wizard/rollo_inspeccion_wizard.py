# -*- coding: utf-8 -*-
from odoo import api, fields, models, _
from odoo.exceptions import UserError

from odoo.addons.quimibond_rollos_bitacora.models.mrp_rollo_bitacora import CLASIFICACIONES


class RolloInspeccionWizard(models.TransientModel):
    """Inspección de un rollo (diseño §3, punto 10; §12.2).

    El rollo se elige escaneando el código de barras de la etiqueta de Rama (el lote-serie). Por rollo:
    clasificación inicial, no. de empalmes, defectos y metros medidos (referencia). Inspección NO pesa
    (usa el kg neto de Rama) y no lleva control de calidad. Al guardar imprime la etiqueta preliminar.
    """
    _name = 'rollo.inspeccion.wizard'
    _description = 'Inspección de rollo'

    production_id = fields.Many2one('mrp.production', 'Orden (kg)', required=True, readonly=True)
    workorder_id = fields.Many2one('mrp.workorder', 'Operación de Inspección', required=True, readonly=True)
    product_id = fields.Many2one(related='production_id.product_id', string='Artículo')
    workcenter_id = fields.Many2one(
        'mrp.workcenter', 'Máquina de Inspección (Revisadora)', required=True,
        domain="[('ft_tipo_id.name', '=', 'Inspección')]",
        default=lambda self: self._default_workcenter())

    empleado_numero = fields.Char('Número de empleado', required=True)
    empleado_nombre = fields.Char('Operador', compute='_compute_empleado_nombre')

    rollo_codigo = fields.Char('Escanear rollo')
    rollo_id = fields.Many2one('mrp.rollo.bitacora', 'Rollo', compute='_compute_rollo')
    rollo_no_encontrado = fields.Boolean(compute='_compute_rollo')
    kg_neto = fields.Float('Kg netos (Rama)', related='rollo_id.kg_rama', digits=(12, 4))
    estado_inspeccion = fields.Selection(related='rollo_id.estado_inspeccion')
    pendientes = fields.Integer('Rollos pendientes', compute='_compute_pendientes')

    clasificacion = fields.Selection(CLASIFICACIONES, 'Clasificación inicial')
    no_empalmes = fields.Integer('No. de empalmes')
    metros = fields.Float('Metros', digits=(12, 2))
    defecto_ids = fields.Many2many(
        'calidad.defecto.tela', 'rollo_inspeccion_wizard_defecto_rel', 'wizard_id', 'defecto_id', 'Defectos',
        domain="[('tipo_tela', '=', 'tejido_acabado')]")

    @api.model
    def _default_workcenter(self):
        orden = self.env['mrp.workorder'].browse(self.env.context.get('default_workorder_id'))
        return orden.workcenter_id.id or False

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
            rollo = Bitacora.search(
                [('production_id', '=', wiz.production_id.id), ('name', '=', codigo)], limit=1) if codigo else Bitacora
            wiz.rollo_id = rollo
            wiz.rollo_no_encontrado = bool(codigo) and not rollo

    @api.depends('production_id', 'rollo_id')
    def _compute_pendientes(self):
        for wiz in self:
            wiz.pendientes = len(wiz.production_id.rollo_bitacora_ids.filtered(
                lambda r: r.estado_inspeccion != 'inspeccionado'))

    @api.onchange('rollo_id')
    def _onchange_rollo_id(self):
        """Si el rollo ya tenía datos (reinspección), se cargan para corregirlos."""
        rollo = self.rollo_id
        if rollo and rollo.estado_inspeccion == 'inspeccionado':
            self.clasificacion = rollo.clasificacion_inicial
            self.no_empalmes = rollo.no_empalmes
            self.metros = rollo.metros_insp
            self.defecto_ids = rollo.defecto_ids

    def action_confirmar(self):
        self.ensure_one()
        rollo = self.rollo_id
        if not rollo:
            raise UserError(_('Escanea el código de barras de la etiqueta de Rama del rollo.'))
        if rollo.estado_inspeccion == 'inspeccionado' and not self.env.user.has_group('mrp.group_mrp_manager'):
            raise UserError(_('El rollo %s ya fue inspeccionado. Solo un responsable de Manufactura puede '
                              'corregirlo.', rollo.name))
        if not self.clasificacion:
            raise UserError(_('Elige la clasificación inicial del rollo.'))
        if self.metros <= 0:
            raise UserError(_('Captura los metros medidos del rollo.'))
        if self.no_empalmes < 0:
            raise UserError(_('El número de empalmes no puede ser negativo.'))
        self.env['mrp.inspeccion.restriccion']._validar_centro(self.workcenter_id, self.production_id.product_id)

        ahora = fields.Datetime.now()
        rollo.write({
            'estado_inspeccion': 'inspeccionado',
            'clasificacion_inicial': self.clasificacion,
            'no_empalmes': self.no_empalmes,
            'metros_insp': self.metros,
            'defecto_ids': [(6, 0, self.defecto_ids.ids)],
            'workcenter_insp_id': self.workcenter_id.id,
            'operador_insp': ' - '.join(filter(None, [self.empleado_numero, self.empleado_nombre])),
            'fecha_registro_insp': ahora,
            'fecha_etiqueta_preliminar': ahora,
        })
        rollo.zpl_etiqueta_preliminar = rollo._zpl_etiqueta_preliminar()
        accion = rollo.action_imprimir_etiqueta_preliminar()
        accion.update({'close_on_report_download': True})
        return accion
