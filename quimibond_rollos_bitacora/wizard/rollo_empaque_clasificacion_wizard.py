# -*- coding: utf-8 -*-
"""Clasificación final en Empaque (3.12.0, A3.3; Dirección, 2026-11-07).

Inspección clasifica de forma preliminar en la orden en kg; Empaque, ya en la orden en metros, escanea el rollo final
(``mrp.rollo.final``, el lote de la etiqueta preliminar) y confirma o cambia su calidad, sus metros y, si lo pesa, su peso.
Un cambio pide motivo. Al confirmar queda «Clasificado en Empaque» y sale la etiqueta final con lo definitivo. No se mueve
inventario: la orden en metros registra los rollos al cerrar (3.11.0) y no cierra con rollos sin clasificar.

El peso es opcional: si Empaque pesa, con la misma tara que Inspección (operación «Clasificación final» de las taras por
producto), ese peso manda; si no, el de Inspección.
"""
from odoo import api, fields, models, _
from odoo.exceptions import UserError

from .pesaje_manual import modo_por_defecto, motivo_por_defecto

from ..models.mrp_rollo_bitacora import CLASIFICACIONES, PARAM_ALERTA_DIFERENCIA_PCT


class RolloEmpaqueClasificacionWizard(models.TransientModel):
    _name = 'rollo.empaque.clasificacion.wizard'
    _inherit = ['scale.wizard.mixin']
    _description = 'Empaque: Clasificación final de un rollo'

    production_id = fields.Many2one('mrp.production', 'Orden (m)', required=True, readonly=True)
    workorder_id = fields.Many2one('mrp.workorder', 'Operación de Clasificación final', readonly=True)
    product_id = fields.Many2one(related='production_id.product_id', string='Artículo')
    workcenter_id = fields.Many2one(
        'mrp.workcenter', 'Centro de Clasificación final', default=lambda self: self._default_workcenter())
    empleado_numero = fields.Char('Número de empleado', required=True)
    empleado_nombre = fields.Char('Operador', compute='_compute_empleado')

    codigo = fields.Char('Lote del rollo final', help='Escanea la etiqueta preliminar de Inspección.')
    rollo_id = fields.Many2one('mrp.rollo.final', 'Rollo final', compute='_compute_rollo')
    codigo_error = fields.Char(compute='_compute_rollo')
    estado_rollo = fields.Selection(related='rollo_id.estado', string='Estado del rollo')
    clasificacion_insp = fields.Selection(related='rollo_id.clasificacion', string='Calidad de Inspección')
    metros_insp = fields.Float('Metros de Inspección', related='rollo_id.metros', digits=(12, 2))
    kg_insp = fields.Float('Kg de Inspección', related='rollo_id.kg_neto', digits=(12, 4))

    pesar = fields.Boolean('Pesar en Empaque', help='Si no se pesa, manda el peso de Inspección.')
    kg_bruto = fields.Float('Peso bruto (kg)', digits=(12, 4))
    tara_kg = fields.Float('Tara (kg)', compute='_compute_pesos', digits=(12, 4))
    tara_faltante = fields.Boolean(compute='_compute_pesos')
    kg_neto = fields.Float('Peso neto (kg)', compute='_compute_pesos', digits=(12, 4))
    motivo_manual = fields.Char('Motivo de la captura manual', default=lambda self: motivo_por_defecto(self))
    weighing_mode = fields.Selection(default=lambda self: modo_por_defecto(self))

    metros = fields.Float('Metros definitivos', digits=(12, 2), compute='_compute_valores', store=True, readonly=False)
    clasificacion = fields.Selection(CLASIFICACIONES, 'Calidad definitiva', compute='_compute_valores', store=True,
                                     readonly=False)
    defecto_ids = fields.Many2many('calidad.defecto.tela', 'rollo_empaque_clasif_defecto_rel', 'wizard_id', 'defecto_id',
                                   'Defectos', compute='_compute_valores', store=True, readonly=False,
                                   domain="[('tipo_tela', '=', 'tejido_acabado')]")
    motivo_ajuste = fields.Char('Motivo del cambio')
    cambia = fields.Boolean('Cambia la calidad o los metros', compute='_compute_alertas')
    alertas = fields.Text('Alertas', compute='_compute_alertas')
    pendientes = fields.Integer('Rollos por clasificar', compute='_compute_pendientes')

    @api.model
    def _default_workcenter(self):
        return self.env['mrp.workorder'].browse(self.env.context.get('default_workorder_id')).workcenter_id.id or False

    @api.depends('empleado_numero')
    def _compute_empleado(self):
        for wiz in self:
            emp = self.env['hr.employee']._qb_operador(wiz.empleado_numero)
            wiz.empleado_nombre = emp.name if emp else (_('EMPLEADO NO ENCONTRADO') if wiz.empleado_numero else False)

    @api.depends('codigo', 'production_id')
    def _compute_rollo(self):
        Final = self.env['mrp.rollo.final']
        for wiz in self:
            codigo = (wiz.codigo or '').strip()
            rollo = Final
            if codigo:
                rollo = Final.search([('name', '=', codigo), ('production_m_id', '=', wiz.production_id.id)], limit=1)
            wiz.rollo_id = rollo
            wiz.codigo_error = codigo if codigo and not rollo else False

    @api.depends('rollo_id')
    def _compute_valores(self):
        """Propone lo que ya tiene el rollo: lo de Empaque si ya lo clasificó (se puede corregir), si no lo de Inspección."""
        for wiz in self:
            rollo = wiz.rollo_id
            if not rollo:
                wiz.metros = 0.0
                wiz.clasificacion = False
                wiz.defecto_ids = False
                continue
            clasificado = bool(rollo.clasificacion_final)
            wiz.metros = rollo.metros_final if clasificado else rollo.metros
            wiz.clasificacion = rollo.clasificacion_final or rollo.clasificacion
            wiz.defecto_ids = rollo.defecto_final_ids if clasificado else rollo.defecto_ids

    @api.depends('product_id', 'kg_bruto', 'pesar')
    def _compute_pesos(self):
        Tara = self.env['mrp.tara.producto']
        for wiz in self:
            # Misma tara que Inspección (Dirección, 2026-11-07): una sola para Inspección y Clasificación final.
            tara = Tara._buscar_tara(wiz.product_id, 'clasificacion_final') if wiz.product_id else Tara
            wiz.tara_faltante = bool(wiz.pesar and wiz.product_id and not tara)
            wiz.tara_kg = tara.tara_kg
            wiz.kg_neto = wiz.kg_bruto - tara.tara_kg if (wiz.pesar and tara) else 0.0

    @api.depends('rollo_id', 'metros', 'clasificacion', 'kg_neto', 'pesar')
    def _compute_alertas(self):
        limite = float(self.env['ir.config_parameter'].sudo().get_param(PARAM_ALERTA_DIFERENCIA_PCT, 3.0))
        for wiz in self:
            rollo = wiz.rollo_id
            avisos = []
            wiz.cambia = bool(rollo) and (
                wiz.clasificacion != rollo.clasificacion or abs(wiz.metros - rollo.metros) > 0.005)
            if rollo and wiz.metros and rollo.metros and abs(wiz.metros - rollo.metros) > rollo.metros * limite / 100.0:
                avisos.append(_('%(n).2f m contra %(i).2f m de Inspección: más de %(l).1f %% de diferencia.',
                                n=wiz.metros, i=rollo.metros, l=limite))
            if rollo and wiz.pesar and wiz.kg_neto and abs(wiz.kg_neto - rollo.kg_neto) > rollo.kg_neto * limite / 100.0:
                avisos.append(_('%(n).2f kg contra %(i).2f kg de Inspección: más de %(l).1f %% de diferencia.',
                                n=wiz.kg_neto, i=rollo.kg_neto, l=limite))
            wiz.alertas = '\n'.join(avisos) or False

    @api.depends('production_id', 'rollo_id')
    def _compute_pendientes(self):
        for wiz in self:
            wiz.pendientes = len(wiz.production_id.rollo_final_m_ids.filtered(lambda r: r.estado == 'inspeccionado'))

    # ------------------------------------------------------------------ registro
    def _validar(self):
        self.ensure_one()
        orden = self.production_id
        if orden.state in ('done', 'cancel'):
            raise UserError(_('La orden %s ya está cerrada o cancelada: no se clasifican rollos.', orden.name))
        if not orden._insp_empaque_activo():
            raise UserError(_('La Clasificación final en Empaque no está activa.'))
        if self.codigo_error:
            raise UserError(_('El rollo %(c)s no está en %(o)s. Revisa que sea de un baño que ya cerró y que esa orden '
                              'en metros lo haya recibido.', c=self.codigo_error, o=orden.name))
        rollo = self.rollo_id
        if not rollo:
            raise UserError(_('Escanea la etiqueta preliminar del rollo.'))
        if rollo.estado == 'registrado':
            raise UserError(_('El rollo %s ya está registrado en inventario.', rollo.name))
        if self.pesar:
            if self.tara_faltante:
                raise UserError(_(
                    'No hay tara para %s. Captúrala en Manufactura → Configuración → Taras por producto (operación '
                    '«Clasificación final»); el pesaje no asume tara 0.', self.product_id.display_name))
            if self.kg_bruto <= 0:
                raise UserError(_('Captura el peso bruto del rollo.'))
            if self.kg_neto <= 0:
                raise UserError(_('El peso bruto (%(b).4f kg) no supera la tara (%(t).4f kg).', b=self.kg_bruto,
                                  t=self.tara_kg))
            if self.weighing_mode == 'manual' and not (self.motivo_manual or '').strip():
                raise UserError(_('Captura el motivo de la captura manual del peso.'))
        if not self.clasificacion:
            raise UserError(_('Elige la calidad definitiva del rollo.'))
        if self.metros < 0 or (self.metros == 0 and self.clasificacion != 'DESP'):
            raise UserError(_('Captura los metros del rollo.'))
        if self.clasificacion in ('SEGUNDA', 'FE') and not self.defecto_ids:
            raise UserError(_('Captura al menos un defecto: un rollo de %s necesita la causa.',
                              dict(CLASIFICACIONES).get(self.clasificacion)))
        if self.cambia and not (self.motivo_ajuste or '').strip():
            raise UserError(_('Captura el motivo del cambio: la calidad o los metros no son los de Inspección '
                              '(%(c)s, %(m).2f m).', c=dict(CLASIFICACIONES).get(rollo.clasificacion), m=rollo.metros))
        return self.env['hr.employee']._qb_operador_requerido(self.empleado_numero)

    def action_confirmar(self):
        self.ensure_one()
        rollo = self.rollo_id
        if rollo:  # dos estaciones que escanean el mismo rollo esperan una a la otra
            self.env.cr.execute('SELECT id FROM mrp_rollo_final WHERE id = %s FOR UPDATE', [rollo.id])
            self.env.invalidate_all()
        emp = self._validar()
        if emp and not emp.siglas:
            raise UserError(_('El operador %s no tiene siglas: captúralas en el empleado.', emp.name))
        ahora = fields.Datetime.now()
        operador = ' - '.join(filter(None, [self.empleado_numero, emp.name or False]))
        valores = {
            'clasificacion_final': self.clasificacion,
            'metros_final': self.metros,
            'defecto_final_ids': [(6, 0, self.defecto_ids.ids if self.clasificacion in ('SEGUNDA', 'FE') else [])],
            'motivo_ajuste_final': (self.motivo_ajuste or False) if self.cambia else False,
            'workcenter_final_id': self.workcenter_id.id or False,
            'operador_final': operador,
            'siglas_operador_final': emp.siglas or False,
            'fecha_final': ahora,
            'estado': 'clasificado',
            'kg_bruto_final': self.kg_bruto if self.pesar else 0.0,
            'tara_final': self.tara_kg if self.pesar else 0.0,
            'kg_neto_final': self.kg_neto if self.pesar else 0.0,
        }
        rollo.write(valores)
        rollo.zpl_etiqueta = rollo._zpl_etiqueta()
        if self.pesar:
            self.production_id._historial_pesaje(self.workcenter_id, rollo.name, self.kg_bruto, self.tara_kg,
                                                 self.kg_neto, operador, fecha=ahora)
        if rollo.reetiquetado:
            self.production_id.message_post(body=_(
                'Empaque cambió el rollo %(r)s: %(a)s → %(d)s, %(m).2f m. Motivo: %(mo)s.', r=rollo.name,
                a=dict(CLASIFICACIONES).get(rollo.clasificacion), d=dict(CLASIFICACIONES).get(rollo.clasificacion_final),
                m=rollo.metros_final, mo=self.motivo_ajuste or '—'))
        # Etiqueta final siempre (Dirección, 2026-11-07): la que va pegada al producto, con lo definitivo.
        accion = rollo.action_imprimir_etiqueta()
        accion.update({'close_on_report_download': True})
        return accion
