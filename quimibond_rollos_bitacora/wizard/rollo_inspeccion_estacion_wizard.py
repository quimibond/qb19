# -*- coding: utf-8 -*-
"""Inspección como estación única (diseño 2026-11-03, D2, D3, D4, D6, D8 y §3.2).

Por cada rollo final que sale de la revisadora: se escanea el rollo de Rama (o varios del mismo baño para juntarlos),
se pesa con la tara de Inspección, se capturan metros, calidad y defectos, se le asigna su lote ``M…`` y se imprime la
etiqueta final. Para cortar, se registra un rollo final por pedazo con «Último pedazo» apagado y el último con él
encendido: entonces el rollo de Rama queda inspeccionado. No se mueve inventario (D7).
"""
import re

from odoo import api, fields, models, _
from odoo.exceptions import UserError

from ..models.mrp_rollo_bitacora import CLASIFICACIONES
from ..models.mrp_rollo_final import PARAM_ALERTA_MERMA_PCT


class RolloInspeccionEstacionWizard(models.TransientModel):
    _name = 'rollo.inspeccion.estacion.wizard'
    _inherit = ['scale.wizard.mixin']
    _description = 'Inspección: rollo final'

    production_id = fields.Many2one('mrp.production', 'Orden (kg)', required=True, readonly=True)
    workorder_id = fields.Many2one('mrp.workorder', 'Operación de Inspección', required=True, readonly=True)
    product_id = fields.Many2one(related='production_id.product_id', string='Artículo')
    workcenter_id = fields.Many2one(
        'mrp.workcenter', 'Revisadora', required=True, default=lambda self: self._default_workcenter(),
        domain="[('ft_tipo_id.name', '=', 'Inspección')]")
    empleado_numero = fields.Char('Número de empleado', required=True)
    empleado_nombre = fields.Char('Operador', compute='_compute_empleado')

    rollo_codigos = fields.Char('Rollos de Rama', help='Escanea la etiqueta de Rama. Para juntar rollos del mismo baño, '
                                                       'escanea varios separados por espacio.')
    bitacora_ids = fields.Many2many('mrp.rollo.bitacora', compute='_compute_bitacora', string='Rollos de Rama')
    codigos_error = fields.Char(compute='_compute_bitacora')
    termina_rollo = fields.Boolean('Último pedazo', default=True,
                                   help='Apágalo si vas a cortar: el rollo de Rama queda pendiente hasta registrar el '
                                        'último pedazo.')
    kg_rama = fields.Float('Kg de Rama', compute='_compute_bitacora', digits=(12, 4))
    kg_ya_cortados = fields.Float('Kg de pedazos anteriores', compute='_compute_bitacora', digits=(12, 4))

    kg_bruto = fields.Float('Peso bruto (kg)', digits=(12, 4))
    tara_kg = fields.Float('Tara de Inspección (kg)', compute='_compute_pesos', digits=(12, 4))
    tara_faltante = fields.Boolean(compute='_compute_pesos')
    kg_neto = fields.Float('Peso neto (kg)', compute='_compute_pesos', digits=(12, 4))
    motivo_manual = fields.Char('Motivo de la captura manual')
    metros = fields.Float('Metros', digits=(12, 2))
    no_empalmes = fields.Integer('No. de empalmes')
    clasificacion = fields.Selection(CLASIFICACIONES, 'Calidad')
    defecto_ids = fields.Many2many('calidad.defecto.tela', 'rollo_insp_estacion_defecto_rel', 'wizard_id', 'defecto_id',
                                   'Defectos', domain="[('tipo_tela', '=', 'tejido_acabado')]")

    orden_m_id = fields.Many2one('mrp.production', 'Orden (m)', compute='_compute_lote')
    lote_name = fields.Char('Lote del rollo final', compute='_compute_lote')
    merma_kg = fields.Float('Merma (kg)', compute='_compute_merma', digits=(12, 4))
    merma_pct = fields.Float('Merma (%)', compute='_compute_merma', digits=(12, 2))
    alertas = fields.Text('Alertas', compute='_compute_merma')
    pendientes = fields.Integer('Rollos de Rama sin inspeccionar', compute='_compute_pendientes')

    @api.model
    def _default_workcenter(self):
        return self.env['mrp.workorder'].browse(self.env.context.get('default_workorder_id')).workcenter_id.id or False

    @api.model
    def _empleado(self, numero):
        Empleado = self.env['hr.employee']
        campo = 'x_studio_nmero_de_trabajador'
        if not numero or campo not in Empleado._fields:
            return Empleado
        return Empleado.search([(campo, '=', numero)], limit=1)

    @api.depends('empleado_numero')
    def _compute_empleado(self):
        for wiz in self:
            emp = wiz._empleado(wiz.empleado_numero)
            wiz.empleado_nombre = emp.name if emp else (_('EMPLEADO NO ENCONTRADO') if wiz.empleado_numero else False)

    @api.model
    def _codigos(self, texto):
        return [c for c in re.split(r'[\s,;]+', (texto or '').strip()) if c]

    @api.depends('rollo_codigos', 'production_id')
    def _compute_bitacora(self):
        Bitacora = self.env['mrp.rollo.bitacora']
        for wiz in self:
            codigos = wiz._codigos(wiz.rollo_codigos)
            rollos = Bitacora.search([('name', 'in', codigos)]) if codigos else Bitacora
            wiz.bitacora_ids = rollos
            faltan = [c for c in codigos if c not in rollos.mapped('name')]
            wiz.codigos_error = ', '.join(faltan) or False
            wiz.kg_rama = sum(rollos.mapped('kg_rama'))
            wiz.kg_ya_cortados = sum(rollos.rollo_final_insp_ids.mapped('kg_neto'))

    @api.depends('product_id', 'kg_bruto')
    def _compute_pesos(self):
        Tara = self.env['mrp.tara.producto']
        for wiz in self:
            # La tara de Inspección es la que el archivo de Quimibond llama «INSPECCION TARAS» (hoy, operación
            # «Clasificación final» en la tabla de taras de José).
            tara = Tara._buscar_tara(wiz.product_id, 'clasificacion_final') if wiz.product_id else Tara
            wiz.tara_faltante = bool(wiz.product_id) and not tara
            wiz.tara_kg = tara.tara_kg
            wiz.kg_neto = wiz.kg_bruto - tara.tara_kg if tara else 0.0

    @api.depends('production_id')
    def _compute_lote(self):
        Final = self.env['mrp.rollo.final']
        for wiz in self:
            orden_kg = wiz.production_id
            orden_m = orden_kg._insp_orden_metros() if orden_kg else self.env['mrp.production']
            wiz.orden_m_id = orden_m
            # 3.2.0: sin orden en metros (venta en kg) el lote toma la orden en kg.
            wiz.lote_name = Final._nombre_lote(orden_m, Final._siguiente_secuencia(orden_m, orden_kg), orden_kg) \
                if orden_kg else False

    @api.depends('kg_neto', 'kg_rama', 'kg_ya_cortados', 'termina_rollo', 'metros', 'kg_bruto')
    def _compute_merma(self):
        """Merma del grupo (rollos de Rama ↔ rollos finales, §4.6), con alerta por porcentaje antes de guardar."""
        limite = float(self.env['ir.config_parameter'].sudo().get_param(PARAM_ALERTA_MERMA_PCT, 3.0))
        for wiz in self:
            avisos = []
            salida = wiz.kg_ya_cortados + wiz.kg_neto
            wiz.merma_kg = wiz.kg_rama - salida if wiz.kg_rama else 0.0
            wiz.merma_pct = wiz.merma_kg / wiz.kg_rama * 100.0 if wiz.kg_rama else 0.0
            if wiz.kg_rama and wiz.kg_neto:
                if salida > wiz.kg_rama:
                    avisos.append(_('Los rollos finales pesan %(s).2f kg y los de Rama %(r).2f kg: sale más de lo que '
                                    'entró.', s=salida, r=wiz.kg_rama))
                elif wiz.termina_rollo and wiz.merma_pct > limite:
                    avisos.append(_('Merma de %(kg).2f kg (%(p).1f %%), arriba del límite de %(l).1f %%.',
                                    kg=wiz.merma_kg, p=wiz.merma_pct, l=limite))
            wiz.alertas = '\n'.join(avisos) or False

    @api.depends('production_id', 'bitacora_ids')
    def _compute_pendientes(self):
        for wiz in self:
            wiz.pendientes = len(wiz.production_id.rollo_bitacora_ids.filtered(
                lambda r: r.estado_inspeccion != 'inspeccionado'))

    # ------------------------------------------------------------------ registro
    def _validar(self):
        self.ensure_one()
        rollos = self.bitacora_ids
        if self.codigos_error:
            raise UserError(_('No existe el rollo de Rama %s.', self.codigos_error))
        if not rollos:
            raise UserError(_('Escanea la etiqueta de Rama del rollo.'))
        otros = rollos.filtered(lambda r: r.production_id != self.production_id)
        if otros:
            raise UserError(_(
                'Solo se juntan rollos del mismo baño: %(r)s es de %(o)s y esta orden es %(kg)s.',
                r=', '.join(otros.mapped('name')), o=', '.join(otros.production_id.mapped('name')),
                kg=self.production_id.name))
        hechos = rollos.filtered(lambda r: r.estado_inspeccion == 'inspeccionado')
        if hechos:
            raise UserError(_('El rollo %s ya fue inspeccionado.', ', '.join(hechos.mapped('name'))))
        if len(rollos) > 1:
            if rollos.rollo_final_insp_ids:
                raise UserError(_('No se puede cortar y juntar en la misma captura: %s ya tiene pedazos.',
                                  ', '.join(rollos.filtered('rollo_final_insp_ids').mapped('name'))))
            if not self.termina_rollo:
                raise UserError(_('Al juntar rollos sale un solo rollo final: deja encendido «Último pedazo».'))
        if self.tara_faltante:
            raise UserError(_(
                'No hay tara de Inspección para %s. Captúrala en Manufactura → Configuración → Taras por producto '
                '(operación «Clasificación final»); el pesaje no asume tara 0.', self.product_id.display_name))
        if self.kg_bruto <= 0:
            raise UserError(_('Captura el peso bruto del rollo final.'))
        if self.kg_neto <= 0:
            raise UserError(_('El peso bruto (%(b).4f kg) no supera la tara (%(t).4f kg).', b=self.kg_bruto,
                              t=self.tara_kg))
        if self.weighing_mode == 'manual' and not (self.motivo_manual or '').strip():
            raise UserError(_('Captura el motivo de la captura manual del peso.'))
        if self.metros <= 0:
            raise UserError(_('Captura los metros del rollo final.'))
        if not self.clasificacion:
            raise UserError(_('Elige la calidad del rollo final.'))
        emp = self._empleado(self.empleado_numero)
        if not emp and 'x_studio_nmero_de_trabajador' in self.env['hr.employee']._fields:
            raise UserError(_('No existe el empleado %s.', self.empleado_numero))
        if emp and not emp.siglas:
            raise UserError(_('El operador %s no tiene siglas: captúralas en el empleado (van en la etiqueta).',
                              emp.name))
        self.env['mrp.inspeccion.restriccion']._validar_centro(self.workcenter_id, self.product_id)
        return emp

    def action_confirmar(self):
        self.ensure_one()
        emp = self._validar()
        Final = self.env['mrp.rollo.final']
        orden_m = self.orden_m_id
        secuencia = Final._siguiente_secuencia(orden_m, self.production_id)
        ahora = fields.Datetime.now()
        operador = ' - '.join(filter(None, [self.empleado_numero, emp.name or False]))
        rollo = Final.create({
            'name': Final._nombre_lote(orden_m, secuencia, self.production_id),
            'secuencia': secuencia,
            'production_id': self.production_id.id,
            'production_m_id': orden_m.id or False,
            'bitacora_ids': [(6, 0, self.bitacora_ids.ids)],
            'kg_bruto': self.kg_bruto,
            'tara': self.tara_kg,
            'kg_neto': self.kg_neto,
            'captura_manual': self.weighing_mode == 'manual',
            'motivo_manual': (self.motivo_manual or False) if self.weighing_mode == 'manual' else False,
            'metros': self.metros,
            'no_empalmes': self.no_empalmes,
            'clasificacion': self.clasificacion,
            'defecto_ids': [(6, 0, self.defecto_ids.ids)],
            'workcenter_id': self.workcenter_id.id,
            'operador': operador,
            'siglas_operador': emp.siglas or False,
            'fecha': ahora,
        })
        rollo.zpl_etiqueta = rollo._zpl_etiqueta()
        if self.termina_rollo:
            for rama in self.bitacora_ids:
                finales = rama.rollo_final_insp_ids
                principal = finales.sorted(lambda f: -f.kg_neto)[:1]
                rama.write({
                    'estado_inspeccion': 'inspeccionado',
                    'clasificacion_inicial': principal.clasificacion,
                    'metros_insp': sum(finales.mapped('metros')),
                    'no_empalmes': sum(finales.mapped('no_empalmes')),
                    'defecto_ids': [(6, 0, finales.defecto_ids.ids)],
                    'workcenter_insp_id': self.workcenter_id.id,
                    'operador_insp': operador,
                    'fecha_registro_insp': ahora,
                })
        accion = rollo.action_imprimir_etiqueta()
        accion.update({'close_on_report_download': True})
        return accion
