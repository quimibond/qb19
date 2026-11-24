# -*- coding: utf-8 -*-
"""Inspección como estación única (diseño 2026-11-03, D2, D3, D4, D6, D8 y §3.2).

Por cada rollo final que sale de la revisadora: se escanea el rollo de Rama (o varios del mismo baño para juntarlos),
se pesa con la tara de Inspección, se capturan metros, calidad y defectos, se le asigna su lote ``M…`` y se imprime la
etiqueta final. Para dividir un rollo de Rama en varios rollos, se registra cada rollo con «Último rollo de esta tela»
apagado y el último con él encendido: entonces el rollo de Rama queda inspeccionado. Lo defectuoso que se quita a un rollo se
registra aparte como tramo (3.22.0: ``rollo.tramo.wizard``) y los tramos se juntan en un rollo de FE o Segunda
(``rollo.tramos.juntar.wizard``). No se mueve inventario (D7).

3.3.0: alerta 2 del v5.4 §9.1 (metros contra kg × rendimiento de acabado), bloqueo de los rollos y de la orden mientras
se registra (dos revisadoras a la vez) y el rollo escaneado se busca primero en este baño.
"""
import re

from odoo import api, fields, models, _
from odoo.exceptions import UserError

from .pesaje_manual import TOLERANCIA_KG, modo_por_defecto, motivo_por_defecto

from ..models.mrp_rollo_bitacora import CLASIFICACIONES, CLASIFICACIONES_CAPTURA, PARAM_ALERTA_DIFERENCIA_PCT
from ..models.mrp_rollo_final import PARAM_ALERTA_MERMA_PCT

# 3.24.0 (piloto, 2026-11-23): tolerancia en % de lo que Inspección puede sumar por encima de lo que pesó Rama sin pedir
# motivo. Vacío (por defecto): arriba de TOLERANCIA_KG avisa y pide motivo; el límite se fija con los pesos del piloto.
PARAM_TOLERANCIA_PESO_PCT = 'quimibond_rollos_bitacora.tolerancia_peso_pct'


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

    rollo_codigos = fields.Char('Códigos de Rama', help='Escanea la etiqueta de Rama. Para juntar rollos del mismo baño, '
                                                       'escanea varios separados por espacio.')
    bitacora_ids = fields.Many2many('mrp.rollo.bitacora', compute='_compute_bitacora', string='Rollos de Rama')
    codigos_error = fields.Char(compute='_compute_bitacora')
    termina_rollo = fields.Boolean('Último rollo de esta tela', default=True,
                                   help='Apágalo solo si de este rollo de Rama va a salir otro rollo más: el rollo de '
                                        'Rama queda pendiente hasta registrar el último. Lo defectuoso que se quita al rollo '
                                        'se registra con «QUITAR TRAMO».')
    kg_rama = fields.Float('Kg de Rama', compute='_compute_bitacora', digits=(12, 4))
    kg_ya_cortados = fields.Float('Kg de rollos ya registrados', compute='_compute_bitacora', digits=(12, 4))
    kg_tramos = fields.Float('Kg de tramos quitados', compute='_compute_bitacora', digits=(12, 4),
                             help='3.22.0: lo que se le quitó a estos rollos como tramo (va a un rollo de FE o Segunda).')

    kg_bruto = fields.Float('Peso bruto (kg)', digits=(12, 4))
    tara_kg = fields.Float('Tara de Inspección (kg)', compute='_compute_pesos', digits=(12, 4))
    tara_faltante = fields.Boolean(compute='_compute_pesos')
    kg_neto = fields.Float('Peso neto (kg)', compute='_compute_pesos', digits=(12, 4))
    motivo_manual = fields.Char('Motivo de la captura manual', default=lambda self: motivo_por_defecto(self))
    weighing_mode = fields.Selection(default=lambda self: modo_por_defecto(self))  # 3.3.9
    metros = fields.Float('Metros', digits=(12, 2),
                          help='Obligatorios: PQ, FE y Segunda se registran en metros.')
    no_empalmes = fields.Integer('No. de empalmes')
    clasificacion = fields.Selection(CLASIFICACIONES_CAPTURA, 'Calidad')
    defecto_ids = fields.Many2many('calidad.defecto.tela', 'rollo_insp_estacion_defecto_rel', 'wizard_id', 'defecto_id',
                                   'Defectos', domain="[('tipo_tela', '=', 'tejido_acabado')]")

    orden_m_id = fields.Many2one('mrp.production', 'Orden (m)', compute='_compute_lote')
    lote_name = fields.Char('Lote del rollo final', compute='_compute_lote')
    metros_teoricos = fields.Float('Metros teóricos', compute='_compute_merma', digits=(12, 2),
                                   help='Kg netos × rendimiento de tela acabada de la ficha (alerta 2, v5.4 §9.1).')
    merma_kg = fields.Float('Merma (kg)', compute='_compute_merma', digits=(12, 4))
    merma_pct = fields.Float('Merma (%)', compute='_compute_merma', digits=(12, 2))
    alertas = fields.Text('Alertas', compute='_compute_merma')
    # 3.22.2: el tramo se captura con «QUITAR TRAMO» y NO se guarda hasta registrar el rollo (se crea junto con él).
    tramo_bitacora_id = fields.Many2one('mrp.rollo.bitacora', 'Rollo del tramo', readonly=True)
    # 3.24.0: el tramo no se pesa (piloto, 2026-11-24): sus kg son lo que le falta al rollo para llegar al peso de Rama y
    # sus metros esos kg × el rendimiento de la ficha; se recalculan con el peso del rollo.
    tramo_kg_neto = fields.Float('Tramo: kg teóricos', compute='_compute_tramo', digits=(12, 4))
    tramo_metros = fields.Float('Tramo: metros teóricos', compute='_compute_tramo', digits=(12, 2))
    tramo_metros_captura = fields.Float('Tramo: metros capturados', digits=(12, 2), readonly=True,
                                        help='Solo cuando la ficha no tiene rendimiento.')
    tramo_defecto_ids = fields.Many2many('calidad.defecto.tela', 'rollo_insp_estacion_tramo_defecto_rel', 'wizard_id',
                                         'defecto_id', 'Tramo: defectos', readonly=True)
    sin_tramo_alerta = fields.Boolean('Hay diferencia sin tramo', compute='_compute_merma')
    confirma_sin_tramo = fields.Boolean(
        'Sí, registrar así', default=False,
        help='Ajuste de tramos (2026-11-16): el rollo pesa menos que lo que vino de Rama y no se quitó ningún tramo. '
             'Por defecto NO: márcalo solo si la diferencia es merma y quieres registrar el rollo así.')
    exceso_alerta = fields.Boolean('Sale más de lo que entró', compute='_compute_merma')
    confirma_exceso = fields.Boolean(
        'Sí, registrar así', default=False,
        help='3.24.0: Inspección pesó más que Rama (dos básculas con taras distintas). Ya no bloquea: se registra con '
             'motivo y queda en el chatter de la orden.')
    motivo_exceso = fields.Char('Motivo del exceso de peso')
    pendientes = fields.Integer('Rollos de Rama sin inspeccionar', compute='_compute_pendientes')

    @api.model
    def _default_workcenter(self):
        return self.env['mrp.workorder'].browse(self.env.context.get('default_workorder_id')).workcenter_id.id or False

    @api.model
    def _empleado(self, numero):
        return self.env['hr.employee']._qb_operador(numero)  # 3.5.1: número = referencia S-<número>

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
            rollos = Bitacora
            if codigos:
                # 3.3.0: primero los rollos de este baño; el mismo nombre en otra orden solo si aquí no existe.
                encontrados = Bitacora.search([('name', 'in', codigos)])
                propios = encontrados.filtered(lambda r: r.production_id == wiz.production_id)
                ajenos = encontrados.filtered(lambda r: r.name not in propios.mapped('name'))
                rollos = propios | ajenos
            wiz.bitacora_ids = rollos
            faltan = [c for c in codigos if c not in rollos.mapped('name')]
            wiz.codigos_error = ', '.join(faltan) or False
            wiz.kg_rama = sum(rollos.mapped('kg_rama'))
            wiz.kg_ya_cortados = sum(rollos.rollo_final_insp_ids.mapped('kg_neto'))
            wiz.kg_tramos = sum(rollos.tramo_ids.mapped('kg_neto'))

    @api.depends('tramo_bitacora_id', 'bitacora_ids', 'kg_rama', 'kg_neto', 'kg_ya_cortados', 'kg_tramos',
                 'tramo_metros_captura', 'product_id')
    def _compute_tramo(self):
        for wiz in self:
            kg = 0.0
            if wiz.tramo_bitacora_id and wiz.bitacora_ids == wiz.tramo_bitacora_id:
                kg = max(wiz.kg_rama - wiz.kg_neto - wiz.kg_ya_cortados - wiz.kg_tramos, 0.0)
            wiz.tramo_kg_neto = kg
            rendimiento = wiz._rendimiento_acabado() if kg else 0.0
            wiz.tramo_metros = round(kg * rendimiento, 2) if rendimiento else (wiz.tramo_metros_captura if kg else 0.0)

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

    def _rendimiento_acabado(self):
        """Rendimiento de tela acabada (m/kg) de la ficha del acabado en kg de esta orden; 0 si no hay ficha."""
        self.ensure_one()
        Ficha = self.env.get('ficha.tecnica.acabado')
        if Ficha is None or 'rendimiento_tela_acabada' not in Ficha._fields or not self.product_id:
            return 0.0
        ficha = Ficha.search([('product_proceso_id', '=', self.product_id.id)], order='id desc', limit=1)
        return ficha.rendimiento_tela_acabada or 0.0

    @api.depends('kg_neto', 'kg_rama', 'kg_ya_cortados', 'kg_tramos', 'termina_rollo', 'metros', 'kg_bruto', 'confirma_sin_tramo', 'tramo_kg_neto')
    def _compute_merma(self):
        """Merma del grupo (rollos de Rama ↔ rollos finales, §4.6), con alerta por porcentaje antes de guardar. 3.3.0:
        alerta 2 del v5.4 §9.1, metros contra kg netos × rendimiento de la ficha (parámetro de Ajustes, 3 %)."""
        params = self.env['ir.config_parameter'].sudo()
        limite = float(params.get_param(PARAM_ALERTA_MERMA_PCT, 3.0))
        limite_metros = float(params.get_param(PARAM_ALERTA_DIFERENCIA_PCT, 3.0))
        for wiz in self:
            avisos = []
            # 3.22.1: sin rollo pesado no hay nada que calcular: todo en cero (antes quedaba la diferencia de Rama contra
            # los rollos ya registrados, que parecía merma de rollos previos).
            wiz.metros_teoricos = 0.0
            wiz.merma_kg = 0.0
            wiz.merma_pct = 0.0
            wiz.sin_tramo_alerta = False
            wiz.exceso_alerta = False
            wiz.alertas = False
            if not (wiz.kg_rama and wiz.kg_neto > 0):
                continue
            rendimiento = wiz._rendimiento_acabado()
            wiz.metros_teoricos = wiz.kg_neto * rendimiento if rendimiento else 0.0
            if wiz.metros_teoricos and wiz.metros and \
                    abs(wiz.metros - wiz.metros_teoricos) > wiz.metros_teoricos * limite_metros / 100.0:
                avisos.append(_('%(m).2f m contra %(t).2f m teóricos (%(kg).2f kg × %(r).4f m/kg de la ficha): '
                                'revisa los metros o el peso.', m=wiz.metros, t=wiz.metros_teoricos, kg=wiz.kg_neto,
                                r=rendimiento))
            salida = wiz.kg_ya_cortados + wiz.kg_tramos + wiz.tramo_kg_neto + wiz.kg_neto
            wiz.merma_kg = wiz.kg_rama - salida
            wiz.merma_pct = wiz.merma_kg / wiz.kg_rama * 100.0
            if salida > wiz.kg_rama + TOLERANCIA_KG:
                exceso = salida - wiz.kg_rama
                tolerancia = (params.get_param(PARAM_TOLERANCIA_PESO_PCT) or '').strip()
                try:
                    tolerancia = float(tolerancia) if tolerancia else 0.0
                except ValueError:
                    tolerancia = 0.0
                if tolerancia and exceso <= wiz.kg_rama * tolerancia / 100.0:
                    avisos.append(_('Inspección suma %(e).3f kg más que Rama (%(s).2f contra %(r).2f kg), dentro de la '
                                    'tolerancia de %(t).2f %%.', e=exceso, s=salida, r=wiz.kg_rama, t=tolerancia))
                else:
                    wiz.exceso_alerta = True
                    avisos.append(_('El rollo (%(n).2f kg) con lo ya registrado y los tramos suma %(s).2f kg y el rollo '
                                    'de Rama pesó %(r).2f kg: sale %(e).3f kg más de lo que entró (%(p).2f %%). Revisa '
                                    'el peso; si procede así, marca «Sí, registrar así» y escribe el motivo.',
                                    n=wiz.kg_neto, s=salida, r=wiz.kg_rama, e=exceso, p=exceso / wiz.kg_rama * 100.0))
            elif wiz.termina_rollo and wiz.merma_pct > limite:
                if not (wiz.kg_tramos or wiz.tramo_kg_neto):
                    wiz.sin_tramo_alerta = True
                    avisos.append(_('Lo registrado del rollo de Rama (%(s).2f kg: este rollo %(n).2f kg y lo ya registrado '
                                    '%(f).2f kg) es menor a los %(r).2f kg de Rama y no se ha quitado ningún tramo: '
                                    'diferencia de %(kg).2f kg (%(p).1f %%). Si procede así, marca «Sí, registrar así»; '
                                    'si es un tramo, usa «QUITAR TRAMO».',
                                    s=salida, n=wiz.kg_neto, f=wiz.kg_ya_cortados, r=wiz.kg_rama, kg=wiz.merma_kg,
                                    p=wiz.merma_pct))
                else:
                    avisos.append(_('Merma de %(kg).2f kg (%(p).1f %%), arriba del límite de %(l).1f %%.',
                                    kg=wiz.merma_kg, p=wiz.merma_pct, l=limite))
            wiz.alertas = '\n'.join(avisos) or False

    @api.depends('production_id', 'bitacora_ids')
    def _compute_pendientes(self):
        for wiz in self:
            wiz.pendientes = len(wiz.production_id.rollo_bitacora_ids.filtered(
                lambda r: r.estado_inspeccion != 'inspeccionado'))

    # ------------------------------------------------------------------ quitar tramo
    def _reabrir(self):
        """Vuelve a abrir esta misma ventana (conserva empleado, rollo y peso capturados)."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'name': _('Inspeccionar rollo'), 'res_model': self._name,
            'res_id': self.id, 'view_mode': 'form', 'views': [(False, 'form')], 'target': 'new',
        }

    def action_quitar_tramo(self):
        """Ajuste de tramos (2026-11-16): con el rollo ya pesado, lo que le falta para el peso de Rama es el peso
        inicial del tramo; se pesa, se capturan metros y defectos y se guarda sin etiqueta."""
        self.ensure_one()
        if self.production_id.state in ('done', 'cancel'):
            raise UserError(_('La orden %s ya está cerrada o cancelada: no se quitan tramos.', self.production_id.name))
        if self.codigos_error:
            raise UserError(_('No existe el rollo de Rama %s.', self.codigos_error))
        if len(self.bitacora_ids) != 1:
            raise UserError(_('El tramo se quita a un solo rollo de Rama: escanea uno.'))
        rollo = self.bitacora_ids
        if rollo.production_id != self.production_id:
            raise UserError(_('El rollo %(r)s es de %(o)s y esta orden es %(kg)s.', r=rollo.name,
                              o=rollo.production_id.name, kg=self.production_id.name))
        if rollo.estado_inspeccion == 'inspeccionado':
            raise UserError(_('El rollo %s ya fue inspeccionado.', rollo.name))
        if self.tara_faltante:
            raise UserError(_(
                'No hay tara de Inspección para %s. Captúrala en Manufactura → Configuración → Taras por producto '
                '(operación «Clasificación final»); el pesaje no asume tara 0.', self.product_id.display_name))
        if self.kg_neto <= 0:
            raise UserError(_('Primero pesa el rollo: los kg del tramo son lo que le falta al rollo para llegar al peso '
                              'de Rama.'))
        # 3.24.0: kg teóricos del tramo = peso de Rama − peso neto de ESTE rollo (el tramo no se pesa).
        faltan = self.kg_rama - self.kg_neto
        if faltan <= TOLERANCIA_KG:
            raise UserError(_('El rollo pesa %(n).2f kg y el rollo de Rama %(r).2f kg: no hay diferencia que quitar como '
                              'tramo.', n=self.kg_neto, r=self.kg_rama))
        self.env['hr.employee']._qb_operador_requerido(self.empleado_numero)
        tramo = self.env['rollo.tramo.wizard'].create({
            'estacion_id': self.id,
            'production_id': self.production_id.id,
            'workorder_id': self.workorder_id.id,
            'workcenter_id': self.workcenter_id.id,
            'empleado_numero': self.empleado_numero,
            'bitacora_id': rollo.id,
            'kg_rollo': self.kg_neto,
            'kg_ya_cortados': self.kg_ya_cortados,
            'kg_tramos': self.kg_tramos,
            'rendimiento': self._rendimiento_acabado(),
            'metros': self.tramo_metros_captura if self.tramo_bitacora_id == rollo else 0.0,
            'defecto_ids': [(6, 0, self.tramo_defecto_ids.ids if self.tramo_bitacora_id == rollo else [])],
        })
        return {
            'type': 'ir.actions.act_window', 'name': _('Quitar tramo'), 'res_model': tramo._name, 'res_id': tramo.id,
            'view_mode': 'form', 'views': [(False, 'form')], 'target': 'new',
        }

    # ------------------------------------------------------------------ registro
    def _validar(self):
        self.ensure_one()
        if self.production_id.state in ('done', 'cancel'):
            # 3.3.5: el botón de la operación ya lo impedía, pero el asistente abierto dejaba capturar un rollo final en
            # una orden cancelada (prueba 4a en main, 2026-11-05).
            raise UserError(_('La orden %s ya está cerrada o cancelada: no se inspeccionan rollos.',
                              self.production_id.name))
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
                raise UserError(_('Un rollo de Rama que ya se dividió no se junta con otros en la misma captura: '
                                  '%s ya tiene rollos registrados.',
                                  ', '.join(rollos.filtered('rollo_final_insp_ids').mapped('name'))))
            if not self.termina_rollo:
                raise UserError(_('Al juntar rollos sale un solo rollo final: deja encendido «Último rollo de esta tela».'))
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
        if self.tramo_bitacora_id and self.tramo_bitacora_id != rollos:
            raise UserError(_('El tramo capturado es del rollo %s: vuelve a pulsar «QUITAR TRAMO» para este rollo.',
                              self.tramo_bitacora_id.name or '-'))
        if self.tramo_bitacora_id and self.tramo_kg_neto <= TOLERANCIA_KG:
            raise UserError(_(
                'Con el peso del rollo (%(n).2f kg) ya no hay diferencia contra los %(r).2f kg de Rama: no hay tramo '
                'que quitar. Vuelve a pulsar «QUITAR TRAMO» si todavía hace falta.', n=self.kg_neto, r=self.kg_rama))
        salida = self.kg_ya_cortados + self.kg_tramos + self.tramo_kg_neto + self.kg_neto
        if self.exceso_alerta:
            # 3.24.0: ya no bloquea (dos básculas con taras distintas): pide confirmar y escribir el motivo.
            if not (self.confirma_exceso and (self.motivo_exceso or '').strip()):
                raise UserError(_(
                    'Lo que pesó el rollo (%(n).2f kg) más lo ya registrado (%(f).2f kg) y los tramos (%(t).2f kg) suma '
                    '%(s).2f kg y pasa de los %(r).2f kg del rollo de Rama. Revisa el peso; si procede así, marca «Sí, '
                    'registrar así», escribe el motivo y registra de nuevo.',
                    n=self.kg_neto, f=self.kg_ya_cortados, t=self.kg_tramos + self.tramo_kg_neto, s=salida,
                    r=self.kg_rama))
        if self.sin_tramo_alerta and not self.confirma_sin_tramo:
            raise UserError(_(
                'Lo registrado del rollo de Rama (%(s).2f kg: este rollo %(n).2f kg y lo ya registrado %(f).2f kg) es '
                'menor a los %(r).2f kg de Rama y no se ha quitado ningún tramo. Si es un tramo, usa «QUITAR TRAMO»; si '
                'procede así, marca «Sí, registrar así» y registra de nuevo.',
                s=salida, n=self.kg_neto, f=self.kg_ya_cortados, r=self.kg_rama))
        if not self.clasificacion:
            raise UserError(_('Elige la calidad del rollo final: PQ, FE o Segunda.'))
        if self.clasificacion in ('SEGUNDA', 'FE') and not self.defecto_ids:
            # 3.3.1 (Dirección, 2026-11-05): sin defecto no queda evidencia de por qué bajó la calidad (ISO 9001).
            raise UserError(_('Captura al menos un defecto: un rollo de %s necesita la causa.',
                              dict(CLASIFICACIONES).get(self.clasificacion)))
        emp = self.env['hr.employee']._qb_operador_requerido(self.empleado_numero)
        if emp and not emp.siglas:
            raise UserError(_('El operador %s no tiene siglas: captúralas en el empleado (van en la etiqueta).',
                              emp.name))
        self.env['mrp.inspeccion.restriccion']._validar_centro(self.workcenter_id, self.product_id)
        return emp

    def _bloquear(self):
        """3.3.0: bloquea los rollos de Rama y la orden mientras se registra: dos revisadoras que escanean el mismo rollo,
        o que sacan el consecutivo de la misma orden, esperan una a la otra en vez de duplicar."""
        self.ensure_one()
        if self.bitacora_ids:
            self.env.cr.execute('SELECT id FROM mrp_rollo_bitacora WHERE id IN %s FOR UPDATE', [tuple(self.bitacora_ids.ids)])
        ordenes = (self.orden_m_id | self.production_id).ids
        self.env.cr.execute('SELECT id FROM mrp_production WHERE id IN %s FOR UPDATE', [tuple(ordenes)])
        self.env.invalidate_all()

    def action_confirmar(self):
        self.ensure_one()
        self._bloquear()
        emp = self._validar()
        Final = self.env['mrp.rollo.final']
        orden_m = self.orden_m_id
        self.production_id._asignar_numero_bano()
        secuencia = Final._siguiente_secuencia(orden_m, self.production_id)
        ahora = fields.Datetime.now()
        operador = ' - '.join(filter(None, [self.empleado_numero, emp.name or False]))
        exceso_kg = max(self.kg_ya_cortados + self.kg_tramos + self.tramo_kg_neto + self.kg_neto - self.kg_rama, 0.0)
        if exceso_kg <= TOLERANCIA_KG:
            exceso_kg = 0.0
        rollo = Final.create({
            'exceso_rama_kg': exceso_kg,
            'exceso_rama_pct': exceso_kg / self.kg_rama * 100.0 if (exceso_kg and self.kg_rama) else 0.0,
            'exceso_rama_motivo': (self.motivo_exceso or '').strip() or False
            if (exceso_kg and self.exceso_alerta and self.confirma_exceso) else False,
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
            'metros_teoricos': self.metros_teoricos,
            # 3.16.1: sin metros capturados (Desperdicio, 3.6.2) no hay contra qué comparar: sin alerta.
            'alerta_metros': bool(self.metros_teoricos and self.metros)
            and abs(self.metros - self.metros_teoricos) > self.metros_teoricos
            * float(self.env['ir.config_parameter'].sudo().get_param(PARAM_ALERTA_DIFERENCIA_PCT, 3.0)) / 100.0,
        })
        rollo.zpl_etiqueta = rollo._zpl_etiqueta()
        rollo.zpl_etiqueta_preliminar = rollo._zpl_etiqueta_preliminar()
        if self.exceso_alerta and self.confirma_exceso:
            exceso = self.kg_ya_cortados + self.kg_tramos + self.tramo_kg_neto + self.kg_neto - self.kg_rama
            self.production_id.message_post(body=_(
                'Rollo %(r)s registrado con %(e).3f kg más que el rollo de Rama (%(k).2f kg) por %(o)s. Motivo: %(m)s.',
                r=rollo.name, e=exceso, k=self.kg_rama, o=operador, m=(self.motivo_exceso or '').strip()))
        if self.tramo_bitacora_id:
            # 3.22.2: el tramo se guarda junto con el rollo, nunca solo. 3.24.0: con kg y metros teóricos, sin peso ni tara.
            tramo = self.env['mrp.rollo.tramo'].create({
                'production_id': self.production_id.id,
                'bitacora_id': self.tramo_bitacora_id.id,
                'teorico': True,
                'kg_neto': self.tramo_kg_neto,
                'metros': self.tramo_metros,
                'defecto_ids': [(6, 0, self.tramo_defecto_ids.ids)],
                'workcenter_id': self.workcenter_id.id,
                'operador': operador,
            })
            self.production_id.message_post(body=_(
                'Tramo de %(kg).2f kg y %(m).2f m (teóricos) quitado al rollo %(r)s por %(o)s (defectos: %(d)s). Queda '
                'pendiente de juntar en un rollo de FE o Segunda.', kg=tramo.kg_neto, m=tramo.metros,
                r=tramo.bitacora_id.name, o=operador, d=tramo.siglas_defectos or '-'))
        self.production_id._historial_pesaje(self.workcenter_id, rollo.name, self.kg_bruto, self.tara_kg,
                                             rollo.kg_neto, operador, fecha=ahora)
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
        if Final._accion_al_confirmar() == 'final':
            accion = rollo.action_imprimir_etiqueta()
        else:
            accion = rollo.action_imprimir_etiqueta_preliminar()
        accion.update({'close_on_report_download': True})
        return accion
