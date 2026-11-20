# -*- coding: utf-8 -*-
from markupsafe import Markup, escape

from odoo import Command, _, api, fields, models
from odoo.exceptions import UserError

from ..models.mrp_production import clave_rollo
from ..models.tintoreria_programacion import BANDAS, ORIGENES_TIEMPO


class TintoreriaProgramacionWizard(models.TransientModel):
    _name = 'tintoreria.programacion.wizard'
    _description = 'Programar Tintorería'

    line_ids = fields.One2many('tintoreria.programacion.wizard.line', 'wizard_id', 'Órdenes')
    bano_ids = fields.One2many('tintoreria.programacion.wizard.bano', 'wizard_id', 'Rama por baño')

    @api.onchange('line_ids')
    def _onchange_line_ids_banos(self):
        """1.10.0: un renglón por baño (orden parcial) para elegir su Rama. Al cambiar el HTJ cambia el número de baños:
        las Ramas ya elegidas se conservan por orden y número de baño."""
        anteriores = {(b.production_id.id, b.secuencia): b.rama_id.id for b in self.bano_ids}
        nuevos = []
        for linea in self.line_ids:
            if not (linea.production_id and linea.workcenter_id):
                continue
            try:
                plan = linea.production_id._calcular_plan_tintoreria(linea.workcenter_id)
            except UserError:
                continue
            detalle = plan['banos'] or []
            for numero in range(1, plan['n_banos'] + 1):
                bano = detalle[numero - 1] if numero <= len(detalle) else {}
                nuevos.append(Command.create({
                    'production_id': linea.production_id.id,
                    'secuencia': numero,
                    'kg': bano.get('kg') or plan['kg_bano'],
                    'rama_id': anteriores.get((linea.production_id.id, numero)) or False,
                }))
        self.bano_ids = [Command.clear()] + nuevos

    @api.model
    def default_get(self, fields_list):
        res = super().default_get(fields_list)
        if self.env.context.get('active_model') == 'mrp.production' and 'line_ids' in fields_list:
            ordenes = self.env['mrp.production'].browse(
                self.env.context.get('active_ids') or []).filtered('tintoreria_pendiente')
            if not ordenes:
                raise UserError(_("Selecciona órdenes pendientes de programar Tintorería."))
            res['line_ids'] = [Command.create({'production_id': orden.id}) for orden in ordenes]
        return res

    def action_confirmar(self):
        self.ensure_one()
        sin_htj = self.line_ids.filtered(lambda ln: not ln.workcenter_id)
        if sin_htj:
            raise UserError(_("Asigna el HTJ a todas las órdenes: %s.", ', '.join(sin_htj.production_id.mapped('name'))))
        con_error = self.line_ids.filtered('advertencia')
        if con_error:
            raise UserError('\n'.join('%s: %s' % (ln.production_id.name, ln.advertencia) for ln in con_error))
        # 1.10.0: Rama opcional por baño; se valida todo antes de programar la primera orden.
        for linea in self.line_ids:
            linea._validar_ramas()
        programaciones = self.env['tintoreria.programacion']
        for linea in self.line_ids:
            programaciones |= linea.production_id._programar_tintoreria(linea.workcenter_id, linea._ramas_por_bano())
        return {
            'type': 'ir.actions.act_window',
            'name': _('Programaciones de Tintorería'),
            'res_model': 'tintoreria.programacion',
            'view_mode': 'list,form',
            'domain': [('id', 'in', programaciones.ids)],
        }


class TintoreriaProgramacionWizardLine(models.TransientModel):
    _name = 'tintoreria.programacion.wizard.line'
    _description = 'Orden a programar en Tintorería'

    wizard_id = fields.Many2one('tintoreria.programacion.wizard', required=True, ondelete='cascade')
    production_id = fields.Many2one('mrp.production', 'Orden', required=True, readonly=True)
    product_id = fields.Many2one(related='production_id.product_id', string='Producto')
    kg_total = fields.Float('kg pedidos', related='production_id.product_qty', digits=(16, 3))
    lote_crudo = fields.Char('Lote de crudo', compute='_compute_plan')
    n_rollos = fields.Integer('Rollos surtidos', compute='_compute_plan')
    kg_rollos = fields.Float('kg de crudo surtidos', compute='_compute_plan', digits=(16, 3))
    detalle_banos = fields.Html('Baños y rollos', compute='_compute_plan', sanitize=False)
    workcenter_id = fields.Many2one('mrp.workcenter', 'HTJ')
    rendimiento = fields.Float('Rendimiento (m/kg)', compute='_compute_plan', digits=(12, 4))
    banda = fields.Selection(BANDAS, 'Banda', compute='_compute_plan')
    capacidad_kg = fields.Float('Capacidad por baño (kg)', compute='_compute_plan', digits=(16, 3))
    n_banos = fields.Integer('No. de baños', compute='_compute_plan')
    kg_bano = fields.Float('kg por baño', compute='_compute_plan', digits=(16, 3))
    kg_crudo_bano = fields.Float('kg de crudo por baño', compute='_compute_plan', digits=(16, 3))
    minutos_bano = fields.Float('Min por baño', compute='_compute_plan', digits=(12, 2))
    origen_tiempo = fields.Selection(ORIGENES_TIEMPO, 'Origen del tiempo', compute='_compute_plan')
    advertencia = fields.Char('Advertencia', compute='_compute_plan')

    # ------------------------------------------------------------------
    # 1.10.0: Rama por baño (opcional)
    # ------------------------------------------------------------------
    def _ramas_por_bano(self):
        """Rama elegida por baño, en orden (vacío si no se eligió ninguna: el planificador la elige como hoy)."""
        self.ensure_one()
        banos = self.wizard_id.bano_ids.filtered(lambda b: b.production_id == self.production_id).sorted('secuencia')
        return [b.rama_id for b in banos] if any(banos.mapped('rama_id')) else []

    def _validar_ramas(self):
        self.ensure_one()
        ramas = self._ramas_por_bano()
        if not ramas:
            return
        plan = self.production_id._calcular_plan_tintoreria(self.workcenter_id)
        if len(ramas) != plan['n_banos']:
            raise UserError(_("%(o)s: cambió el HTJ y ahora son %(n)s baño(s). Revisa la columna Rama.",
                              o=self.production_id.name, n=plan['n_banos']))
        if not self.production_id._workorder_rama():
            raise UserError(_("La lista de materiales de %s no tiene operación de Rama.", self.production_id.name))
        for numero, rama in enumerate(ramas, 1):
            problema = self.production_id._rama_validar(rama)
            if problema:
                raise UserError(_("Baño %(i)s de %(o)s: no se puede programar en la Rama %(r)s. %(p)s",
                                  i=numero, o=self.production_id.name, r=rama.display_name, p=problema))

    @api.depends('production_id', 'workcenter_id')
    def _compute_plan(self):
        for linea in self:
            linea.rendimiento = linea.capacidad_kg = linea.kg_bano = linea.kg_crudo_bano = linea.minutos_bano = 0.0
            linea.origen_tiempo = False
            linea.n_banos = 0
            linea.banda = False
            linea.advertencia = False
            linea.lote_crudo = False
            linea.n_rollos = 0
            linea.kg_rollos = 0.0
            linea.detalle_banos = False
            if not linea.production_id:
                continue
            try:
                rollos = linea.production_id._tintoreria_rollos_crudo()  # bloquea si el crudo no se ha surtido
                if rollos:
                    linea.lote_crudo = ' / '.join(dict.fromkeys(
                        clave_rollo(r['lote'].name)[0] for r in rollos))
                    linea.n_rollos = len(rollos)
                    linea.kg_rollos = sum(r['kg'] for r in rollos)
                if linea.workcenter_id:
                    plan = linea.production_id._calcular_plan_tintoreria(linea.workcenter_id)
                    linea.rendimiento = plan['rendimiento']
                    linea.banda = plan['banda']
                    linea.capacidad_kg = plan['capacidad_kg']
                    linea.n_banos = plan['n_banos']
                    linea.kg_bano = plan['kg_bano']
                    linea.kg_crudo_bano = plan['kg_crudo_bano']
                    linea.minutos_bano = plan['minutos']
                    linea.origen_tiempo = plan['origen_tiempo']
                    if plan['banos']:
                        linea.detalle_banos = Markup('<br/>').join(
                            Markup('%s: %s kg · %s (%s rollos)') % (
                                _('Baño %s', numero), '{:,.2f}'.format(bano['kg']), escape(bano['texto']),
                                bano['n_rollos'])
                            for numero, bano in enumerate(plan['banos'], 1))
                else:
                    linea.rendimiento = linea.production_id._rendimiento_tejido()
            except UserError as error:
                linea.advertencia = error.args[0]


class TintoreriaProgramacionWizardBano(models.TransientModel):
    _name = 'tintoreria.programacion.wizard.bano'
    _description = 'Rama de un baño (orden parcial) a programar'
    _order = 'secuencia'

    wizard_id = fields.Many2one('tintoreria.programacion.wizard', required=True, ondelete='cascade')
    production_id = fields.Many2one('mrp.production', 'Orden', readonly=True)
    secuencia = fields.Integer('Baño', readonly=True)
    kg = fields.Float('kg', readonly=True, digits=(16, 3))
    rama_id = fields.Many2one(
        'mrp.workcenter', 'Rama', domain="[('ft_tipo_id.code', '=', 'acabado')]",
        help='Opcional. Si se elige, la operación de Rama de este baño queda fijada y el planificador no la mueve; '
             'vacía, la elige el planificador como siempre.')
    validacion = fields.Char('Validación', compute='_compute_validacion')
    rama_invalida = fields.Boolean(compute='_compute_validacion')

    @api.depends('rama_id', 'production_id')
    def _compute_validacion(self):
        for bano in self:
            orden = bano.production_id
            problema = orden._rama_validar(bano.rama_id) if (orden and bano.rama_id) else False
            bano.rama_invalida = bool(problema)
            bano.validacion = False if not bano.rama_id else (
                ('✗ %s' % problema) if problema else _('✓ Se puede programar'))
