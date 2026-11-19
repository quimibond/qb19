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
        programaciones = self.env['tintoreria.programacion']
        for linea in self.line_ids:
            programaciones |= linea.production_id._programar_tintoreria(linea.workcenter_id)
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
