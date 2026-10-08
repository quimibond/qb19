# -*- coding: utf-8 -*-
"""Orden de muestra (57.125.0, C1 bloque F; brief §6.10).

Diseño de Procesos pide la corrida **desde el proyecto** con cantidad y fecha
deseada; a Planeación le llega la orden de fabricación armada: artículo del
proyecto, folio FT en origen, tipo de operación «Tejido Desarrollo», lista de
materiales y ruta, y el sobrante a la ubicación de desarrollos ligado a su
proyecto (``sgi_dev_project_id`` en la orden).

**Cantidad sugerida** = la mayor entre lo que pide el cliente (cantidad de la
muestra de la solicitud), lo necesario para entregar los metros PQ del
parámetro según el rendimiento de primera esperado, y el mínimo de baño de
tintorería si el artículo lleva teñido. Se muestra el motivo y se puede
ajustar. Los parámetros que el brief no define (rendimiento esperado, mínimo
de baño) quedan vacíos: sin valor no entran al cálculo y el motivo lo dice.

La fecha de máquina (``date_start`` de la orden) se refleja sola en una tarea
del proyecto; sin la Solicitud de desarrollo aprobada (bloque E) no se pide.
"""
import logging

from odoo import api, fields, models
from odoo.exceptions import UserError


_logger = logging.getLogger(__name__)

PARAM_PICKING_TYPE = 'quimibond_sgi.dev_sample_picking_type_id'
PARAM_LOCATION = 'quimibond_sgi.dev_sample_location_id'
PARAM_PLANNING_JOB = 'quimibond_sgi.dev_planning_job_id'
PARAM_PQ_M = 'quimibond_sgi.dev_sample_pq_m'
PARAM_EXPECTED_YIELD = 'quimibond_sgi.dev_sample_expected_yield_pct'
PARAM_MIN_BATH_KG = 'quimibond_sgi.dev_sample_min_bath_kg'
PICKING_TYPE_NAME = 'Tejido Desarrollo'
LOCATION_NAME = '31 DESARROLLOS'
PLANNING_JOB_NAME = 'PLANEADOR DE PRODUCCION'
DEFAULT_PQ_M = 50.0


def _float_param(env, key, default=0.0):
    raw = env['ir.config_parameter'].sudo().get_param(key, '') or ''
    try:
        return float(raw) if raw.strip() else default
    except ValueError:
        return default


def _rec_param(env, key, model):
    raw = (env['ir.config_parameter'].sudo().get_param(key, '') or '').strip()
    if raw.isdigit():
        return env[model].sudo().browse(int(raw)).exists()
    return env[model]


class ProjectProjectDevSample(models.Model):
    _inherit = 'project.project'

    sgi_dev_mo_ids = fields.One2many('mrp.production', 'sgi_dev_project_id', string="Órdenes de muestra")
    sgi_dev_mo_count = fields.Integer(compute='_compute_sgi_dev_mo_count')

    @api.depends('sgi_dev_mo_ids')
    def _compute_sgi_dev_mo_count(self):
        for project in self:
            project.sgi_dev_mo_count = len(project.sgi_dev_mo_ids)

    # ------------------------------------------------------------------------
    def _sgi_dev_yield_m_per_kg(self):
        """m/kg del desarrollo: 1000 / (masa × ancho) de la tabla de
        características; si no, el renglón de rendimiento. 0 si falta."""
        self.ensure_one()
        from odoo.addons.quimibond_ficha_tecnica_tela.models.ficha_tecnica_caracteristica import (
            CODE_MASS, CODE_WIDTH)
        lines = self.sgi_dev_line_ids
        mass = self._sgi_dev_pick(lines, CODE_MASS)
        width = self._sgi_dev_pick(lines, CODE_WIDTH)
        if mass and width and mass.spec_nominal and width.spec_nominal:
            return 1000.0 / (mass.spec_nominal * width.spec_nominal)
        rend = lines.filtered(lambda l: l.caracteristica_id.formula == 'rendimiento' and l.spec_nominal)[:1]
        return rend.spec_nominal if rend else 0.0

    def _sgi_dev_sample_products(self):
        self.ensure_one()
        return (self.sgi_dev_product_crudo_id | self.sgi_dev_product_tenido_id | self.sgi_dev_product_id)

    def _sgi_dev_sample_suggestion(self, product):
        """(cantidad en la unidad del artículo, motivo). La regla se calcula en
        metros de tela acabada y se pasa a kg con el rendimiento si el
        artículo se mide en peso."""
        self.ensure_one()
        env = self.env
        rend = self._sgi_dev_yield_m_per_kg()
        razones = []
        candidatos = []
        oc_m = self.sgi_dev_sample_m or 0.0
        if not oc_m and self.sgi_dev_sample_kg and rend:
            oc_m = self.sgi_dev_sample_kg * rend
        if oc_m:
            candidatos.append(('lo que pide el cliente', oc_m))
        pq = _float_param(env, PARAM_PQ_M, DEFAULT_PQ_M)
        esperado = _float_param(env, PARAM_EXPECTED_YIELD)
        if pq:
            factor = esperado / 100.0 if esperado else 1.0
            candidatos.append(('%g m PQ%s' % (pq, (' al %g %% de primera esperado' % esperado) if esperado
                                               else ' (rendimiento de primera esperado sin definir: 100 %)'),
                               pq / factor))
        bath = _float_param(env, PARAM_MIN_BATH_KG)
        if self.sgi_dev_product_tenido_id:
            if bath and rend:
                candidatos.append(('mínimo de baño de tintorería %g kg' % bath, bath * rend))
            elif bath:
                razones.append('El mínimo de baño (%g kg) no se pudo pasar a metros: faltan masa y ancho.' % bath)
            else:
                razones.append('Mínimo de baño de tintorería sin definir (parámetro vacío).')
        if not candidatos:
            return 0.0, 'Sin datos para sugerir: capture la cantidad.'
        motivo, metros = max(candidatos, key=lambda c: c[1])
        razones.insert(0, 'Sugerida: %g m por %s (opciones: %s).' % (
            metros, motivo, '; '.join('%s = %g m' % (m, round(v, 1)) for m, v in candidatos)))
        if product and self._sgi_dev_uom_is_weight(product.uom_id):
            if not rend:
                razones.append('%s se mide en kg y faltan masa y ancho para convertir: capture la cantidad.'
                               % product.display_name)
                return 0.0, ' '.join(razones)
            razones.append('En kg: %g m ÷ %.2f m/kg.' % (metros, rend))
            return metros / rend, ' '.join(razones)
        return metros, ' '.join(razones)

    def action_sgi_dev_request_sample(self):
        self.ensure_one()
        self._sgi_dev_check_run_allowed()
        if not self._sgi_dev_sample_products():
            raise UserError("El proyecto no tiene artículo en desarrollo: genere los artículos (pestaña "
                            "Artículo) antes de pedir la corrida.")
        return {'type': 'ir.actions.act_window', 'name': "Pedir corrida de muestra",
                'res_model': 'sgi.dev.sample.wizard', 'view_mode': 'form', 'target': 'new',
                'context': {'default_project_id': self.id}}

    def action_sgi_dev_view_mos(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': "Órdenes de muestra de %s" % (self.sgi_ft_folio or self.name),
                'res_model': 'mrp.production', 'view_mode': 'list,form',
                'domain': [('sgi_dev_project_id', '=', self.id)],
                'context': {'default_sgi_dev_project_id': self.id}}


class SgiDevSampleWizard(models.TransientModel):
    _name = 'sgi.dev.sample.wizard'
    _description = "Pedir la corrida de muestra del desarrollo"

    project_id = fields.Many2one('project.project', required=True, ondelete='cascade')
    product_ids = fields.Many2many('product.product', compute='_compute_product_ids')
    product_id = fields.Many2one('product.product', string="Artículo a fabricar", required=True,
                                 domain="[('id', 'in', product_ids)]",
                                 help="Crudo, teñido o acabado del proyecto. Por omisión el crudo: la corrida "
                                      "arranca en Tejido Desarrollo.")
    product_uom_id = fields.Many2one(related='product_id.uom_id')
    bom_id = fields.Many2one('mrp.bom', string="Lista de materiales", compute='_compute_bom_id', store=True,
                             readonly=False)
    suggested_qty = fields.Float(string="Cantidad sugerida", digits=(16, 3), readonly=True)
    suggestion_note = fields.Text(string="Por qué", readonly=True)
    product_qty = fields.Float(string="Cantidad a fabricar", digits=(16, 3), required=True)
    date_planned = fields.Datetime(string="Fecha deseada de máquina", required=True,
                                   default=lambda self: fields.Datetime.now())
    picking_type_id = fields.Many2one('stock.picking.type', string="Tipo de operación", required=True,
                                      domain="[('code', '=', 'mrp_operation')]")
    location_dest_id = fields.Many2one('stock.location', string="A dónde entra el sobrante", required=True,
                                       domain="[('usage', '=', 'internal')]")
    note = fields.Text(string="Nota para Planeación")

    @api.model
    def default_get(self, fields_list):
        vals = super().default_get(fields_list)
        project = self.env['project.project'].browse(vals.get('project_id') or self.env.context.get('default_project_id'))
        if project:
            products = project._sgi_dev_sample_products()
            product = project.sgi_dev_product_crudo_id or project.sgi_dev_product_tenido_id or project.sgi_dev_product_id
            vals.setdefault('project_id', project.id)
            if product and 'product_id' in fields_list:
                vals['product_id'] = product.id
                qty, note = project._sgi_dev_sample_suggestion(product)
                vals.update({'suggested_qty': qty, 'suggestion_note': note, 'product_qty': qty})
            _ = products
        picking = _rec_param(self.env, PARAM_PICKING_TYPE, 'stock.picking.type')
        if picking:
            vals.setdefault('picking_type_id', picking.id)
        location = _rec_param(self.env, PARAM_LOCATION, 'stock.location')
        if location:
            vals.setdefault('location_dest_id', location.id)
        elif picking and picking.default_location_dest_id:
            vals.setdefault('location_dest_id', picking.default_location_dest_id.id)
        return vals

    @api.depends('project_id')
    def _compute_product_ids(self):
        for wiz in self:
            wiz.product_ids = wiz.project_id._sgi_dev_sample_products() if wiz.project_id else False

    @api.depends('product_id')
    def _compute_bom_id(self):
        for wiz in self:
            wiz.bom_id = self.env['mrp.bom']._bom_find(
                wiz.product_id, company_id=wiz.project_id.company_id.id).get(wiz.product_id) if wiz.product_id else False

    @api.onchange('product_id')
    def _onchange_product_id(self):
        if self.product_id and self.project_id:
            qty, note = self.project_id._sgi_dev_sample_suggestion(self.product_id)
            self.suggested_qty, self.suggestion_note, self.product_qty = qty, note, qty

    def _planning_users(self):
        job = _rec_param(self.env, PARAM_PLANNING_JOB, 'hr.job')
        if not job:
            return self.env['res.users']
        employees = self.env['hr.employee'].sudo().search([('job_id', '=', job.id)])
        return employees.user_id.filtered(lambda u: u.active and not u.share)

    def action_confirm(self):
        self.ensure_one()
        project = self.project_id
        project._sgi_dev_check_run_allowed()
        if self.product_qty <= 0:
            raise UserError("La cantidad a fabricar debe ser mayor que cero.")
        if not self.bom_id:
            raise UserError("%s no tiene lista de materiales: sin ella la orden no sabe qué consumir ni qué ruta "
                            "lleva (C1.04 y C1.04b)." % self.product_id.display_name)
        folio = project.sgi_ft_folio or project.name
        mo = self.env['mrp.production'].create({
            'product_id': self.product_id.id, 'product_qty': self.product_qty,
            'product_uom_id': self.product_id.uom_id.id, 'bom_id': self.bom_id.id,
            'picking_type_id': self.picking_type_id.id, 'location_dest_id': self.location_dest_id.id,
            'origin': folio, 'date_start': self.date_planned, 'company_id': project.company_id.id,
            'sgi_dev_project_id': project.id,
        })
        task = self.env['project.task'].create({
            'name': "Corrida de muestra %s (%s)" % (mo.name, self.product_id.display_name),
            'project_id': project.id, 'date_deadline': self.date_planned,
            'user_ids': [(6, 0, [self.env.uid])],
            'description': "<p>Orden %s: %g %s de %s. Fecha de máquina deseada %s.%s</p>" % (
                mo.name, self.product_qty, self.product_id.uom_id.name, self.product_id.display_name,
                self.date_planned, (' ' + self.note) if self.note else ''),
        })
        mo.write({'sgi_dev_task_id': task.id})
        body = ("Corrida de muestra pedida: %s, %g %s de %s para el %s. %s%s" % (
            mo.name, self.product_qty, self.product_id.uom_id.name, self.product_id.display_name,
            fields.Datetime.context_timestamp(self, self.date_planned).strftime('%d/%m/%Y'),
            self.suggestion_note or '', (' Nota: ' + self.note) if self.note else ''))
        project.message_post(body=body)
        mo.message_post(body="Orden de muestra del desarrollo %s pedida desde el proyecto por %s.%s"
                        % (folio, self.env.user.name, (' ' + self.note) if self.note else ''))
        for user in self._planning_users():
            mo.activity_schedule('mail.mail_activity_data_todo', user_id=user.id,
                                 date_deadline=fields.Date.context_today(self),
                                 summary="Emitir la orden de muestra %s (%s)" % (mo.name, folio),
                                 note="Confirmar y programar la máquina. Fecha deseada: %s." % self.date_planned)
        return {'type': 'ir.actions.act_window', 'res_model': 'mrp.production', 'res_id': mo.id,
                'view_mode': 'form', 'target': 'current'}


class MrpProductionDevSample(models.Model):
    _inherit = 'mrp.production'

    sgi_dev_project_id = fields.Many2one('project.project', string="Proyecto de desarrollo", index=True,
                                         ondelete='set null', copy=False,
                                         help="Desarrollo cuya muestra fabrica esta orden; el sobrante queda ligado a él.")
    sgi_dev_task_id = fields.Many2one('project.task', string="Tarea de la corrida", ondelete='set null', copy=False)

    def write(self, vals):
        res = super().write(vals)
        if 'date_start' in vals:
            for mo in self.filtered(lambda m: m.sgi_dev_task_id and m.date_start):
                deadline = fields.Datetime.to_datetime(mo.date_start)
                if mo.sgi_dev_task_id.date_deadline != deadline:
                    mo.sgi_dev_task_id.sudo().write({'date_deadline': deadline})
        if vals.get('state') == 'done':
            for mo in self.filtered('sgi_dev_project_id'):
                mo.sgi_dev_project_id.message_post(body="Orden de muestra %s terminada: %g %s de %s en %s." % (
                    mo.name, mo.qty_produced or mo.product_qty, mo.product_uom_id.name, mo.product_id.display_name,
                    mo.location_dest_id.display_name))
        return res
