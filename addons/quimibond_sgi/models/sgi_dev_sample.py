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

57.154.0 (Dirección General 2026-10-13): «Pedir corrida de muestra» arma la
**cadena completa** del desarrollo, crudo → teñido → acabado (los artículos que
existan), una orden por artículo con su tipo de operación (crudo «Tejido
Desarrollo», teñido «Tintorería», acabado «Acabado», o los parámetros
``dev_sample_picking_type_tenido_id`` / ``_acabado_id``). La cantidad sugerida
es la del último artículo y las demás se calculan **de atrás hacia adelante**
con su lista de materiales. Cada orden usa solo una lista del propio artículo
(OP-DES-01560 salió con la lista de otro) y con ruta: sin operaciones la orden
no genera órdenes de trabajo y Planeación no la ve en máquina. Si algo falta no
se crea ninguna orden.
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
PARAM_PICKING_TYPE_TENIDO = 'quimibond_sgi.dev_sample_picking_type_tenido_id'
PARAM_PICKING_TYPE_ACABADO = 'quimibond_sgi.dev_sample_picking_type_acabado_id'
PICKING_TYPE_NAME = 'Tejido Desarrollo'
# 57.194.0: el mínimo de baño es un parámetro de la ficha de la máquina de tintorería (por jet).
BATH_PARAM_CODE = 'minimo_bano'
TIPO_TINTORERIA = 'tintoreria'
# 57.154.0: etapas de la cadena de la muestra. Teñido y acabado usan su parámetro si está capturado (para
# separar desarrollo) y, si no, el tipo de operación de planta con ese nombre.
SAMPLE_STAGES = [('crudo', "Crudo (tejido)"), ('tenido', "Teñido"), ('acabado', "Acabado")]
STAGE_PICKING_PARAMS = {'crudo': PARAM_PICKING_TYPE, 'tenido': PARAM_PICKING_TYPE_TENIDO,
                        'acabado': PARAM_PICKING_TYPE_ACABADO}
STAGE_PICKING_NAMES = {'crudo': PICKING_TYPE_NAME, 'tenido': 'Tintorería', 'acabado': 'Acabado'}
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

    def _sgi_dev_min_bath(self):
        """57.194.0 (Dirección General 2026-10-26): (kg, de dónde sale) del mínimo de baño para teñir el
        desarrollo. Es un parámetro de la máquina de tintorería (``minimo_bano`` en la ficha de la máquina; el
        jet TINTORERIA 4 tiene 150 kg). En orden: el menor de los jets de la ruta del artículo teñido; si la ruta
        no tiene jet con mínimo, el parámetro general del SGI; si tampoco hay, el menor de los jets de la planta
        (la muestra se corre en el jet más chico). (0, '') sin ninguno."""
        self.ensure_one()
        today = fields.Date.context_today(self)
        company = self.company_id or self.env.company

        def con_minimo(workcenters):
            filas = []
            for wc in workcenters:
                config = wc._ft_config_at(today)
                kg = config._ft_value(BATH_PARAM_CODE) if config else 0.0
                if kg and kg > 0:
                    filas.append((kg, wc))
            return min(filas, key=lambda f: f[0]) if filas else (0.0, None)

        tenido = self.sgi_dev_product_tenido_id
        if tenido:
            bom = self.env['sgi.dev.sample.wizard'].sudo()._sgi_dev_own_bom(tenido, company)
            jets = bom.sudo().operation_ids.workcenter_id.filtered(lambda w: w.ft_tipo_id.code == TIPO_TINTORERIA)
            kg, wc = con_minimo(jets)
            if wc:
                return kg, '%s, de la ruta de %s' % (wc.name, tenido.display_name)
        general = _float_param(self.env, PARAM_MIN_BATH_KG)
        if general:
            return general, 'parámetro general del SGI'
        jets = self.env['mrp.workcenter'].sudo().search([('ft_tipo_id.code', '=', TIPO_TINTORERIA),
                                                         ('company_id', 'in', (company.id, False))])
        kg, wc = con_minimo(jets)
        if wc:
            return kg, '%s, el menor de los jets (la ruta del teñido no tiene jet con mínimo)' % wc.name
        return 0.0, ''

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
        if self.sgi_dev_product_tenido_id:
            bath, bath_src = self._sgi_dev_min_bath()
            if bath and rend:
                candidatos.append(('mínimo de baño de tintorería %g kg (%s)' % (bath, bath_src), bath * rend))
            elif bath:
                razones.append('El mínimo de baño (%g kg) no se pudo pasar a metros: faltan masa y ancho.' % bath)
            else:
                razones.append('Mínimo de baño de tintorería sin definir (ni en los jets ni en el parámetro).')
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
    """57.154.0: la corrida de muestra es la cadena del desarrollo (crudo → teñido → acabado), una orden
    por artículo. Los renglones se arman al abrir y se pueden ajustar antes de confirmar."""
    _name = 'sgi.dev.sample.wizard'
    _description = "Pedir la corrida de muestra del desarrollo"

    project_id = fields.Many2one('project.project', required=True, ondelete='cascade')
    line_ids = fields.One2many('sgi.dev.sample.wizard.line', 'wizard_id', string="Órdenes de la corrida")
    suggested_qty = fields.Float(string="Cantidad sugerida del último artículo", digits=(16, 3), readonly=True)
    suggestion_note = fields.Text(string="Por qué", readonly=True)
    date_planned = fields.Datetime(string="Fecha deseada de máquina", required=True,
                                   default=lambda self: fields.Datetime.now())
    note = fields.Text(string="Nota para Planeación")
    machine_warning = fields.Text(
        string="Revise la máquina", compute='_compute_machine_warning',
        help="Avisos de la ficha de la máquina sobre la ruta de cada lista: circular con otra galga que la del "
             "artículo o jet en el que la cantidad no cabe en una carga. No impiden pedir la corrida.")
    warning = fields.Text(string="Antes de confirmar", compute='_compute_warning',
                          help="Lo que impide crear las órdenes: lista de materiales que no es del artículo, "
                               "sin lista propia, sin ruta, sin cantidad o sin tipo de operación.")

    @api.model
    def default_get(self, fields_list):
        vals = super().default_get(fields_list)
        project = self.env['project.project'].browse(vals.get('project_id') or self.env.context.get('default_project_id'))
        if project and 'line_ids' in fields_list:
            vals.setdefault('project_id', project.id)
            plan, qty, note = self._sgi_dev_sample_plan(project)
            vals.update({'suggested_qty': qty, 'suggestion_note': note,
                         'line_ids': [(0, 0, line) for line in plan]})
        return vals

    # ------------------------------------------------------------------
    @api.model
    def _sgi_dev_sample_chain(self, project):
        """[(etapa, artículo)] en el orden en que se fabrica: crudo → teñido → acabado, sin repetir."""
        chain, seen = [], set()
        for stage, product in (('crudo', project.sgi_dev_product_crudo_id),
                               ('tenido', project.sgi_dev_product_tenido_id),
                               ('acabado', project.sgi_dev_product_id)):
            if product and product.id not in seen:
                chain.append((stage, product))
                seen.add(product.id)
        return chain

    @api.model
    def _sgi_dev_own_bom(self, product, company):
        """La lista de materiales del propio artículo (de su variante o de su plantilla); vacía si no tiene."""
        if not product:
            return self.env['mrp.bom']
        return self.env['mrp.bom']._bom_find(product, company_id=company.id, bom_type='normal').get(product) \
            or self.env['mrp.bom']

    @api.model
    def _sgi_dev_picking_type(self, stage, company):
        picking = _rec_param(self.env, STAGE_PICKING_PARAMS[stage], 'stock.picking.type')
        if picking:
            return picking
        return self.env['stock.picking.type'].sudo().search([
            ('name', '=', STAGE_PICKING_NAMES[stage]), ('code', '=', 'mrp_operation'),
            ('company_id', '=', company.id)], limit=1)

    @api.model
    def _sgi_dev_input_qty(self, project, bom, out_product, out_qty, in_product):
        """(cantidad de ``in_product`` para fabricar ``out_qty`` de ``out_product``, cómo se calculó). Con la
        lista de materiales del siguiente artículo si lo consume; si no, la misma cantidad pasada de unidad
        con el rendimiento (m/kg) de la tabla de características."""
        if bom and out_qty:
            lines = bom.bom_line_ids.filtered(lambda l: l.product_id == in_product)
            if lines:
                try:
                    out_in_bom = out_product.uom_id._compute_quantity(out_qty, bom.product_uom_id)
                    factor = out_in_bom / (bom.product_qty or 1.0)
                    total = sum(l.product_uom_id._compute_quantity(l.product_qty * factor, in_product.uom_id)
                                for l in lines)
                    per = sum(l.product_qty for l in lines)
                    return total, "%g %s por cada %g %s de %s (su lista de materiales)" % (
                        per, lines[:1].product_uom_id.name, bom.product_qty, bom.product_uom_id.name,
                        out_product.display_name)
                except Exception as exc:  # unidades sin conversión entre sí
                    _logger.info("Corrida de muestra: sin conversión de la lista %s (%s).", bom.display_name, exc)
        out_weight = project._sgi_dev_uom_is_weight(out_product.uom_id)
        in_weight = project._sgi_dev_uom_is_weight(in_product.uom_id)
        if out_weight == in_weight:
            return out_qty, "La lista de %s no lleva a %s: se pide la misma cantidad." % (
                out_product.display_name, in_product.display_name)
        rend = project._sgi_dev_yield_m_per_kg()
        if not rend:
            return 0.0, ("La lista de %s no lleva a %s y faltan masa y ancho para pasar de unidad: capture la "
                         "cantidad." % (out_product.display_name, in_product.display_name))
        qty = out_qty / rend if in_weight else out_qty * rend
        return qty, "La lista de %s no lleva a %s: la misma cantidad pasada de unidad con %.2f m/kg." % (
            out_product.display_name, in_product.display_name, rend)

    @api.model
    def _sgi_dev_sample_plan(self, project):
        """(renglones, cantidad sugerida, por qué): la cantidad del último artículo es la sugerida y las
        anteriores salen de atrás hacia adelante con la lista de materiales del siguiente."""
        chain = self._sgi_dev_sample_chain(project)
        if not chain:
            return [], 0.0, ''
        company = project.company_id or self.env.company
        location = _rec_param(self.env, PARAM_LOCATION, 'stock.location')
        last_stage, last_product = chain[-1]
        qty, note = project._sgi_dev_sample_suggestion(last_product)
        plan = []
        for index in range(len(chain) - 1, -1, -1):
            stage, product = chain[index]
            bom = self._sgi_dev_own_bom(product, company)
            if index == len(chain) - 1:
                line_qty, why = qty, "Cantidad sugerida de la muestra (ver «Por qué»)."
            else:
                nxt = plan[0]
                line_qty, why = self._sgi_dev_input_qty(
                    project, self.env['mrp.bom'].browse(nxt['bom_id']), self.env['product.product'].browse(
                        nxt['product_id']), nxt['product_qty'], product)
            picking = self._sgi_dev_picking_type(stage, company)
            if index == len(chain) - 1 and location:
                dest = location
            else:
                dest = picking.default_location_dest_id
            plan.insert(0, {
                'sequence': (index + 1) * 10, 'stage': stage, 'product_id': product.id,
                'bom_id': bom.id or False, 'product_qty': round(line_qty, 3), 'qty_note': why,
                'picking_type_id': picking.id or False, 'location_dest_id': dest.id or False,
            })
        return plan, qty, note

    # ------------------------------------------------------------------
    @api.depends('line_ids.bom_id', 'line_ids.product_qty', 'line_ids.picking_type_id', 'line_ids.location_dest_id')
    def _compute_warning(self):
        for wiz in self:
            wiz.warning = "\n".join(wiz._sgi_dev_problems()) or False

    @api.depends('line_ids.bom_id.operation_ids.workcenter_id', 'line_ids.product_qty', 'line_ids.product_id',
                 'date_planned')
    def _compute_machine_warning(self):
        """57.158.0: lo que avisaría la orden de trabajo en cada máquina de la ruta, antes de crear la orden."""
        kg = self.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        for wiz in self:
            avisos = []
            for line in wiz.line_ids.sorted('sequence'):
                qty_kg = 0.0
                uom = line.product_id.uom_id
                if kg and uom and line.product_qty:
                    try:
                        qty_kg = uom._compute_quantity(line.product_qty, kg) if uom != kg else line.product_qty
                    except Exception:  # unidad que no es de peso: no se compara contra la carga del jet
                        qty_kg = 0.0
                for workcenter in line.bom_id.operation_ids.workcenter_id:
                    aviso = workcenter._ft_aviso_para(line.product_id, qty_kg, wiz.date_planned)
                    if aviso:
                        avisos.append("%s: %s" % (dict(SAMPLE_STAGES).get(line.stage, line.stage), aviso))
            wiz.machine_warning = "\n".join(avisos) or False

    def _sgi_dev_problems(self):
        """Lo que impide crear la cadena, un texto por problema."""
        self.ensure_one()
        problems = []
        if not self.line_ids:
            problems.append("El proyecto no tiene artículos en desarrollo: genérelos antes de pedir la corrida.")
        for line in self.line_ids.sorted('sequence'):
            name = line.product_id.display_name
            if not line.bom_id:
                problems.append("%s no tiene lista de materiales propia: captúrela (C1.04) antes de pedir la "
                                "corrida." % name)
            elif not line._sgi_dev_bom_is_own():
                problems.append("La lista %s no es de %s: elija una lista del propio artículo."
                                % (line.bom_id.display_name, name))
            elif not line.bom_id.operation_ids:
                problems.append("La lista de %s no tiene operaciones (ruta): sin ruta la orden no genera órdenes de "
                                "trabajo y Planeación no la ve en máquina. Capture la ruta (C1.04b)." % name)
            if line.product_qty <= 0:
                problems.append("%s: la cantidad debe ser mayor que cero." % name)
            if not line.picking_type_id:
                problems.append("%s: falta el tipo de operación (%s)." % (name, STAGE_PICKING_NAMES.get(line.stage)))
            if not line.location_dest_id:
                problems.append("%s: falta a dónde entra lo fabricado." % name)
        return problems

    def _planning_users(self):
        job = _rec_param(self.env, PARAM_PLANNING_JOB, 'hr.job')
        if not job:
            return self.env['res.users']
        employees = self.env['hr.employee'].sudo().search([('job_id', '=', job.id)])
        return employees.user_id.filtered(lambda u: u.active and not u.share)

    def action_confirm(self):
        """Crea una orden por renglón, en el orden de la cadena. Si algún renglón tiene un problema no se
        crea ninguna."""
        self.ensure_one()
        project = self.project_id
        project._sgi_dev_check_run_allowed()
        problems = self._sgi_dev_problems()
        if problems:
            raise UserError("No se pidió ninguna orden de la corrida:\n- %s" % "\n- ".join(problems))
        folio = project.sgi_ft_folio or project.name
        planners = self._planning_users()
        mos = self.env['mrp.production']
        for line in self.line_ids.sorted('sequence'):
            stage_label = dict(SAMPLE_STAGES).get(line.stage)
            mo = self.env['mrp.production'].create({
                'product_id': line.product_id.id, 'product_qty': line.product_qty,
                'product_uom_id': line.product_id.uom_id.id, 'bom_id': line.bom_id.id,
                'picking_type_id': line.picking_type_id.id, 'location_dest_id': line.location_dest_id.id,
                'origin': folio, 'date_start': self.date_planned, 'company_id': project.company_id.id,
                'sgi_dev_project_id': project.id,
            })
            task = self.env['project.task'].create({
                'name': "Corrida de muestra %s: %s (%s)" % (mo.name, stage_label, line.product_id.display_name),
                'project_id': project.id, 'date_deadline': self.date_planned,
                'user_ids': [(6, 0, [self.env.uid])],
                'description': "<p>Orden %s (%s): %g %s de %s. Fecha de máquina deseada %s.%s</p>" % (
                    mo.name, stage_label, line.product_qty, line.product_id.uom_id.name,
                    line.product_id.display_name, self.date_planned, (' ' + self.note) if self.note else ''),
            })
            mo.write({'sgi_dev_task_id': task.id})
            mo.message_post(body="Orden de muestra del desarrollo %s (%s) pedida desde el proyecto por %s. %s%s"
                            % (folio, stage_label, self.env.user.name, line.qty_note or '',
                               (' ' + self.note) if self.note else ''))
            for user in planners:
                mo.activity_schedule('mail.mail_activity_data_todo', user_id=user.id,
                                     date_deadline=fields.Date.context_today(self),
                                     summary="Emitir la orden de muestra %s (%s)" % (mo.name, folio),
                                     note="Confirmar y programar la máquina. Fecha deseada: %s." % self.date_planned)
            mos |= mo
        when = fields.Datetime.context_timestamp(self, self.date_planned).strftime('%d/%m/%Y')
        body = "Corrida de muestra pedida para el %s: %s. %s%s" % (
            when, "; ".join("%s %g %s de %s" % (mo.name, mo.product_qty, mo.product_uom_id.name,
                                                mo.product_id.display_name) for mo in mos),
            self.suggestion_note or '', (' Nota: ' + self.note) if self.note else '')
        project.message_post(body=body)
        if len(mos) == 1:
            return {'type': 'ir.actions.act_window', 'res_model': 'mrp.production', 'res_id': mos.id,
                    'view_mode': 'form', 'target': 'current'}
        return {'type': 'ir.actions.act_window', 'name': "Corrida de muestra %s" % folio,
                'res_model': 'mrp.production', 'view_mode': 'list,form', 'target': 'current',
                'domain': [('id', 'in', mos.ids)]}


class SgiDevSampleWizardLine(models.TransientModel):
    """57.154.0: una orden de la corrida de muestra (crudo, teñido o acabado)."""
    _name = 'sgi.dev.sample.wizard.line'
    _description = "Orden de la corrida de muestra"
    _order = 'sequence, id'

    wizard_id = fields.Many2one('sgi.dev.sample.wizard', required=True, ondelete='cascade')
    sequence = fields.Integer(default=10)
    stage = fields.Selection(SAMPLE_STAGES, string="Etapa", required=True)
    product_id = fields.Many2one('product.product', string="Artículo", required=True)
    product_tmpl_id = fields.Many2one(related='product_id.product_tmpl_id')
    product_uom_id = fields.Many2one(related='product_id.uom_id', string="Unidad")
    bom_id = fields.Many2one('mrp.bom', string="Lista de materiales",
                             domain="[('product_tmpl_id', '=', product_tmpl_id), '|', ('product_id', '=', False), "
                                    "('product_id', '=', product_id)]",
                             help="Solo listas del propio artículo.")
    operation_count = fields.Integer(string="Operaciones de la ruta", compute='_compute_operation_count')
    product_qty = fields.Float(string="Cantidad", digits=(16, 3))
    qty_note = fields.Char(string="Cómo se calculó", readonly=True)
    picking_type_id = fields.Many2one('stock.picking.type', string="Tipo de operación",
                                      domain="[('code', '=', 'mrp_operation')]")
    location_dest_id = fields.Many2one('stock.location', string="A dónde entra",
                                       domain="[('usage', '=', 'internal')]")

    @api.depends('bom_id.operation_ids')
    def _compute_operation_count(self):
        for line in self:
            line.operation_count = len(line.bom_id.operation_ids)

    def _sgi_dev_bom_is_own(self):
        self.ensure_one()
        bom = self.bom_id
        return bool(bom) and bom.product_tmpl_id == self.product_id.product_tmpl_id and (
            not bom.product_id or bom.product_id == self.product_id)


class MrpProductionDevSample(models.Model):
    _inherit = 'mrp.production'

    sgi_dev_project_id = fields.Many2one('project.project', string="Proyecto de desarrollo", index=True,
                                         ondelete='set null', copy=False,
                                         help="Desarrollo cuya muestra fabrica esta orden; el sobrante queda ligado a él.")
    sgi_dev_task_id = fields.Many2one('project.task', string="Tarea de la corrida", ondelete='set null', copy=False)
    # 57.127.0 (Jose, 1c y 3.2): C1.09 se mide por quien emite (confirma) la orden y C1.10 por quien
    # valida la corrida. El botón «Validar corrida» es el enganche de las aprobaciones de C1.10 y de
    # las fichas de proceso de tintorería y acabado (Sacramento).
    sgi_dev_issued_by_id = fields.Many2one('res.users', string="Orden emitida por (Planeación)", readonly=True, copy=False)
    sgi_dev_issued_date = fields.Datetime(string="Orden emitida el", readonly=True, copy=False)
    sgi_dev_run_validated_by_id = fields.Many2one('res.users', string="Corrida validada por", readonly=True, copy=False)
    sgi_dev_run_validated_date = fields.Datetime(string="Corrida validada el", readonly=True, copy=False)

    def action_confirm(self):
        res = super().action_confirm()
        stamp = {'sgi_dev_issued_by_id': self.env.uid, 'sgi_dev_issued_date': fields.Datetime.now()}
        for mo in self.filtered(lambda m: m.sgi_dev_project_id and not m.sgi_dev_issued_by_id
                                and m.state not in ('draft', 'cancel')):
            mo.write(stamp)
        return res

    def action_sgi_dev_validate_run(self):
        """Validación de la corrida de muestra por el supervisor del área (C1.10). La aprobación por
        puesto la pone la regla de Studio del SGI sobre este botón."""
        for mo in self:
            if not mo.sgi_dev_project_id:
                raise UserError("Solo se valida la corrida de una orden de muestra de un desarrollo.")
            if mo.state != 'done':
                raise UserError("La corrida se valida con la orden terminada.")
            if mo.sgi_dev_run_validated_by_id:
                raise UserError("La corrida ya fue validada por %s." % mo.sgi_dev_run_validated_by_id.name)
            mo.write({'sgi_dev_run_validated_by_id': self.env.uid, 'sgi_dev_run_validated_date': fields.Datetime.now()})
            mo.message_post(body="Corrida de muestra validada por %s." % self.env.user.name)
            mo.sgi_dev_project_id.message_post(body="Corrida %s validada por %s." % (mo.name, self.env.user.name))
        return True

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
