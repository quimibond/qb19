# -*- coding: utf-8 -*-
"""Fichas de proceso del desarrollo sobre las fichas técnicas de tela (57.155.0).

Dirección General 2026-10-13: una sola ficha por proceso, la de
``quimibond_ficha_tecnica_tela`` (``ficha.tecnica.tejido`` y
``ficha.tecnica.acabado``; la de tintorería cuando exista). El SGI deja de
usar ``sgi.machine.sheet`` para el desarrollo y les agrega solo la capa de
control:

- proyecto (``project_id``) y orden de la corrida (``production_id``);
- **propuso** (Diseño y Desarrollo de Procesos), **validó** (Jefe de
  Manufactura del parámetro ``dev_validator_job_tejido_id``, su jefe directo o
  un administrador del SGI) y **midió** (Laboratorio), con fechas;
- por cada parámetro de la ficha, el **valor real** de la corrida, el número
  de ajuste y el motivo (lista ``motivo_ajuste``): ``sgi.dev.process.reading``.
  El valor nominal, la tolerancia y la unidad se leen en vivo de la ficha;
  nunca se copian.

«Validar corrida» (C1.10) se valida contra la ficha validada del artículo de
la orden (``mrp.production.action_sgi_dev_validate_run``).
"""
import re

from odoo import api, fields, models
from odoo.exceptions import UserError

# (campo del valor, tolerancia arriba, tolerancia abajo, unidad, etiqueta, sección)
TEJIDO_PARAMS = [
    ('velocidad', None, None, None, "Velocidad (rpm)", 'maquina'),
    ('longitud_malla_polea1', 'longitud_malla_tol', None, 'longitud_malla_tol_unit', "Longitud de malla, polea 1", 'maquina'),
    ('longitud_malla_polea2', 'longitud_malla_tol', None, 'longitud_malla_tol_unit', "Longitud de malla, polea 2", 'maquina'),
    ('consumo_cm_vta_polea1', 'consumo_cm_vta_tol', None, 'consumo_cm_vta_tol_unit', "Consumo cm/vuelta, polea 1", 'maquina'),
    ('consumo_cm_vta_polea2', 'consumo_cm_vta_tol', None, 'consumo_cm_vta_tol_unit', "Consumo cm/vuelta, polea 2", 'maquina'),
    ('polea_alimentacion_polea1', 'polea_alimentacion_tol', None, 'polea_alimentacion_tol_unit', "Polea de alimentación 1", 'maquina'),
    ('polea_alimentacion_polea2', 'polea_alimentacion_tol', None, 'polea_alimentacion_tol_unit', "Polea de alimentación 2", 'maquina'),
    ('tension', 'tension_tol', None, 'tension_tol_unit', "Tensión", 'maquina'),
    ('punto_cilindro', 'punto_cilindro_tol', None, 'punto_cilindro_tol_unit', "Punto cilindro", 'maquina'),
    ('punto_plato', 'punto_plato_tol', None, 'punto_plato_tol_unit', "Punto plato", 'maquina'),
    ('altura_plato', 'altura_plato_tol', None, 'altura_plato_tol_unit', "Altura plato", 'maquina'),
    ('ancho_bastidor', 'ancho_bastidor_tol', None, 'ancho_bastidor_tol_unit', "Ancho de bastidor", 'maquina'),
    ('estiraje', 'estiraje_tol', None, 'estiraje_tol_unit', "Estiraje", 'maquina'),
    ('ancho_rollo', 'ancho_rollo_tol', None, 'ancho_rollo_tol_unit', "Ancho de rollo", 'maquina'),
    ('peso_promedio_rollo', 'peso_promedio_rollo_tol', None, 'peso_promedio_rollo_tol_unit', "Peso promedio de rollo", 'maquina'),
    ('peso_acondicionado', 'peso_acondicionado_tol', 'peso_acondicionado_tol_minus', 'peso_acondicionado_tol_unit', "Peso", 'tela'),
    ('ancho_acondicionado', 'ancho_acondicionado_tol', None, 'ancho_acondicionado_tol_unit', "Ancho", 'tela'),
    ('espesor_acondicionado', 'espesor_acondicionado_tol', None, 'espesor_acondicionado_tol_unit', "Espesor", 'tela'),
    ('columnas', 'columnas_tol', None, 'columnas_tol_unit', "Columnas", 'tela'),
    ('mallas', 'mallas_tol', None, 'mallas_tol_unit', "Mallas", 'tela'),
    ('elongacion_carga_largo', 'elongacion_carga_largo_tol', None, 'elongacion_carga_largo_tol_unit', "Elongación bajo carga, largo", 'tela'),
    ('elongacion_carga_ancho', 'elongacion_carga_ancho_tol', None, 'elongacion_carga_ancho_tol_unit', "Elongación bajo carga, ancho", 'tela'),
]
ACABADO_PARAMS = [
    ('rendimiento_tela_acabada', None, None, None, "Rendimiento de tela acabada (m/kg)", 'tela'),
    ('peso_acabado', 'peso_acabado_tol', 'peso_acabado_tol_minus', 'peso_acabado_tol_unit', "Peso", 'tela'),
    ('ancho_acabado', 'ancho_acabado_tol', None, 'ancho_acabado_tol_unit', "Ancho", 'tela'),
    ('encogimiento_largo', 'encogimiento_largo_tol', None, 'encogimiento_largo_tol_unit', "Encogimiento a lo largo", 'tela'),
    ('encogimiento_ancho', 'encogimiento_ancho_tol', None, 'encogimiento_ancho_tol_unit', "Encogimiento a lo ancho", 'tela'),
    ('espesor_acabado', 'espesor_acabado_tol', None, 'espesor_acabado_tol_unit', "Espesor", 'tela'),
    ('elongacion_largo_acabado', 'elongacion_largo_acabado_tol', None, 'elongacion_largo_acabado_tol_unit', "Elongación largo", 'tela'),
    ('elongacion_ancho_acabado', 'elongacion_ancho_acabado_tol', None, 'elongacion_ancho_acabado_tol_unit', "Elongación ancho", 'tela'),
]
SECTIONS = [('maquina', "Datos de máquina"), ('tela', "Datos de tela")]


def _spec_depends():
    """Campos de las fichas de los que dependen nominal, tolerancia y resultado de un renglón."""
    deps = []
    for parent, params in (('tejido_id', TEJIDO_PARAMS), ('acabado_id', ACABADO_PARAMS)):
        for spec in params:
            deps.extend('%s.%s' % (parent, name) for name in spec[:4] if name)
    return deps


NUMBER = re.compile(r'[-+]?\d+(?:[.,]\d+)?')


def _as_float(value):
    """Número de un campo de la ficha (algunos valores siguen siendo texto: tensión, puntos, estiraje)."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    match = NUMBER.search(str(value))
    return float(match.group(0).replace(',', '.')) if match else None


class SgiDevProcessSheetMixin(models.AbstractModel):
    """Capa de control del desarrollo sobre una ficha técnica de proceso (tejido, acabado)."""
    _name = 'sgi.dev.process.sheet.mixin'
    _description = "Control del desarrollo sobre una ficha de proceso"
    _sgi_params = []

    project_id = fields.Many2one('project.project', string="Proyecto de desarrollo", index=True, ondelete='set null',
                                 domain="[('sgi_is_ft', '=', True), ('is_template', '=', False)]",
                                 help="Desarrollo de producto al que pertenece esta ficha.")
    production_id = fields.Many2one('mrp.production', string="Orden de la corrida", ondelete='set null',
                                    help="Orden de muestra con la que se corrieron los valores reales.")
    sgi_proposed_by_id = fields.Many2one('res.users', string="Propuso (Diseño y Desarrollo de Procesos)",
                                         readonly=True, copy=False)
    sgi_proposed_date = fields.Datetime(string="Propuesta el", readonly=True, copy=False)
    sgi_validated_by_id = fields.Many2one('res.users', string="Validó (Jefe de Manufactura)", readonly=True, copy=False)
    sgi_validated_date = fields.Datetime(string="Validada el", readonly=True, copy=False)
    sgi_measured_by_id = fields.Many2one('res.users', string="Midió (Laboratorio)", readonly=True, copy=False)
    sgi_measured_date = fields.Datetime(string="Medida el", readonly=True, copy=False)

    # ------------------------------------------------------------------
    def _sgi_product(self):
        """Artículo de la ficha (el crudo en tejido, el acabado en acabado)."""
        return self.env['product.product']

    def action_sgi_load_readings(self):
        """Un renglón por parámetro de la ficha que todavía no tiene el suyo."""
        Reading = self.env['sgi.dev.process.reading']
        for sheet in self:
            have = set(sheet.sgi_reading_ids.mapped('param_key'))
            vals = []
            for index, (key, _tol, _minus, _unit, label, section) in enumerate(self._sgi_params):
                if key not in have:
                    vals.append({sheet._sgi_reading_parent_field(): sheet.id, 'param_key': key, 'name': label,
                                 'section': section, 'sequence': (index + 1) * 10})
            if vals:
                Reading.create(vals)
        return True

    def action_sgi_propose(self):
        for sheet in self:
            if sheet.sgi_validated_by_id:
                raise UserError("%s ya está validada: una ficha validada no se vuelve a proponer." % sheet.display_name)
            if not sheet.sgi_reading_ids:
                sheet.action_sgi_load_readings()
            sheet.write({'sgi_proposed_by_id': self.env.uid, 'sgi_proposed_date': fields.Datetime.now()})
        return True

    def action_sgi_measure(self):
        for sheet in self:
            sheet.write({'sgi_measured_by_id': self.env.uid, 'sgi_measured_date': fields.Datetime.now()})
        return True

    def _sgi_validation_problems(self):
        self.ensure_one()
        problems = []
        if not self.sgi_proposed_by_id:
            problems.append("Primero propone Diseño y Desarrollo de Procesos.")
        if not self.sgi_reading_ids:
            problems.append("La ficha no tiene parámetros: «Cargar parámetros» y capture los valores reales.")
        missing = self.sgi_reading_ids.filtered(lambda r: not r.captured)
        if missing:
            problems.append("Falta el valor real de: %s." % ", ".join(missing.mapped('name')))
        out = self.sgi_reading_ids.filtered(lambda r: r.result == 'fuera' and not r.reason_id)
        if out:
            problems.append("Fuera de tolerancia sin motivo de ajuste: %s." % ", ".join(out.mapped('name')))
        return problems

    def action_sgi_validate(self):
        from .sgi_dev_process_sheet import PARAM_VALIDATOR_JOB_TEJIDO
        Sheet = self.env['sgi.machine.sheet']
        for sheet in self:
            if sheet.sgi_validated_by_id:
                raise UserError("%s ya fue validada por %s." % (sheet.display_name, sheet.sgi_validated_by_id.name))
            if not (self.env.user.has_group('quimibond_sgi.group_sgi_manager')
                    or Sheet._sgi_dev_user_has_job(self.env.user, PARAM_VALIDATOR_JOB_TEJIDO)):
                raise UserError("La ficha de proceso la valida el Jefe de Manufactura (Ajustes → SGI → Desarrollo de "
                                "producto), su jefe directo o un administrador del SGI.")
            problems = sheet._sgi_validation_problems()
            if problems:
                raise UserError("%s no se puede validar:\n- %s" % (sheet.display_name, "\n- ".join(problems)))
            sheet.write({'sgi_validated_by_id': self.env.uid, 'sgi_validated_date': fields.Datetime.now()})
            if sheet.project_id:
                sheet.project_id.sudo().message_post(body="Ficha de proceso %s validada por %s." % (
                    sheet.display_name, self.env.user.name), author_id=self.env.user.partner_id.id)
        return True

    @api.model_create_multi
    def create(self, vals_list):
        sheets = super().create(vals_list)
        sheets.filtered('project_id').action_sgi_load_readings()
        return sheets


class FichaTecnicaTejidoSgi(models.Model):
    _name = 'ficha.tecnica.tejido'
    _inherit = ['ficha.tecnica.tejido', 'sgi.dev.process.sheet.mixin']
    _sgi_params = TEJIDO_PARAMS

    sgi_reading_ids = fields.One2many('sgi.dev.process.reading', 'tejido_id', string="Valores de la corrida")

    def _sgi_product(self):
        return self.product_proceso_id

    def _sgi_reading_parent_field(self):
        return 'tejido_id'


class FichaTecnicaAcabadoSgi(models.Model):
    _name = 'ficha.tecnica.acabado'
    _inherit = ['ficha.tecnica.acabado', 'sgi.dev.process.sheet.mixin']
    _sgi_params = ACABADO_PARAMS

    sgi_reading_ids = fields.One2many('sgi.dev.process.reading', 'acabado_id', string="Valores de la corrida")

    def _sgi_product(self):
        return self.product_acabado_id

    def _sgi_reading_parent_field(self):
        return 'acabado_id'


class SgiDevProcessReading(models.Model):
    """Valor real de un parámetro de la ficha en la corrida del desarrollo, con su número de ajuste y motivo.
    Nominal, tolerancia y unidad se leen de la ficha."""
    _name = 'sgi.dev.process.reading'
    _description = "Valor real de un parámetro de la ficha de proceso"
    _order = 'section, sequence, id'

    tejido_id = fields.Many2one('ficha.tecnica.tejido', string="Ficha de tejido", ondelete='cascade', index=True)
    acabado_id = fields.Many2one('ficha.tecnica.acabado', string="Ficha de acabado", ondelete='cascade', index=True)
    sequence = fields.Integer(default=10)
    section = fields.Selection(SECTIONS, string="Sección", default='maquina', required=True)
    param_key = fields.Char(string="Campo de la ficha", required=True, readonly=True)
    name = fields.Char(string="Parámetro", required=True, readonly=True)
    nominal = fields.Char(string="Nominal", compute='_compute_spec')
    tolerance = fields.Char(string="Tolerancia", compute='_compute_spec')
    unit = fields.Char(string="Unidad", compute='_compute_spec')
    real = fields.Float(string="Real (corrida)", digits=(16, 3),
                        help="Lo que se corrió de verdad; lo valida el Jefe de Manufactura.")
    captured = fields.Boolean(string="Capturado", readonly=True,
                              help="Se capturó el valor real (un 0 capturado cuenta).")
    adjustment = fields.Integer(string="Número de ajuste", help="Cuántas veces se ajustó el parámetro en la corrida.")
    reason_id = fields.Many2one('sgi.dev.option', string="Motivo del ajuste", domain="[('kind', '=', 'motivo_ajuste')]")
    result = fields.Selection([('ok', "Dentro"), ('fuera', "Fuera de tolerancia"), ('na', "Sin tolerancia"),
                               ('pendiente', "Sin capturar")], string="Resultado", compute='_compute_spec')

    def _sgi_sheet(self):
        self.ensure_one()
        return self.tejido_id or self.acabado_id

    def _sgi_spec(self):
        """(nominal, tolerancia arriba, tolerancia abajo, unidad) del parámetro, leídos de la ficha."""
        self.ensure_one()
        sheet = self._sgi_sheet()
        for key, tol, minus, unit, _label, _section in (sheet._sgi_params if sheet else []):
            if key == self.param_key:
                up = _as_float(sheet[tol]) if tol else None
                down = _as_float(sheet[minus]) if minus else None
                return sheet[key], up, (down or up), (sheet[unit] if unit else '') or ''
        return False, None, None, ''

    @api.depends('param_key', 'real', 'captured', 'tejido_id', 'acabado_id', *_spec_depends())
    def _compute_spec(self):
        for reading in self:
            nominal, up, down, unit = reading._sgi_spec()
            nominal_num = _as_float(nominal)
            reading.nominal = '' if nominal in (False, None) else (
                ('%g' % nominal_num) if isinstance(nominal, (int, float)) else str(nominal))
            if up and down and abs(up - down) > 1e-9:
                reading.tolerance = "+%g / −%g" % (up, down)
            elif up:
                reading.tolerance = "± %g" % up
            else:
                reading.tolerance = ''
            reading.unit = unit
            if not reading.captured:
                reading.result = 'pendiente'
            elif nominal_num is None or not up:
                reading.result = 'na'
            else:
                reading.result = 'ok' if nominal_num - down - 1e-9 <= reading.real <= nominal_num + up + 1e-9 else 'fuera'

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if 'real' in vals:
                vals['captured'] = True
        return super().create(vals_list)

    def write(self, vals):
        if 'real' in vals:
            vals = dict(vals, captured=True)
        return super().write(vals)


# ---------------------------------------------------------------------------
# Proyecto: fichas de proceso de los artículos del desarrollo
# ---------------------------------------------------------------------------
SHEET_STAGES = [('crudo', "Crudo (tejido)"), ('tenido', "Teñido (tintorería)"), ('acabado', "Acabado")]
SHEET_MODELS = {'crudo': ('ficha.tecnica.tejido', 'product_proceso_id'),
                'acabado': ('ficha.tecnica.acabado', 'product_acabado_id')}
TINTORERIA_PENDING = ("La ficha de tintorería todavía no existe (formato F-P-D01-33; la construye Jose Sacramento "
                      "en quimibond_ficha_tecnica_tela).")


class ProjectProjectDevSheets(models.Model):
    _inherit = 'project.project'

    def _sgi_dev_find_sheet(self, stage, product):
        """La ficha de proceso del artículo: la del proyecto si hay, si no la más reciente. Vacía si falta o
        si la etapa no tiene modelo de ficha (tintorería)."""
        self.ensure_one()
        if stage not in SHEET_MODELS or not product:
            return None
        model, field = SHEET_MODELS[stage]
        Sheet = self.env[model]
        sheets = Sheet.search([(field, '=', product.id)])
        if not sheets and product.default_code:
            sheets = Sheet.search([('articulo', '=', product.default_code)])
        return sheets.sorted(lambda s: (s.project_id == self, s.id), reverse=True)[:1]

    def action_sgi_dev_process_sheets(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': "Fichas de proceso de %s" % (self.sgi_ft_folio or self.name),
                'res_model': 'sgi.dev.process.sheets.wizard', 'view_mode': 'form', 'target': 'new',
                'context': {'default_project_id': self.id}}


class SgiDevProcessSheetsWizard(models.TransientModel):
    """57.155.0: las fichas de proceso de los artículos del proyecto (crudo, teñido, acabado); abre la que
    existe o crea la que falta con el artículo ya puesto."""
    _name = 'sgi.dev.process.sheets.wizard'
    _description = "Fichas de proceso del desarrollo"

    project_id = fields.Many2one('project.project', required=True, ondelete='cascade')
    line_ids = fields.One2many('sgi.dev.process.sheets.wizard.line', 'wizard_id', string="Fichas")

    @api.model
    def default_get(self, fields_list):
        vals = super().default_get(fields_list)
        project = self.env['project.project'].browse(vals.get('project_id') or self.env.context.get('default_project_id'))
        if project and 'line_ids' in fields_list:
            vals.setdefault('project_id', project.id)
            lines = []
            for index, (stage, product) in enumerate(self.env['sgi.dev.sample.wizard']._sgi_dev_sample_chain(project)):
                sheet = project._sgi_dev_find_sheet(stage, product)
                line = {'sequence': (index + 1) * 10, 'stage': stage, 'product_id': product.id}
                if sheet:
                    line['tejido_id' if stage == 'crudo' else 'acabado_id'] = sheet.id
                lines.append((0, 0, line))
            vals['line_ids'] = lines
        return vals


class SgiDevProcessSheetsWizardLine(models.TransientModel):
    _name = 'sgi.dev.process.sheets.wizard.line'
    _description = "Ficha de proceso de un artículo del desarrollo"
    _order = 'sequence, id'

    wizard_id = fields.Many2one('sgi.dev.process.sheets.wizard', required=True, ondelete='cascade')
    sequence = fields.Integer(default=10)
    stage = fields.Selection(SHEET_STAGES, string="Etapa", required=True)
    product_id = fields.Many2one('product.product', string="Artículo", required=True)
    tejido_id = fields.Many2one('ficha.tecnica.tejido', string="Ficha de tejido")
    acabado_id = fields.Many2one('ficha.tecnica.acabado', string="Ficha de acabado")
    has_sheet = fields.Boolean(compute='_compute_status')
    can_create = fields.Boolean(compute='_compute_status')
    status = fields.Char(string="Estado", compute='_compute_status')

    @api.depends('stage', 'tejido_id', 'acabado_id')
    def _compute_status(self):
        for line in self:
            sheet = line.tejido_id or line.acabado_id
            line.has_sheet = bool(sheet)
            line.can_create = not sheet and line.stage in SHEET_MODELS
            if line.stage not in SHEET_MODELS:
                line.status = TINTORERIA_PENDING
            elif not sheet:
                line.status = "Falta la ficha"
            elif sheet.sgi_validated_by_id:
                line.status = "Validada por %s" % sheet.sgi_validated_by_id.name
            elif sheet.sgi_proposed_by_id:
                line.status = "Propuesta; falta validar"
            else:
                line.status = "Sin proponer"

    def action_open(self):
        """Abre la ficha; si falta, la ficha nueva con el artículo, el proyecto y (en acabado) la ficha de
        tejido del crudo ya puestos."""
        self.ensure_one()
        project = self.wizard_id.project_id
        sheet = self.tejido_id or self.acabado_id
        if self.stage not in SHEET_MODELS:
            raise UserError(TINTORERIA_PENDING)
        model, field = SHEET_MODELS[self.stage]
        action = {'type': 'ir.actions.act_window', 'res_model': model, 'view_mode': 'form', 'target': 'current',
                  'name': "Ficha de proceso: %s" % self.product_id.display_name}
        if sheet:
            action['res_id'] = sheet.id
            if not sheet.project_id:
                sheet.write({'project_id': project.id})
                sheet.action_sgi_load_readings()
            return action
        ctx = {'default_articulo': self.product_id.default_code or self.product_id.name,
               'default_%s' % field: self.product_id.id, 'default_project_id': project.id,
               'default_fecha_elaboracion': fields.Date.context_today(self)}
        if self.stage == 'acabado':
            tejido = project._sgi_dev_find_sheet('crudo', project.sgi_dev_product_crudo_id)
            if tejido:
                ctx['default_tejido_id'] = tejido.id
            # 57.160.0: el acabado en proceso (K, kg) del proyecto, junto a la tela acabada.
            if project.sgi_dev_product_proceso_id:
                ctx['default_product_proceso_id'] = project.sgi_dev_product_proceso_id.id
        elif self.stage == 'crudo':
            # 57.158.0: la galga del artículo y, si una sola circular la corre hoy, esa máquina (la ficha toma
            # de su configuración galga, diámetro, agujas, alimentadores y marca). Con varias, Desarrollo
            # elige: la lista ya ofrece solo las de esa galga.
            galga, circulares = project._sgi_dev_circulares()
            if galga:
                ctx['default_galga_requerida'] = galga
            if len(circulares) == 1:
                ctx['default_maquina_tejido'] = circulares.id
        action['context'] = ctx
        return action


# ---------------------------------------------------------------------------
# C1.10: «Validar corrida» contra la ficha del artículo de la orden
# ---------------------------------------------------------------------------
class MrpProductionDevSheet(models.Model):
    _inherit = 'mrp.production'

    def _sgi_dev_run_sheet(self):
        """(etapa, ficha) del artículo de la orden; ficha vacía si falta o si es teñido (sin modelo aún)."""
        self.ensure_one()
        project = self.sgi_dev_project_id
        product = self.product_id
        if product == project.sgi_dev_product_crudo_id:
            stage = 'crudo'
        elif product == project.sgi_dev_product_id:
            stage = 'acabado'
        elif product == project.sgi_dev_product_tenido_id:
            stage = 'tenido'
        else:
            stage = 'crudo' if self.env['ficha.tecnica.tejido'].search_count(
                [('product_proceso_id', '=', product.id)], limit=1) else 'acabado'
        return stage, project._sgi_dev_find_sheet(stage, product)

    def _sgi_dev_run_sheet_problems(self):
        self.ensure_one()
        stage, sheet = self._sgi_dev_run_sheet()
        if stage == 'tenido':
            return []
        if not sheet:
            return ["%s no tiene ficha de proceso: créela desde el proyecto (pestaña Muestra → «Fichas de "
                    "proceso») con los parámetros propuestos y los valores reales de la corrida."
                    % self.product_id.display_name]
        if not sheet.sgi_validated_by_id:
            return ["La ficha de proceso %s no está validada por el Jefe de Manufactura: la corrida se valida "
                    "contra la ficha validada." % sheet.display_name]
        return []

    def action_sgi_dev_validate_run(self):
        for mo in self.filtered(lambda m: m.sgi_dev_project_id and m.state == 'done'
                                and not m.sgi_dev_run_validated_by_id):
            problems = mo._sgi_dev_run_sheet_problems()
            if problems:
                raise UserError("\n".join(problems))
        res = super().action_sgi_dev_validate_run()
        for mo in self.filtered('sgi_dev_project_id'):
            stage, sheet = mo._sgi_dev_run_sheet()
            if sheet and not sheet.production_id:
                sheet.write({'production_id': mo.id})
            if stage == 'tenido':
                mo.message_post(body="Corrida de teñido validada sin ficha de proceso: %s" % TINTORERIA_PENDING)
        return res
