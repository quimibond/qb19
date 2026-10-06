# -*- coding: utf-8 -*-
"""Análisis del desarrollo: parecidos, pruebas de laboratorio y factibilidad
(57.120.0, C1 bloque 4; brief 6.3 a 6.5).

- **Artículos parecidos** (``sgi.dev.similar``, asistente): con la
  especificación capturada, Odoo lista artículos de línea y desarrollos
  anteriores ordenados por cercanía en peso, ancho, composición, dibujo y
  galga (leídos del código del artículo según el DAT P-D02-01). Si todas
  las características del artículo caen dentro de la tolerancia del cliente
  propone «producto de línea»; si una sola queda fuera, «producto nuevo».
  Diseño de Producto confirma con «Usar como base»: el artículo queda ligado
  al proyecto. Sustituye al formato Experiencias previas.
- **Solicitud de pruebas a laboratorio** (``sgi.dev.lab.request``): Diseño
  marca qué renglones van a laboratorio (``lab_requested``) y genera la
  solicitud con un botón; la autoriza el puesto del parámetro
  ``quimibond_sgi.dev_lab_authorizer_job_id`` (Coordinador de Laboratorio y
  MP) o el Jefe MAST; el laboratorista captura el resultado **en el mismo
  renglón** de la tabla y la solicitud se cierra sola cuando todos tienen
  valor. El laboratorio solo mide: el dictamen es de Diseño (``verdict``).
  Queda el tiempo que tardó.
- **Checklist de factibilidad** (``sgi.dev.feasibility.item`` catálogo por
  línea y ``sgi.dev.feasibility`` renglones del proyecto): Diseño de
  Procesos marca sí o no con observación. Los renglones que Odoo puede
  contestar se contestan solos: existencia de materia prima (lista de
  materiales del artículo contra existencias); la capacidad de máquina
  llega con el cotizador nuevo (plan de costeo v2). **El catálogo está
  vacío**: la lista de recursos por línea no está definida (brief, sección 8).
- **Una sola pantalla de revisión**: Ventas aprueba juntos el análisis de
  Diseño de Producto y la factibilidad de Diseño de Procesos
  (``sgi_dev_review_state``). Sin esa aprobación el proyecto no pasa a
  Cotización. El AMEF de proceso no va antes de cotizar.
"""
import re

from odoo import api, fields, models
from odoo.exceptions import UserError

CODE_RE = re.compile(r'^(?P<comp>[A-Z])(?P<dib>[A-Z])(?P<peso>\d{3})(?P<hilo>[A-Z])(?P<galga>\d{2})'
                     r'(?P<op>[HIJ])(?P<color>[A-Z]{2})(?P<ancho>\d{3})(?P<acab>[A-Z]{2})?$')
PARAM_LAB_JOB = 'quimibond_sgi.dev_lab_authorizer_job_id'
# Puesto por omisión (decisión de Jose, 2026-10-06): hr.job 188 en producción.
LAB_JOB_DEFAULT_NAME = 'Coordinador de Laboratorio'

DEV_LINES = [('tejido_circular', "Tejido circular"), ('entretelas', "Entretelas")]
FEAS_AUTO = [('none', "La contesta una persona"), ('materia_prima', "Existencia de materia prima"),
             ('capacidad', "Capacidad de máquina")]
FEAS_ANSWERS = [('pendiente', "Pendiente"), ('si', "Sí"), ('no', "No"), ('na', "No aplica")]
REVIEW_STATES = [('pendiente', "Pendiente"), ('aprobado', "Aprobado"), ('regresado', "Regresado")]


def parse_article_code(code):
    """Posiciones del código de tejido y acabado o None si no sigue la regla."""
    m = CODE_RE.match((code or '').strip().upper())
    if not m:
        return None
    d = m.groupdict()
    return {'comp': d['comp'], 'dib': d['dib'], 'peso': int(d['peso']), 'hilo': d['hilo'],
            'galga_code': d['galga'], 'op': d['op'], 'color': d['color'], 'ancho': int(d['ancho']),
            'acab': d['acab'] or ''}


# ---------------------------------------------------------------------------
# Artículos parecidos
# ---------------------------------------------------------------------------
class SgiDevSimilar(models.TransientModel):
    """Asistente: artículos de línea y desarrollos anteriores parecidos a lo que pide el cliente."""
    _name = 'sgi.dev.similar'
    _description = "Artículos parecidos al desarrollo"

    project_id = fields.Many2one('project.project', required=True, ondelete='cascade',
                                 help="Proyecto de desarrollo que se compara.")
    peso = fields.Integer(string="Peso pedido (g/m²)", readonly=True, help="Masa nominal que pide el cliente.")
    ancho = fields.Integer(string="Ancho pedido (cm)", readonly=True, help="Ancho nominal que pide el cliente.")
    line_ids = fields.One2many('sgi.dev.similar.line', 'wizard_id', string="Parecidos")

    def action_refresh(self):
        self.ensure_one()
        self.line_ids.unlink()
        self.write({'line_ids': [(0, 0, vals) for vals in self.project_id._sgi_dev_similar_candidates()]})
        return {'type': 'ir.actions.act_window', 'res_model': self._name, 'res_id': self.id,
                'view_mode': 'form', 'target': 'new'}


class SgiDevSimilarLine(models.TransientModel):
    """Un artículo parecido con su distancia y si cumple la tolerancia del cliente."""
    _name = 'sgi.dev.similar.line'
    _description = "Artículo parecido"
    _order = 'score, id'

    wizard_id = fields.Many2one('sgi.dev.similar', required=True, ondelete='cascade')
    product_id = fields.Many2one('product.product', string="Artículo", required=True, readonly=True)
    source = fields.Selection([('linea', "De línea"), ('desarrollo', "Desarrollo anterior")], string="Origen",
                              readonly=True, help="Artículo de línea o de un desarrollo anterior.")
    peso = fields.Integer(string="Peso (g/m²)", readonly=True)
    ancho = fields.Integer(string="Ancho (cm)", readonly=True)
    galga = fields.Integer(string="Galga", readonly=True)
    score = fields.Float(string="Distancia", readonly=True, digits=(16, 1),
                         help="Menor es más parecido: % de diferencia en peso y ancho más castigo por composición, "
                              "dibujo y galga distintos.")
    within_tolerance = fields.Boolean(string="Dentro de tolerancia", readonly=True,
                                      help="Peso y ancho dentro de lo que pide el cliente, con la misma composición, "
                                           "dibujo y galga: puede ser producto de línea.")
    note = fields.Char(string="Diferencias", readonly=True)

    def action_use_as_base(self):
        self.ensure_one()
        project = self.wizard_id.project_id
        project.write({
            'sgi_dev_base_product_id': self.product_id.id,
            'sgi_dev_analysis_result': 'linea' if self.within_tolerance else 'nuevo',
        })
        project.message_post(body="Artículo base: %s (%s). Propuesta: %s." % (
            self.product_id.display_name, "dentro de tolerancia" if self.within_tolerance else "con diferencias",
            "producto de línea" if self.within_tolerance else "producto nuevo"))
        return {'type': 'ir.actions.act_window_close'}


# ---------------------------------------------------------------------------
# Solicitud de pruebas a laboratorio
# ---------------------------------------------------------------------------
class SgiDevLabRequest(models.Model):
    """Solicitud de pruebas de laboratorio sobre renglones de la tabla de características."""
    _name = 'sgi.dev.lab.request'
    _description = "Solicitud de pruebas a laboratorio (desarrollo)"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'id desc'

    name = fields.Char(string="Solicitud", compute='_compute_name', store=True)
    project_id = fields.Many2one('project.project', string="Proyecto", required=True, ondelete='cascade', index=True,
                                 help="Proyecto de desarrollo cuyos renglones se miden.")
    kind = fields.Selection([('muestra_cliente', "Muestra del cliente"), ('corrida', "Corrida de muestra")],
                            string="Qué se mide", default='muestra_cliente', required=True,
                            help="La muestra que mandó el cliente (análisis) o la corrida propia (paso 12).")
    state = fields.Selection([('borrador', "Borrador"), ('solicitada', "Solicitada"), ('autorizada', "Autorizada"),
                              ('medida', "Medida"), ('cancelada', "Cancelada")], string="Estado", default='borrador',
                             required=True, tracking=True)
    line_ids = fields.Many2many('sgi.dev.characteristic', 'sgi_dev_lab_request_line_rel', 'request_id', 'line_id',
                                string="Renglones a medir", help="Renglones de la tabla del proyecto que mide el laboratorio.")
    requested_by_id = fields.Many2one('res.users', string="Solicitó", default=lambda self: self.env.user, readonly=True)
    date_requested = fields.Datetime(string="Solicitada el", readonly=True)
    authorized_by_id = fields.Many2one('res.users', string="Autorizó", readonly=True)
    date_authorized = fields.Datetime(string="Autorizada el", readonly=True)
    date_measured = fields.Datetime(string="Medida el", readonly=True)
    pending_count = fields.Integer(string="Sin resultado", compute='_compute_pending', store=True,
                                   help="Renglones que aún no tienen valor medido.")
    hours_total = fields.Float(string="Horas del laboratorio", compute='_compute_hours', digits=(16, 1),
                               help="Horas calendario de la solicitud a la última medición.")
    note = fields.Text(string="Observaciones", help="Instrucciones para el laboratorio; no capture aquí valores.")
    company_id = fields.Many2one('res.company', default=lambda self: self.env.company)

    @api.depends('project_id.name', 'kind')
    def _compute_name(self):
        for req in self:
            req.name = "Pruebas %s · %s" % (dict(req._fields['kind'].selection).get(req.kind, ''),
                                            req.project_id.name or '')

    def _line_measured(self, line):
        if self.kind == 'corrida':
            return bool(line.run_count) or bool(line.run_text)
        return bool(line.sample_value) or bool(line.sample_text)

    @api.depends('line_ids.sample_value', 'line_ids.sample_text', 'line_ids.run_count', 'line_ids.run_text', 'kind')
    def _compute_pending(self):
        for req in self:
            req.pending_count = len([l for l in req.line_ids if not req._line_measured(l)])

    @api.depends('date_requested', 'date_measured')
    def _compute_hours(self):
        now = fields.Datetime.now()
        for req in self:
            start = req.date_requested
            req.hours_total = max(((req.date_measured or now) - start).total_seconds(), 0) / 3600.0 if start else 0.0

    @api.model
    def _authorizer_job(self):
        """El puesto del parámetro o, si está vacío, el Coordinador de Laboratorio y MP por nombre."""
        Job = self.env['hr.job'].sudo()
        raw = self.env['ir.config_parameter'].sudo().get_param(PARAM_LAB_JOB, '') or ''
        try:
            job = Job.browse(int(raw)).exists()
        except ValueError:
            job = Job
        return job or Job.search([('name', 'ilike', LAB_JOB_DEFAULT_NAME)], limit=1)

    @api.model
    def _sgi_dev_set_lab_job_default(self):
        """Deja el puesto por omisión escrito en el parámetro (migración / instalación)."""
        Param = self.env['ir.config_parameter'].sudo()
        if not (Param.get_param(PARAM_LAB_JOB, '') or '').strip():
            job = self.env['hr.job'].sudo().search([('name', 'ilike', LAB_JOB_DEFAULT_NAME)], limit=1)
            if job:
                Param.set_param(PARAM_LAB_JOB, str(job.id))
            return job
        return self.env['hr.job']

    @api.model
    def _authorizer_users(self):
        """Usuarios del puesto autorizador; el Jefe MAST siempre puede."""
        job = self._authorizer_job()
        if not job:
            return self.env['res.users']
        return self.env['hr.employee'].sudo().search([('job_id', '=', job.id)]).mapped('user_id')

    def action_request(self):
        for req in self:
            if not req.line_ids:
                raise UserError("Marque al menos un renglón para medir.")
            req.write({'state': 'solicitada', 'date_requested': fields.Datetime.now(),
                       'requested_by_id': self.env.uid})
            users = req._authorizer_users()
            if users:
                req.activity_schedule('mail.mail_activity_data_todo', user_id=users[0].id,
                                      summary="Autorizar pruebas de laboratorio",
                                      note="Solicitud de pruebas del desarrollo %s." % req.project_id.name)
        return True

    def action_authorize(self):
        for req in self:
            if not (self.env.user in req._authorizer_users()
                    or self.env.user.has_group('quimibond_sgi.group_sgi_manager')):
                raise UserError("Solo el puesto configurado (Coordinador de Laboratorio y MP) o el Jefe MAST "
                                "autoriza las pruebas.")
            req.write({'state': 'autorizada', 'authorized_by_id': self.env.uid, 'date_authorized': fields.Datetime.now()})
            req.activity_ids.filtered(lambda a: a.summary == "Autorizar pruebas de laboratorio").action_feedback()
        return True

    def action_cancel(self):
        self.write({'state': 'cancelada'})
        return True

    def _sgi_dev_check_measured(self):
        for req in self.filtered(lambda r: r.state == 'autorizada' and not r.pending_count and r.line_ids):
            req.write({'state': 'medida', 'date_measured': fields.Datetime.now()})
            req.project_id.message_post(body="Pruebas de laboratorio medidas (%s): %d renglones en %.1f h." % (
                req.name, len(req.line_ids), req.hours_total))


class SgiDevCharacteristicLab(models.Model):
    _inherit = 'sgi.dev.characteristic'

    lab_request_ids = fields.Many2many('sgi.dev.lab.request', 'sgi_dev_lab_request_line_rel', 'line_id', 'request_id',
                                       string="Solicitudes de laboratorio")

    def write(self, vals):
        res = super().write(vals)
        if vals.keys() & {'sample_value', 'sample_text', 'run_1', 'run_2', 'run_3', 'run_text'}:
            self.mapped('lab_request_ids')._sgi_dev_check_measured()
        return res


# ---------------------------------------------------------------------------
# Checklist de factibilidad
# ---------------------------------------------------------------------------
class SgiDevFeasibilityItem(models.Model):
    """Recurso o pregunta del checklist de factibilidad, por línea (catálogo; nace vacío)."""
    _name = 'sgi.dev.feasibility.item'
    _description = "Recurso del checklist de factibilidad"
    _order = 'line, sequence, id'

    line = fields.Selection(DEV_LINES, string="Línea", required=True, index=True,
                            help="Línea de producción cuyo checklist incluye el recurso.")
    sequence = fields.Integer(default=10, help="Orden en el checklist.")
    name = fields.Char(string="Recurso o pregunta", required=True,
                       help="Qué se verifica (máquina, materia prima, laboratorio, personal…).")
    auto = fields.Selection(FEAS_AUTO, string="Lo contesta Odoo", default='none', required=True,
                            help="Si Odoo puede contestar el renglón solo: existencia de materia prima (lista de "
                                 "materiales contra existencias) o capacidad de máquina (llega con el cotizador nuevo).")
    active = fields.Boolean(default=True, help="Los recursos archivados no se cargan en proyectos nuevos.")


class SgiDevFeasibility(models.Model):
    """Renglón del checklist de factibilidad de un proyecto: sí, no o no aplica, con observación."""
    _name = 'sgi.dev.feasibility'
    _description = "Checklist de factibilidad del desarrollo"
    _order = 'sequence, id'

    project_id = fields.Many2one('project.project', required=True, ondelete='cascade', index=True)
    item_id = fields.Many2one('sgi.dev.feasibility.item', string="Recurso", required=True, ondelete='restrict',
                              help="Recurso del catálogo que se verifica.")
    sequence = fields.Integer(default=10)
    auto = fields.Selection(related='item_id.auto', string="Lo contesta Odoo")
    answer = fields.Selection(FEAS_ANSWERS, string="Respuesta", default='pendiente', required=True,
                              help="Sí, no o no aplica. Pendiente mientras nadie conteste.")
    observation = fields.Char(string="Observación", help="Texto libre; no capture aquí valores.")
    answered_by_id = fields.Many2one('res.users', string="Contestó", readonly=True)

    def write(self, vals):
        if 'answer' in vals and not self.env.context.get('sgi_dev_auto'):
            vals = dict(vals, answered_by_id=self.env.uid)
        return super().write(vals)


class ProjectProjectDevAnalysis(models.Model):
    _inherit = 'project.project'

    sgi_dev_line_key = fields.Selection(DEV_LINES, string="Línea", default='tejido_circular',
                                        help="Línea de producción del desarrollo; define el checklist de factibilidad.")
    sgi_dev_feasibility_ids = fields.One2many('sgi.dev.feasibility', 'project_id', string="Checklist de factibilidad")
    sgi_dev_feasibility_pending = fields.Integer(string="Factibilidad pendiente", compute='_compute_sgi_dev_feasibility',
                                                 store=True, help="Renglones del checklist sin contestar.")
    sgi_dev_feasibility_no = fields.Integer(string="Factibilidad en «No»", compute='_compute_sgi_dev_feasibility',
                                            store=True, help="Renglones del checklist contestados con «No».")
    sgi_dev_lab_request_ids = fields.One2many('sgi.dev.lab.request', 'project_id', string="Solicitudes de laboratorio")
    sgi_dev_lab_request_count = fields.Integer(string="Número de solicitudes de laboratorio",
                                               compute='_compute_sgi_dev_lab',
                                               help="Cuántas solicitudes de pruebas tiene el proyecto.")
    sgi_dev_lab_open_count = fields.Integer(string="Laboratorio en curso", compute='_compute_sgi_dev_lab',
                                            help="Solicitudes solicitadas o autorizadas sin medir.")
    sgi_dev_review_state = fields.Selection(REVIEW_STATES, string="Revisión de Ventas", default='pendiente',
                                            tracking=True, copy=False,
                                            help="Ventas aprueba juntos el análisis de Diseño de Producto y la "
                                                 "factibilidad de Diseño de Procesos. Sin aprobación no se cotiza.")
    sgi_dev_reviewed_by_id = fields.Many2one('res.users', string="Revisó", readonly=True, copy=False)
    sgi_dev_review_date = fields.Datetime(string="Revisado el", readonly=True, copy=False)
    sgi_dev_review_note = fields.Char(string="Motivo del regreso", copy=False,
                                      help="Qué falta cuando Ventas regresa el análisis.")

    @api.depends('sgi_dev_feasibility_ids.answer')
    def _compute_sgi_dev_feasibility(self):
        for project in self:
            lines = project.sgi_dev_feasibility_ids
            project.sgi_dev_feasibility_pending = len(lines.filtered(lambda l: l.answer == 'pendiente'))
            project.sgi_dev_feasibility_no = len(lines.filtered(lambda l: l.answer == 'no'))

    def _compute_sgi_dev_lab(self):
        for project in self:
            reqs = project.sgi_dev_lab_request_ids
            project.sgi_dev_lab_request_count = len(reqs)
            project.sgi_dev_lab_open_count = len(reqs.filtered(lambda r: r.state in ('solicitada', 'autorizada')))

    # --- Parecidos ---------------------------------------------------------------
    def _sgi_dev_target(self):
        """Lo que pide el cliente, para comparar: peso, ancho (cm), composición, dibujo, galga y sus límites."""
        self.ensure_one()
        lines = self.sgi_dev_line_ids
        masa = self._sgi_dev_pick(lines, 'masa')
        ancho = self._sgi_dev_pick(lines, 'ancho')
        galga = self._sgi_dev_pick(lines, 'galga')

        def cm(value):
            return value * 100 if value and value < 10 else value

        peso = self.sgi_dev_code_peso or (masa.spec_nominal if masa else 0)
        ancho_cm = self.sgi_dev_code_ancho or cm(ancho.spec_nominal if ancho else 0)
        galga_n = self.sgi_dev_code_galga or (galga.spec_nominal if galga else 0)
        masa_lim = masa._limits('spec') if masa and masa._has_spec() else (None, None)
        ancho_lim = ancho._limits('spec') if ancho and ancho._has_spec() else (None, None)
        return {
            'peso': peso, 'ancho': ancho_cm, 'galga': galga_n,
            'comp': (self.sgi_dev_code_composicion_id.code or '').upper(),
            'dib': (self.sgi_dev_code_dibujo_id.code or '').upper(),
            'peso_lim': masa_lim, 'ancho_lim': tuple(cm(v) if v is not None else None for v in ancho_lim),
        }

    def _sgi_dev_similar_candidates(self, limit=20):
        self.ensure_one()
        target = self._sgi_dev_target()
        if not target['peso'] or not target['ancho']:
            raise UserError("Capture la masa y el ancho que pide el cliente en la tabla de características antes "
                            "de buscar parecidos.")
        Clave = self.env['ficha.tecnica.clave.codigo']
        Product = self.env['product.product']
        products = Product.search([('default_code', '!=', False), ('sale_ok', '=', True),
                                   ('company_id', 'in', [False, self.company_id.id or self.env.company.id])])
        dev_products = Product.search([('sgi_dev_project_id', '!=', False), ('sgi_dev_project_id', '!=', self.id),
                                       ('sgi_dev_role', '=', 'acabado')])
        out = []
        for product in products | dev_products:
            parsed = parse_article_code(product.default_code)
            if not parsed or parsed['op'] != 'J':
                continue
            gauge = Clave.gauge_from_code(parsed['galga_code'])
            dp = abs(parsed['peso'] - target['peso']) / target['peso'] * 100
            da = abs(parsed['ancho'] - target['ancho']) / target['ancho'] * 100
            diffs, penalty = [], 0.0
            if target['comp'] and parsed['comp'] != target['comp']:
                diffs.append("composición %s" % parsed['comp'])
                penalty += 50
            if target['dib'] and parsed['dib'] != target['dib']:
                diffs.append("dibujo %s" % parsed['dib'])
                penalty += 50
            if target['galga'] and gauge and gauge != target['galga']:
                diffs.append("galga %d" % gauge)
                penalty += 20
            score = dp + da + penalty
            lo, hi = target['peso_lim']
            peso_ok = (lo is None or parsed['peso'] >= lo) and (hi is None or parsed['peso'] <= hi) if (lo, hi) != (None, None) else dp < 1e-9
            lo, hi = target['ancho_lim']
            ancho_ok = (lo is None or parsed['ancho'] >= lo) and (hi is None or parsed['ancho'] <= hi) if (lo, hi) != (None, None) else da < 1e-9
            within = peso_ok and ancho_ok and not penalty
            if dp > 60 or da > 60:
                continue
            if not peso_ok:
                diffs.append("peso %d" % parsed['peso'])
            if not ancho_ok:
                diffs.append("ancho %d" % parsed['ancho'])
            out.append({
                'product_id': product.id, 'source': 'desarrollo' if product.sgi_dev_project_id else 'linea',
                'peso': parsed['peso'], 'ancho': parsed['ancho'], 'galga': gauge, 'score': round(score, 1),
                'within_tolerance': within, 'note': ", ".join(diffs) or "igual",
            })
        out.sort(key=lambda d: d['score'])
        return out[:limit]

    def action_sgi_dev_find_similar(self):
        self.ensure_one()
        target = self._sgi_dev_target()
        wizard = self.env['sgi.dev.similar'].create({
            'project_id': self.id, 'peso': int(round(target['peso'] or 0)), 'ancho': int(round(target['ancho'] or 0)),
            'line_ids': [(0, 0, vals) for vals in self._sgi_dev_similar_candidates()],
        })
        return {'type': 'ir.actions.act_window', 'name': "Artículos parecidos", 'res_model': 'sgi.dev.similar',
                'res_id': wizard.id, 'view_mode': 'form', 'target': 'new'}

    # --- Laboratorio -------------------------------------------------------------
    def action_sgi_dev_request_lab_tests(self):
        self.ensure_one()
        Request = self.env['sgi.dev.lab.request']
        kind = 'corrida' if self.sgi_dev_stage_seq >= self._sgi_dev_stage_order('muestra') else 'muestra_cliente'
        lines = self.sgi_dev_line_ids.filtered(lambda l: l.lab_requested and not l.lab_request_ids.filtered(
            lambda r: r.kind == kind and r.state in ('borrador', 'solicitada', 'autorizada')))
        if kind == 'muestra_cliente':
            lines = lines.filtered(lambda l: not l.sample_value and not l.sample_text)
        else:
            lines = lines.filtered(lambda l: not l.run_count and not l.run_text)
        if not lines:
            raise UserError("No hay renglones marcados «Medir en la muestra» sin resultado ni solicitud en curso.")
        request = Request.create({'project_id': self.id, 'kind': kind, 'line_ids': [(6, 0, lines.ids)]})
        request.action_request()
        return {'type': 'ir.actions.act_window', 'res_model': 'sgi.dev.lab.request', 'res_id': request.id,
                'view_mode': 'form'}

    def action_sgi_dev_open_lab_requests(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': "Pruebas de laboratorio", 'res_model': 'sgi.dev.lab.request',
                'view_mode': 'list,form', 'domain': [('project_id', '=', self.id)],
                'context': {'default_project_id': self.id}}

    # --- Factibilidad --------------------------------------------------------------
    def _sgi_dev_auto_answer(self, item):
        """Respuesta automática o 'pendiente' si Odoo no puede saberlo."""
        self.ensure_one()
        if item.auto == 'materia_prima':
            product = self.sgi_dev_product_id or self.sgi_dev_base_product_id
            bom = self.env['mrp.bom']._bom_find(product)[product] if product else False
            if not bom or not bom.bom_line_ids:
                return 'pendiente', "Sin lista de materiales: la contesta una persona."
            missing = bom.bom_line_ids.filtered(lambda l: l.product_id.qty_available <= 0).mapped('product_id.display_name')
            if missing:
                return 'no', "Sin existencia: %s" % ", ".join(missing)
            return 'si', "Todos los componentes tienen existencia."
        if item.auto == 'capacidad':
            return 'pendiente', "La capacidad de máquina la contesta el cotizador nuevo."
        return 'pendiente', ''

    def action_sgi_dev_load_feasibility(self):
        Item = self.env['sgi.dev.feasibility.item']
        for project in self:
            items = Item.search([('line', '=', project.sgi_dev_line_key or 'tejido_circular')])
            existing = project.sgi_dev_feasibility_ids.mapped('item_id')
            vals = []
            for item in items - existing:
                answer, obs = project._sgi_dev_auto_answer(item)
                vals.append((0, 0, {'item_id': item.id, 'sequence': item.sequence, 'answer': answer,
                                    'observation': obs or False}))
            if vals:
                project.with_context(sgi_dev_auto=True).write({'sgi_dev_feasibility_ids': vals})
            # Las automáticas se vuelven a contestar con el dato de hoy.
            for line in project.sgi_dev_feasibility_ids.filtered(lambda l: l.auto != 'none'):
                answer, obs = project._sgi_dev_auto_answer(line.item_id)
                if answer != 'pendiente':
                    line.with_context(sgi_dev_auto=True).write({'answer': answer, 'observation': obs})
        return True

    # --- Revisión única de Ventas ----------------------------------------------------
    def action_sgi_dev_review_approve(self):
        for project in self.filtered('sgi_is_ft'):
            if not project.sgi_dev_analysis_result:
                raise UserError("Falta el resultado del análisis (producto de línea, nuevo o no factible).")
            if project.sgi_dev_feasibility_pending:
                raise UserError("Hay %d renglón(es) del checklist de factibilidad sin contestar."
                                % project.sgi_dev_feasibility_pending)
            project.write({'sgi_dev_review_state': 'aprobado', 'sgi_dev_reviewed_by_id': self.env.uid,
                           'sgi_dev_review_date': fields.Datetime.now(), 'sgi_dev_review_note': False})
            project.message_post(body="Ventas aprobó el análisis y la factibilidad.")
        return True

    def action_sgi_dev_review_return(self):
        for project in self.filtered('sgi_is_ft'):
            if not project.sgi_dev_review_note:
                raise UserError("Escriba el motivo del regreso.")
            project.write({'sgi_dev_review_state': 'regresado', 'sgi_dev_reviewed_by_id': self.env.uid,
                           'sgi_dev_review_date': fields.Datetime.now()})
            project.message_post(body="Ventas regresó el análisis: %s" % project.sgi_dev_review_note)
        return True

    def write(self, vals):
        if 'stage_id' in vals:
            keys = self._sgi_dev_stage_keys()
            key, seq = keys.get(vals['stage_id'], ('', 0))
            gate = self._sgi_dev_stage_order('cotizacion')
            if key and gate <= seq < self._sgi_dev_stage_order('cerrado_sin_producto'):
                blocked = self.filtered(lambda p: p.sgi_is_ft and p.sgi_dev_review_state != 'aprobado'
                                        and p.sgi_dev_stage_seq < gate)
                if blocked:
                    raise UserError("Antes de cotizar, Ventas debe aprobar el análisis y la factibilidad en una sola "
                                    "revisión (%s)." % ", ".join(blocked.mapped('name')))
        return super().write(vals)
