# -*- coding: utf-8 -*-
"""57.123.0 (C1, ajustes al modelo del SGI). Jose, 2026-10-07 (punto 3):

- C1.04 tenía dos ejecutores reales. Se parte en C1.04 (artículo y lista de
  materiales, Diseño de Producto) y C1.04b (ruta y centros de trabajo, Diseño
  de Procesos), **sin renumerar** C1.05 a C1.19: el sub-paso lleva su numeral
  propio (``number_label``) y se intercala por secuencia.
- Las aprobaciones «sin configurar» de C1.02, C1.03, C1.07 y C1.11 se ligan a
  su botón real: «Aprobar análisis y factibilidad» (proyecto), «Aprobar
  solicitud» (firma «Aprobó», botón nuevo) y «Dictamen de Diseño» (solicitud de
  laboratorio, botón nuevo). C1.10 (Jefe de Manufactura) y C1.15 se quedan
  como están: las decide Jose.
- Escalamiento: donde ejecutan Diseño y Desarrollo de Producto o de Procesos,
  primer nivel el dueño del proceso (ya estaba) y segundo nivel Dirección de
  Operaciones con los días del parámetro
  ``quimibond_sgi.dev_escalation_director_days``; vacío = todavía no se crea.

Nada de esto toca nombre, pasos, criterios, menú, formatos ni roles de las
demás fichas: solo C1.04 (recorte del texto que se va a C1.04b) y las ligas
de aprobación y escalamiento.
"""
import logging

from odoo import api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

PARAM_ESCALATION_DAYS = 'quimibond_sgi.dev_escalation_director_days'
C1_PROCESS_CODE = 'C1'
# Puestos que ejecutan las actividades a escalar y puesto del segundo nivel (por nombre, en
# cualquier idioma: así vale en producción y en una copia).
DESIGN_JOB_NAMES = ('diseño y desarrollo de producto', 'diseño y desarrollo de procesos')
DIRECTOR_JOB_NAME = 'director de operaciones'
# Aprobaciones «sin configurar» → botón real. Clave: numeral de la actividad.
C1_APPROVAL_BUTTONS = {
    'C1.02': ('project.project', 'action_sgi_dev_review_approve'),
    'C1.03': ('project.project', 'action_sgi_dev_review_approve'),
    'C1.07': ('project.project', 'action_sgi_dev_approve_request'),
    'C1.11': ('sgi.dev.lab.request', 'action_verdict'),
    'C1.15': ('sgi.dev.tech.sheet', 'action_approve'),  # 57.132.0 (Jose 5.3): rol 1928, Ventas aprueba la ficha interna
    'C1.13': ('sgi.dev.change.request', 'action_approve'),  # 57.137.0 (Jessica 9): Dirección firma la solicitud de modificación
}
C1_04_TEXTS = {
    'name': ("Dar de alta el artículo de desarrollo con su lista de materiales y ruta preliminar",
             "Dar de alta el artículo de desarrollo con su lista de materiales"),
    'how_steps': (" → Diseño de procesos asigna la ruta y los centros de trabajo.", "."),
    'done_criteria': ("lista de materiales y ruta antes de costear.", "y lista de materiales antes de costear."),
}
C1_04B_VALS = {
    'name': "Asignar la ruta preliminar y los centros de trabajo del artículo de desarrollo",
    'how_steps': "Con los artículos y la lista de materiales de C1.04, Diseño de procesos captura las operaciones "
                 "de la lista de materiales con su centro de trabajo y tiempo estimado → la ruta queda antes de "
                 "costear.",
    'done_criteria': "La lista de materiales del artículo de desarrollo tiene sus operaciones con centro de trabajo "
                     "antes de costear.",
    'on_fail': False,
    'check_against': False,
    'exec_channel': 'odoo',  # 57.127.0 (Jose, 1a): se hace en Odoo; el plazo se queda vacío a propósito.
}


class SgiProcessActivityDev(models.Model):
    _inherit = 'sgi.process.activity'

    @api.model
    def _sgi_dev_c1_activity(self, number):
        return self.sudo().with_context(active_test=False).search(
            [('number', '=', number), ('process_id.code', '=', C1_PROCESS_CODE)], limit=1)

    @api.model
    def _sgi_dev_jobs_named(self, names):
        """Puestos cuyo nombre (en cualquier idioma) contiene alguno de ``names``."""
        Project = self.env['project.project']
        jobs = self.env['hr.job'].sudo().browse()
        for name in names:
            jobs |= Project._sgi_dev_search_langs('hr.job', [('name', 'ilike', name)])
        return jobs

    # ------------------------------------------------------------------
    # C1.04 → C1.04 + C1.04b
    # ------------------------------------------------------------------
    @api.model
    def _sgi_dev_split_c1_04(self):
        """Crea C1.04b (ruta y centros de trabajo, Diseño de Procesos) a partir de C1.04 y recorta
        de C1.04 lo que se va. Idempotente: si C1.04b ya existe no hace nada. Devuelve C1.04b."""
        existing = self._sgi_dev_c1_activity('C1.04b')
        if existing:
            return existing
        base = self._sgi_dev_c1_activity('C1.04')
        if not base:
            return self.browse()
        design_product, design_process = (self._sgi_dev_jobs_named([n])[:1] for n in DESIGN_JOB_NAMES)
        Project = self.env['project.project']
        # 57.123.1: el entregable existe antes que la actividad; un procedimiento vigente no admite
        # una actividad «por su entregable» sin entregable con modelo (producción, 2026-10-08).
        route = Project._sgi_dev_ensure_c1_deliverable('C1-RUTA', base.company_id)
        # Roles del sub-paso: ejecuta Diseño de Procesos, participa Diseño de Producto, escala como C1.04.
        # Van en el alta para que se validen juntos (exactamente un ejecutor).
        role_vals = []
        if design_process:
            role_vals.append({'role': 'ejecuta', 'target_type': 'job', 'job_id': design_process.id})
        if design_product:
            role_vals.append({'role': 'participa', 'target_type': 'job', 'job_id': design_product.id})
        for esc in base.role_ids.filtered(lambda r: r.role == 'escala'):
            role_vals.append({'role': 'escala', 'target_type': esc.target_type, 'job_id': esc.job_id.id,
                              'family_id': esc.family_id.id, 'relative_role': esc.relative_role,
                              'after_days': esc.after_days})
        # Alta explícita (no ``copy``: copiaría ejecuciones, ligas y roles).
        vals = dict(C1_04B_VALS, process_id=base.process_id.id, company_id=base.company_id.id,
                    role_ids=[(0, 0, r) for r in role_vals],
                    stage_id=base.stage_id.id, block=base.block, value_class=base.value_class,
                    sequence=base.sequence + 5, number_label='C1.04b',
                    measure_method='entregable' if route else 'manual',
                    output_deliverable_ids=[(6, 0, route.ids)], measure_deliverable_id=route.id or False,
                    automation_level_current=base.automation_level_current,
                    automation_level_target=base.automation_level_target, automation_method=base.automation_method,
                    instruction_id=base.instruction_id.id, related_procedure_id=base.related_procedure_id.id,
                    odoo_menu_id=base.odoo_menu_id.id, odoo_ref=base.odoo_ref,
                    format_document_ids=[(6, 0, base.format_document_ids.ids)])
        new = self.sudo().create(vals)
        # C1.04b recibe la lista de materiales de C1.04 (plazo sin definir: vacío).
        bom = base.output_deliverable_ids[:1]
        if bom:
            self.env['sgi.activity.input'].sudo().create({'activity_id': new.id, 'deliverable_id': bom.id})
        # C1.04 se queda con el artículo y la lista de materiales.
        vals = {}
        for field_name, (old, replacement) in C1_04_TEXTS.items():
            text = base[field_name] or ''
            if old in text:
                vals[field_name] = text.replace(old, replacement)
        if vals:
            base.sudo().write(vals)
        # El catálogo de medición de C1 deja C1-RUTA con su filtro definitivo (idempotente).
        report = Project._sgi_dev_apply_c1_measures()
        _logger.info("SGI C1.04b: entregables de medición %s", report)
        # Costear (C1.05) recibe también la ruta, con el mismo plazo que la lista de materiales.
        nxt = self._sgi_dev_c1_activity('C1.05')
        if route and nxt and route not in nxt.input_ids.deliverable_id:
            days = nxt.input_ids.filtered(lambda i: i.deliverable_id == bom)[:1].max_days
            self.env['sgi.activity.input'].sudo().create(
                {'activity_id': nxt.id, 'deliverable_id': route.id, 'max_days': days or 0})
        return new

    # ------------------------------------------------------------------
    # Aprobaciones ligadas a su botón
    # ------------------------------------------------------------------
    @api.model
    def _sgi_dev_link_c1_approvals(self):
        """Los renglones «Aprueba» por botón de C1.02, C1.03, C1.07 y C1.11 que no tienen botón
        apuntan al botón real. Luego intenta sincronizar la regla nativa (satélite de Studio); si
        el satélite no está, el estado queda «Por sincronizar». Devuelve los roles tocados."""
        IrModel = self.env['ir.model'].sudo()
        touched = self.env['sgi.activity.role'].sudo().browse()
        for number, (model_name, method) in C1_APPROVAL_BUTTONS.items():
            activity = self._sgi_dev_c1_activity(number)
            if not activity or model_name not in self.env:
                continue
            roles = activity.role_ids.filtered(
                lambda r: r.role == 'aprueba' and r.approval_kind == 'boton' and not r.approval_method)
            if not roles:
                continue
            roles.sudo().write({'approval_model_id': IrModel._get(model_name).id, 'approval_method': method})
            touched |= roles
        if touched:
            try:
                touched.action_sgi_sync_approval()
            except UserError as exc:
                _logger.warning("SGI C1: las aprobaciones quedan «Por sincronizar»: %s", exc)
        return touched

    # ------------------------------------------------------------------
    # Escalamiento de segundo nivel
    # ------------------------------------------------------------------
    @api.model
    def _sgi_dev_escalation_days(self):
        raw = (self.env['ir.config_parameter'].sudo().get_param(PARAM_ESCALATION_DAYS, '') or '').strip()
        try:
            return int(raw)
        except ValueError:
            return 0

    @api.model
    def _sgi_dev_sync_c1_escalation(self):
        """En las actividades de C1 que ejecuta Diseño y Desarrollo de Producto o de Procesos, el
        segundo nivel de escalamiento es Dirección de Operaciones a los días del parámetro. Con el
        parámetro vacío no crea nada (el primer nivel, dueño del proceso, ya existe). Idempotente:
        actualiza los días si cambian. Devuelve los roles creados o actualizados."""
        Role = self.env['sgi.activity.role'].sudo()
        done = Role.browse()
        days = self._sgi_dev_escalation_days()
        if days <= 0:
            return done
        director = self._sgi_dev_jobs_named([DIRECTOR_JOB_NAME])[:1]
        design_jobs = self._sgi_dev_jobs_named(DESIGN_JOB_NAMES)
        if not director or not design_jobs:
            return done
        activities = self.sudo().with_context(active_test=False).search([('process_id.code', '=', C1_PROCESS_CODE)])
        for activity in activities:
            executors = activity.role_ids.filtered(lambda r: r.role == 'ejecuta').job_id
            if not executors & design_jobs:
                continue
            level2 = activity.role_ids.filtered(lambda r: r.role == 'escala' and r.job_id == director)
            if level2:
                if level2[:1].after_days != days:
                    level2[:1].write({'after_days': days})
                    done |= level2[:1]
                continue
            done |= Role.create({'activity_id': activity.id, 'role': 'escala', 'target_type': 'job',
                                 'job_id': director.id, 'after_days': days})
        return done


class ProjectProjectDevApproval(models.Model):
    _inherit = 'project.project'

    def action_sgi_dev_approve_request(self):
        """Firma «Aprobó» de la solicitud de desarrollo (Dirección de Operaciones). Es el botón que
        aprueba C1.07; la fecha se sella sola (``sgi_dev_approved_date``)."""
        for project in self.filtered(lambda p: p.sgi_is_ft and not p.sgi_dev_approved_by_id):
            project.write({'sgi_dev_approved_by_id': self.env.uid})
            project.message_post(body="Solicitud de desarrollo aprobada por %s." % self.env.user.name)
        return True


class SgiDevLabRequestVerdict(models.Model):
    _inherit = 'sgi.dev.lab.request'

    verdict_by_id = fields.Many2one('res.users', string="Dictaminó (Diseño de Producto)", readonly=True, copy=False)
    date_verdict = fields.Datetime(string="Dictaminada el", readonly=True, copy=False)

    def action_verdict(self):
        """Dictamen de Diseño de Producto sobre los resultados medidos. Es el botón que aprueba C1.11."""
        for req in self:
            if req.state != 'medida':
                raise UserError("Solo se dictamina una solicitud ya medida.")
            if req.pending_count:
                raise UserError("Faltan renglones sin resultado.")
            req.write({'verdict_by_id': self.env.uid, 'date_verdict': fields.Datetime.now()})
            req.project_id.message_post(body="Dictamen de Diseño sobre %s: %d renglones." % (req.name, len(req.line_ids)))
        return True


class ResConfigSettingsDev(models.TransientModel):
    _inherit = 'res.config.settings'

    sgi_dev_escalation_director_days = fields.Integer(
        string="Días hábiles para escalar a Dirección de Operaciones (C1)",
        config_parameter=PARAM_ESCALATION_DAYS,
        help="Segundo nivel de escalamiento de las actividades de C1 que ejecutan Diseño y Desarrollo de "
             "Producto o de Procesos. Vacío o 0: todavía no se crea el segundo nivel.")

    def set_values(self):
        res = super().set_values()
        self.env['sgi.process.activity']._sgi_dev_sync_c1_escalation()
        return res
