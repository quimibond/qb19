# -*- coding: utf-8 -*-
"""57.122.0 (C1, uso diario del proyecto de desarrollo). Jose, 2026-10-07 (punto 4):

- La tarjeta de un desarrollo abre su formulario, no las tareas (nadie
  encontraba «⋮ → Ajustes»). Las tareas siguen en el botón de la ficha y en
  los enlaces «Tareas» de la tarjeta.
- Al elegir el tipo de desarrollo con la tabla vacía, las características del
  tipo se proponen solas (el 491 llevaba un día en Análisis con 0 renglones).
- Asistente para marcar como desarrollo, varios a la vez, los proyectos viejos
  con nombre de código de artículo o en la columna «Por revisar». Nada se marca
  solo.
"""
from odoo import api, fields, models

from .sgi_dev_analysis import CODE_RE

REVIEW_STAGE_NAMES = {'por revisar'}


class ProjectProjectDevBoard(models.Model):
    _inherit = 'project.project'

    sgi_dev_missing_partner = fields.Boolean(
        string="Falta el cliente", compute='_compute_sgi_dev_missing_partner',
        help="Desarrollo de origen cliente sin cliente capturado.")

    @api.depends('sgi_is_ft', 'is_template', 'sgi_dev_origin', 'partner_id')
    def _compute_sgi_dev_missing_partner(self):
        for project in self:
            project.sgi_dev_missing_partner = bool(
                project.sgi_is_ft and not project.is_template
                and project.sgi_dev_origin == 'cliente' and not project.partner_id)

    # ------------------------------------------------------------------
    # Tarjeta → formulario
    # ------------------------------------------------------------------
    def action_view_tasks(self):
        """La tarjeta de un desarrollo abre su ficha. Con ``sgi_dev_force_tasks`` (botón de la
        ficha, enlaces «Tareas») se abren las tareas como siempre."""
        if len(self) == 1 and self.sgi_is_ft and not self.is_template \
                and not self.env.context.get('sgi_dev_force_tasks'):
            return self._sgi_dev_form_action()
        return super().action_view_tasks()

    def action_sgi_dev_view_tasks(self):
        return self.with_context(sgi_dev_force_tasks=True).action_view_tasks()

    def _sgi_dev_form_action(self):
        # 57.123.2: con ``context`` y ``domain``. Los módulos que extienden
        # ``action_view_tasks`` y cargan después del SGI (sale_project, que no
        # es dependencia) reciben esta acción de ``super()`` y le escriben
        # ``action['context'][...]``: sin la llave tronaba con KeyError al
        # abrir la tarjeta de un desarrollo en producción (2026-10-08).
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': self.display_name,
            'res_model': 'project.project',
            'res_id': self.id,
            'view_mode': 'form',
            'views': [(self.env.ref('project.edit_project').id, 'form')],
            'target': 'current',
            'context': dict(self.env.context),
            'domain': [],
        }

    # ------------------------------------------------------------------
    # Características del tipo, solas
    # ------------------------------------------------------------------
    def _sgi_dev_autoload_lines(self):
        """Propone las características del tipo en los desarrollos que no tienen ninguna."""
        empty = self.filtered(lambda p: p.sgi_is_ft and not p.is_template and not p.sgi_dev_line_ids)
        if empty:
            empty.action_sgi_dev_load_lines()
        return empty

    @api.model_create_multi
    def create(self, vals_list):
        projects = super().create(vals_list)
        if not self.env.context.get('sgi_dev_migration') and not self.env.context.get('sgi_dev_no_autoload'):
            projects._sgi_dev_autoload_lines()
        return projects

    def write(self, vals):
        res = super().write(vals)
        if 'sgi_dev_type' in vals and not self.env.context.get('sgi_dev_migration') \
                and not self.env.context.get('sgi_dev_no_autoload'):
            self._sgi_dev_autoload_lines()
        return res

    # ------------------------------------------------------------------
    # Candidatos a desarrollo (asistente)
    # ------------------------------------------------------------------
    @api.model
    def _sgi_dev_mark_candidates(self):
        """Proyectos activos que no son desarrollo ni plantilla y que (a) se llaman como un código
        de artículo en algún idioma o (b) están en la columna «Por revisar». Solo la empresa activa
        y los proyectos sin empresa."""
        Project = self.sudo()
        candidates = Project.browse()
        domain = [('sgi_is_ft', '=', False), ('is_template', '=', False),
                  ('company_id', 'in', [self.env.company.id, False])]
        for project in Project.search(domain):
            names = self._sgi_dev_lang_names(project)
            if any(CODE_RE.match((name or '').strip().upper()) for name in names):
                candidates |= project
            elif project.stage_id and self._sgi_dev_lang_keys(project.stage_id) & REVIEW_STAGE_NAMES:
                candidates |= project
        return candidates


class SgiDevMarkWizard(models.TransientModel):
    """Marca como desarrollo de producto varios proyectos existentes de una vez. La lista se llena
    con los candidatos (nombre de código de artículo o columna «Por revisar»); quite los que no
    sean desarrollos antes de marcar. No mueve de etapa: eso lo hace la persona en la lista de
    desarrollos."""
    _name = 'sgi.dev.mark.wizard'
    _description = "Marcar proyectos existentes como desarrollo de producto"

    project_ids = fields.Many2many(
        'project.project', 'sgi_dev_mark_wizard_project_rel', 'wizard_id', 'project_id',
        string="Proyectos a marcar", domain="[('sgi_is_ft', '=', False), ('is_template', '=', False)]",
        help="Se proponen los proyectos con nombre de código de artículo y los de la columna «Por revisar». "
             "Quite los que no sean desarrollos y agregue los que falten.")
    load_lines = fields.Boolean(
        string="Proponer las características del tipo general", default=False,
        help="Al marcar, llena la tabla de características con la plantilla del tipo general. Si no, la "
             "tabla se llena al elegir el tipo de desarrollo en cada proyecto.")
    candidate_count = fields.Integer(string="Candidatos", compute='_compute_candidate_count')

    @api.model
    def default_get(self, fields_list):
        res = super().default_get(fields_list)
        if 'project_ids' in fields_list and not res.get('project_ids'):
            res['project_ids'] = [(6, 0, self.env['project.project']._sgi_dev_mark_candidates().ids)]
        return res

    @api.depends('project_ids')
    def _compute_candidate_count(self):
        for wizard in self:
            wizard.candidate_count = len(wizard.project_ids)

    def action_mark(self):
        self.ensure_one()
        projects = self.project_ids.filtered(lambda p: not p.sgi_is_ft and not p.is_template)
        ctx = {} if self.load_lines else {'sgi_dev_no_autoload': True}
        projects.with_context(**ctx).write({'sgi_is_ft': True})
        if self.load_lines:
            projects._sgi_dev_autoload_lines()
        action = self.env.ref('quimibond_sgi.sgi_dev_project_board_action').read()[0]
        action['domain'] = [('id', 'in', projects.ids)]
        return action
