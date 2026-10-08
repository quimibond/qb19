# -*- coding: utf-8 -*-
"""La cotización y el proyecto de desarrollo de producto (C1).

Toma del proyecto lo que ya está capturado una sola vez (regla del brief: sin
recaptura): cliente, artículo en desarrollo, gramaje / ancho / galga de la
tabla de características (`sgi.dev.characteristic`, claves `masa`, `ancho`,
`galga`) y volumen y precio objetivo de la solicitud. También apunta las
fichas C1 de costo y cotización a este modelo y liga las cotizaciones
importadas del cotizador anterior con su proyecto FT.
"""
import logging

from odoo import api, fields, models

_logger = logging.getLogger(__name__)

CODE_GAUGE = 'galga'
# Ligas decididas a mano (Jose, 2026-10-07): cotización vieja → (proyecto, galga).
LEGACY_PROJECT_LINKS = {121: (491, '21')}
C1_COT_DELIVERABLES = {
    'C1-COSTO': {
        'measure_domain': "[('project_id', '!=', False), ('approved_date', '!=', False)]",
        'measure_date_field': 'approved_date', 'measure_user_field': 'approved_by_id',
    },
    'C1-COTIZACION': {
        'measure_domain': "[('project_id', '!=', False), ('presented_date', '!=', False)]",
        'measure_date_field': 'presented_date', 'measure_user_field': 'presented_by_id',
    },
}


class QbCotizadorCotizacionSgi(models.Model):
    _inherit = 'qb.cotizador.cotizacion'

    project_id = fields.Many2one(
        'project.project', string='Proyecto de desarrollo', index=True, tracking=True,
        domain="[('sgi_is_ft', '=', True), ('is_template', '=', False)]",
        help='Proyecto de desarrollo de producto (C1) al que pertenece la cotización.')
    project_revision = fields.Integer(
        string='Revisión del desarrollo cotizada', readonly=True, copy=False,
        help='Revisión del proyecto con la que se calculó el costo.')

    # ------------------------------------------------------------------
    @api.model
    def _sgi_pick_line(self, project, code):
        return project._sgi_dev_pick(project.sgi_dev_line_ids, code)

    def _qb_sgi_vals_from_project(self, project):
        """Lo que la cotización toma del proyecto; solo lo que el proyecto tiene."""
        vals = {}
        if project.partner_id:
            vals['partner_id'] = project.partner_id.id
        if project.sgi_dev_product_id:
            vals['product_id'] = project.sgi_dev_product_id.id
        elif project.sgi_dev_product_name:
            vals['spec_descripcion'] = project.sgi_dev_product_name
        from odoo.addons.quimibond_ficha_tecnica_tela.models.ficha_tecnica_caracteristica import (
            CODE_MASS, CODE_WIDTH)
        mass = self._sgi_pick_line(project, CODE_MASS)
        width = self._sgi_pick_line(project, CODE_WIDTH)
        gauge = self._sgi_pick_line(project, CODE_GAUGE)
        if mass and mass.spec_nominal:
            vals['spec_gramaje'] = mass.spec_nominal
        if width and width.spec_nominal:
            vals['spec_ancho'] = width.spec_nominal
        if gauge and (gauge.spec_nominal or gauge.spec_text):
            vals['spec_galga'] = gauge.spec_text or ('%g' % gauge.spec_nominal)
        if project.sgi_dev_volume:
            vals['volumen'] = project.sgi_dev_volume
            vals['volumen_uom'] = project.sgi_dev_volume_uom or 'm'
        if project.sgi_dev_target_price:
            vals['precio_objetivo'] = project.sgi_dev_target_price
            if project.sgi_dev_currency_id:
                vals['currency_id'] = project.sgi_dev_currency_id.id
        return vals

    @api.onchange('project_id')
    def _onchange_project_id(self):
        if self.project_id:
            self.update(self._qb_sgi_vals_from_project(self.project_id))

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('project_id') and not self.env.context.get('qb_sgi_no_fill'):
                project = self.env['project.project'].browse(vals['project_id'])
                for k, v in self._qb_sgi_vals_from_project(project).items():
                    vals.setdefault(k, v)
        return super().create(vals_list)

    def action_tomar_del_proyecto(self):
        for rec in self.filtered('project_id'):
            rec.write(rec._qb_sgi_vals_from_project(rec.project_id))
        return True

    def action_calcular(self):
        res = super().action_calcular()
        for rec in self.filtered('project_id'):
            rec.project_revision = rec.project_id.sgi_dev_revision
        return res

    def action_aprobar(self):
        res = super().action_aprobar()
        for rec in self.filtered(lambda r: r.project_id and r.project_id.qb_costo_bloqueado):
            rec.project_id.write({'qb_costo_bloqueado': False, 'qb_costo_bloqueo_nota': False})
            rec.project_id.message_post(body='Cotización %s aprobada tras la revisión: el desarrollo sigue.'
                                        % rec.folio)
        return res

    # ------------------------------------------------------------------
    # Fichas C1 medidas con la cotización
    # ------------------------------------------------------------------
    @api.model
    def _qb_sgi_apuntar_entregables(self):
        """C1-COSTO y C1-COTIZACION se miden con la cotización; la actividad
        que solo entrega ese entregable pasa a medirse «por su entregable»."""
        if 'sgi.deliverable' not in self.env:
            return
        Deliverable = self.env['sgi.deliverable'].sudo()
        ir_model = self.env['ir.model'].sudo()._get(self._name)
        for code, vals in C1_COT_DELIVERABLES.items():
            for deliverable in Deliverable.with_context(active_test=False).search([('code', '=', code)]):
                deliverable.write(dict(vals, odoo_model_id=ir_model.id))
                activities = self.env['sgi.process.activity'].sudo().search(
                    [('output_deliverable_ids', 'in', deliverable.id), ('measure_method', '=', 'manual')])
                for activity in activities.filtered(lambda a: a.output_deliverable_ids == deliverable):
                    try:
                        with self.env.cr.savepoint():
                            activity.write({'measure_method': 'entregable',
                                            'measure_deliverable_id': deliverable.id})
                    except Exception as exc:  # noqa: BLE001 — la ficha sigue manual; se avisa
                        _logger.warning('qb_costeo_sgi: %s sigue con medición manual: %s',
                                        activity.display_name, exc)
        return True

    # ------------------------------------------------------------------
    # Cotizaciones importadas → proyecto FT
    # ------------------------------------------------------------------
    @api.model
    def _qb_sgi_ligar_legadas(self):
        """Liga las cotizaciones importadas sin proyecto: por el artículo del
        proyecto, por la lista de ligas a mano y por el código del artículo
        en el nombre del proyecto."""
        Project = self.env['project.project'].sudo().with_context(active_test=False)
        n = 0
        for cot in self.sudo().with_context(active_test=False).search(
                [('legacy_id', '!=', 0), ('project_id', '=', False)]):
            project = Project.browse()
            galga = None
            if cot.legacy_id in LEGACY_PROJECT_LINKS:
                pid, galga = LEGACY_PROJECT_LINKS[cot.legacy_id]
                project = Project.browse(pid).filtered('sgi_is_ft')
            if not project and cot.product_id:
                project = cot.product_id.product_tmpl_id.sgi_dev_project_id
                if not project:
                    project = Project.search([('sgi_is_ft', '=', True), ('is_template', '=', False),
                                              ('sgi_dev_product_id', '=', cot.product_id.id)], limit=1)
                if not project and cot.product_id.default_code:
                    project = Project.search([('sgi_is_ft', '=', True), ('is_template', '=', False),
                                              ('sgi_dev_product_name', 'ilike', cot.product_id.default_code)],
                                             limit=1)
            if not project:
                continue
            vals = {'project_id': project.id}
            if galga:
                vals['spec_galga'] = galga
            cot.with_context(qb_sgi_no_fill=True).write(vals)
            cot.message_post(body='Ligada al proyecto de desarrollo %s%s.' % (
                project.display_name, (' (galga %s por regla del proyecto)' % galga) if galga else ''))
            n += 1
        if n:
            _logger.info('qb_costeo_sgi: %d cotizaciones ligadas a su proyecto FT', n)
        return n
