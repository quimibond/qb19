# -*- coding: utf-8 -*-
"""El proyecto de desarrollo ve sus cotizaciones y se detiene cuando una
revisión cambia el costo (spec §6.1, «Recálculo por revisión»)."""
from odoo import api, fields, models
from odoo.exceptions import UserError

from odoo.addons.qb_cotizador.models.settings import PARAM_REVISION_TOLERANCIA


class ProjectProjectCotizador(models.Model):
    _inherit = 'project.project'

    qb_cotizacion_ids = fields.One2many('qb.cotizador.cotizacion', 'project_id', string='Cotizaciones')
    qb_cotizacion_count = fields.Integer(compute='_compute_qb_cotizacion_count')
    qb_costo_bloqueado = fields.Boolean(
        string='Detenido por costo', copy=False, tracking=True,
        help='Una revisión del desarrollo cambió el costo: el proyecto no cambia de etapa hasta que el '
             'puesto que aprueba libere la cotización revisada.')
    qb_costo_bloqueo_nota = fields.Char(string='Por qué está detenido', copy=False)

    @api.depends('qb_cotizacion_ids')
    def _compute_qb_cotizacion_count(self):
        data = self.env['qb.cotizador.cotizacion'].with_context(active_test=False)._read_group(
            [('project_id', 'in', self.ids)], ['project_id'], ['__count'])
        counts = {p.id: c for p, c in data}
        for project in self:
            project.qb_cotizacion_count = counts.get(project.id, 0)

    def action_qb_cotizaciones(self):
        self.ensure_one()
        action = self.env['ir.actions.act_window']._for_xml_id('qb_cotizador.action_qb_cotizador_cotizacion')
        action['domain'] = [('project_id', '=', self.id)]
        action['context'] = {'default_project_id': self.id, 'default_partner_id': self.partner_id.id,
                             'search_default_vivas': 1}
        return action

    def _qb_cotizaciones_vivas(self):
        return self.qb_cotizacion_ids.filtered(lambda c: c.state in ('borrador', 'por_aprobar', 'presentada'))

    def action_sgi_dev_new_revision(self):
        res = super().action_sgi_dev_new_revision()
        self._qb_recalcular_por_revision()
        return res

    def _qb_recalcular_por_revision(self):
        """Recalcula la cotización viva del proyecto. Si el piso lleno cambia
        más que la tolerancia, la cotización vuelve a «Por aprobar» y el
        proyecto se detiene; si no cambia, pasa."""
        tol = self.env['qb.cotizador.cotizacion']._param_float(PARAM_REVISION_TOLERANCIA, 0.5) / 100.0
        for project in self:
            for cot in project._qb_cotizaciones_vivas():
                if cot.costo_fuente == 'legado':
                    continue
                antes = cot.piso_lleno
                try:
                    with self.env.cr.savepoint():
                        cot.action_calcular()
                except UserError as exc:
                    project.message_post(body='Revisión %d: no se pudo recalcular %s: %s'
                                         % (project.sgi_dev_revision, cot.folio, exc))
                    continue
                cambio = abs(cot.piso_lleno - antes) / antes if antes else (1.0 if cot.piso_lleno else 0.0)
                if cambio <= tol:
                    project.message_post(body='Revisión %d: el costo de %s no cambió (piso lleno $%.4f).'
                                         % (project.sgi_dev_revision, cot.folio, cot.piso_lleno))
                    continue
                delta = 100 * (cot.piso_lleno - antes) / antes if antes else 0.0
                nota = 'Revisión %d: el piso lleno de %s pasó de $%.4f a $%.4f (%+.1f %%); espera aprobación.' % (
                    project.sgi_dev_revision, cot.folio, antes, cot.piso_lleno, delta)
                if cot.state == 'presentada':
                    cot.write({'state': 'por_aprobar', 'solicitada_por_id': self.env.uid,
                               'solicitada_el': fields.Datetime.now(), 'approved_by_id': False,
                               'approved_date': False})
                elif cot.state == 'borrador':
                    cot._validar_para_aprobacion()
                    cot.write({'state': 'por_aprobar', 'solicitada_por_id': self.env.uid,
                               'solicitada_el': fields.Datetime.now()})
                cot.message_post(body=nota)
                project.write({'qb_costo_bloqueado': True, 'qb_costo_bloqueo_nota': nota})
                project.message_post(body=nota)
        return True

    def write(self, vals):
        if 'stage_id' in vals and not self.env.context.get('qb_costo_ignorar_bloqueo'):
            detenidos = self.filtered(lambda p: p.qb_costo_bloqueado and p.stage_id.id != vals['stage_id'])
            if detenidos:
                nota = detenidos[0].qb_costo_bloqueo_nota or 'la cotización revisada espera aprobación.'
                raise UserError('%s está detenido por costo: %s' % (detenidos[0].display_name, nota))
        return super().write(vals)
