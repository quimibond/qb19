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

    def action_qb_cotizacion_nueva(self):
        """1.3.0: cotización nueva desde la pestaña «Cotización» del desarrollo; nace con lo que el
        proyecto ya tiene (cliente, artículo o descripción, gramaje, ancho, galga, volumen, precio)."""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window', 'name': 'Cotización del desarrollo',
            'res_model': 'qb.cotizador.cotizacion', 'view_mode': 'form', 'target': 'current',
            'context': {'default_project_id': self.id, 'default_partner_id': self.partner_id.id},
        }

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
        res = super().write(vals)
        if 'stage_id' in vals and not self.env.context.get('sgi_dev_migration'):
            self.filtered(lambda p: p.sgi_is_ft and p.sgi_dev_stage_key == 'liberado')._qb_sgi_avisar_sin_tarifa()
        return res

    # ------------------------------------------------------------------
    # 1.3.0 (Jose 2026-10-08, 5.6): precio en tarifa automático al ganar
    # ------------------------------------------------------------------
    def action_sgi_dev_generate_products(self):
        res = super().action_sgi_dev_generate_products()
        self._qb_sgi_ligar_articulo()
        return res

    def _qb_sgi_ligar_articulo(self):
        """El artículo acabado que generó el proyecto entra a sus cotizaciones vivas que no lo
        tenían; si alguna ya estaba ganada con el cliente aprobando, su precio va a la tarifa."""
        for project in self.filtered(lambda p: p.sgi_is_ft and p.sgi_dev_product_id):
            cots = project.qb_cotizacion_ids._qb_sgi_vivas_para_tarifa().filtered(lambda c: not c.product_id)
            if not cots:
                continue
            cots.write({'product_id': project.sgi_dev_product_id.id})
            for cot in cots:
                cot.message_post(body='Artículo %s ligado desde el desarrollo %s.' % (
                    project.sgi_dev_product_id.display_name, project.sgi_ft_folio or project.name))
            cots._sincronizar_tarifa()
        return True

    def _qb_sgi_cliente_aprobo(self, medium, date, contact=None, ref=None, evidence=None, filename=None):
        """La aprobación de la muestra registrada en el envío (SGI) es la aprobación del cliente de
        la cotización: se copia a las cotizaciones vivas del proyecto que no la tenían. Si alguna ya
        está ganada, el precio va a la tarifa (qb_cotizador); si no, irá al ganarla."""
        for project in self.filtered('sgi_is_ft'):
            cots = project.qb_cotizacion_ids._qb_sgi_vivas_para_tarifa().filtered(lambda c: not c.cliente_aprobo)
            if not cots:
                continue
            if not (medium and date):
                project.message_post(body='La aprobación del cliente no se copió a la cotización: falta medio o fecha.')
                continue
            vals = {'cliente_aprobo': True, 'cliente_medio': medium, 'cliente_fecha': date}
            if evidence:
                vals.update({'cliente_evidencia': evidence, 'cliente_evidencia_nombre': filename or 'evidencia'})
            cots.write(vals)
            for cot in cots:
                cot.message_post(body='El cliente aprobó la muestra del desarrollo %s (%s, %s%s%s).' % (
                    project.sgi_ft_folio or project.name, medium or '—', date or '—',
                    (', %s' % contact.name) if contact else '', (', ref. %s' % ref) if ref else ''))
        return True

    def _qb_sgi_avisar_sin_tarifa(self):
        """Al liberar: si ninguna cotización ganada del desarrollo puso su precio en la tarifa,
        queda dicho en el chatter (C1.17 no cierra sola)."""
        for project in self:
            cots = project.qb_cotizacion_ids
            if cots.filtered('pricelist_item_id'):
                continue
            ganadas = cots.filtered(lambda c: c.state == 'ganada')
            if ganadas:
                motivo = ('falta la aprobación del cliente en la cotización' if not ganadas.filtered('cliente_aprobo')
                          else 'la cotización ganada no tiene artículo')
            elif cots.filtered(lambda c: c.state in ('presentada', 'vencida')):
                motivo = 'ninguna cotización se ha marcado ganada'
            else:
                motivo = 'el desarrollo no tiene cotización'
            project.message_post(body='Liberado sin precio en la tarifa del cliente: %s. C1.17 queda abierta hasta '
                                      'que una cotización ganada ponga su precio en la tarifa.' % motivo)
        return True
