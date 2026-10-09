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
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

CODE_GAUGE = 'galga'
# Ligas decididas a mano (Jose, 2026-10-07): cotización vieja → proyecto.
# 1.2.0 (Jose 2026-10-08, punto 1): la liga nunca cambia valores de origen
# (la galga «21» que ponía 1.0.0 se regresó a «18» por MCP).
LEGACY_PROJECT_LINKS = {121: 491}
C1_COT_DELIVERABLES = {
    'C1-COSTO': {
        'measure_domain': "[('project_id', '!=', False), ('approved_date', '!=', False)]",
        'measure_date_field': 'approved_date', 'measure_user_field': 'approved_by_id',
        'complete_domain': "[('precio_objetivo', '>', 0)]",
        'complete_criteria': "Cotización del desarrollo aprobada por el puesto con precio al cliente.",
    },
    # 1.2.0 (Jose 2026-10-08, punto 3): C1.06 se mide con la cotización
    # presentada (y lo que fue presentada: vencida, ganada, perdida).
    'C1-COTIZACION': {
        'measure_domain': "[('project_id', '!=', False), ('presented_date', '!=', False), "
                          "('state', 'in', ('presentada', 'vencida', 'ganada', 'perdida'))]",
        'measure_date_field': 'presented_date', 'measure_user_field': 'presented_by_id',
        'complete_domain': "[('validez_hasta', '!=', False)]",
        'complete_criteria': "Presentada al cliente con precio, vigencia, mínimo, tiempo de entrega y rollo.",
    },
    # 1.3.0 (Jose 2026-10-08, 5.6): C1.17 se mide con el precio de la cotización ganada en la
    # tarifa del cliente (antes: producto vendible por categoría), completo cuando el artículo ya
    # está liberado con lista de materiales.
    'C1-ARTICULO': {
        'measure_domain': "[('project_id', '!=', False), ('pricelist_item_id', '!=', False)]",
        'measure_date_field': 'tarifa_fecha', 'measure_user_field': 'tarifa_user_id',
        'complete_domain': "[('product_id.product_tmpl_id.sgi_dev_state', '=', 'liberado'), "
                           "('product_id.product_tmpl_id.bom_ids', '!=', False)]",
        'complete_criteria': "Precio de la cotización ganada en la tarifa del cliente; artículo liberado con "
                             "lista de materiales.",
    },
}
# Modelo del cotizador anterior: las fichas que aún se medían con él pasan al entregable.
LEGACY_MODEL = 'qb.cotizacion'


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
        # 1.4.0 (Dirección General 2026-10-09): el artículo base del desarrollo es el producto hermano
        # de la cotización; sin artículo generado, el costo se toma de él.
        if project.sgi_dev_base_product_id:
            vals['hermano_product_id'] = project.sgi_dev_base_product_id.id
            if not project.sgi_dev_product_id:
                vals['costo_fuente'] = 'hermano'
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

    def _validar_para_aprobacion(self):
        res = super()._validar_para_aprobacion()
        # 1.4.0: con renglones «Por capturar» en las listas del desarrollo, el costo no está completo.
        if self.project_id and self.project_id.sgi_dev_bom_pending_count:
            raise UserError("El desarrollo %s tiene %d renglón(es) de lista de materiales por capturar (componentes "
                            "que dependen de la clave que cambió respecto al artículo base). Captúrelos antes de "
                            "pedir aprobación." % (self.project_id.display_name, self.project_id.sgi_dev_bom_pending_count))
        return res

    # ------------------------------------------------------------------
    # 1.3.0 (Jose 2026-10-08, 5.6): precio en tarifa automático al ganar
    # ------------------------------------------------------------------
    def _producto_para_tarifa(self):
        """La cotización de un desarrollo se hace antes de que exista el artículo: al ganar, el
        precio va al artículo acabado que generó el proyecto."""
        product = super()._producto_para_tarifa()
        if not product and self.project_id.sgi_dev_product_id:
            product = self.project_id.sgi_dev_product_id
        return product

    def _qb_sgi_vivas_para_tarifa(self):
        """Cotizaciones del proyecto que pueden llevar su precio a la tarifa (no perdidas ni
        reemplazadas)."""
        return self.filtered(lambda c: c.state in ('presentada', 'vencida', 'ganada'))

    # ------------------------------------------------------------------
    # 1.1.0 (Jose 2026-10-08, punto 2): el candado es una aprobación de Odoo
    # (Aprobaciones), no un estado editable. La solicitud nace en la categoría
    # y el asunto del rol «Aprueba» de C1.05 (puesto 183 por el SGI): quien
    # aprueba ahí presenta la cotización; quien rechaza la regresa a borrador.
    # ------------------------------------------------------------------
    approval_request_id = fields.Many2one('approval.request', string="Solicitud de aprobación", readonly=True,
                                          copy=False, ondelete='set null')
    approval_request_status = fields.Selection(related='approval_request_id.request_status', string="Estado en Aprobaciones")

    @api.model
    def _qb_sgi_approval_role(self):
        Role = self.env['sgi.activity.role'].sudo()
        return Role.search([('role', '=', 'aprueba'), ('approval_kind', '=', 'solicitud'),
                            ('activity_id.process_id.code', '=', 'C1'), ('activity_id.number', '=', 'C1.05'),
                            ('approval_category_id', '!=', False)], limit=1)

    @api.model
    def _qb_sgi_approval_subject(self):
        """(categoría, asunto) de Aprobaciones para la cotización; (vacío, vacío) si C1.05 no tiene
        aprobación como solicitud (entonces aprueba el puesto desde la cotización)."""
        role = self._qb_sgi_approval_role()
        if not role:
            return self.env['approval.category'], self.env['sgi.approval.subject']
        Subject = self.env['sgi.approval.subject'].sudo()
        subject = Subject.search([('role_id', '=', role.id), ('category_id', '=', role.approval_category_id.id)], limit=1)
        if not subject:
            subject = Subject.create({'name': 'Cotización de desarrollo (precio antes de cotizar)',
                                      'category_id': role.approval_category_id.id, 'role_id': role.id})
        return role.approval_category_id, subject

    def _qb_sgi_approval_pending(self):
        self.ensure_one()
        return bool(self.approval_request_id) and self.approval_request_id.request_status in ('new', 'pending')

    def action_enviar_aprobacion(self):
        res = super().action_enviar_aprobacion()
        for rec in self:
            if rec._qb_sgi_approval_pending():
                continue
            category, subject = self._qb_sgi_approval_subject()
            if not category:
                rec.message_post(body="C1.05 no tiene aprobación como solicitud en el SGI: aprueba el puesto "
                                      "configurado desde la cotización.")
                continue
            label = rec.folio or rec.name
            # 1.5.0 (Dirección General 2026-10-09): la solicitud y sus aprobadores se crean con sudo.
            # El vendedor (Usuario interno, sin grupos de Aprobaciones) no puede crear
            # `approval.approver`, que nace solo al poner la categoría; `request_owner_id` sigue
            # siendo quien la manda, para que en Aprobaciones se vea quién la pidió.
            request = self.env['approval.request'].sudo().create({
                'name': "Cotización %s · %s" % (label, rec.partner_id.name or 'sin cliente'),
                'category_id': category.id, 'sgi_subject_id': subject.id, 'request_owner_id': self.env.uid,
                'reference': label, 'company_id': rec.company_id.id, 'qb_cotizacion_id': rec.id,
                'reason': "<p>%s %s · precio %s %s (%.4f MXN) · piso lleno %.4f · piso ocioso %.4f · margen neto "
                          "%.1f %% · semáforo %s · costo de la muestra %.2f MXN.%s</p>" % (
                              rec.product_id.default_code or rec.spec_descripcion or '', label, rec.precio_objetivo,
                              rec.currency_id.name, rec.precio_mxn, rec.piso_lleno, rec.piso_ocioso,
                              rec.margen_neto_pct, rec.semaforo or 'sin precio', rec.costo_muestra,
                              (" Proyecto %s." % rec.project_id.display_name) if rec.project_id else ''),
            })
            request.sudo().action_confirm()
            rec.write({'approval_request_id': request.id})
            rec.activity_unlink(['mail.mail_activity_data_todo'])
            rec.message_post(body="Solicitud %s enviada a Aprobaciones (%s): la cotización se presenta cuando se "
                                  "apruebe ahí." % (request.name, category.name))
        return res

    def action_volver_a_borrador(self):
        """1.5.0: retirar la cotización cancela su solicitud viva en Aprobaciones (con sudo: el
        vendedor no tiene grupos de Aprobaciones); volver a enviar crea una solicitud nueva."""
        res = super().action_volver_a_borrador()
        for rec in self.filtered(lambda r: r.approval_request_id and r.approval_request_id.request_status in ('new', 'pending')):
            request = rec.approval_request_id.sudo().with_context(qb_from_approval=True)
            request.action_cancel()
            rec.message_post(body="Solicitud %s cancelada en Aprobaciones: la cotización se retiró." % request.name)
        return res

    def _puede_aprobar(self, user):
        if self.env.context.get('qb_from_approval'):
            return True
        if self._qb_sgi_approval_pending():
            return False
        return super()._puede_aprobar(user)

    def action_aprobar(self):
        for rec in self:
            if rec._qb_sgi_approval_pending() and not self.env.context.get('qb_from_approval'):
                raise UserError("Esta cotización se aprueba en Aprobaciones (solicitud %s), no desde aquí."
                                % rec.approval_request_id.name)
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
        legacy = self.env['ir.model'].sudo()._get(LEGACY_MODEL) if LEGACY_MODEL in self.env else None
        for code, vals in C1_COT_DELIVERABLES.items():
            for deliverable in Deliverable.with_context(active_test=False).search([('code', '=', code)]):
                dvals = {k: v for k, v in vals.items() if k in deliverable._fields}
                deliverable.write(dict(dvals, odoo_model_id=ir_model.id))
                # Manual o, desde 1.2.0, «Registro en Odoo» sobre el cotizador anterior: al entregable.
                domain = [('output_deliverable_ids', 'in', deliverable.id), ('measure_method', 'in', ('manual', 'odoo'))]
                activities = self.env['sgi.process.activity'].sudo().search(domain).filtered(
                    lambda a: a.measure_method == 'manual'
                    or not a.measure_model_id or (legacy and a.measure_model_id == legacy))
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
            if cot.legacy_id in LEGACY_PROJECT_LINKS:
                project = Project.browse(LEGACY_PROJECT_LINKS[cot.legacy_id]).filtered('sgi_is_ft')
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
            # qb_sgi_no_fill: la liga no recaptura nada del proyecto (los valores de origen se respetan).
            cot.with_context(qb_sgi_no_fill=True).write({'project_id': project.id})
            cot.message_post(body='Ligada al proyecto de desarrollo %s.' % project.display_name)
            n += 1
        if n:
            _logger.info('qb_costeo_sgi: %d cotizaciones ligadas a su proyecto FT', n)
        return n


class ApprovalRequestCotizacion(models.Model):
    _inherit = 'approval.request'

    qb_cotizacion_id = fields.Many2one('qb.cotizador.cotizacion', string="Cotización", index=True,
                                       ondelete='set null', copy=False)

    def _qb_after_decision(self):
        motivo = self.env.ref('qb_cotizador.motivo_regreso_otro', raise_if_not_found=False)
        for request in self.filtered('qb_cotizacion_id'):
            cot = request.qb_cotizacion_id
            if cot.state != 'por_aprobar':
                continue
            if request.request_status == 'approved':
                cot.with_context(qb_from_approval=True).action_aprobar()
            elif request.request_status in ('refused', 'cancel') and motivo:
                cot.with_context(qb_from_approval=True)._regresar(
                    motivo, "%s en Aprobaciones (%s) por %s." % (
                        'Rechazada' if request.request_status == 'refused' else 'Cancelada', request.name,
                        self.env.user.name))

    def action_approve(self, approver=None):
        res = super().action_approve(approver=approver)
        self._qb_after_decision()
        return res

    def action_refuse(self, approver=None):
        res = super().action_refuse(approver=approver)
        self._qb_after_decision()
        return res

    def action_cancel(self):
        res = super().action_cancel()
        self._qb_after_decision()
        return res
