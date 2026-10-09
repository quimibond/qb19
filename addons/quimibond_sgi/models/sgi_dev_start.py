# -*- coding: utf-8 -*-
"""Arranque del desarrollo (57.124.0, C1 bloque E; brief §6.9).

La Solicitud de desarrollos ya se imprime con su clave nueva (F-C1-01 / 11 /
15 / 16 según el tipo, por ``sgi.format.map``). Aquí va lo que pasa al
**aprobarla** Dirección de Operaciones (C1.07) y la compuerta que abre:

- **Compuerta:** sin la aprobación no se pide la corrida de muestra: el
  proyecto no pasa a «Muestra» y una orden de fabricación de un artículo en
  desarrollo no se confirma.
- **Aviso** a las partes interesadas (puestos del parámetro
  ``quimibond_sgi.dev_request_notify_job_ids``; personas sueltas en
  ``dev_request_notify_user_ids``) con el PDF de la solicitud adjunto.
- **Existencias:** se explota la lista de materiales del artículo hasta las
  hojas compradas con la cantidad de la muestra y se compara contra lo
  disponible (``sgi.dev.mp.line``).
- **Requisición a Compras** ligada al proyecto (``approval.request`` de la
  categoría de compras, la misma que mide S1-NECESIDAD) con lo que falta;
  arranca el reloj de materia prima y lo cierra cuando llegó (o se rechazó).
  Sustituye al Word F-P-D01-16 (anexo D del brief).
"""
import base64
import logging

from odoo import api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

PARAM_NOTIFY_JOBS = 'quimibond_sgi.dev_request_notify_job_ids'
PARAM_NOTIFY_USERS = 'quimibond_sgi.dev_request_notify_user_ids'
PARAM_REQ_CATEGORY = 'quimibond_sgi.dev_requisition_category_id'
# Partes interesadas de la sección 4 del brief (menos Inspección, que no tiene
# puesto: Jose decide; va en dev_request_notify_user_ids).
NOTIFY_JOB_NAMES = (
    'DISEÑO Y DESARROLLO DE PROCESOS', 'JEFE DE MANUFACTURA', 'COORDINADOR DE LABORATORIO Y MP',
    'JEFE DE INVENTARIOS Y ALMACENES', 'INGENIERO DE CALIDAD',
)
MAX_BOM_DEPTH = 8


def _ids_param(env, key):
    raw = env['ir.config_parameter'].sudo().get_param(key, '') or ''
    out = []
    for tok in raw.split(','):
        tok = tok.strip()
        if tok.isdigit():
            out.append(int(tok))
    return out


class SgiDevMpLine(models.Model):
    """Materia prima que pide la muestra: lo que necesita la lista de materiales contra lo que hay."""
    _name = 'sgi.dev.mp.line'
    _description = "Materia prima de la muestra del desarrollo"
    _order = 'qty_missing desc, id'

    project_id = fields.Many2one('project.project', required=True, ondelete='cascade', index=True)
    product_id = fields.Many2one('product.product', string="Materia prima", required=True,
                                 help="Hoja de la lista de materiales (lo que se compra).")
    uom_id = fields.Many2one('uom.uom', string="Unidad", required=True)
    qty_needed = fields.Float(string="Necesaria", digits=(16, 3),
                              help="Lo que consume la muestra según la lista de materiales.")
    qty_available = fields.Float(string="Disponible", digits=(16, 3),
                                 help="Existencia libre en los almacenes de la compañía al revisar.")
    qty_missing = fields.Float(string="Falta", digits=(16, 3), compute='_compute_qty_missing', store=True)
    checked_at = fields.Datetime(string="Revisado el", readonly=True)
    requisition_id = fields.Many2one('approval.request', string="Requisición", readonly=True, copy=False,
                                     help="Requisición a Compras que pidió lo que falta.")

    @api.depends('qty_needed', 'qty_available')
    def _compute_qty_missing(self):
        for line in self:
            line.qty_missing = max(line.qty_needed - line.qty_available, 0.0)


class ProjectProjectDevStart(models.Model):
    _inherit = 'project.project'

    sgi_dev_mp_line_ids = fields.One2many('sgi.dev.mp.line', 'project_id', string="Materia prima de la muestra")
    sgi_dev_mp_missing_count = fields.Integer(string="Materias primas que faltan",
                                              compute='_compute_sgi_dev_mp_missing_count')
    sgi_dev_requisition_ids = fields.One2many('approval.request', 'sgi_dev_project_id',
                                              string="Requisiciones a Compras")
    sgi_dev_requisition_count = fields.Integer(compute='_compute_sgi_dev_requisition_count')

    @api.depends('sgi_dev_mp_line_ids.qty_missing')
    def _compute_sgi_dev_mp_missing_count(self):
        for project in self:
            project.sgi_dev_mp_missing_count = len(project.sgi_dev_mp_line_ids.filtered(
                lambda l: l.qty_missing > 0 and not l.requisition_id))

    @api.depends('sgi_dev_requisition_ids')
    def _compute_sgi_dev_requisition_count(self):
        for project in self:
            project.sgi_dev_requisition_count = len(project.sgi_dev_requisition_ids)

    # ------------------------------------------------------------------------
    # Compuerta: sin aprobación no hay corrida
    # ------------------------------------------------------------------------
    def _sgi_dev_check_run_allowed(self):
        for project in self.filtered('sgi_is_ft'):
            if not project.sgi_dev_approved_by_id:
                raise UserError(
                    "%s: la corrida de muestra necesita la Solicitud de desarrollo aprobada por Dirección de "
                    "Operaciones (botón «Aprobar solicitud» en la pestaña Solicitud de desarrollo, C1.07)."
                    % project.display_name)
        return True

    # 57.128.0: la compuerta de Dirección de Operaciones aplica a la corrida (asistente y orden de
    # fabricación), no al cambio de etapa: el folio FT se asigna al entrar a «Muestra», antes de que
    # Diseño de Producto elabore la solicitud que Dirección de Operaciones aprueba. La etapa «Muestra» la abre la aprobación del
    # cliente (sgi_dev_customer.py).

    # ------------------------------------------------------------------------
    # Al aprobar: aviso con PDF y revisión de existencias
    # ------------------------------------------------------------------------
    def action_sgi_dev_approve_request(self):
        pending = self.filtered(lambda p: p.sgi_is_ft and not p.sgi_dev_approved_by_id)
        res = super().action_sgi_dev_approve_request()
        for project in pending.filtered('sgi_dev_approved_by_id'):
            try:
                project._sgi_dev_mp_check()
            except UserError as exc:
                project.message_post(body="No se revisaron existencias: %s" % exc)
            project._sgi_dev_notify_approved()
        return res

    @api.model
    def _sgi_dev_notify_users(self):
        users = self.env['res.users']
        jobs = self.env['hr.job'].sudo().browse(_ids_param(self.env, PARAM_NOTIFY_JOBS)).exists()
        if jobs:
            employees = self.env['hr.employee'].sudo().search([('job_id', 'in', jobs.ids)])
            users |= employees.user_id
        users |= self.env['res.users'].sudo().browse(_ids_param(self.env, PARAM_NOTIFY_USERS)).exists()
        return users.filtered(lambda u: u.active and not u.share)

    def _sgi_dev_request_pdf(self):
        self.ensure_one()
        report = self.env.ref('quimibond_sgi.action_report_dev_request')
        pdf, _kind = self.env['ir.actions.report'].sudo()._render_qweb_pdf(report.report_name, res_ids=self.ids)
        return self.env['ir.attachment'].sudo().create({
            'name': "Solicitud de desarrollo %s.pdf" % (self.sgi_ft_folio or self.name),
            'type': 'binary', 'datas': base64.b64encode(pdf), 'mimetype': 'application/pdf',
            'res_model': 'project.project', 'res_id': self.id,
        })

    def _sgi_dev_notify_approved(self):
        """Aviso a las partes interesadas con el PDF de la solicitud aprobada."""
        for project in self:
            users = self._sgi_dev_notify_users()
            try:
                attachment = project._sgi_dev_request_pdf()
            except Exception as exc:  # noqa: BLE001 — sin PDF el aviso sale igual
                _logger.warning("SGI desarrollo: no se pudo generar el PDF de la solicitud %s: %s", project.id, exc)
                attachment = self.env['ir.attachment']
            missing = project.sgi_dev_mp_line_ids.filtered(lambda l: l.qty_missing > 0)
            body = ("Solicitud de desarrollo <b>%s</b> aprobada por %s. Partes interesadas: %s."
                    % (project.sgi_ft_folio or project.name, project.sgi_dev_approved_by_id.name,
                       ", ".join(users.mapped('name')) or "ninguna configurada (Ajustes → SGI)"))
            if missing:
                body += "<br/>Materia prima que falta para la muestra: %s." % ", ".join(
                    "%s (%s %s)" % (l.product_id.display_name, round(l.qty_missing, 3), l.uom_id.name) for l in missing)
            elif project.sgi_dev_mp_line_ids:
                body += "<br/>Hay existencia de toda la materia prima de la muestra."
            project.message_post(body=body, partner_ids=users.partner_id.ids, attachment_ids=attachment.ids,
                                 subject="Solicitud de desarrollo aprobada: %s" % (project.sgi_ft_folio or project.name),
                                 message_type='comment', subtype_xmlid='mail.mt_comment')
        return True

    # ------------------------------------------------------------------------
    # Existencias de la materia prima de la muestra
    # ------------------------------------------------------------------------
    def _sgi_dev_sample_product(self):
        self.ensure_one()
        return self.sgi_dev_product_id

    @api.model
    def _sgi_dev_uom_is_weight(self, uom):
        kg = self.env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        if kg and uom:
            try:
                return bool(uom._has_common_reference(kg))
            except AttributeError:
                pass
        return (uom.name or '').lower().startswith('k')

    def _sgi_dev_sample_qty(self, product):
        """Cantidad de la muestra en la unidad del artículo (kg o m)."""
        self.ensure_one()
        if self._sgi_dev_uom_is_weight(product.uom_id):
            return self.sgi_dev_sample_kg or 0.0
        return self.sgi_dev_sample_m or 0.0

    @api.model
    def _sgi_dev_explode(self, product, qty, uom, needs=None, depth=0):
        """{producto hoja: (cantidad, unidad)} explotando las listas de materiales
        hasta lo que se compra (sin receta)."""
        needs = {} if needs is None else needs
        bom = self.env['mrp.bom'].sudo()._bom_find(product, company_id=self.env.company.id).get(product)
        if not bom or depth >= MAX_BOM_DEPTH:
            cur = needs.get(product.id)
            needs[product.id] = (cur[0] + qty, cur[1]) if cur else (qty, uom)
            return needs
        qty_bom = uom._compute_quantity(qty, bom.product_uom_id) if uom != bom.product_uom_id else qty
        factor = qty_bom / (bom.product_qty or 1.0)
        for line in bom.bom_line_ids:
            if line._skip_bom_line(product):
                continue
            self._sgi_dev_explode(line.product_id, line.product_qty * factor, line.product_uom_id, needs, depth + 1)
        return needs

    def _sgi_dev_mp_check(self):
        """Reescribe las líneas de materia prima de la muestra con lo que falta."""
        Line = self.env['sgi.dev.mp.line'].sudo()
        now = fields.Datetime.now()
        for project in self.filtered('sgi_is_ft'):
            product = project._sgi_dev_sample_product()
            if not product:
                raise UserError("%s no tiene artículo en desarrollo: genere el artículo y su lista de materiales "
                                "antes de revisar existencias." % project.display_name)
            qty = project._sgi_dev_sample_qty(product)
            if not qty:
                raise UserError("%s no tiene cantidad de muestra (m o kg) en la solicitud." % project.display_name)
            if not self.env['mrp.bom'].sudo()._bom_find(product, company_id=project.company_id.id).get(product):
                raise UserError("%s no tiene lista de materiales: sin ella no se sabe qué materia prima pide la "
                                "muestra." % product.display_name)
            needs = project.with_company(project.company_id)._sgi_dev_explode(product, qty, product.uom_id)
            keep = project.sgi_dev_mp_line_ids.filtered('requisition_id')
            (project.sgi_dev_mp_line_ids - keep).unlink()
            kept_by_product = {l.product_id.id: l for l in keep}
            for pid, (need, uom) in needs.items():
                comp = self.env['product.product'].sudo().browse(pid).with_company(project.company_id)
                available = comp.free_qty if comp.is_storable else 0.0
                if uom != comp.uom_id:
                    available = comp.uom_id._compute_quantity(available, uom)
                vals = {'qty_needed': need, 'uom_id': uom.id, 'qty_available': max(available, 0.0),
                        'checked_at': now}
                if pid in kept_by_product:
                    kept_by_product[pid].write(vals)
                else:
                    Line.create(dict(vals, project_id=project.id, product_id=pid))
        return True

    def action_sgi_dev_mp_check(self):
        self._sgi_dev_mp_check()
        for project in self:
            missing = project.sgi_dev_mp_line_ids.filtered(lambda l: l.qty_missing > 0)
            project.message_post(body="Existencias revisadas para la muestra: %s." % (
                "falta " + ", ".join("%s (%s %s)" % (l.product_id.display_name, round(l.qty_missing, 3), l.uom_id.name)
                                     for l in missing) if missing else "hay de todo"))
        return True

    # ------------------------------------------------------------------------
    # Requisición a Compras ligada al proyecto
    # ------------------------------------------------------------------------
    @api.model
    def _sgi_dev_requisition_category(self, company):
        Category = self.env['approval.category'].sudo()
        raw = self.env['ir.config_parameter'].sudo().get_param(PARAM_REQ_CATEGORY, '') or ''
        if raw.strip().isdigit():
            cat = Category.browse(int(raw.strip())).exists()
            if cat:
                return cat
        return Category.search([('approval_type', '=', 'purchase'), ('company_id', 'in', [company.id, False])],
                               order='company_id desc, id', limit=1)

    def action_sgi_dev_mp_requisition(self):
        """Requisición a Compras con la materia prima que falta (una por clic)."""
        self.ensure_one()
        lines = self.sgi_dev_mp_line_ids.filtered(lambda l: l.qty_missing > 0 and not l.requisition_id)
        if not lines:
            raise UserError("No falta materia prima sin requisición. Revise existencias primero.")
        category = self._sgi_dev_requisition_category(self.company_id)
        if not category:
            raise UserError("No hay una categoría de Aprobaciones de tipo compra: configúrela en Ajustes → SGI.")
        label = self.sgi_ft_folio or self.name
        request = self.env['approval.request'].create({
            'name': "Materia prima para la muestra de %s" % label,
            'category_id': category.id, 'request_owner_id': self.env.uid,
            'reference': label, 'company_id': self.company_id.id, 'sgi_dev_project_id': self.id,
            'reason': "<p>Desarrollo %s (%s). Cantidad de muestra: %s m / %s kg.</p>" % (
                label, self.partner_id.name or 'interno', self.sgi_dev_sample_m, self.sgi_dev_sample_kg),
            'product_line_ids': [(0, 0, {
                'product_id': l.product_id.id, 'quantity': l.qty_missing, 'product_uom_id': l.uom_id.id,
                'description': "%s · muestra %s" % (l.product_id.display_name, label),
            }) for l in lines],
        })
        lines.write({'requisition_id': request.id})
        if not self.sgi_dev_mp_pending:
            self.env['sgi.dev.mp.wait'].create({
                'project_id': self.id,
                'note': ", ".join(lines.mapped('product_id.display_name'))[:200]})
        self.message_post(body="Requisición a Compras %s creada con %d materia(s) prima(s); el reloj de "
                               "materia prima quedó corriendo." % (request.name, len(lines)))
        return {'type': 'ir.actions.act_window', 'res_model': 'approval.request', 'res_id': request.id,
                'view_mode': 'form', 'target': 'current'}

    def action_sgi_dev_view_requisitions(self):
        self.ensure_one()
        return {'type': 'ir.actions.act_window', 'name': "Requisiciones de %s" % (self.sgi_ft_folio or self.name),
                'res_model': 'approval.request', 'view_mode': 'list,form',
                'domain': [('sgi_dev_project_id', '=', self.id)],
                'context': {'default_sgi_dev_project_id': self.id}}

    def _sgi_dev_mp_wait_autoclose(self):
        """Cierra el reloj de materia prima cuando ya no hay requisición viva:
        todas aprobadas con sus compras recibidas, o rechazadas / canceladas."""
        for project in self.filtered(lambda p: p.sgi_is_ft and p.sgi_dev_mp_pending and p.sgi_dev_requisition_ids):
            if all(r._sgi_dev_resolved() for r in project.sgi_dev_requisition_ids):
                project.action_sgi_dev_mp_wait_stop()
                project.message_post(body="La materia prima de la muestra llegó (o la requisición se cerró): el "
                                          "reloj de materia prima se detuvo.")
        return True

    @api.model
    def cron_sgi_dev_mp_wait(self):
        self.search([('sgi_is_ft', '=', True), ('sgi_dev_mp_wait_ids.date_end', '=', False)])._sgi_dev_mp_wait_autoclose()
        return True


class ApprovalRequestDevStart(models.Model):
    _inherit = 'approval.request'

    sgi_dev_project_id = fields.Many2one('project.project', string="Proyecto de desarrollo", index=True,
                                         ondelete='set null', copy=False,
                                         help="Desarrollo cuya muestra pide esta materia prima (C1.08).")

    def _sgi_dev_resolved(self):
        """Ya no detiene el desarrollo: rechazada / cancelada, o aprobada con
        todas sus órdenes de compra recibidas."""
        self.ensure_one()
        if self.request_status in ('refused', 'cancel'):
            return True
        if self.request_status != 'approved':
            return False
        orders = self.env['purchase.order'].sudo().search([('sgi_approval_request_id', '=', self.id)]) \
            if 'sgi_approval_request_id' in self.env['purchase.order']._fields else self.env['purchase.order']
        if not orders:
            lines = self.product_line_ids.mapped('purchase_order_line_id')
            orders = lines.mapped('order_id')
        if not orders:
            return False
        return all(o.state == 'cancel' or getattr(o, 'receipt_status', 'full') in ('full', False)
                   for o in orders)

    # request_status es calculado y almacenado: no pasa por write(). Los botones sí.
    def _sgi_dev_after_decision(self):
        self.mapped('sgi_dev_project_id')._sgi_dev_mp_wait_autoclose()

    def action_approve(self, approver=None):
        res = super().action_approve(approver=approver)
        self._sgi_dev_after_decision()
        return res

    def action_refuse(self, approver=None):
        res = super().action_refuse(approver=approver)
        self._sgi_dev_after_decision()
        return res

    def action_cancel(self):
        res = super().action_cancel()
        self._sgi_dev_after_decision()
        return res


class MrpProductionDevStart(models.Model):
    _inherit = 'mrp.production'

    def _sgi_dev_check_request_approved(self):
        for mo in self:
            tmpl = mo.product_id.product_tmpl_id
            project = tmpl.sgi_dev_project_id
            if project and tmpl.sgi_dev_state == 'desarrollo':
                project._sgi_dev_check_run_allowed()

    def action_confirm(self):
        self._sgi_dev_check_request_approved()
        return super().action_confirm()
