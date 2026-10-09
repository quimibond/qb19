# -*- coding: utf-8 -*-
"""Ficha técnica interna y Especificaciones del producto (57.132.0; brief §6.12; Jose 2026-10-08, 5.3).

Dos documentos distintos, los dos impresos desde la tabla de características
del proyecto y guardados como registro con revisión (lo que se firmó o se
mandó no cambia aunque la tabla siga viva):

* **Ficha técnica interna** (F-P-D01-24, ``sgi.dev.tech.sheet``): los dos
  juegos de límites, la especificación del cliente y el **control interno**,
  con las firmas de los seis puestos del brief (lista fija por puesto en el
  parámetro ``quimibond_sgi.dev_tech_sheet_sign_job_ids``) y la aprobación de
  C1.15: el botón «Aprobar ficha» es el que liga el rol 1928 (Administrador
  de Ventas) por la regla nativa de Studio. Nunca sale al cliente.
* **Especificaciones del producto** (F-P-D01-08, ``sgi.dev.customer.spec``):
  documento al cliente, bilingüe, solo con los renglones «En especificación
  del cliente» y **solo con su tolerancia** (el control interno no se copia),
  más lo exclusivo de este documento: uso principal (el del proyecto) e
  instrucciones de cuidado de la lista «Instrucción de cuidado» (sin texto
  libre; la lista nace vacía).

Los renglones de los dos documentos heredan el mixin de características de
``quimibond_ficha_tecnica_tela`` (mismos límites y etiquetas que la tabla).
"""
import base64

from odoo import api, fields, models
from odoo.exceptions import UserError

PARAM_SIGN_JOBS = 'quimibond_sgi.dev_tech_sheet_sign_job_ids'
# Seis puestos del brief §6.12, por nombre (producción); los que no existan se reportan.
SIGN_JOB_NAMES = ('JEFE DE MANUFACTURA', 'JEFE DE CALIDAD', 'SUPERVISOR DE INSPECCION Y EMPAQUE',
                  'DISEÑO Y DESARROLLO DE PRODUCTO', 'DIRECTOR DE OPERACIONES',
                  'ADMINISTRADOR DE VENTAS Y MARKETING')
DOC_STATES = [('borrador', "Borrador"), ('vigente', "Vigente"), ('sustituida', "Sustituida")]


def _ids_param(env, key):
    raw = env['ir.config_parameter'].sudo().get_param(key, '') or ''
    return [int(t) for t in raw.split(',') if t.strip().isdigit()]


class SgiDevOptionEn(models.Model):
    _inherit = 'sgi.dev.option'

    name_en = fields.Char(string="Opción en inglés",
                          help="Para los documentos bilingües al cliente (instrucciones de cuidado).")


class SgiDevDocMixin(models.AbstractModel):
    """Lo común a los dos documentos: proyecto, artículo, revisión, estado y PDF emitido."""
    _name = 'sgi.dev.doc.mixin'
    _description = "Documento del desarrollo impreso desde la tabla"

    project_id = fields.Many2one('project.project', string="Desarrollo", required=True, ondelete='cascade', index=True,
                                 domain="[('sgi_is_ft', '=', True), ('is_template', '=', False)]")
    partner_id = fields.Many2one(related='project_id.partner_id', string="Cliente")
    product_id = fields.Many2one('product.product', string="Artículo", compute='_compute_product_id', store=True,
                                 readonly=False)
    company_id = fields.Many2one('res.company', default=lambda self: self.env.company, required=True)
    revision = fields.Integer(string="Revisión del desarrollo", readonly=True, copy=False,
                              help="Revisión del proyecto con la que se tomó la tabla.")
    date = fields.Date(string="Fecha", default=fields.Date.context_today, required=True)
    state = fields.Selection(DOC_STATES, string="Estado", default='borrador', required=True, tracking=True, copy=False)
    attachment_id = fields.Many2one('ir.attachment', string="PDF emitido", readonly=True, copy=False)
    line_count = fields.Integer(compute='_compute_line_count')

    @api.depends('project_id.sgi_dev_product_id')
    def _compute_product_id(self):
        for doc in self:
            doc.product_id = doc.project_id.sgi_dev_product_id

    def _compute_line_count(self):
        for doc in self:
            doc.line_count = len(doc.line_ids)

    def _doc_source_lines(self):
        """Renglones de la tabla del proyecto que entran a este documento."""
        self.ensure_one()
        return self.project_id.sgi_dev_line_ids

    def _doc_line_vals(self, line):
        """Valores del renglón del documento a partir del renglón de la tabla."""
        return line._limit_vals()

    def action_load_lines(self):
        """Toma (o retoma) los renglones de la tabla del proyecto: el documento en borrador refleja la
        tabla de hoy; emitido ya no cambia."""
        for doc in self:
            if doc.state != 'borrador':
                raise UserError("El documento ya está emitido: cree uno nuevo para la revisión siguiente.")
            doc.line_ids.unlink()
            doc.write({'line_ids': [(0, 0, doc._doc_line_vals(line)) for line in doc._doc_source_lines()],
                       'revision': doc.project_id.sgi_dev_revision})
        return True

    @api.model_create_multi
    def create(self, vals_list):
        docs = super().create(vals_list)
        for doc in docs:
            if not doc.line_ids and not self.env.context.get('sgi_dev_doc_no_lines'):
                doc.action_load_lines()
        return docs

    def unlink(self):
        if any(d.state != 'borrador' for d in self):
            raise UserError("Un documento emitido no se borra; queda como sustituido por la revisión siguiente.")
        return super().unlink()

    def _doc_report_xmlid(self):
        raise NotImplementedError()

    def _doc_emit(self, label):
        """Emite: vigente, las anteriores vigentes del mismo proyecto quedan sustituidas, PDF en el registro."""
        self.ensure_one()
        if self.state != 'borrador':
            raise UserError("El documento ya está emitido.")
        if not self.line_ids:
            raise UserError("El documento no tiene renglones: cargue la tabla del proyecto.")
        previous = self.search([('project_id', '=', self.project_id.id), ('state', '=', 'vigente'), ('id', '!=', self.id)])
        previous.write({'state': 'sustituida'})
        self.write({'state': 'vigente'})
        report = self.env.ref(self._doc_report_xmlid())
        pdf, _kind = self.env['ir.actions.report'].sudo()._render_qweb_pdf(report.report_name, res_ids=self.ids)
        att = self.env['ir.attachment'].create({
            'name': "%s %s rev%s.pdf" % (label, self.product_id.default_code or self.project_id.sgi_ft_folio
                                         or self.project_id.name, self.revision),
            'datas': base64.b64encode(pdf), 'mimetype': 'application/pdf',
            'res_model': self._name, 'res_id': self.id})
        self.write({'attachment_id': att.id})
        self.message_post(body="%s emitida por %s (revisión %s del desarrollo)%s." % (
            label, self.env.user.name, self.revision,
            (", sustituye a %d anterior(es)" % len(previous)) if previous else ''), attachment_ids=att.ids)
        self.project_id.message_post(body="%s emitida (revisión %s)." % (label, self.revision))
        return att

    def action_print(self):
        self.ensure_one()
        return self.env.ref(self._doc_report_xmlid()).report_action(self)


# ---------------------------------------------------------------------------
# Ficha técnica interna (control interno; nunca al cliente)
# ---------------------------------------------------------------------------
class SgiDevTechSheet(models.Model):
    _name = 'sgi.dev.tech.sheet'
    _description = "Ficha técnica interna del desarrollo"
    _inherit = ['sgi.dev.doc.mixin', 'mail.thread']
    _order = 'id desc'

    name = fields.Char(string="Ficha", compute='_compute_name', store=True)
    line_ids = fields.One2many('sgi.dev.tech.sheet.line', 'sheet_id', string="Características")
    sign_ids = fields.One2many('sgi.dev.tech.sheet.sign', 'sheet_id', string="Firmas")
    signed_count = fields.Integer(compute='_compute_signed')
    pending_sign_count = fields.Integer(compute='_compute_signed')
    can_sign = fields.Boolean(compute='_compute_signed', help="El usuario tiene un puesto que firma y aún no firmó.")
    approved_by_id = fields.Many2one('res.users', string="Aprobó (Ventas, C1.15)", readonly=True, copy=False)
    approved_at = fields.Datetime(string="Aprobada el", readonly=True, copy=False)

    @api.depends('project_id.name', 'product_id.default_code', 'revision')
    def _compute_name(self):
        for sheet in self:
            sheet.name = "Ficha técnica interna %s · rev. %s" % (
                sheet.product_id.default_code or sheet.project_id.sgi_ft_folio or sheet.project_id.name or '',
                sheet.revision)

    @api.model
    def _sgi_dev_my_sign_jobs(self):
        """Puestos que el usuario firma: los suyos y, por suplencia (57.143.0),
        los de las personas de las que es jefe directo."""
        employees = self.env.user.employee_ids.sudo()
        reports = self.env['hr.employee'].sudo().search([('parent_id', 'in', employees.ids)]) \
            if employees else self.env['hr.employee'].sudo()
        return employees.job_id | reports.job_id

    @api.depends('sign_ids.user_id', 'sign_ids.job_id')
    @api.depends_context('uid')
    def _compute_signed(self):
        my_jobs = self._sgi_dev_my_sign_jobs()
        for sheet in self:
            signed = sheet.sign_ids.filtered('user_id')
            sheet.signed_count = len(signed)
            sheet.pending_sign_count = len(sheet.sign_ids) - len(signed)
            sheet.can_sign = bool((sheet.sign_ids - signed).filtered(lambda s: s.job_id in my_jobs))

    @api.model
    def _sign_jobs(self):
        return self.env['hr.job'].sudo().browse(_ids_param(self.env, PARAM_SIGN_JOBS)).exists()

    @api.model
    def _sgi_dev_set_sign_jobs_default(self):
        """Deja los seis puestos del brief en el parámetro, por nombre, si está vacío. Devuelve
        (puestos encontrados, nombres no encontrados)."""
        Param = self.env['ir.config_parameter'].sudo()
        if (Param.get_param(PARAM_SIGN_JOBS, '') or '').strip():
            return self._sign_jobs(), []
        Project = self.env['project.project']
        found, missing = self.env['hr.job'], []
        for name in SIGN_JOB_NAMES:
            jobs = Project._sgi_dev_search_langs('hr.job', [('name', 'ilike', name)])
            job = (jobs.filtered('active') or jobs)[:1]
            if job:
                found |= job
            else:
                missing.append(name)
        if found:
            Param.set_param(PARAM_SIGN_JOBS, ",".join(str(i) for i in found.ids))
        return found, missing

    @api.model_create_multi
    def create(self, vals_list):
        sheets = super().create(vals_list)
        Sign = self.env['sgi.dev.tech.sheet.sign']
        for sheet in sheets:
            if not sheet.sign_ids:
                Sign.create([{'sheet_id': sheet.id, 'job_id': job.id, 'sequence': i * 10}
                             for i, job in enumerate(sheet._sign_jobs(), start=1)])
        return sheets

    def _doc_report_xmlid(self):
        return 'quimibond_sgi.action_report_dev_tech_sheet'

    def action_sign(self):
        """Firma los renglones de los puestos del usuario (o de los que suple como
        jefe directo) que aún no están firmados."""
        own_jobs = self.env.user.employee_ids.mapped('job_id')
        my_jobs = self._sgi_dev_my_sign_jobs()
        for sheet in self:
            mine = sheet.sign_ids.filtered(lambda s: not s.user_id and s.job_id in my_jobs)
            if not mine:
                raise UserError("Su puesto no firma esta ficha (ni es jefe directo de quien la firma) o ya la firmó.")
            mine.write({'user_id': self.env.uid, 'date': fields.Datetime.now()})
            labels = ["%s%s" % (sign.job_id.name, "" if sign.job_id in own_jobs else " · por suplencia")
                      for sign in mine]
            sheet.message_post(body="Firmó %s (%s)." % (self.env.user.name, ", ".join(labels)))
        return True

    def action_approve(self):
        """Aprobación de C1.15 (Administrador de Ventas por la regla nativa de Studio): la ficha queda
        vigente con su PDF. Las firmas que falten se imprimen en blanco; quién firmó y quién aprobó
        queda en el registro."""
        for sheet in self:
            sheet.write({'approved_by_id': self.env.uid, 'approved_at': fields.Datetime.now()})
            sheet._doc_emit("Ficha técnica interna")
        return True


class SgiDevTechSheetLine(models.Model):
    _name = 'sgi.dev.tech.sheet.line'
    _description = "Característica de la ficha técnica interna"
    _inherit = 'ficha.tecnica.caracteristica.mixin'
    _order = 'sequence, id'

    sheet_id = fields.Many2one('sgi.dev.tech.sheet', string="Ficha", required=True, ondelete='cascade', index=True)
    name_en = fields.Char(related='caracteristica_id.name_en', string="Characteristic")


class SgiDevTechSheetSign(models.Model):
    _name = 'sgi.dev.tech.sheet.sign'
    _description = "Firma de la ficha técnica interna"
    _order = 'sheet_id, sequence, id'

    sheet_id = fields.Many2one('sgi.dev.tech.sheet', string="Ficha", required=True, ondelete='cascade', index=True)
    sequence = fields.Integer(default=10)
    job_id = fields.Many2one('hr.job', string="Puesto", required=True)
    user_id = fields.Many2one('res.users', string="Firmó", readonly=True)
    date = fields.Datetime(string="Fecha de firma", readonly=True)


# ---------------------------------------------------------------------------
# Especificaciones del producto (documento al cliente, bilingüe)
# ---------------------------------------------------------------------------
class SgiDevCustomerSpec(models.Model):
    _name = 'sgi.dev.customer.spec'
    _description = "Especificaciones del producto para el cliente"
    _inherit = ['sgi.dev.doc.mixin', 'mail.thread']
    _order = 'id desc'

    name = fields.Char(string="Especificaciones", compute='_compute_name', store=True)
    line_ids = fields.One2many('sgi.dev.customer.spec.line', 'spec_id', string="Características")
    main_use = fields.Text(related='project_id.sgi_dev_use', string="Uso principal / Main use", readonly=True,
                           help="El de la solicitud del proyecto; se captura una sola vez.")
    care_ids = fields.Many2many('sgi.dev.option', 'sgi_dev_customer_spec_care_rel', 'spec_id', 'option_id',
                                string="Instrucciones de cuidado", domain=[('kind', '=', 'cuidado')],
                                help="De la lista «Instrucción de cuidado» (nombre en español e inglés).")
    emitted_by_id = fields.Many2one('res.users', string="Emitió (Ventas)", readonly=True, copy=False)
    emitted_at = fields.Datetime(string="Emitida el", readonly=True, copy=False)

    @api.depends('project_id.name', 'product_id.default_code', 'revision')
    def _compute_name(self):
        for spec in self:
            spec.name = "Especificaciones del producto %s · rev. %s" % (
                spec.product_id.default_code or spec.project_id.sgi_ft_folio or spec.project_id.name or '',
                spec.revision)

    def _doc_source_lines(self):
        self.ensure_one()
        return self.project_id.sgi_dev_line_ids.filtered('in_customer_spec')

    def _doc_line_vals(self, line):
        # Al cliente va su tolerancia; el control interno no se copia (brief §5.1).
        return dict(line._limit_vals(), ctrl_tol_minus=0.0, ctrl_tol_plus=0.0)

    def _doc_report_xmlid(self):
        return 'quimibond_sgi.action_report_dev_customer_spec'

    def action_emit(self):
        for spec in self:
            spec.write({'emitted_by_id': self.env.uid, 'emitted_at': fields.Datetime.now()})
            spec._doc_emit("Especificaciones del producto")
        return True


class SgiDevCustomerSpecLine(models.Model):
    _name = 'sgi.dev.customer.spec.line'
    _description = "Característica de las especificaciones del producto"
    _inherit = 'ficha.tecnica.caracteristica.mixin'
    _order = 'sequence, id'

    spec_id = fields.Many2one('sgi.dev.customer.spec', string="Especificaciones", required=True, ondelete='cascade',
                              index=True)
    name_en = fields.Char(related='caracteristica_id.name_en', string="Characteristic")


# ---------------------------------------------------------------------------
# Proyecto
# ---------------------------------------------------------------------------
class ProjectProjectDevDocs(models.Model):
    _inherit = 'project.project'

    sgi_dev_tech_sheet_ids = fields.One2many('sgi.dev.tech.sheet', 'project_id', string="Fichas técnicas internas")
    sgi_dev_tech_sheet_count = fields.Integer(compute='_compute_sgi_dev_doc_counts')
    sgi_dev_customer_spec_ids = fields.One2many('sgi.dev.customer.spec', 'project_id',
                                                string="Especificaciones del producto")
    sgi_dev_customer_spec_count = fields.Integer(compute='_compute_sgi_dev_doc_counts')

    @api.depends('sgi_dev_tech_sheet_ids', 'sgi_dev_customer_spec_ids')
    def _compute_sgi_dev_doc_counts(self):
        for project in self:
            project.sgi_dev_tech_sheet_count = len(project.sgi_dev_tech_sheet_ids)
            project.sgi_dev_customer_spec_count = len(project.sgi_dev_customer_spec_ids)

    def _sgi_dev_doc_action(self, model, xmlid, records):
        self.ensure_one()
        if not self.sgi_dev_line_ids:
            raise UserError("El proyecto no tiene tabla de características.")
        if len(records) == 1:
            return {'type': 'ir.actions.act_window', 'res_model': model, 'res_id': records.id,
                    'view_mode': 'form', 'target': 'current'}
        action = self.env['ir.actions.act_window']._for_xml_id(xmlid)
        action['domain'] = [('project_id', '=', self.id)]
        action['context'] = {'default_project_id': self.id}
        return action

    def action_sgi_dev_tech_sheets(self):
        return self._sgi_dev_doc_action('sgi.dev.tech.sheet', 'quimibond_sgi.sgi_dev_tech_sheet_action',
                                        self.sgi_dev_tech_sheet_ids)

    def action_sgi_dev_new_tech_sheet(self):
        self.ensure_one()
        if not self.sgi_dev_line_ids:
            raise UserError("El proyecto no tiene tabla de características.")
        sheet = self.env['sgi.dev.tech.sheet'].create({'project_id': self.id})
        return {'type': 'ir.actions.act_window', 'res_model': sheet._name, 'res_id': sheet.id,
                'view_mode': 'form', 'target': 'current'}

    def action_sgi_dev_customer_specs(self):
        return self._sgi_dev_doc_action('sgi.dev.customer.spec', 'quimibond_sgi.sgi_dev_customer_spec_action',
                                        self.sgi_dev_customer_spec_ids)

    def action_sgi_dev_new_customer_spec(self):
        self.ensure_one()
        if not self.sgi_dev_line_ids.filtered('in_customer_spec'):
            raise UserError("Ningún renglón de la tabla está marcado «En especificación del cliente».")
        spec = self.env['sgi.dev.customer.spec'].create({'project_id': self.id})
        return {'type': 'ir.actions.act_window', 'res_model': spec._name, 'res_id': spec.id,
                'view_mode': 'form', 'target': 'current'}
