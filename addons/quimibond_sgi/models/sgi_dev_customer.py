# -*- coding: utf-8 -*-
"""Aprobación del cliente para iniciar el desarrollo (57.128.0; brief paso 7 y §6.1; Jose 2026-10-08, 3.1).

Al presentar la cotización el cliente aprueba el inicio por correo, orden de
compra, WhatsApp o cotización firmada (si el desarrollo es interno, quien lo
pidió en Dirección). Se registra en la etapa «Aprobación del cliente» con
medio (lista), fecha, quién aprobó y la evidencia adjunta; con eso el
proyecto pasa a «Muestra» y recibe su folio FT. Sin ese registro el proyecto
no entra a «Muestra» (los que ya estaban ahí no se tocan).
"""
from odoo import fields, models
from odoo.exceptions import UserError

CUSTOMER_APPROVAL_MEDIA = [
    ('correo', "Correo"), ('oc', "Orden de compra"), ('whatsapp', "WhatsApp"),
    ('cotizacion_firmada', "Cotización firmada"), ('direccion', "Dirección (desarrollo interno)"),
]
APPROVAL_STAGE = 'muestra'


class ProjectProjectDevCustomerApproval(models.Model):
    _inherit = 'project.project'

    sgi_dev_customer_approval_medium = fields.Selection(CUSTOMER_APPROVAL_MEDIA, string="Medio de aprobación del cliente",
                                                        tracking=True, copy=False)
    sgi_dev_customer_approval_date = fields.Date(string="Fecha de aprobación del cliente", copy=False)
    sgi_dev_customer_approval_contact_id = fields.Many2one('res.partner', string="Quién aprobó (contacto)", copy=False,
                                                           help="Persona del cliente que aprobó; en un desarrollo "
                                                                "interno, quien lo pidió en Dirección.")
    sgi_dev_customer_approval_ref = fields.Char(string="Referencia", copy=False,
                                                help="Número de orden de compra o asunto del correo.")
    sgi_dev_customer_approval_file = fields.Binary(string="Evidencia", attachment=True, copy=False)
    sgi_dev_customer_approval_filename = fields.Char(copy=False)
    sgi_dev_customer_approved_at = fields.Datetime(string="Aprobación del cliente registrada el", readonly=True, copy=False)
    sgi_dev_customer_approved_by_id = fields.Many2one('res.users', string="Registró", readonly=True, copy=False)

    def _sgi_dev_check_customer_approval(self):
        for project in self:
            faltan = []
            if not project.sgi_dev_customer_approval_medium:
                faltan.append('el medio')
            if not project.sgi_dev_customer_approval_date:
                faltan.append('la fecha')
            if not project.sgi_dev_customer_approval_file:
                faltan.append('la evidencia adjunta')
            if faltan:
                raise UserError("%s: para registrar la aprobación del cliente falta %s." % (
                    project.display_name, ", ".join(faltan)))
        return True

    def action_sgi_dev_register_customer_approval(self):
        """Registra la aprobación del cliente y, si el proyecto estaba en «Aprobación del cliente» (o
        antes), lo pasa a «Muestra», donde recibe su folio FT."""
        muestra = self._sgi_dev_stage(APPROVAL_STAGE)
        seq = self._sgi_dev_stage_order(APPROVAL_STAGE)
        for project in self.filtered('sgi_is_ft'):
            project._sgi_dev_check_customer_approval()
            project.write({'sgi_dev_customer_approved_at': fields.Datetime.now(),
                           'sgi_dev_customer_approved_by_id': self.env.uid})
            project.message_post(body="Aprobación del cliente registrada: %s, %s%s%s." % (
                dict(CUSTOMER_APPROVAL_MEDIA).get(project.sgi_dev_customer_approval_medium),
                project.sgi_dev_customer_approval_date,
                (", %s" % project.sgi_dev_customer_approval_contact_id.name) if project.sgi_dev_customer_approval_contact_id else "",
                (", ref. %s" % project.sgi_dev_customer_approval_ref) if project.sgi_dev_customer_approval_ref else ""))
            if muestra and project.sgi_dev_stage_seq < seq and project.sgi_dev_analysis_result not in ('linea', 'no_factible'):
                project.write({'stage_id': muestra.id})
        return True

    def write(self, vals):
        if 'stage_id' in vals and not self.env.context.get('sgi_dev_migration'):
            keys = self._sgi_dev_stage_keys()
            new_key, new_seq = keys.get(vals['stage_id'], ('', 0))
            seq = self._sgi_dev_stage_order(APPROVAL_STAGE)
            if new_seq >= seq and new_key != 'cerrado_sin_producto':
                pendientes = self.filtered(
                    lambda p: p.sgi_is_ft and not p.is_template and p.sgi_dev_stage_seq < seq
                    and p.sgi_dev_analysis_result not in ('linea', 'no_factible') and not p.sgi_dev_customer_approved_at)
                if pendientes:
                    raise UserError("%s: registre la aprobación del cliente (medio, fecha y evidencia) en la pestaña "
                                    "Desarrollo antes de pasar a «Muestra»." % pendientes[0].display_name)
        return super().write(vals)
