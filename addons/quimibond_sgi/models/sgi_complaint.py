# -*- coding: utf-8 -*-
from odoo import api, fields, models
from odoo.exceptions import UserError


class HelpdeskTeam(models.Model):
    _inherit = 'helpdesk.team'

    # D-006 (decisión 9): las reclamaciones reales viven en equipos que no
    # creó el SGI («Reclamaciones entretelas», «ATENCION A CLIENTES»). La
    # marca sustituye al XML ID como fuente de «qué equipo es reclamación»:
    # menú, KPI CA-01, SLA vencido, revisión por la dirección y «Generar NC».
    sgi_is_complaint = fields.Boolean(
        string="Equipo de reclamaciones (SGI)",
        help="Los tickets de este equipo son reclamaciones de clientes: salen "
             "en el menú Reclamaciones del SGI, cuentan para el indicador de "
             "reclamaciones y muestran los datos de la reclamación y el "
             "botón «Generar NC».")

    # Decisión 9 (tanda 2): los equipos de reclamación de producción.
    _SGI_COMPLAINT_TEAM_NAMES = ('Reclamaciones entretelas', 'ATENCION A CLIENTES')

    @api.model
    def _sgi_mark_complaint_teams(self):
        """Marca como reclamación el equipo del SGI y los equipos de la
        decisión 9 de la empresa del SGI (por nombre exacto, archivados
        incluidos). Idempotente: solo escribe los que no estaban marcados.
        Devuelve los equipos marcados en esta llamada."""
        teams = self.with_context(active_test=False).search([
            ('name', 'in', list(self._SGI_COMPLAINT_TEAM_NAMES)),
            ('company_id', '=', self.env['sgi.config']._sgi_company().id),
        ])
        own = self.env.ref('quimibond_sgi.sgi_helpdesk_team_complaints',
                           raise_if_not_found=False)
        if own:
            teams |= own
        todo = teams.filtered(lambda t: not t.sgi_is_complaint)
        todo.write({'sgi_is_complaint': True})
        return todo

    @api.model
    def _sgi_complaint_domain(self):
        """Dominio de ``helpdesk.ticket`` para las reclamaciones del SGI."""
        return [('team_id.sgi_is_complaint', '=', True)]


class HelpdeskTicket(models.Model):
    _inherit = 'helpdesk.ticket'

    sgi_is_complaint_team = fields.Boolean(
        related='team_id.sgi_is_complaint', string="Es reclamación (SGI)")

    sgi_sale_order_id = fields.Many2one('sale.order', string="Pedido de venta")
    sgi_product_id = fields.Many2one('product.product', string="Producto")
    sgi_lot_id = fields.Many2one('stock.lot', string="Lote")
    sgi_qty_affected = fields.Float(string="Metros afectados")
    sgi_disposition = fields.Selection([
        ('devolucion', "Devolución"),
        ('reposicion', "Reposición"),
        ('nota_credito', "Nota de crédito"),
        ('concesion', "Concesión"),
        ('na', "N/A"),
    ], string="Disposición")
    sgi_alert_id = fields.Many2one('quality.alert', string="No Conformidad", readonly=True)

    def action_sgi_generate_nc(self):
        self.ensure_one()
        # Idempotente: la vista oculta el botón con NC ligada, pero un
        # formulario desactualizado o una llamada RPC repetida no debe
        # duplicar la NC (folio consumido + concentrado contaminado).
        if self.sgi_alert_id:
            return {
                'type': 'ir.actions.act_window',
                'name': "No Conformidad",
                'res_model': 'quality.alert',
                'res_id': self.sgi_alert_id.id,
                'view_mode': 'form',
            }
        if not self.team_id.sgi_is_complaint:
            raise UserError(
                "«Generar NC» solo aplica a los tickets de un equipo de "
                "reclamaciones del SGI. Mueva el ticket a ese equipo o levante "
                "la No Conformidad desde Calidad.")
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal', raise_if_not_found=False)
        vals = {
            'title': "Reclamación: %s" % (self.name or ''),
            'sgi_origin_type': 'reclamacion',
            'partner_id': self.partner_id.id,
            'product_id': self.sgi_product_id.id,
            'product_tmpl_id': self.sgi_product_id.product_tmpl_id.id if self.sgi_product_id else False,
            'lot_ids': [(6, 0, self.sgi_lot_id.ids)] if self.sgi_lot_id else False,
            'sgi_deviation': "Reclamación de cliente %s.\n%s" % (
                self.partner_id.display_name or '', self.description or ''),
            'sgi_complaint_ticket_id': self.id,
        }
        if team:
            vals['team_id'] = team.id
        alert = self.env['quality.alert'].sgi_auto_create('reclamacion_cliente', vals)
        self.sgi_alert_id = alert.id
        return {
            'type': 'ir.actions.act_window',
            'name': "No Conformidad",
            'res_model': 'quality.alert',
            'res_id': alert.id,
            'view_mode': 'form',
        }
