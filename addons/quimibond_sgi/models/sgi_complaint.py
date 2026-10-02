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
    def _sgi_complaint_teams_by_name(self):
        """Equipos de la empresa del SGI (archivados incluidos) cuyo nombre es
        uno de ``_SGI_COMPLAINT_TEAM_NAMES`` en CUALQUIER idioma instalado, sin
        distinguir mayúsculas ni espacios de los extremos.

        57.85.0: ``helpdesk.team.name`` es traducible. Un ``search`` sin
        ``lang`` en el contexto (el de las migraciones) compara solo el valor
        ``en_US`` del JSONB; los equipos 2 y 14 de producción se crearon antes
        de Odoo 16 o se renombraron con el usuario en español, así que su
        nombre en español vive en la llave ``es_MX`` y la de ``en_US`` guarda
        otro texto. Por eso la 57.19.0 solo marcó el equipo 30 (el del XML ID).
        Ahora se compara el nombre en cada idioma instalado."""
        wanted = {n.strip().casefold() for n in self._SGI_COMPLAINT_TEAM_NAMES}
        company = self.env['sgi.config']._sgi_company()
        teams = self.with_context(active_test=False).search(
            [('company_id', '=', company.id)])
        langs = {code for code, _name in self.env['res.lang'].get_installed()}
        langs.add('en_US')
        found = self.browse()
        for lang in sorted(langs):
            for team in teams.with_context(lang=lang):
                if (team.name or '').strip().casefold() in wanted:
                    found |= team
        return found.with_context(lang=self.env.lang)

    @api.model
    def _sgi_mark_complaint_teams(self):
        """Marca como reclamación el equipo del SGI y los equipos de la
        decisión 9 de la empresa del SGI (por nombre en cualquier idioma
        instalado, archivados incluidos; ver ``_sgi_complaint_teams_by_name``).
        Idempotente: solo AGREGA la marca a los que no la tenían; nunca la
        quita. Devuelve los equipos marcados en esta llamada."""
        teams = self._sgi_complaint_teams_by_name()
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

    sgi_sale_order_id = fields.Many2one('sale.order', string="Pedido de venta",
                                        help="Pedido de venta del producto reclamado.")
    sgi_product_id = fields.Many2one('product.product', string="Producto",
                                     help="Producto que el cliente reclama.")
    sgi_lot_id = fields.Many2one('stock.lot', string="Lote",
                                 help="Lote del producto reclamado, para rastrear la producción de origen.")
    sgi_qty_affected = fields.Float(string="Metros afectados",
                                    help="Metros de producto afectados según la reclamación.")
    sgi_disposition = fields.Selection([
        ('devolucion', "Devolución"),
        ('reposicion', "Reposición"),
        ('nota_credito', "Nota de crédito"),
        ('concesion', "Concesión"),
        ('na', "N/A"),
    ], string="Disposición",
        help="Qué se hizo con el producto reclamado: devolución, reposición, nota de crédito o concesión al "
             "cliente.")
    sgi_alert_id = fields.Many2one('quality.alert', string="No conformidad", readonly=True,
                                   help="No conformidad que se generó desde esta reclamación con el botón "
                                        "«Generar NC».")

    def action_sgi_generate_nc(self):
        self.ensure_one()
        # Idempotente: la vista oculta el botón con NC ligada, pero un
        # formulario desactualizado o una llamada RPC repetida no debe
        # duplicar la NC (folio consumido + concentrado contaminado).
        if self.sgi_alert_id:
            return {
                'type': 'ir.actions.act_window',
                'name': "No conformidad",
                'res_model': 'quality.alert',
                'res_id': self.sgi_alert_id.id,
                'view_mode': 'form',
            }
        if not self.team_id.sgi_is_complaint:
            raise UserError(
                "«Generar NC» solo aplica a los tickets de un equipo de "
                "reclamaciones del SGI. Mueva el ticket a ese equipo o levante "
                "la no conformidad desde Calidad.")
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
            'name': "No conformidad",
            'res_model': 'quality.alert',
            'res_id': alert.id,
            'view_mode': 'form',
        }
