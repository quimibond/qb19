# -*- coding: utf-8 -*-
"""Asistente de conciliación SAT ↔ Odoo. Se abre desde un CFDI (busca
facturas) o desde una factura (busca CFDI). Lista las candidatas que cuadran
en contraparte y monto, con la diferencia de monto y de fecha, y la persona
elige con un clic. La regla de negocio vive en sat.cfdi._reconcile_with."""
from odoo import Command, _, api, fields, models
from odoo.exceptions import UserError


class SatReconcileWizard(models.TransientModel):
    _name = 'sat.reconcile.wizard'
    _description = 'Conciliar CFDI del SAT con factura de Odoo'

    cfdi_id = fields.Many2one('sat.cfdi', string='CFDI del SAT', readonly=True)
    move_id = fields.Many2one('account.move', string='Factura de Odoo', readonly=True)
    days = fields.Integer(string='Fecha a ± días', default=180,
                          help='0 = sin límite de fecha.')
    tolerance_pct = fields.Float(string='Tolerancia de monto (%)', default=0.5, digits=(5, 2),
                                 help='Diferencia de total admitida para proponer una candidata (mínimo $1).')
    attach_xml = fields.Boolean(
        string='Adjuntar el XML del SAT a la factura', default=True,
        help='Odoo lee el XML adjunto y registra el folio fiscal (localización mexicana). '
             'Si Syntage no entrega el XML, la conciliación se hace de todos modos.')
    # Líneas reales (no calculadas): el asistente se crea al abrirse, para
    # que los botones por fila trabajen sobre registros que existen.
    line_ids = fields.One2many('sat.reconcile.wizard.line', 'wizard_id', string='Candidatas')
    summary = fields.Char(compute='_compute_summary')

    @api.model
    def open_for(self, record):
        """Crea el asistente para un sat.cfdi o un account.move y devuelve la
        acción que lo abre ya con sus candidatas."""
        vals = {'cfdi_id': record.id} if record._name == 'sat.cfdi' else {'move_id': record.id}
        wiz = self.create(vals)
        return {
            'type': 'ir.actions.act_window', 'name': _('Conciliar SAT ↔ Odoo'),
            'res_model': self._name, 'res_id': wiz.id, 'view_mode': 'form', 'target': 'new',
        }

    @api.model_create_multi
    def create(self, vals_list):
        wizards = super().create(vals_list)
        wizards._fill_lines()
        return wizards

    def action_refresh(self):
        self._fill_lines()
        return {'type': 'ir.actions.act_window', 'res_model': self._name, 'res_id': self.id,
                'view_mode': 'form', 'target': 'new', 'name': _('Conciliar SAT ↔ Odoo')}

    @api.model
    def default_get(self, fields_list):
        vals = super().default_get(fields_list)
        model, rid = self.env.context.get('active_model'), self.env.context.get('active_id')
        if model == 'sat.cfdi' and rid:
            vals.setdefault('cfdi_id', rid)
        elif model == 'account.move' and rid:
            vals.setdefault('move_id', rid)
        return vals

    @api.depends('cfdi_id', 'move_id')
    def _compute_summary(self):
        for wiz in self:
            if wiz.cfdi_id:
                c = wiz.cfdi_id
                wiz.summary = _('%(name)s · %(partner)s · %(total)s %(cur)s · %(date)s') % {
                    'name': c.name, 'partner': c.counterparty_name or c.counterparty_rfc or '—',
                    'total': '{:,.2f}'.format(c.total or 0.0), 'cur': c.moneda or 'MXN',
                    'date': fields.Date.to_date(c.fecha_emision) if c.fecha_emision else '—'}
            elif wiz.move_id:
                m = wiz.move_id
                wiz.summary = _('%(name)s · %(partner)s · %(total)s %(cur)s · %(date)s') % {
                    'name': m.name or _('Borrador'), 'partner': m.commercial_partner_id.name or '—',
                    'total': '{:,.2f}'.format(m.amount_total or 0.0), 'cur': m.currency_id.name,
                    'date': m.invoice_date or '—'}
            else:
                wiz.summary = False

    def _fill_lines(self):
        for wiz in self:
            tol = (wiz.tolerance_pct or 0.0) / 100.0
            cmds = [Command.clear()]
            if wiz.cfdi_id:
                cfdi = wiz.cfdi_id
                date = fields.Date.to_date(cfdi.fecha_emision) if cfdi.fecha_emision else None
                for seq, move in enumerate(cfdi._reconcile_candidates(days=wiz.days, tol_pct=tol)):
                    linked = cfdi.sudo().search([('move_id', '=', move.id), ('id', '!=', cfdi.id)], limit=1)
                    cmds.append(Command.create({
                        'sequence': seq, 'move_id': move.id, 'name': move.name or _('Borrador'),
                        'partner_name': move.commercial_partner_id.name, 'date': move.invoice_date,
                        'amount': move.amount_total, 'currency_id': move.currency_id.id,
                        'state': dict(move._fields['state']._description_selection(self.env)).get(move.state, move.state),
                        'delta_amount': move.amount_total - abs(cfdi.total or 0.0),
                        'delta_days': (move.invoice_date - date).days if (date and move.invoice_date) else 0,
                        'exact': abs(move.amount_total - abs(cfdi.total or 0.0)) <= 0.01
                        and move.currency_id.name == (cfdi.moneda or 'MXN'),
                        'linked_to': linked and _('Ya ligada al CFDI %s') % linked.uuid.upper() or False,
                    }))
            elif wiz.move_id:
                move = wiz.move_id
                for seq, cfdi in enumerate(move._sat_reconcile_candidates(days=wiz.days, tol_pct=tol)):
                    d = fields.Date.to_date(cfdi.fecha_emision) if cfdi.fecha_emision else None
                    cmds.append(Command.create({
                        'sequence': seq, 'cfdi_id': cfdi.id, 'name': cfdi.name,
                        'partner_name': cfdi.counterparty_name, 'date': d,
                        'amount': cfdi.total, 'currency_id': self.env['res.currency'].with_context(
                            active_test=False).search([('name', '=', cfdi.moneda or 'MXN')], limit=1).id,
                        'state': dict(cfdi._fields['estado_sat']._description_selection(self.env)).get(
                            cfdi.estado_sat, cfdi.estado_sat),
                        'delta_amount': (cfdi.total or 0.0) - abs(move.amount_total),
                        'delta_days': (d - move.invoice_date).days if (d and move.invoice_date) else 0,
                        'exact': abs((cfdi.total or 0.0) - abs(move.amount_total)) <= 0.01
                        and move.currency_id.name == (cfdi.moneda or 'MXN'),
                        'linked_to': cfdi.match_status == 'ignorado' and _('Ignorado: %s') % (cfdi.ignore_reason or '') or False,
                    }))
            wiz.line_ids = cmds

    def _reconcile(self, cfdi, move):
        self.ensure_one()
        if not cfdi or not move:
            raise UserError(_('Falta el CFDI o la factura para conciliar.'))
        cfdi._reconcile_with(move, attach_xml=self.attach_xml)
        return {'type': 'ir.actions.act_window_close'}


class SatReconcileWizardLine(models.TransientModel):
    _name = 'sat.reconcile.wizard.line'
    _description = 'Candidata de conciliación SAT ↔ Odoo'
    _order = 'sequence, id'

    wizard_id = fields.Many2one('sat.reconcile.wizard', required=True, ondelete='cascade')
    sequence = fields.Integer()
    move_id = fields.Many2one('account.move', string='Factura')
    cfdi_id = fields.Many2one('sat.cfdi', string='CFDI')
    name = fields.Char(string='Documento')
    partner_name = fields.Char(string='Contraparte')
    date = fields.Date(string='Fecha')
    amount = fields.Monetary(string='Total', currency_field='currency_id')
    currency_id = fields.Many2one('res.currency', string='Moneda')
    state = fields.Char(string='Estado')
    delta_amount = fields.Float(string='Δ monto', digits=(16, 2), help='Candidata − documento origen.')
    delta_days = fields.Integer(string='Δ días', help='Candidata − documento origen.')
    exact = fields.Boolean(string='Cuadra', help='Mismo total al centavo y misma moneda.')
    linked_to = fields.Char(string='Aviso')

    def action_pick(self):
        self.ensure_one()
        wiz = self.wizard_id
        return wiz._reconcile(wiz.cfdi_id or self.cfdi_id, wiz.move_id or self.move_id)
