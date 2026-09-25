# -*- coding: utf-8 -*-
"""DIR-4 (52.0.0): nivel del indicador y Tablero de dirección (I-9).

Dirección ve 10 a 12 indicadores y no 90: los oficiales de nivel
«dirección», con sus últimos 6 periodos, más lo que exige decisión hoy:
rojos sin plan, acuerdos de la Revisión por la Dirección vencidos y los
procesos con más actividades atrasadas. Todo con vistas nativas (mismo
patrón que Mi procedimiento: un transitorio con relaciones calculadas).
E1-02 se mide con el modo `acuerdos_rxd`: acuerdos cumplidos a tiempo.
"""
from odoo import api, fields, models

_SEM = {'verde': 'V', 'amarillo': 'A', 'rojo': 'R'}


class SgiIndicatorLevel(models.Model):
    _inherit = 'sgi.indicator'

    calc_mode = fields.Selection(
        selection_add=[('acuerdos_rxd', "Acuerdos de la RxD cumplidos a tiempo (E1-02)")],
        ondelete={'acuerdos_rxd': 'set default'})
    level = fields.Selection([
        ('direccion', "Dirección"),
        ('proceso', "Proceso"),
        ('actividad', "Actividad"),
    ], string="Nivel", default='proceso', required=True, tracking=True,
        help="Dirección: va al Tablero de dirección. Proceso: lo sigue el dueño del "
             "proceso. Actividad: medición de un paso del procedimiento.")
    last_six = fields.Char(string="Últimos 6 periodos", compute='_compute_last_six',
                           help="Periodo: valor y semáforo (V verde, A amarillo, R rojo).")

    def _compute_last_six(self):
        for indicator in self:
            measures = indicator.measure_ids.filtered(
                lambda m: m.state != 'pendiente').sorted('period_date', reverse=True)[:6]
            indicator.last_six = " · ".join(
                "%s: %s %s" % (m.period_date.strftime('%m/%y'), round(m.value, 2),
                               _SEM.get(m.semaphore, '-'))
                for m in reversed(measures)) or False

    def _calc_acuerdos_rxd(self, date_from, date_to):
        """E1-02: % de acuerdos de la Revisión por la Dirección con compromiso
        en el periodo que se terminaron a tiempo. Sin acuerdos → sin dato."""
        lines = self.env['sgi.action.line'].sudo().search([
            ('review_id', '!=', False),
            ('date_commit', '>=', date_from), ('date_commit', '<=', date_to)])
        if not lines:
            return None
        on_time = len(lines.filtered(lambda l: l.date_done and l.date_done <= l.date_commit))
        return round(on_time * 100.0 / len(lines), 2)


class SgiDirectionBoard(models.TransientModel):
    _name = 'sgi.direction.board'
    _description = "Tablero de dirección (I-9)"

    date = fields.Date(default=fields.Date.context_today, readonly=True)
    objective_ids = fields.Many2many('sgi.objective', string="Objetivos integrales", compute='_compute_board')
    indicator_ids = fields.Many2many('sgi.indicator', string="Indicadores de dirección", compute='_compute_board')
    red_no_plan_measure_ids = fields.Many2many(
        'sgi.indicator.measure', string="Rojos sin causa ni plan", compute='_compute_board')
    overdue_agreement_ids = fields.Many2many(
        'sgi.action.line', string="Acuerdos de la RxD vencidos", compute='_compute_board')
    delayed_process_ids = fields.Many2many(
        'sgi.process', string="Procesos con más atrasos", compute='_compute_board')
    indicator_count = fields.Integer(compute='_compute_board')
    red_count = fields.Integer(compute='_compute_board')

    @api.depends('date')
    def _compute_board(self):
        Indicator = self.env['sgi.indicator'].sudo()
        for board in self:
            board.objective_ids = self.env['sgi.objective'].sudo().search([]).ids
            indicators = Indicator.search([('status', '=', 'oficial'), ('level', '=', 'direccion')], order='code')
            if not indicators:
                indicators = Indicator.search([('status', '=', 'oficial')], order='code')
            board.indicator_ids = indicators.ids
            board.indicator_count = len(indicators)
            reds = self.env['sgi.indicator.measure'].sudo().search(
                [('semaphore', '=', 'rojo'), ('state', '!=', 'pendiente')], order='period_date desc')
            reds = reds.filtered(lambda m: m.plan_required and not m.plan_done)
            board.red_no_plan_measure_ids = reds.ids
            board.red_count = len(reds)
            board.overdue_agreement_ids = self.env['sgi.action.line'].sudo().search(
                [('review_id', '!=', False), ('date_done', '=', False),
                 ('date_commit', '<', fields.Date.context_today(board))], order='date_commit').ids
            processes = self.env['sgi.process'].sudo().search([('parent_id', '!=', False)])
            processes = processes.filtered(lambda p: p.activity_red_count > 0).sorted(
                key=lambda p: -p.activity_red_count)[:10]
            board.delayed_process_ids = processes.ids

    @api.model
    def action_open(self):
        """Menú Dirección → Tablero de dirección."""
        board = self.create({})
        return {
            'type': 'ir.actions.act_window', 'name': "Tablero de dirección",
            'res_model': 'sgi.direction.board', 'res_id': board.id,
            'view_mode': 'form', 'target': 'current',
        }

    def action_open_spreadsheet(self):
        return self.env.ref('quimibond_sgi.sgi_spreadsheet_dashboard_action').read()[0]
