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
_BOARD_LIMIT = 12


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

    @api.model
    def _sgi_activate_acuerdos_rxd(self, code='E1-02'):
        """D-13 (57.6.0): E1-02 deja de ser manual y se mide con
        ``acuerdos_rxd``. Respeta lo que MAST haya puesto: solo cambia el
        indicador ACTIVO con esa clave que siga en «manual» y sin términos de
        fórmula. El modo anterior queda en el chatter. Idempotente. Devuelve
        los ids cambiados."""
        indicators = self.search([('code', '=', code), ('calc_mode', '=', 'manual')])
        done = []
        for indicator in indicators.filtered(lambda i: not i.term_ids):
            indicator.write({'calc_mode': 'acuerdos_rxd'})
            indicator.message_post(body=(
                "57.6.0 (D-13): el indicador pasa de «Captura manual» a «Acuerdos de la "
                "RxD cumplidos a tiempo»: se mide solo con los acuerdos de la Revisión por "
                "la Dirección. Para regresar: modo «Captura manual»."))
            done.append(indicator.id)
        return done

    def _sgi_rxd_agreements(self, date_from, date_to):
        """Acuerdos de la Revisión por la Dirección ya realizada (o cerrada)
        con fecha límite dentro del periodo: el universo de E1-02."""
        return self.env['sgi.management.review.agreement'].sudo().search([
            ('review_id.state', 'in', ('realizada', 'cerrada')),
            ('deadline', '>=', date_from), ('deadline', '<=', date_to)])

    def _calc_acuerdos_rxd(self, date_from, date_to):
        """E1-02: % de acuerdos de la Revisión por la Dirección con fecha
        límite en el periodo que se cumplieron a tiempo (``done_date`` ≤
        ``deadline``). 57.6.0 (D-13): cuenta el acuerdo, no solo su acción;
        el cumplimiento capturado a mano en un acuerdo sin acción también
        cuenta. Sin acuerdos en el periodo → sin dato."""
        agreements = self._sgi_rxd_agreements(date_from, date_to)
        if not agreements:
            return None
        on_time = len(agreements.filtered(
            lambda a: a.done_date and a.done_date <= a.deadline))
        return round(on_time * 100.0 / len(agreements), 2)

    def _note_acuerdos_rxd(self, date_from, date_to):
        if not self._sgi_rxd_agreements(date_from, date_to):
            return ("Sin acuerdos de la Revisión por la Dirección con fecha límite "
                    "en el periodo.")
        return ''


class SgiDirectionBoard(models.TransientModel):
    _name = 'sgi.direction.board'
    _description = "Tablero de dirección (I-9)"

    date = fields.Date(string="Fecha", default=fields.Date.context_today, readonly=True)
    objective_ids = fields.Many2many('sgi.objective', string="Objetivos integrales", compute='_compute_board')
    indicator_ids = fields.Many2many('sgi.indicator', string="Indicadores de dirección", compute='_compute_board')
    red_no_plan_measure_ids = fields.Many2many(
        'sgi.indicator.measure', string="Rojos sin causa ni plan", compute='_compute_board')
    overdue_agreement_ids = fields.Many2many(
        'sgi.action.line', string="Acuerdos de la RxD vencidos", compute='_compute_board')
    delayed_process_ids = fields.Many2many(
        'sgi.process', string="Procesos con más atrasos", compute='_compute_board')
    indicator_count = fields.Integer(compute='_compute_board')
    indicator_note = fields.Char(string="Nota de indicadores", compute='_compute_board')
    red_count = fields.Integer(compute='_compute_board')

    def _sgi_board_indicators(self, Indicator):
        """Indicadores del tablero (máximo _BOARD_LIMIT) y nota para Dirección.

        1. Los oficiales de nivel dirección.
        2. Si no hay oficiales, los de nivel dirección en cualquier estado (en
           prueba incluidos), con una nota que lo dice: antes se caía a TODOS
           los oficiales y, sin oficiales, Dirección veía el tablero vacío.
        3. Si tampoco hay de nivel dirección, nada, con una nota clara."""
        indicators = Indicator.search(
            [('status', '=', 'oficial'), ('level', '=', 'direccion')],
            order='code', limit=_BOARD_LIMIT)
        if indicators:
            return indicators, False
        indicators = Indicator.search(
            [('level', '=', 'direccion')], order='code', limit=_BOARD_LIMIT)
        if indicators:
            return indicators, (
                "Aún no hay indicadores oficiales de nivel dirección: se muestran los "
                "de nivel dirección en cualquier estado (incluye los que están en prueba).")
        return indicators, (
            "No hay indicadores de nivel dirección. Marca el nivel «Dirección» en la "
            "ficha de los indicadores que deba ver Dirección (máximo %d)." % _BOARD_LIMIT)

    @api.depends('date')
    def _compute_board(self):
        Indicator = self.env['sgi.indicator'].sudo()
        for board in self:
            board.objective_ids = self.env['sgi.objective'].sudo().search([]).ids
            indicators, note = board._sgi_board_indicators(Indicator)
            board.indicator_ids = indicators.ids
            board.indicator_count = len(indicators)
            board.indicator_note = note
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
        """Menú Dirección → Tablero (el título es el nombre del menú, D-005)."""
        board = self.create({})
        return {
            'type': 'ir.actions.act_window', 'name': "Tablero",
            'res_model': 'sgi.direction.board', 'res_id': board.id,
            'view_mode': 'form', 'target': 'current',
        }

    def action_open_spreadsheet(self):
        return self.env.ref('quimibond_sgi.sgi_spreadsheet_dashboard_action').read()[0]
