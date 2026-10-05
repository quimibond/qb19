# -*- coding: utf-8 -*-
"""57.109.0: reportar un riesgo u oportunidad en lenguaje normal.

En producción los 25 riesgos los capturó Jose: nadie más usa la matriz de
probabilidad × impacto. «SGI → Reportar → Riesgo u oportunidad» pregunta qué
puede pasar, qué pasaría, qué tan seguido y qué tan grave (o qué tanto se
ganaría) en palabras, y qué se hace hoy. Las respuestas proponen la
probabilidad y el impacto (1 a 5) de la matriz de riesgos y oportunidades;
el riesgo nace «Identificado» y el Jefe MAST recibe «Evaluar riesgo
reportado» para confirmarlo.
"""
from markupsafe import Markup

from odoo import api, fields, models

HOW_OFTEN = [
    ('1', "Es muy difícil que pase"),
    ('2', "Nunca ha pasado, pero podría"),
    ('3', "Ha pasado alguna vez en el año"),
    ('4', "Pasa casi cada mes"),
    ('5', "Pasa seguido (cada semana)"),
]
HOW_BAD = [
    ('1', "Una molestia que se arregla en el día"),
    ('2', "Retrasa entregas o genera retrabajo"),
    ('3', "Queja de un cliente o pérdida de dinero importante"),
    ('4', "Podemos perder un cliente, un proveedor clave o una certificación"),
    ('5', "Multa, accidente grave, daño ambiental o paro de la planta"),
]
HOW_GOOD = [
    ('1', "Un poco más de orden o de ahorro"),
    ('2', "Ahorro o mejora que se nota en el área"),
    ('3', "Ahorro o venta importante"),
    ('4', "Un cliente o un mercado nuevo"),
    ('5', "Cambia los resultados de la empresa"),
]


class SgiRiskReport(models.TransientModel):
    """Reportar un riesgo u oportunidad con cinco preguntas."""
    _name = 'sgi.risk.report'
    _description = "Reportar un riesgo u oportunidad"

    kind = fields.Selection([
        ('riesgo', "Algo malo que puede pasar (riesgo)"),
        ('oportunidad', "Algo bueno que podemos aprovechar (oportunidad)"),
    ], string="¿Qué quiere reportar?", required=True, default='riesgo')
    name = fields.Char(string="¿Qué puede pasar?", required=True)
    consequence = fields.Text(string="¿Qué pasaría si pasa?")
    process_id = fields.Many2one('sgi.process', string="¿En qué proceso?",
                                 domain=[('active', '=', True)])
    how_often = fields.Selection(HOW_OFTEN, string="¿Qué tan seguido?", required=True, default='2')
    how_bad = fields.Selection(HOW_BAD, string="¿Qué tan grave sería?", default='2')
    how_good = fields.Selection(HOW_GOOD, string="¿Qué tanto ganaríamos?", default='2')
    existing_controls = fields.Text(string="¿Qué se hace hoy?")
    preview_html = fields.Html(string="Cómo queda", compute='_compute_preview_html', sanitize=False)

    def _sgi_impact(self):
        self.ensure_one()
        return self.how_good if self.kind == 'oportunidad' else self.how_bad

    @api.depends('kind', 'how_often', 'how_bad', 'how_good')
    def _compute_preview_html(self):
        Risk = self.env['sgi.risk']
        levels = dict(Risk._fields['attention_level']._description_selection(self.env))
        for rep in self:
            impact = rep._sgi_impact()
            if not (rep.how_often and impact):
                rep.preview_html = False
                continue
            score = int(rep.how_often) * int(impact)
            level = Risk._sgi_level('ryo', score)
            rep.preview_html = Markup(
                "Probabilidad <b>%s</b> × %s <b>%s</b> = <b>%d</b>: atención <b>%s</b>. El Jefe MAST lo "
                "revisa y lo confirma.") % (
                rep.how_often, "beneficio" if rep.kind == 'oportunidad' else "impacto", impact, score,
                (levels.get(level) or '—').lower())

    def action_report(self):
        self.ensure_one()
        risk = self.env['sgi.risk'].create({
            'name': self.name, 'kind': self.kind, 'instrument': 'ryo',
            'consequence': self.consequence or False, 'process_id': self.process_id.id or False,
            'existing_controls': self.existing_controls or False,
            'eval_probability': self.how_often, 'eval_impact': self._sgi_impact(),
        })
        often = dict(HOW_OFTEN)[self.how_often]
        size = dict(HOW_GOOD if self.kind == 'oportunidad' else HOW_BAD)[self._sgi_impact()]
        risk.message_post(body=Markup(
            "Reportado con «Reportar un riesgo u oportunidad». Qué tan seguido: «%s». %s: «%s». "
            "Probabilidad e impacto propuestos; los confirma el Jefe MAST.") % (
            often, "Qué tanto ganaríamos" if self.kind == 'oportunidad' else "Qué tan grave", size))
        Cron = self.env['sgi.cron']
        manager_id = Cron._sgi_manager_user_id()
        if manager_id:
            Cron.sudo()._sgi_schedule(
                risk.sudo(), "Evaluar riesgo reportado",
                "Lo reportó %s con preguntas sencillas: confirme probabilidad, impacto, proceso y "
                "categoría." % self.env.user.name, manager_id)
        return {
            'type': 'ir.actions.client', 'tag': 'display_notification',
            'params': {
                'type': 'success',
                'message': "Gracias: el %s quedó registrado (%s) y el Jefe MAST lo va a revisar." % (
                    "riesgo" if self.kind == 'riesgo' else "la oportunidad", risk.display_name),
                'next': {'type': 'ir.actions.act_window_close'},
            },
        }
