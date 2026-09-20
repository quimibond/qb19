# -*- coding: utf-8 -*-
"""Contexto de la organización: partes interesadas (ISO 4.1 / 4.2).

El FODA ya vive como instrumento de sgi.risk (4.1); faltaba el registro de
partes interesadas con sus necesidades y expectativas (4.2) y la decisión de
cuáles se vuelven requisito del SGI — de las primeras preguntas de cualquier
auditoría de certificación. La revisión periódica la vigila un cron con el
patrón estándar del addon.
"""
from dateutil.relativedelta import relativedelta

from odoo import models, fields, api


class SgiInterestedParty(models.Model):
    _name = 'sgi.interested.party'
    _description = "Parte interesada (ISO 4.2)"
    # mail.activity.mixin es indispensable: el cron de revisión del contexto
    # agenda actividades SOBRE la parte (misma lección del eslabón atorado).
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'party_type, category, name'

    name = fields.Char(string="Parte interesada", required=True, tracking=True)
    party_type = fields.Selection([
        ('interna', "Interna"),
        ('externa', "Externa"),
    ], string="Tipo", default='externa', required=True)
    category = fields.Selection([
        ('cliente', "Cliente"),
        ('trabajador', "Trabajadores"),
        ('autoridad', "Autoridad / regulador"),
        ('proveedor', "Proveedor / contratista"),
        ('corporativo', "Accionistas / corporativo"),
        ('comunidad', "Comunidad / vecinos"),
        ('otro', "Otra"),
    ], string="Categoría", default='cliente', required=True, tracking=True)
    needs = fields.Text(
        string="Necesidades y expectativas", required=True,
        help="Qué espera esta parte del SGI (calidad, cumplimiento legal, "
             "condiciones seguras, continuidad de suministro…).")
    becomes_requirement = fields.Boolean(
        string="Se adopta como requisito del SGI", tracking=True,
        help="La organización DECIDE cuáles necesidades se vuelven requisito "
             "(4.2): márcalo y documenta cómo se atiende.")
    requirement_note = fields.Text(
        string="Cómo se atiende",
        help="Con qué proceso, documento, requisito legal o control se "
             "responde a la expectativa adoptada.")
    process_ids = fields.Many2many('sgi.process', string="Procesos relacionados")
    risk_ids = fields.Many2many(
        'sgi.risk', string="Riesgos/oportunidades ligados",
        help="Riesgos u oportunidades (incluido FODA) que nacen de esta parte.")
    legal_ids = fields.Many2many(
        'sgi.legal.requirement', string="Requisitos legales ligados")
    review_frequency_months = fields.Integer(
        string="Frecuencia de revisión (meses)", default=12)
    last_review_date = fields.Date(string="Última revisión", tracking=True)
    next_review_date = fields.Date(
        string="Próxima revisión", compute='_compute_next_review_date',
        store=True, readonly=False)
    active = fields.Boolean(default=True)

    @api.depends('last_review_date', 'review_frequency_months')
    def _compute_next_review_date(self):
        for party in self:
            if party.last_review_date and party.review_frequency_months:
                party.next_review_date = party.last_review_date + relativedelta(
                    months=party.review_frequency_months)
            elif not party.last_review_date:
                party.next_review_date = (party.next_review_date
                                          or fields.Date.context_today(party))

    def action_mark_reviewed(self):
        """Sella la revisión periódica del contexto (4.1/4.2)."""
        today = fields.Date.context_today(self)
        for party in self:
            party.last_review_date = today
            party.message_post(
                body="Revisión del contexto registrada: las necesidades y "
                     "expectativas siguen vigentes (o se actualizaron en esta "
                     "misma edición).")
        return True
