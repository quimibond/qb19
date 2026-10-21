# -*- coding: utf-8 -*-
from odoo import models, fields, api
from odoo.exceptions import ValidationError


class SgiNorm(models.Model):
    """Norma de referencia (ISO 9001, 14001, 45001 y las que se capturen) con sus cláusulas."""
    _name = 'sgi.norm'
    _description = "Norma ISO"
    _order = 'code'

    code = fields.Char(string="Clave", required=True, index=True)
    name = fields.Char(string="Nombre", required=True)
    clause_ids = fields.One2many('sgi.norm.clause', 'norm_id', string="Cláusulas")
    # 57.161.0: lo muestra la lista de normas del MIID y del P-G01.
    clause_count = fields.Integer(string="Cláusulas en Odoo", compute='_compute_clause_count')
    # 57.162.0: el MIID (1.2) lo lee de aquí; antes era texto del manual.
    scope = fields.Text(string="Alcance certificado",
                        help="Alcance del certificado de esta norma, como lo imprime el Manual del SGI (1.2). "
                             "Vacío: la norma no sale en el alcance.")
    active = fields.Boolean(default=True)

    @api.depends('clause_ids')
    def _compute_clause_count(self):
        for norm in self:
            norm.clause_count = len(norm.clause_ids)

    _code_uniq = models.Constraint(
        'unique(code)',
        "La clave de norma debe ser única.",
    )


class SgiNormClause(models.Model):
    """Cláusula de una norma; «Cumple con» liga actividades y procesos a ella y se ve cuáles no
    tienen cobertura."""
    _name = 'sgi.norm.clause'
    _description = "Cláusula de norma ISO"
    _order = 'norm_id, code'

    norm_id = fields.Many2one('sgi.norm', string="Norma", required=True, ondelete='cascade',
                              help="Norma a la que pertenece la cláusula.")
    code = fields.Char(string="Numeral", required=True)
    name = fields.Char(string="Requisito", required=True)
    # 57.162.0: no aplicabilidades y reglas del sistema como datos de la
    # cláusula; el MIID (1.3 y 5.2) los lee de aquí.
    exclusion = fields.Boolean(string="No aplica",
                               help="El requisito (o un inciso suyo) no aplica a PNTQ. Sale en el Manual del SGI "
                                    "(1.3) con su justificación.")
    exclusion_detail = fields.Char(string="Inciso que no aplica",
                                   help="Inciso o numeral de detalle, p. ej. «8.4.1 b)». Vacío: toda la cláusula.")
    exclusion_reason = fields.Text(string="Justificación de la no aplicabilidad")
    system_rule_ids = fields.Many2many('sgi.system.rule', 'sgi_system_rule_clause_rel', 'clause_id', 'rule_id',
                                       string="Reglas del sistema",
                                       help="Lo que el sistema hace cumplir sin una actividad (candados y avisos).")

    @api.constrains('exclusion', 'exclusion_reason')
    def _check_exclusion_reason(self):
        for clause in self.filtered(lambda c: c.exclusion and not (c.exclusion_reason or '').strip()):
            raise ValidationError("Escriba por qué no aplica el requisito %s." % clause.display_name)

    @api.depends('code', 'name')
    def _compute_display_name(self):
        for clause in self:
            clause.display_name = "%s %s" % (clause.code, clause.name) if clause.code else clause.name


class SgiSystemRule(models.Model):
    """57.162.0: regla que el sistema hace cumplir con candados y avisos (no
    es una actividad). La liga a sus cláusulas y a la pantalla donde se ve;
    el Manual del SGI (5.2) imprime estas reglas."""
    _name = 'sgi.system.rule'
    _description = "Regla del sistema del SGI"
    _order = 'sequence, id'

    sequence = fields.Integer(string="Orden", default=10)
    name = fields.Char(string="Tema", required=True)
    clause_ids = fields.Many2many('sgi.norm.clause', 'sgi_system_rule_clause_rel', 'rule_id', 'clause_id',
                                  string="Cláusulas")
    clause_label = fields.Char(string="Numeral", compute='_compute_clause_label',
                               help="Los numerales de sus cláusulas, sin repetir (p. ej. 6.1, 8.2).")
    text = fields.Text(string="Regla", required=True)
    menu_id = fields.Many2one('ir.ui.menu', string="Pantalla en Odoo", ondelete='set null')
    active = fields.Boolean(default=True)

    @api.depends('clause_ids.code')
    def _compute_clause_label(self):
        def key(code):
            return [int(x) if x.isdigit() else x for x in (code or '').split('.')]
        for rule in self:
            codes = sorted(set(rule.clause_ids.mapped('code')), key=key)
            rule.clause_label = ", ".join(codes)

    def action_open_menu(self):
        self.ensure_one()
        return self.env['sgi.miid.section']._sgi_open_menu(self.menu_id)
