# -*- coding: utf-8 -*-
"""Catálogo de conceptos de NOI para los reportes de nómina (1.23.0).

Cada concepto (P001 SUELDO, D001 ISR…) dice qué reglas salariales de Odoo lo
forman. Lo usa el asistente ``qb.nomina.reporte`` (``wizard/reporte_nomina.py``)
para armar el «Reporte de la nómina» y el «Percepción/deducción» con las claves
que RH ya conoce de NOI. El cálculo está en ``reporte_calculo.py``."""
from odoo import _, api, fields, models
from odoo.exceptions import ValidationError

from .reporte_calculo import TIPOS, parsear_codigos


class NominaConceptoNoi(models.Model):
    _name = 'qb.nomina.concepto.noi'
    _description = "Concepto de NOI para reportes de nómina"
    _order = 'tipo_orden, clave, name'

    name = fields.Char(string="Descripción", required=True)
    clave = fields.Char(string="Clave NOI", help="Como en NOI: P001, D001…")
    tipo = fields.Selection([('percepcion', "Percepción"), ('deduccion', "Deducción"),
                             ('informativo', "Informativo"), ('ignorar', "No reportar")],
                            string="Tipo", required=True, default='percepcion',
                            help="Informativo: sale en el reporte de percepción/deducción y en el resumen, "
                                 "pero no suma ni resta al neto (subsidio para el empleo aplicado). No reportar: reglas con concepto "
                                 "de CFDI que no son un renglón propio (Bruto sin vacaciones repite el sueldo).")
    tipo_orden = fields.Integer(compute='_compute_tipo_orden', store=True)
    clave_sat = fields.Char(string="Clave SAT", size=3)
    codigos = fields.Char(
        string="Reglas (códigos)", required=True,
        help="Códigos de las reglas salariales que forman el concepto, separados por coma. Un «-» al frente "
             "invierte el signo de esa regla (ISR = ISR, SUBSIDY: el ISR que imprime NOI es el ISR menos el "
             "subsidio aplicado; IMSS = -IMSS_EMPLOYEE_TOTAL porque esa línea es positiva).")
    codigos_exento = fields.Char(
        string="Reglas exentas",
        help="De las reglas anteriores, las que son la parte exenta (HE_EXEMPT, PDOM_EXEMPT…). "
             "El resto del concepto es gravado. Sólo percepciones.")
    active = fields.Boolean(default=True)

    @api.depends('tipo')
    def _compute_tipo_orden(self):
        for c in self:
            c.tipo_orden = TIPOS.index(c.tipo) if c.tipo in TIPOS else 9

    @api.depends('clave', 'name')
    def _compute_display_name(self):
        for c in self:
            c.display_name = '%s %s' % (c.clave, c.name) if c.clave else c.name

    @api.constrains('codigos', 'codigos_exento')
    def _check_codigos(self):
        for c in self:
            todos = {cod for cod, _s in parsear_codigos(c.codigos)}
            if not todos:
                raise ValidationError(_("El concepto %s no tiene reglas.") % c.display_name)
            sobran = {cod for cod, _s in parsear_codigos(c.codigos_exento)} - todos
            if sobran:
                raise ValidationError(_("Las reglas exentas %(s)s del concepto %(c)s no están en sus reglas.",
                                        s=', '.join(sorted(sobran)), c=c.display_name))

    def _qb_para_calculo(self):
        return [{
            'id': c.id, 'clave': c.clave or '', 'nombre': c.name, 'tipo': c.tipo, 'sat': c.clave_sat or '',
            'codigos': c.codigos, 'codigos_exento': c.codigos_exento or '', 'secuencia': c.tipo_orden,
        } for c in self]

    @api.model
    def qb_nomina_colgar_menu_reportes(self):
        """Cuelga «Reportes NOI» de la raíz de la app de Nómina, como los menús de
        IMSS e Incidencias (un ``parent`` por xmlid que no resuelve tumba la instalación)."""
        menu = self.env.ref('quimibond_nomina.menu_qb_nomina_reportes', raise_if_not_found=False)
        if not menu:
            return False
        Menu = self.env['ir.ui.menu'].sudo().with_context(active_test=False)
        icono = self.env['qb.nomina.rule.sentinel'].PAYROLL_APP_ICON
        root = Menu.search([('parent_id', '=', False), ('web_icon', '=', icono)], limit=1)
        if not root:
            menu.sudo().write({'active': False})
            return False
        menu.sudo().write({'parent_id': root.id, 'active': True})
        return True
