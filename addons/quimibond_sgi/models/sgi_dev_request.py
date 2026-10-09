# -*- coding: utf-8 -*-
"""Solicitud de desarrollo en Proyectos FT (56.0.0).

Sustituye los Excel F-P-D01-02 (general), F-P-D01-18 (Entretelas V10),
F-P-D01-26 (Carda) y F-P-D01-27 (Tramado): un solo formulario en el proyecto
«FT-xxx-aaaa <artículo>» con el tipo de desarrollo, los datos del cliente y
las características pedidas (una fila por característica, con unidad, valor
y norma). El PDF imprime la clave del formato que corresponde al tipo.
"""
import json

from odoo import api, fields, models

from .sgi_dev_characteristic import DEV_TYPES

# C-006: cada tipo apunta a su formato por el mapeo ``format_ref_dev_<tipo>``
# (sgi.format.map ligado al documento), no por el texto de la clave.
DEV_FORMAT_REF = 'format_ref_dev_%s'
# 57.117.0: los renglones por tipo ya no viven aquí sino en el catálogo
# ``sgi.dev.characteristic.template`` (data/sgi_dev_characteristic_data.xml).


class ProjectProjectDevRequest(models.Model):
    _inherit = 'project.project'

    # 57.118.0 (decisión 3 de Jose): bandera «desarrollo de producto» que pone el
    # tipo de proyecto (plantilla o captura), no el nombre. El folio FT es un
    # campo aparte (sgi_ft_folio, models/sgi_dev_project.py).
    sgi_is_ft = fields.Boolean(
        string="Desarrollo de producto", tracking=True,
        help="El proyecto es un desarrollo de producto (procedimiento C1): habilita las pestañas de "
             "desarrollo, el folio FT, las etapas de avance y las mediciones del SGI. Las plantillas de "
             "Diseño y Desarrollo ya lo traen marcado.")
    sgi_dev_type = fields.Selection(DEV_TYPES, string="Tipo de desarrollo", default='general',
                                    help="Tipo de desarrollo que se solicita; define qué datos pide la "
                                         "solicitud.")
    sgi_dev_format_code = fields.Char(string="Formato", compute='_compute_sgi_dev_format_code')
    sgi_dev_date = fields.Date(string="Fecha de solicitud",
                               help="Fecha en que se recibió la solicitud de desarrollo.")
    sgi_dev_requester = fields.Char(string="Nombre del solicitante")
    sgi_dev_use = fields.Text(string="Descripción y uso del producto")
    sgi_dev_spec = fields.Text(string="Especificación del cliente")
    sgi_dev_sample_m = fields.Float(string="Cantidad de la muestra (m)",
                                    help="Cantidad de muestra pedida, en metros.")
    sgi_dev_sample_kg = fields.Float(string="Cantidad de la muestra (kg)",
                                     help="Cantidad de muestra pedida, en kilogramos.")
    sgi_dev_volume = fields.Float(string="Volumen estimado",
                                  help="Volumen mensual que el cliente estima comprar si el desarrollo se "
                                       "aprueba.")
    sgi_dev_volume_uom = fields.Selection([('m', "m / mes"), ('kg', "kg / mes")], string="Unidad del volumen", default='m',
                                          help="Unidad del volumen estimado: metros o kilogramos por mes.")
    sgi_dev_target_price = fields.Monetary(string="Precio objetivo", currency_field='sgi_dev_currency_id')
    sgi_dev_currency_id = fields.Many2one('res.currency', string="Moneda del precio",
                                          default=lambda self: self.env.company.currency_id,
                                          help="Moneda del precio objetivo del desarrollo.")
    sgi_dev_norms = fields.Char(string="Norma(s) a cumplir")
    sgi_dev_packaging = fields.Text(string="Datos en la etiqueta y empaque")
    sgi_dev_customer_property = fields.Selection([
        ('muestra', "Muestra del producto"), ('especificacion', "Especificación del producto"),
        ('ambos', "Muestra y especificación"), ('ninguna', "Ninguna"),
    ], string="Propiedad del cliente recibida",
        help="Qué entregó el cliente para el desarrollo (muestra, especificación, ambas o nada). Es "
             "propiedad del cliente y se resguarda.")
    sgi_dev_other = fields.Text(string="Otras características")
    sgi_dev_line_ids = fields.One2many('sgi.dev.characteristic', 'project_id', string="Características del producto")
    sgi_dev_line_count = fields.Integer(string="Características", compute='_compute_sgi_dev_counts', store=True,
                                        help="Renglones de la tabla de características del proyecto.")
    sgi_dev_out_of_spec_count = fields.Integer(
        string="No conformes en corrida", compute='_compute_sgi_dev_counts', store=True,
        help="Renglones cuya corrida quedó fuera de lo que pide el cliente.")
    sgi_dev_deviation_count = fields.Integer(
        string="Fuera del control interno", compute='_compute_sgi_dev_counts', store=True,
        help="Renglones cuya corrida cumple al cliente pero sale del margen interno: se embarca con aviso a "
             "Calidad y a Diseño de Procesos.")
    sgi_dev_lab_pending_count = fields.Integer(
        string="Pendientes de laboratorio", compute='_compute_sgi_dev_counts', store=True,
        help="Renglones marcados para medir en la muestra del cliente que aún no tienen resultado.")
    sgi_dev_prepared_by_id = fields.Many2one('res.users', string="Elaboró (Diseño y Desarrollo)",
                                             help="Persona de Diseño y Desarrollo que elaboró la solicitud.")
    sgi_dev_approved_by_id = fields.Many2one('res.users', string="Aprobó (Dirección de Operaciones)",
                                             help="Persona de Dirección de Operaciones que aprueba la "
                                                  "solicitud de desarrollo.")

    def _sgi_dev_format_map(self):
        self.ensure_one()
        return self.env['sgi.format.map'].sudo()._sgi_ref(
            DEV_FORMAT_REF % (self.sgi_dev_type or 'general'))

    @api.depends('sgi_dev_type')
    def _compute_sgi_dev_format_code(self):
        for project in self:
            project.sgi_dev_format_code = project._sgi_dev_format_map().sgi_live_parts()[0]

    def sgi_dev_format_info(self):
        """'F-P-D01-18 · Rev. 02' para el pie del PDF (clave y revisión vivas
        del documento ligado al mapeo)."""
        self.ensure_one()
        return self._sgi_dev_format_map().sgi_live_label()

    # 57.139.0: renglones del tipo que el usuario borró a propósito (claves
    # «caracteristica|dirección|posición», JSON). Técnico: no se captura ni se muestra.
    sgi_dev_removed_line_keys = fields.Char(string="Renglones del tipo borrados a propósito", copy=False, readonly=True)

    def _sgi_dev_removed_keys(self):
        self.ensure_one()
        try:
            return set(json.loads(self.sgi_dev_removed_line_keys or '[]'))
        except ValueError:
            return set()

    def _sgi_dev_remember_removed_lines(self, keys):
        for project in self:
            project.sudo().write({'sgi_dev_removed_line_keys': json.dumps(sorted(project._sgi_dev_removed_keys() | set(keys)))})

    def action_sgi_dev_load_lines(self):
        """Propone las características del tipo desde el catálogo
        (``sgi.dev.characteristic.template``); solo agrega las que faltan. 57.139.0: con la
        tabla vacía propone todas (y olvida lo borrado antes); con renglones, no vuelve a
        proponer los que el usuario borró a propósito y deja cuenta en el chatter."""
        Template = self.env['sgi.dev.characteristic.template']
        for project in self:
            if not project.sgi_dev_line_ids and project.sgi_dev_removed_line_keys:
                project.sudo().write({'sgi_dev_removed_line_keys': False})
            removed = project._sgi_dev_removed_keys()
            existing = {(l.caracteristica_id.id, l.direction, l.position) for l in project.sgi_dev_line_ids if l.caracteristica_id}
            templates = Template.search([('dev_type', '=', project.sgi_dev_type or 'general')])
            vals, skipped = [], []
            for i, t in enumerate(templates):
                key = (t.caracteristica_id.id, t.direction, t.position)
                if key in existing:
                    continue
                if "%d|%s|%s" % key in removed:
                    skipped.append(t.caracteristica_id.name)
                    continue
                vals.append((0, 0, t._line_vals(sequence=i * 10)))
            if vals:
                project.write({'sgi_dev_line_ids': vals})
            if vals or skipped:
                project.message_post(body="Características del tipo %s: %d agregada(s)%s." % (
                    dict(DEV_TYPES).get(project.sgi_dev_type or 'general'), len(vals),
                    ("; no se volvieron a proponer %d borrada(s) a propósito: %s" % (len(skipped), ", ".join(skipped)))
                    if skipped else ''))
        return True

    @api.depends('sgi_dev_line_ids.run_result', 'sgi_dev_line_ids.sample_result', 'sgi_dev_line_ids.lab_requested',
                 'sgi_dev_line_ids.sample_value', 'sgi_dev_line_ids.sample_text')
    def _compute_sgi_dev_counts(self):
        for project in self:
            lines = project.sgi_dev_line_ids
            project.sgi_dev_line_count = len(lines)
            project.sgi_dev_out_of_spec_count = len(lines.filtered(lambda l: l.run_result == 'no_conforme'))
            project.sgi_dev_deviation_count = len(lines.filtered(lambda l: l.run_result == 'desviacion'))
            project.sgi_dev_lab_pending_count = len(lines.filtered(
                lambda l: l.lab_requested and not l.sample_value and not l.sample_text))

    def action_sgi_dev_print(self):
        self.ensure_one()
        return self.env.ref('quimibond_sgi.action_report_dev_request').report_action(self)
