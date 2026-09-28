# -*- coding: utf-8 -*-
"""Solicitud de desarrollo en Proyectos FT (56.0.0).

Sustituye los Excel F-P-D01-02 (general), F-P-D01-18 (Entretelas V10),
F-P-D01-26 (Carda) y F-P-D01-27 (Tramado): un solo formulario en el proyecto
«FT-xxx-aaaa <artículo>» con el tipo de desarrollo, los datos del cliente y
las características pedidas (una fila por característica, con unidad, valor
y norma). El PDF imprime la clave del formato que corresponde al tipo.
"""
from odoo import api, fields, models

DEV_TYPES = [
    ('general', "General (F-P-D01-02)"),
    ('entretelas_v10', "Entretelas V10 (F-P-D01-18)"),
    ('carda', "Carda (F-P-D01-26)"),
    ('tramado', "Tramado (F-P-D01-27)"),
]
DEV_FORMAT = {
    'general': 'F-P-D01-02', 'entretelas_v10': 'F-P-D01-18',
    'carda': 'F-P-D01-26', 'tramado': 'F-P-D01-27',
}
# (nombre, dirección, unidad) por tipo; el orden es el del Excel.
DEV_DEFAULT_LINES = {
    'general': [
        ("Masa por unidad de área", 'na', "g/m²"), ("Ancho de la tela", 'na', "m"),
        ("Espesor", 'na', "plg/mm"), ("Rendimiento", 'na', "m/kg"),
        ("Tipo de acabado", 'na', ""), ("Tipo y tacto del engomado", 'na', ""),
        ("Elongación estática", 'largo', "%"), ("Elongación estática", 'ancho', "%"),
        ("Densidad del tejido: mallas", 'na', "/10 cm"), ("Densidad del tejido: columnas", 'na', "/10 cm"),
        ("Cambios dimensionales al calor", 'largo', "%"), ("Cambios dimensionales al calor", 'ancho', "%"),
        ("Engomado de orillas (sí/no)", 'na', ""), ("Tamaño de los rollos", 'na', "m"),
    ],
    'entretelas_v10': [
        ("Tela base", 'na', ""), ("Masa por unidad de área total", 'na', "g/m²"),
        ("Cantidad de resina", 'na', "g/m²"), ("Ancho", 'na', "m"),
        ("Cambios dimensionales al lavado ×2", 'largo', "%"), ("Cambios dimensionales al lavado ×2", 'ancho', "%"),
        ("Máquina de proceso", 'na', ""), ("Fuerza máxima (método de la tira)", 'largo', "N/5 cm²"),
        ("Fuerza máxima (método de la tira)", 'ancho', "N/5 cm²"), ("Tipo de resina", 'na', ""),
        ("Adhesión (fusionado)", 'largo', "N/5 cm²"), ("Adhesión (fusionado)", 'ancho', "N/5 cm²"),
        ("Tipo de polvo", 'na', ""), ("Solidez del color al frote (izq / centro / der)", 'na', ""),
        ("Mesh", 'na', ""), ("Rendimiento", 'na', "m/kg"), ("Corte de orillas (sí/no)", 'na', ""),
        ("Tamaño de los rollos", 'na', "m"),
    ],
    'carda': [
        ("Tipo de fibra", 'na', ""), ("Título de fibra", 'na', "dtex"), ("Acabado", 'na', ""),
        ("Masa por unidad de área total (orilla / centro / orilla)", 'na', "g/m²"),
        ("Ancho", 'na', "m"), ("Espesor (orilla / centro / orilla)", 'na', "mm"),
        ("Fuerza máxima (método de la tira)", 'largo', "N/5 cm²"), ("Fuerza máxima (método de la tira)", 'ancho', "N/5 cm²"),
        ("Rendimiento", 'na', "m/kg"), ("Tamaño de los rollos", 'na', "m"),
    ],
    'tramado': [
        ("Composición", 'na', ""), ("Masa por unidad de área total (orilla / centro / orilla)", 'na', "g/m²"),
        ("Ancho", 'na', "m"), ("Espesor (orilla / centro / orilla)", 'na', "mm"),
        ("Fuerza máxima (método de la tira)", 'largo', "N/5 cm²"), ("Fuerza máxima (método de la tira)", 'ancho', "N/5 cm²"),
        ("Hilos por pulgada a lo ancho", 'na', "/plg"), ("Tipo de hilo", 'na', ""),
        ("Rendimiento", 'na', "m/kg"), ("Tamaño de los rollos", 'na', "m"),
    ],
}


class ProjectProjectDevRequest(models.Model):
    _inherit = 'project.project'

    sgi_is_ft = fields.Boolean(
        string="Proyecto FT (desarrollo)", compute='_compute_sgi_is_ft', store=True, readonly=False,
        help="Se marca solo cuando el nombre empieza con «FT-». Habilita la pestaña Solicitud de desarrollo.")
    sgi_dev_type = fields.Selection(DEV_TYPES, string="Tipo de desarrollo", default='general')
    sgi_dev_format_code = fields.Char(string="Formato", compute='_compute_sgi_dev_format_code')
    sgi_dev_date = fields.Date(string="Fecha de solicitud")
    sgi_dev_requester = fields.Char(string="Nombre del solicitante")
    sgi_dev_use = fields.Text(string="Descripción y uso del producto")
    sgi_dev_spec = fields.Text(string="Especificación del cliente")
    sgi_dev_sample_m = fields.Float(string="Cantidad de la muestra (m)")
    sgi_dev_sample_kg = fields.Float(string="Cantidad de la muestra (kg)")
    sgi_dev_volume = fields.Float(string="Volumen estimado")
    sgi_dev_volume_uom = fields.Selection([('m', "m / mes"), ('kg', "kg / mes")], string="Unidad del volumen", default='m')
    sgi_dev_target_price = fields.Monetary(string="Precio objetivo", currency_field='sgi_dev_currency_id')
    sgi_dev_currency_id = fields.Many2one('res.currency', string="Moneda del precio",
                                          default=lambda self: self.env.company.currency_id)
    sgi_dev_norms = fields.Char(string="Norma(s) a cumplir")
    sgi_dev_packaging = fields.Text(string="Datos en la etiqueta y empaque")
    sgi_dev_customer_property = fields.Selection([
        ('muestra', "Muestra del producto"), ('especificacion', "Especificación del producto"),
        ('ambos', "Muestra y especificación"), ('ninguna', "Ninguna"),
    ], string="Propiedad del cliente recibida")
    sgi_dev_other = fields.Text(string="Otras características")
    sgi_dev_line_ids = fields.One2many('sgi.dev.characteristic', 'project_id', string="Características del producto")
    sgi_dev_prepared_by_id = fields.Many2one('res.users', string="Elaboró (Diseño y Desarrollo)")
    sgi_dev_approved_by_id = fields.Many2one('res.users', string="Aprobó (Dirección de Operaciones)")

    @api.depends('name')
    def _compute_sgi_is_ft(self):
        for project in self:
            if (project.name or '').strip().upper().startswith('FT-'):
                project.sgi_is_ft = True
            elif not project.sgi_is_ft:
                project.sgi_is_ft = False

    @api.depends('sgi_dev_type')
    def _compute_sgi_dev_format_code(self):
        for project in self:
            project.sgi_dev_format_code = DEV_FORMAT.get(project.sgi_dev_type or 'general')

    def sgi_dev_format_info(self):
        """'F-P-D01-18 · Rev. 02' para el pie del PDF (revisión viva desde Documentos)."""
        self.ensure_one()
        code = self.sgi_dev_format_code or DEV_FORMAT['general']
        revision = self.env['sgi.format.map'].sudo()._revision_of(code)
        return "%s · Rev. %s" % (code, revision) if revision else code

    def action_sgi_dev_load_lines(self):
        """Propone las características del tipo (solo agrega las que faltan)."""
        for project in self:
            existing = {(l.name, l.direction) for l in project.sgi_dev_line_ids}
            vals = [(0, 0, {'name': name, 'direction': direction, 'unit': unit, 'sequence': i * 10})
                    for i, (name, direction, unit) in enumerate(DEV_DEFAULT_LINES.get(project.sgi_dev_type or 'general', []))
                    if (name, direction) not in existing]
            if vals:
                project.write({'sgi_dev_line_ids': vals})
        return True

    def action_sgi_dev_print(self):
        self.ensure_one()
        return self.env.ref('quimibond_sgi.action_report_dev_request').report_action(self)


class SgiDevCharacteristic(models.Model):
    _name = 'sgi.dev.characteristic'
    _description = "Característica pedida en la solicitud de desarrollo"
    _order = 'sequence, id'

    project_id = fields.Many2one('project.project', required=True, ondelete='cascade', index=True)
    sequence = fields.Integer(default=10)
    name = fields.Char(string="Característica", required=True)
    direction = fields.Selection([('na', "—"), ('largo', "Largo"), ('ancho', "Ancho")], default='na', string="Dirección")
    unit = fields.Char(string="Unidad")
    value = fields.Char(string="Valor pedido")
    tolerance = fields.Char(string="Tolerancia")
    method = fields.Char(string="Método / norma")
    note = fields.Char(string="Observaciones")
