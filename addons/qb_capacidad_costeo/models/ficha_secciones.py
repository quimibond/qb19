# -*- coding: utf-8 -*-
"""Secciones de la ficha técnica de producto (1.66.0).

Sustituyen los Excel de Diseño y Desarrollo: F-P-D01-24 (ficha general D&D),
F-P-D01-05 (acabado), F-P-D01-33 (tintorería) y F-P-D01-08 (especificaciones
del producto con tolerancias, bilingüe). Todo cuelga de `qb.producto.ficha`:
los datos generales van en campos; tejido, teñido, acabado, tintorería,
presentación y especificaciones son renglones (`qb.producto.ficha.spec`) con
valor, tolerancia y método, que también alimentan el PDF bilingüe.
"""
from odoo import api, fields, models

SPEC_SECTIONS = [
    ('tejido', "Tejido"), ('tenido', "Teñido"), ('acabado', "Acabado"),
    ('tintoreria', "Tintorería"), ('presentacion', "Presentación"),
    ('especificacion', "Especificaciones del producto (cliente)"),
]
# Renglones estándar por sección: (nombre, dirección, unidad, método).
SPEC_DEFAULTS = {
    'tejido': [
        ("Máquina de tejido", 'na', "", ""), ("Galga", 'na', "", ""), ("Hilo / título", 'na', "dtex", ""),
        ("Peso crudo", 'na', "g/m²", "MT001"), ("Ancho crudo", 'na', "cm", "MT002"),
        ("Pasadas", 'na', "/plg", "MT004"), ("Columnas", 'na', "/plg", "MT004"),
    ],
    'tenido': [
        ("No. máquina", 'na', "", ""), ("Marca", 'na', "", ""), ("Modelo", 'na', "", ""), ("Fórmula", 'na', "", ""),
    ],
    'acabado': [
        ("Rama", 'na', "", ""), ("Temperatura de la rama", 'na', "°C", ""),
        ("Campo 1 (A / B)", 'na', "°C", ""), ("Campo 2 (A / B)", 'na', "°C", ""), ("Campo 3 (A / B)", 'na', "°C", ""),
        ("Campo 4 (A / B)", 'na', "°C", ""), ("Campo 5 (A / B)", 'na', "°C", ""), ("Campo 6 (A / B)", 'na', "°C", ""),
        ("Campo 7 (A / B)", 'na', "°C", ""), ("Campo 8 (A / B)", 'na', "°C", ""),
        ("Velocidad", 'na', "m/min", ""), ("Ancho entrada 1", 'na', "m", ""), ("Ancho entrada 2", 'na', "m", ""),
        ("Ancho cadena", 'na', "m", ""), ("Ancho salida", 'na', "m", ""),
        ("Rodillo de presión superior (sí / no)", 'na', "", ""), ("Alimentación rodillo superior", 'na', "%", ""),
        ("Alimentación rodillo inferior", 'na', "%", ""), ("Alimentación rueda izquierda", 'na', "%", ""),
        ("Alimentación rueda derecha", 'na', "%", ""), ("Tensión salida", 'na', "", ""),
        ("Extracción humedad", 'na', "", ""), ("Pick up", 'na', "%", ""),
        ("Receta rama: producto 1", 'na', "g/L", ""), ("Receta rama: producto 2", 'na', "g/L", ""),
        ("Receta rama: producto 3", 'na', "g/L", ""), ("Temperatura del foulard", 'na', "°C", ""),
        ("Vaporizar entrada rama (sí / no)", 'na', "", ""), ("Campo enfriamiento salida rama (sí / no)", 'na', "", ""),
        ("Ancho entrada tela", 'na', "m", ""), ("Ancho salida tela", 'na', "m", ""),
        ("Masa por unidad de área entrada", 'na', "g/m²", ""), ("Masa por unidad de área salida", 'na', "g/m²", ""),
        ("Corte de orillas (sí / no)", 'na', "", ""), ("Engomado de orillas (sí / no)", 'na', "", ""),
        ("Tipo de goma (disc / cont)", 'na', "", ""), ("Productos / fórmula no.", 'na', "", ""),
    ],
    'tintoreria': [
        ("Tipo de acabado", 'na', "", ""), ("Relación de baño", 'na', "", ""), ("Volumen de baño", 'na', "L", ""),
        ("Peso total de tela", 'na', "kg", ""), ("Composición de la tela", 'na', "", ""),
        ("Tiempo empleado", 'na', "min", ""), ("Temperatura", 'na', "°C", ""),
        ("Porcentaje del brazo del plegador", 'na', "%", ""), ("Bomba: velocidad", 'na', "", ""),
        ("Bomba: presión", 'na', "bar", ""), ("Acumulador inicial", 'na', "%", ""), ("Acumulador cargado", 'na', "%", ""),
        ("Gradiente de temperatura: subida", 'na', "°C/min", ""), ("Gradiente de temperatura: bajada", 'na', "°C/min", ""),
        ("Auxiliares utilizados", 'na', "", ""),
    ],
    'presentacion': [
        ("Presentación", 'na', "", ""), ("Empaque", 'na', "", ""), ("Etiqueta", 'na', "", ""),
    ],
    'especificacion': [
        ("Masa por unidad de área / Weight", 'na', "g/m²", "MT001 · NMX-A-3801-INNTEX-2012"),
        ("Ancho de la tela / Width on roll", 'na', "m", "MT002 · NMX-A-22198-INNTEX-2012"),
        ("Espesor / Thickness", 'na', "mm", "MT003 · NMX-A-301/2-INNTEX-2013"),
        ("Estabilidad dimensional / Shrinkage (200 °C, 30 s)", 'largo', "%", "MT006 · ISO 9866"),
        ("Estabilidad dimensional / Shrinkage (200 °C, 30 s)", 'ancho', "%", "MT006 · ISO 9866"),
        ("Densidad de tejido / Weave density (pasadas / wales)", 'na', "/plg", "MT004 · NMX-A-134-INNTEX-2013"),
        ("Densidad de tejido / Weave density (columnas / courses)", 'na', "/plg", "MT004 · NMX-A-134-INNTEX-2013"),
        ("Elongación estática / Static elongation", 'largo', "%", "MT005 · DIN 53360"),
        ("Elongación estática / Static elongation", 'ancho', "%", "MT005 · DIN 53360"),
    ],
}


class QbProductoFichaSecciones(models.Model):
    _inherit = 'qb.producto.ficha'

    # Datos generales de D&D (F-P-D01-24)
    dibujo = fields.Char(string="Dibujo")
    tacto = fields.Char(string="Tacto")
    descripcion = fields.Text(string="Descripción")
    revision = fields.Char(string="Revisión de la ficha")
    fecha_emision = fields.Date(string="Fecha de emisión")
    fecha_ultimo_cambio = fields.Date(string="Fecha del último cambio")
    ruta_proceso = fields.Char(string="Ruta de proceso",
                               help="Etapas en orden, p. ej. Tejido → Teñido → Rama → Inspección.")
    uso_principal = fields.Char(string="Principal uso del textil / Main use")
    # Instrucciones de cuidado (F-P-D01-08)
    cuidado_lavado = fields.Boolean(string="Lavado ciclo normal a 30 °C / Machine wash 30 °C")
    cuidado_blanqueo = fields.Boolean(string="Blanqueo / Bleach")
    cuidado_secado = fields.Boolean(string="Secado ciclo normal / Tumble dry normal")
    cuidado_planchado = fields.Boolean(string="Planchado a temperatura baja (150 °C) / Iron low")
    cuidado_lavado_seco = fields.Boolean(string="Lavado en seco ciclo normal / Dry clean")
    aprobado_desarrollos = fields.Boolean(string="Aprobado desarrollos / Development approved")
    spec_line_ids = fields.One2many('qb.producto.ficha.spec', 'ficha_id', string="Renglones de la ficha")
    spec_count = fields.Integer(compute='_compute_spec_count')
    # Firmas (F-P-D01-24 / D01-33)
    firma_manufactura_id = fields.Many2one('res.users', string="Jefe de manufactura")
    firma_calidad_id = fields.Many2one('res.users', string="Jefe de calidad")
    firma_inspeccion_id = fields.Many2one('res.users', string="Supervisor de inspección, enrollado y empaque")
    firma_dyd_id = fields.Many2one('res.users', string="Diseño y desarrollo de producto")
    firma_operaciones_id = fields.Many2one('res.users', string="Dirección de operaciones")
    firma_ventas_id = fields.Many2one('res.users', string="Administrador de ventas y marketing")

    @api.depends('spec_line_ids')
    def _compute_spec_count(self):
        for ficha in self:
            ficha.spec_count = len(ficha.spec_line_ids)

    def spec_lines(self, section):
        self.ensure_one()
        return self.spec_line_ids.filtered(lambda l: l.section == section)

    def action_cargar_renglones(self, sections=None):
        """Agrega los renglones estándar que falten (no pisa lo capturado)."""
        for ficha in self:
            existing = {(l.section, l.name, l.direction) for l in ficha.spec_line_ids}
            vals = []
            for section, rows in SPEC_DEFAULTS.items():
                if sections and section not in sections:
                    continue
                for i, (name, direction, unit, method) in enumerate(rows):
                    if (section, name, direction) not in existing:
                        vals.append((0, 0, {'section': section, 'name': name, 'direction': direction,
                                            'unit': unit, 'method': method, 'sequence': i * 10}))
            if vals:
                ficha.write({'spec_line_ids': vals})
        return True

    def action_print_specs(self):
        self.ensure_one()
        return self.env.ref('qb_capacidad_costeo.action_report_ficha_specs').report_action(self)


class QbProductoFichaSpec(models.Model):
    _name = 'qb.producto.ficha.spec'
    _description = "Renglón de la ficha técnica de producto"
    _order = 'section, sequence, id'

    ficha_id = fields.Many2one('qb.producto.ficha', required=True, ondelete='cascade', index=True)
    section = fields.Selection(SPEC_SECTIONS, required=True, default='especificacion')
    sequence = fields.Integer(default=10)
    name = fields.Char(string="Característica / Characteristic", required=True)
    direction = fields.Selection([('na', "—"), ('largo', "Largo / Length"), ('ancho', "Ancho / Width")],
                                 default='na', string="Dirección / Direction")
    unit = fields.Char(string="Unidad / Units")
    value = fields.Char(string="Valor / Spec")
    tolerance = fields.Char(string="Tolerancia o factor")
    method = fields.Char(string="Método / Method")
    note = fields.Char(string="Observaciones")
    en_coa = fields.Boolean(string="Aparece en el certificado", default=False)
