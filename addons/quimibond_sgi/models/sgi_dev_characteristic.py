# -*- coding: utf-8 -*-
"""Tabla de características del desarrollo de producto (57.117.0, C1 bloque 1).

Una sola tabla por proyecto FT, un renglón por característica, con una
columna por momento del proceso: lo que pide el cliente, lo medido en su
muestra, el límite de control interno, lo obtenido en la corrida y la
aprobación del cliente. Sustituye la misma tabla recapturada en siete
formatos (Solicitud de desarrollos y sus variantes, Análisis de muestras,
Solicitud de pruebas, Aprobación para iniciar, Resultados de acabado y de
proceso, Aprobación de proyecto). Los valores son **números**, no texto, para
comparar contra tolerancia y calcular habilidad; el texto queda solo para las
características cualitativas (tacto, color) y las observaciones.

El catálogo de características, las claves de codificación y el mixin con los
dos juegos de límites viven en ``quimibond_ficha_tecnica_tela`` (decisión de
Jose, 2026-10-06): la ficha del artículo y el proyecto comparten catálogo y
límites, así los renglones pasan del proyecto a la ficha sin recaptura. Aquí
queda solo lo propio del desarrollo:

- ``sgi.dev.characteristic.template``: qué renglones carga cada tipo de
  desarrollo (general, entretelas V10, carda, tramado) y con qué dirección y
  posición (``data/sgi_dev_characteristic_data.xml``, ``noupdate``).
- ``sgi.dev.characteristic``: el renglón del proyecto, con las columnas de
  muestra del cliente, corrida, dictamen y aprobación.
"""
from odoo import api, fields, models

from odoo.addons.quimibond_ficha_tecnica_tela.models.ficha_tecnica_caracteristica import (
    CODE_MASS, CODE_WIDTH, DIRECTIONS, POSITIONS, RESULTS,
)

DEV_TYPES = [
    ('general', "General"),
    ('entretelas_v10', "Entretelas V10"),
    ('carda', "Carda"),
    ('tramado', "Tramado"),
]
VERDICTS = [('cumple', "Cumple"), ('desviacion', "Cumple con desviación"), ('no_cumple', "No cumple")]


class SgiDevCharacteristicTemplate(models.Model):
    """Renglón que carga un tipo de desarrollo en la tabla de características del proyecto."""
    _name = 'sgi.dev.characteristic.template'
    _description = "Característica por tipo de desarrollo"
    _order = 'dev_type, sequence, id'

    dev_type = fields.Selection(DEV_TYPES, string="Tipo de desarrollo", required=True, default='general',
                                help="Tipo de desarrollo cuya tabla incluye este renglón.")
    sequence = fields.Integer(default=10, help="Orden del renglón en la tabla del proyecto.")
    caracteristica_id = fields.Many2one('ficha.tecnica.caracteristica', string="Característica", required=True,
                                        ondelete='cascade', index=True,
                                        help="Característica del catálogo (Fichas Técnicas de Tela → "
                                             "Configuración) que se carga.")
    kind = fields.Selection(related='caracteristica_id.kind', string="Tipo de dato")
    unit = fields.Char(related='caracteristica_id.unit', string="Unidad")
    direction = fields.Selection(DIRECTIONS, string="Dirección", default='na', required=True,
                                 help="Largo o ancho cuando la característica se mide en las dos direcciones.")
    position = fields.Selection(POSITIONS, string="Posición", default='na', required=True,
                                help="Izquierda, centro o derecha cuando se mide en tres puntos (solidez al frote, "
                                     "masa por orillas).")
    lab_default = fields.Boolean(string="Se mide en la muestra del cliente",
                                 help="Al cargar el renglón queda marcado para que el laboratorio lo mida en la "
                                      "muestra del cliente.")
    in_customer_spec = fields.Boolean(string="Va a la especificación del cliente", default=True,
                                      help="El renglón aparece por omisión en las Especificaciones del producto "
                                           "que se entregan al cliente.")
    in_coa = fields.Boolean(string="Va al certificado",
                            help="El renglón aparece por omisión en el certificado de calidad del lote.")
    active = fields.Boolean(default=True, help="Los renglones archivados no se cargan en proyectos nuevos.")

    _template_uniq = models.Constraint(
        'unique(dev_type, caracteristica_id, direction, position)',
        "Esa característica ya está en la tabla de ese tipo de desarrollo con la misma dirección y posición.",
    )

    def _line_vals(self, sequence=None):
        self.ensure_one()
        return {
            'caracteristica_id': self.caracteristica_id.id,
            'direction': self.direction,
            'position': self.position,
            'sequence': self.sequence if sequence is None else sequence,
            'lab_requested': self.lab_default,
            'in_customer_spec': self.in_customer_spec,
            'in_coa': self.in_coa,
        }


class SgiDevCharacteristic(models.Model):
    """Renglón de la tabla de características de un proyecto de desarrollo: lo que pide el cliente,
    lo medido en su muestra, el control interno, lo obtenido en la corrida y lo que aprobó."""
    _name = 'sgi.dev.characteristic'
    _description = "Característica del desarrollo de producto"
    _inherit = 'ficha.tecnica.caracteristica.mixin'
    _order = 'sequence, id'

    project_id = fields.Many2one('project.project', required=True, ondelete='cascade', index=True,
                                 help="Proyecto de desarrollo al que pertenece el renglón.")
    # --- Muestra del cliente (paso 2, Laboratorio a petición de Diseño) --------
    lab_requested = fields.Boolean(string="Medir en la muestra",
                                   help="Diseño de Producto pide al laboratorio medir este renglón en la muestra "
                                        "del cliente.")
    sample_value = fields.Float(string="Medido en la muestra", digits=(16, 3),
                                help="Valor que midió el laboratorio en la muestra del cliente.")
    sample_text = fields.Char(string="Muestra (texto)",
                              help="Lo observado en la muestra del cliente cuando la característica es cualitativa.")
    sample_result = fields.Selection(RESULTS, string="Muestra vs. especificación", compute='_compute_results',
                                     store=True, help="Si lo medido en la muestra del cliente cae dentro de lo que "
                                                      "él mismo pide (calculado).")
    # --- Corrida de la muestra (paso 12, Laboratorio) --------------------------
    run_1 = fields.Float(string="Lectura 1", digits=(16, 3), help="Primera lectura de la corrida de muestra.")
    run_2 = fields.Float(string="Lectura 2", digits=(16, 3), help="Segunda lectura de la corrida de muestra.")
    run_3 = fields.Float(string="Lectura 3", digits=(16, 3), help="Tercera lectura de la corrida de muestra.")
    run_avg = fields.Float(string="Promedio", compute='_compute_run_avg', store=True, digits=(16, 3),
                           help="Promedio de las lecturas capturadas (calculado).")
    run_count = fields.Integer(string="Lecturas", compute='_compute_run_avg', store=True,
                               help="Cuántas lecturas tiene la corrida.")
    run_text = fields.Char(string="Corrida (texto)",
                           help="Lo observado en la corrida cuando la característica es cualitativa.")
    run_result = fields.Selection(RESULTS, string="Resultado de la corrida", compute='_compute_results', store=True,
                                  help="Cumple: dentro del control interno. Fuera del control interno: se puede "
                                       "embarcar con aviso a Calidad y a Diseño de Procesos. No conforme: fuera "
                                       "de lo que pide el cliente.")
    verdict = fields.Selection(VERDICTS, string="Dictamen",
                               help="Lo decide Diseño de Producto, no el laboratorio: cumple, cumple con desviación "
                                    "o no cumple.")
    # --- Cliente (paso 19) ------------------------------------------------------
    customer_approved = fields.Boolean(string="Aprobado por el cliente",
                                       help="El cliente aceptó este valor en la aprobación final.")

    @api.depends('run_1', 'run_2', 'run_3')
    def _compute_run_avg(self):
        for line in self:
            readings = [v for v in (line.run_1, line.run_2, line.run_3) if v]
            line.run_count = len(readings)
            line.run_avg = (sum(readings) / len(readings)) if readings else 0.0

    @api.depends('kind', 'spec_nominal', 'spec_limit', 'spec_tol_minus', 'spec_tol_plus', 'spec_tol_pct',
                 'ctrl_tol_minus', 'ctrl_tol_plus', 'sample_value', 'run_avg', 'run_count')
    def _compute_results(self):
        for line in self:
            line.sample_result = line._result_for(line.sample_value) if line.sample_value else False
            line.run_result = line._result_for(line.run_avg) if line.run_count else False

    # ------------------------------------------------------------------------
    # Rendimiento calculado: 1000 / (masa g/m² × ancho m)
    # ------------------------------------------------------------------------
    _COMPUTED_COLUMNS = ('spec_nominal', 'sample_value', 'run_1', 'run_2', 'run_3')

    @api.model_create_multi
    def create(self, vals_list):
        lines = super().create(vals_list)
        if not self.env.context.get('sgi_dev_formula'):
            lines.mapped('project_id')._sgi_dev_refresh_formulas()
        return lines

    def write(self, vals):
        res = super().write(vals)
        if not self.env.context.get('sgi_dev_formula') and set(vals) & (
                set(self._COMPUTED_COLUMNS) | {'caracteristica_id', 'spec_limit', 'spec_tol_minus',
                                               'spec_tol_plus', 'spec_tol_pct'}):
            self.mapped('project_id')._sgi_dev_refresh_formulas()
        return res

    @api.model
    def _yield_from(self, mass, width):
        """Rendimiento en m/kg: 1000 / (masa g/m² × ancho m). 0 si falta un dato."""
        return round(1000.0 / (mass * width), 3) if mass and width else 0.0


class ProjectProjectDevFormulas(models.Model):
    _inherit = 'project.project'

    @staticmethod
    def _sgi_dev_pick(lines, code):
        """El renglón de una característica sin dirección: el de posición única
        o, si se mide en tres puntos (carda, tramado), el del centro."""
        same = lines.filtered(lambda l: l.caracteristica_code == code and l.direction == 'na')
        return (same.filtered(lambda l: l.position == 'na') or same.filtered(lambda l: l.position == 'centro'))[:1]

    def _sgi_dev_refresh_formulas(self):
        """Rellena los renglones calculados (rendimiento) con la masa y el
        ancho del mismo proyecto, columna por columna. La tolerancia del
        rendimiento sale de los extremos de masa y ancho."""
        Line = self.env['sgi.dev.characteristic']
        for project in self:
            lines = project.sgi_dev_line_ids
            targets = lines.filtered(lambda l: l.caracteristica_id.formula == 'rendimiento')
            if not targets:
                continue
            mass = self._sgi_dev_pick(lines, CODE_MASS)
            width = self._sgi_dev_pick(lines, CODE_WIDTH)
            vals = {}
            for col in Line._COMPUTED_COLUMNS:
                vals[col] = Line._yield_from(mass[col] if mass else 0.0, width[col] if width else 0.0)
            # Tolerancia del rendimiento: masa alta × ancho alto da el mínimo.
            lo = hi = 0.0
            if mass and width and mass._has_spec() and width._has_spec():
                m_lo, m_hi = mass._limits('spec')
                w_lo, w_hi = width._limits('spec')
                if None not in (m_lo, m_hi, w_lo, w_hi) and vals['spec_nominal']:
                    lo = Line._yield_from(m_hi, w_hi)
                    hi = Line._yield_from(m_lo, w_lo)
            vals.update({
                'spec_limit': 'tol', 'spec_tol_pct': False,
                'spec_tol_minus': max(vals['spec_nominal'] - lo, 0.0) if lo else 0.0,
                'spec_tol_plus': max(hi - vals['spec_nominal'], 0.0) if hi else 0.0,
            })
            changed = targets.filtered(lambda l: any(abs(l[k] - v) > 1e-9 if isinstance(v, float) else l[k] != v
                                                     for k, v in vals.items()))
            if changed:
                changed.with_context(sgi_dev_formula=True).write(vals)
