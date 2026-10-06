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

Catálogos:

- ``sgi.dev.characteristic.type``: la característica (unidad, método o norma,
  si es numérica, cualitativa o sí/no, y si se calcula).
- ``sgi.dev.characteristic.template``: qué renglones carga cada tipo de
  desarrollo (general, entretelas V10, carda, tramado) y con qué dirección y
  posición.
- ``sgi.dev.code.catalog``: las claves de la regla de codificación de
  artículos de tejido y acabado (composición, dibujo, tipo de hilo, galga,
  operación, color, acabado), transcritas del DAT P-D02-01 rev. 05.

Los datos van en ``data/sgi_dev_characteristic_data.xml`` (``noupdate``:
Diseño y Desarrollo los ajusta en Configuración sin que el update los
revierta).
"""
from odoo import api, fields, models
from odoo.exceptions import ValidationError

DEV_TYPES = [
    ('general', "General"),
    ('entretelas_v10', "Entretelas V10"),
    ('carda', "Carda"),
    ('tramado', "Tramado"),
]
DIRECTIONS = [('na', "—"), ('largo', "Largo"), ('ancho', "Ancho")]
POSITIONS = [('na', "—"), ('izquierda', "Izquierda"), ('centro', "Centro"), ('derecha', "Derecha")]
KINDS = [('num', "Numérica"), ('text', "Cualitativa"), ('bool', "Sí / no")]
LIMITS = [('tol', "Nominal ± tolerancia"), ('max', "Máximo"), ('min', "Mínimo")]
RESULTS = [('cumple', "Cumple"), ('desviacion', "Fuera del control interno"), ('no_conforme', "No conforme")]
VERDICTS = [('cumple', "Cumple"), ('desviacion', "Cumple con desviación"), ('no_cumple', "No cumple")]
CODE_KINDS = [
    ('composicion', "Composición (posición 1)"),
    ('dibujo', "Dibujo (posición 2)"),
    ('hilo', "Tipo de hilo (posición 6)"),
    ('galga', "Galga (posiciones 7 y 8)"),
    ('operacion', "Operación (posición 9)"),
    ('color', "Color (posiciones 10 y 11)"),
    ('acabado', "Acabado (posiciones 15 y 16)"),
]
# Códigos del catálogo con significado en el código: el rendimiento se
# calcula con la masa y el ancho del mismo proyecto.
CODE_MASS = 'masa'
CODE_WIDTH = 'ancho'


class SgiDevCharacteristicType(models.Model):
    """Característica del catálogo de desarrollo de producto (unidad, método y tipo de dato)."""
    _name = 'sgi.dev.characteristic.type'
    _description = "Característica de desarrollo (catálogo)"
    _order = 'sequence, name, id'

    sequence = fields.Integer(default=10, help="Orden en el que aparece en los catálogos.")
    code = fields.Char(string="Clave", required=True, index=True,
                       help="Clave corta y estable de la característica (p. ej. «masa», «ancho»). "
                            "Con ella se reconocen los renglones al pasar del proyecto a la ficha "
                            "del artículo y al calcular el rendimiento.")
    name = fields.Char(string="Característica", required=True, translate=False,
                       help="Nombre con el que se ve en la tabla del proyecto y en los PDF.")
    name_en = fields.Char(string="Nombre en inglés",
                          help="Nombre para los documentos bilingües al cliente (Especificaciones del producto).")
    kind = fields.Selection(KINDS, string="Tipo de dato", required=True, default='num',
                            help="Numérica: se captura valor y tolerancia y se compara. Cualitativa: solo texto "
                                 "(tacto, color). Sí / no: casilla.")
    unit = fields.Char(string="Unidad", help="Unidad de medida (g/m², m, %, N/5 cm…).")
    method = fields.Char(string="Método / norma",
                         help="Método de prueba o norma con la que se mide (MT001 · NMX-A-3801-INNTEX-2012…).")
    formula = fields.Selection([('none', "Se captura"), ('rendimiento', "Rendimiento = 1000 / (masa × ancho)")],
                               string="Cálculo", default='none', required=True,
                               help="Si la característica se calcula a partir de otras del mismo proyecto. El "
                                    "rendimiento (m/kg) sale de la masa (g/m²) y el ancho (m).")
    active = fields.Boolean(default=True, help="Las características archivadas no se proponen en proyectos nuevos.")
    template_ids = fields.One2many('sgi.dev.characteristic.template', 'type_id', string="En tipos de desarrollo")

    _code_uniq = models.Constraint(
        'unique(code)',
        "La clave de la característica debe ser única.",
    )

    @api.model
    def _by_code(self, code):
        return self.with_context(active_test=False).search([('code', '=', code)], limit=1)


class SgiDevCharacteristicTemplate(models.Model):
    """Renglón que carga un tipo de desarrollo en la tabla de características del proyecto."""
    _name = 'sgi.dev.characteristic.template'
    _description = "Característica por tipo de desarrollo"
    _order = 'dev_type, sequence, id'

    dev_type = fields.Selection(DEV_TYPES, string="Tipo de desarrollo", required=True, default='general',
                                help="Tipo de desarrollo cuya tabla incluye este renglón.")
    sequence = fields.Integer(default=10, help="Orden del renglón en la tabla del proyecto.")
    type_id = fields.Many2one('sgi.dev.characteristic.type', string="Característica", required=True,
                              ondelete='cascade', index=True,
                              help="Característica del catálogo que se carga.")
    kind = fields.Selection(related='type_id.kind', string="Tipo de dato")
    unit = fields.Char(related='type_id.unit', string="Unidad")
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
        'unique(dev_type, type_id, direction, position)',
        "Esa característica ya está en la tabla de ese tipo de desarrollo con la misma dirección y posición.",
    )

    def _line_vals(self, sequence=None):
        self.ensure_one()
        return {
            'type_id': self.type_id.id,
            'direction': self.direction,
            'position': self.position,
            'sequence': self.sequence if sequence is None else sequence,
            'lab_requested': self.lab_default,
            'in_customer_spec': self.in_customer_spec,
            'in_coa': self.in_coa,
        }


class SgiDevCodeCatalog(models.Model):
    """Clave de la regla de codificación de artículos de tejido y acabado (DAT P-D02-01)."""
    _name = 'sgi.dev.code.catalog'
    _description = "Clave de codificación de artículos"
    _order = 'kind, sequence, code, id'

    kind = fields.Selection(CODE_KINDS, string="Parte del código", required=True, index=True,
                            help="Posición del código de 16 caracteres que ocupa la clave.")
    sequence = fields.Integer(default=10, help="Orden dentro de su parte del código.")
    code = fields.Char(string="Clave", required=True, index=True,
                       help="Letra o letras que van en el código del artículo (W, J, Q, NT, AF…). Para la galga, "
                            "las dos cifras más bajas de su rango (01, 11, 21…).")
    name = fields.Char(string="Descripción", required=True,
                       help="Qué significa la clave (poliéster 100 %, jersey, hilo natural, natural, afelpado…).")
    gauge = fields.Integer(string="Galga",
                           help="Solo para la galga: el número de galga (14, 16, 18…) que se codifica con el rango.")
    range_from = fields.Integer(string="Rango desde",
                                help="Solo para la galga: primer número del rango de dos cifras (01 para galga 14).")
    range_to = fields.Integer(string="Rango hasta",
                              help="Solo para la galga: último número del rango (10 para galga 14).")
    active = fields.Boolean(default=True, help="Las claves archivadas no se proponen al generar códigos.")

    _kind_code_uniq = models.Constraint(
        'unique(kind, code)',
        "Esa clave ya existe en esa parte del código.",
    )

    @api.depends('code', 'name', 'kind', 'gauge')
    def _compute_display_name(self):
        for rec in self:
            if rec.kind == 'galga' and rec.gauge:
                rec.display_name = "Galga %s (%02d-%02d)" % (rec.gauge, rec.range_from, rec.range_to)
            else:
                rec.display_name = "%s · %s" % (rec.code or '', rec.name or '')

    @api.constrains('kind', 'gauge', 'range_from', 'range_to')
    def _check_gauge(self):
        for rec in self.filtered(lambda r: r.kind == 'galga'):
            if not rec.gauge or not (1 <= rec.range_from <= rec.range_to <= 99):
                raise ValidationError(
                    "Una clave de galga lleva el número de galga y un rango de dos cifras entre 01 y 99.")

    @api.model
    def gauge_code(self, gauge):
        """'21' para la galga 18: las dos cifras más bajas del rango de esa galga (posiciones 7 y 8)."""
        rec = self.search([('kind', '=', 'galga'), ('gauge', '=', gauge)], limit=1)
        return "%02d" % rec.range_from if rec else ''

    @api.model
    def gauge_from_code(self, two_digits):
        """La galga (18) de las posiciones 7 y 8 del código ('21'…'30')."""
        try:
            n = int(two_digits)
        except (TypeError, ValueError):
            return 0
        rec = self.search([('kind', '=', 'galga'), ('range_from', '<=', n), ('range_to', '>=', n)], limit=1)
        return rec.gauge


class SgiDevCharacteristic(models.Model):
    """Renglón de la tabla de características de un proyecto de desarrollo: lo que pide el cliente,
    lo medido en su muestra, el control interno, lo obtenido en la corrida y lo que aprobó."""
    _name = 'sgi.dev.characteristic'
    _description = "Característica del desarrollo de producto"
    _order = 'sequence, id'

    project_id = fields.Many2one('project.project', required=True, ondelete='cascade', index=True,
                                 help="Proyecto de desarrollo al que pertenece el renglón.")
    sequence = fields.Integer(default=10, help="Orden del renglón en la tabla.")
    type_id = fields.Many2one('sgi.dev.characteristic.type', string="Catálogo", index=True, ondelete='restrict',
                              help="Característica del catálogo. Al elegirla se llenan nombre, unidad, método y "
                                   "tipo de dato; un renglón sin catálogo se escribe libre.")
    type_code = fields.Char(related='type_id.code', string="Clave", store=True)
    name = fields.Char(string="Característica", required=True, compute='_compute_from_type', store=True,
                       readonly=False, precompute=True,
                       help="Nombre de la característica (sale del catálogo; se puede ajustar).")
    kind = fields.Selection(KINDS, string="Tipo de dato", compute='_compute_from_type', store=True,
                            readonly=False, precompute=True, required=True, default='num',
                            help="Numérica (valor y tolerancia), cualitativa (texto) o sí / no.")
    unit = fields.Char(string="Unidad", compute='_compute_from_type', store=True, readonly=False, precompute=True,
                       help="Unidad de medida del renglón.")
    method = fields.Char(string="Método / norma", compute='_compute_from_type', store=True, readonly=False,
                         precompute=True, help="Método de prueba o norma con la que se mide.")
    direction = fields.Selection(DIRECTIONS, default='na', string="Dirección", required=True,
                                 help="Largo o ancho cuando se mide en las dos direcciones.")
    position = fields.Selection(POSITIONS, default='na', string="Posición", required=True,
                                help="Izquierda, centro o derecha cuando se mide en tres puntos.")
    # --- Especificación del cliente (paso 1, Ventas) ---------------------------
    spec_nominal = fields.Float(string="Nominal", digits=(16, 3),
                                help="Valor que pide el cliente. Con límite «Máximo» o «Mínimo» es el tope.")
    spec_limit = fields.Selection(LIMITS, string="Límite", default='tol', required=True,
                                  help="Cómo se lee la especificación: nominal con tolerancia, un máximo que no "
                                       "se rebasa o un mínimo que se alcanza.")
    spec_tol_minus = fields.Float(string="Tol. −", digits=(16, 3),
                                  help="Cuánto puede quedar por debajo del nominal (en la unidad o en %, según "
                                       "la casilla).")
    spec_tol_plus = fields.Float(string="Tol. +", digits=(16, 3),
                                 help="Cuánto puede quedar por encima del nominal (en la unidad o en %).")
    spec_tol_pct = fields.Boolean(string="Tolerancia en %",
                                  help="Las tolerancias son porcentaje del nominal, no unidades.")
    spec_min = fields.Float(string="Mín. cliente", compute='_compute_limits', store=True, digits=(16, 3),
                            help="Límite inferior que acepta el cliente (calculado).")
    spec_max = fields.Float(string="Máx. cliente", compute='_compute_limits', store=True, digits=(16, 3),
                            help="Límite superior que acepta el cliente (calculado).")
    spec_text = fields.Char(string="Especificación (texto)",
                            help="Solo para características cualitativas: lo que pide el cliente en palabras "
                                 "(tacto suave, color natural).")
    spec_bool = fields.Boolean(string="Sí / no pedido",
                               help="Solo para características de sí / no (engomado de orillas, corte de orillas).")
    spec_label = fields.Char(string="Especificación", compute='_compute_labels',
                             help="La especificación del cliente en una sola expresión, para pantalla y PDF.")
    # --- Control interno (al emitir la ficha, Diseño de Producto) --------------
    ctrl_tol_minus = fields.Float(string="Control −", digits=(16, 3),
                                  help="Margen interno por debajo del nominal, más cerrado que el del cliente. "
                                       "Vacío: se usa el del cliente.")
    ctrl_tol_plus = fields.Float(string="Control +", digits=(16, 3),
                                 help="Margen interno por encima del nominal, más cerrado que el del cliente.")
    ctrl_min = fields.Float(string="Mín. interno", compute='_compute_limits', store=True, digits=(16, 3),
                            help="Límite inferior del control interno (calculado).")
    ctrl_max = fields.Float(string="Máx. interno", compute='_compute_limits', store=True, digits=(16, 3),
                            help="Límite superior del control interno (calculado).")
    ctrl_defined = fields.Boolean(string="Con control interno", compute='_compute_limits', store=True,
                                  help="El renglón tiene un margen interno propio.")
    ctrl_label = fields.Char(string="Control interno", compute='_compute_labels',
                             help="El margen de control interno en una sola expresión. Nunca va al cliente.")
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
    # --- Cliente y documentos (paso 19 y liberación) ---------------------------
    customer_approved = fields.Boolean(string="Aprobado por el cliente",
                                       help="El cliente aceptó este valor en la aprobación final.")
    in_customer_spec = fields.Boolean(string="En especificación del cliente", default=True,
                                      help="El renglón aparece en las Especificaciones del producto que se entregan "
                                           "al cliente.")
    in_coa = fields.Boolean(string="En certificado",
                            help="El renglón aparece en el certificado de calidad del lote (siempre contra la "
                                 "especificación del cliente, nunca el control interno).")
    note = fields.Char(string="Observaciones", help="Texto libre; no capture aquí valores.")

    # ------------------------------------------------------------------------
    # Cálculos
    # ------------------------------------------------------------------------
    @api.depends('type_id')
    def _compute_from_type(self):
        for line in self:
            t = line.type_id
            if t:
                line.name = t.name
                line.kind = t.kind
                line.unit = t.unit
                line.method = t.method
            else:
                line.name = line.name or ''
                line.kind = line.kind or 'num'
                line.unit = line.unit or ''
                line.method = line.method or ''

    @staticmethod
    def _bounds(nominal, limit, tol_minus, tol_plus, pct):
        """(mín, máx) de un nominal con su tolerancia. Con «Máximo» el mínimo no
        aplica y con «Mínimo» el máximo no aplica (se devuelve None)."""
        if limit == 'max':
            return None, nominal
        if limit == 'min':
            return nominal, None
        base = abs(nominal) if pct else 1.0
        factor = 0.01 if pct else 1.0
        return nominal - base * tol_minus * factor, nominal + base * tol_plus * factor

    @api.depends('spec_nominal', 'spec_limit', 'spec_tol_minus', 'spec_tol_plus', 'spec_tol_pct',
                 'ctrl_tol_minus', 'ctrl_tol_plus')
    def _compute_limits(self):
        for line in self:
            lo, hi = self._bounds(line.spec_nominal, line.spec_limit, line.spec_tol_minus,
                                  line.spec_tol_plus, line.spec_tol_pct)
            line.spec_min = lo if lo is not None else 0.0
            line.spec_max = hi if hi is not None else 0.0
            line.ctrl_defined = bool(line.ctrl_tol_minus or line.ctrl_tol_plus)
            if line.ctrl_defined:
                clo, chi = self._bounds(line.spec_nominal, line.spec_limit, line.ctrl_tol_minus,
                                        line.ctrl_tol_plus, line.spec_tol_pct)
            else:
                clo, chi = lo, hi
            line.ctrl_min = clo if clo is not None else 0.0
            line.ctrl_max = chi if chi is not None else 0.0

    @api.depends('run_1', 'run_2', 'run_3')
    def _compute_run_avg(self):
        for line in self:
            readings = [v for v in (line.run_1, line.run_2, line.run_3) if v]
            line.run_count = len(readings)
            line.run_avg = (sum(readings) / len(readings)) if readings else 0.0

    def _limits(self, which):
        """(mín, máx) del cliente o del control interno, con None donde no aplica."""
        self.ensure_one()
        tol_minus, tol_plus = ((self.ctrl_tol_minus, self.ctrl_tol_plus) if which == 'ctrl' and self.ctrl_defined
                               else (self.spec_tol_minus, self.spec_tol_plus))
        return self._bounds(self.spec_nominal, self.spec_limit, tol_minus, tol_plus, self.spec_tol_pct)

    @staticmethod
    def _within(value, bounds):
        lo, hi = bounds
        return (lo is None or value >= lo - 1e-9) and (hi is None or value <= hi + 1e-9)

    def _result_for(self, value):
        """Tres resultados: dentro del control interno, fuera del interno pero
        dentro del cliente, fuera del cliente. Sin especificación numérica no
        hay resultado."""
        self.ensure_one()
        if self.kind != 'num' or not self._has_spec():
            return False
        if not self._within(value, self._limits('spec')):
            return 'no_conforme'
        if self.ctrl_defined and not self._within(value, self._limits('ctrl')):
            return 'desviacion'
        return 'cumple'

    def _has_spec(self):
        self.ensure_one()
        return bool(self.spec_nominal or self.spec_tol_minus or self.spec_tol_plus or self.spec_limit != 'tol')

    @api.depends('kind', 'spec_nominal', 'spec_limit', 'spec_tol_minus', 'spec_tol_plus', 'spec_tol_pct',
                 'ctrl_tol_minus', 'ctrl_tol_plus', 'sample_value', 'run_avg', 'run_count')
    def _compute_results(self):
        for line in self:
            line.sample_result = line._result_for(line.sample_value) if line.sample_value else False
            line.run_result = line._result_for(line.run_avg) if line.run_count else False

    @staticmethod
    def _fmt(value):
        return ('%.3f' % value).rstrip('0').rstrip('.')

    def _label(self, tol_minus, tol_plus):
        self.ensure_one()
        unit = (' %s' % self.unit) if self.unit else ''
        if self.kind == 'bool':
            return "Sí" if self.spec_bool else "No"
        if self.kind == 'text':
            return self.spec_text or ''
        if not self._has_spec():
            return ''
        n = self._fmt(self.spec_nominal)
        if self.spec_limit == 'max':
            return "≤ %s%s" % (n, unit)
        if self.spec_limit == 'min':
            return "≥ %s%s" % (n, unit)
        tu = '%' if self.spec_tol_pct else unit
        if not tol_minus and not tol_plus:
            return "%s%s" % (n, unit)
        if abs(tol_minus - tol_plus) < 1e-9:
            return "%s%s ± %s%s" % (n, unit, self._fmt(tol_plus), tu)
        return "%s%s +%s / −%s%s" % (n, unit, self._fmt(tol_plus), self._fmt(tol_minus), tu)

    @api.depends('kind', 'unit', 'spec_nominal', 'spec_limit', 'spec_tol_minus', 'spec_tol_plus', 'spec_tol_pct',
                 'spec_text', 'spec_bool', 'ctrl_tol_minus', 'ctrl_tol_plus', 'ctrl_defined')
    def _compute_labels(self):
        for line in self:
            line.spec_label = line._label(line.spec_tol_minus, line.spec_tol_plus)
            line.ctrl_label = (line._label(line.ctrl_tol_minus, line.ctrl_tol_plus)
                               if line.kind == 'num' and line.ctrl_defined else '')

    @api.constrains('spec_tol_minus', 'spec_tol_plus', 'ctrl_tol_minus', 'ctrl_tol_plus')
    def _check_tolerances(self):
        for line in self:
            if min(line.spec_tol_minus, line.spec_tol_plus, line.ctrl_tol_minus, line.ctrl_tol_plus) < 0:
                raise ValidationError("Las tolerancias se capturan como cantidades positivas (%s)." % line.name)
            if line.ctrl_defined and line.spec_limit == 'tol' and (
                    line.ctrl_tol_minus > line.spec_tol_minus + 1e-9 or line.ctrl_tol_plus > line.spec_tol_plus + 1e-9):
                raise ValidationError(
                    "El control interno de «%s» es más abierto que la tolerancia del cliente; debe ser igual o "
                    "más cerrado." % line.name)

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
                set(self._COMPUTED_COLUMNS) | {'type_id', 'spec_limit', 'spec_tol_minus', 'spec_tol_plus',
                                               'spec_tol_pct'}):
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
        same = lines.filtered(lambda l: l.type_code == code and l.direction == 'na')
        return (same.filtered(lambda l: l.position == 'na') or same.filtered(lambda l: l.position == 'centro'))[:1]

    def _sgi_dev_refresh_formulas(self):
        """Rellena los renglones calculados (rendimiento) con la masa y el
        ancho del mismo proyecto, columna por columna. La tolerancia del
        rendimiento sale de los extremos de masa y ancho."""
        Line = self.env['sgi.dev.characteristic']
        for project in self:
            lines = project.sgi_dev_line_ids
            targets = lines.filtered(lambda l: l.type_id.formula == 'rendimiento')
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
