# -*- coding: utf-8 -*-
"""Catálogo de características y renglones de especificación (2.1.0).

Decisión de Jose (2026-10-06, migración del procedimiento C1 Desarrollo y
alta de producto): la ficha técnica del artículo y el catálogo de
características viven aquí, no en el SGI. El SGI (``sgi.dev.characteristic``)
usa el mismo catálogo y el mismo mixin de límites para la tabla del proyecto
de desarrollo, de modo que al liberar un artículo los renglones pasen a su
ficha sin recaptura.

- ``ficha.tecnica.caracteristica``: la característica (clave estable, nombre
  en español e inglés, tipo de dato numérico / cualitativo / sí-no, unidad,
  método o norma, cálculo).
- ``ficha.tecnica.clave.codigo``: las claves de la regla de codificación de
  artículos de tejido y acabado (DAT P-D02-01 rev. 05): composición, dibujo,
  tipo de hilo, galga por rango, operación, color y acabado.
- ``ficha.tecnica.caracteristica.mixin``: un renglón con **dos juegos de
  límites**: la especificación del cliente (nominal, tolerancia ± en unidades
  o en %, o un máximo / mínimo) y el control interno (más cerrado, sobre el
  mismo nominal; nunca sale al cliente), más las marcas «va a la
  especificación del cliente» y «va al certificado».
- ``ficha.tecnica.spec``: ese renglón colgado de una ficha de tejido o de
  acabado.

Los datos del catálogo van en ``data/ficha_tecnica_caracteristica_data.xml``
(``noupdate``: se ajustan desde el menú de configuración sin que el update
los revierta).
"""
from odoo import api, fields, models
from odoo.exceptions import ValidationError

DIRECTIONS = [('na', "—"), ('largo', "Largo"), ('ancho', "Ancho")]
POSITIONS = [('na', "—"), ('izquierda', "Izquierda"), ('centro', "Centro"), ('derecha', "Derecha")]
KINDS = [('num', "Numérica"), ('text', "Cualitativa"), ('bool', "Sí / no")]
LIMITS = [('tol', "Nominal ± tolerancia"), ('max', "Máximo"), ('min', "Mínimo")]
RESULTS = [('cumple', "Cumple"), ('desviacion', "Fuera del control interno"), ('no_conforme', "No conforme")]
CODE_KINDS = [
    ('composicion', "Composición (posición 1)"),
    ('dibujo', "Dibujo (posición 2)"),
    ('hilo', "Tipo de hilo (posición 6)"),
    ('galga', "Galga (posiciones 7 y 8)"),
    ('operacion', "Operación (posición 9)"),
    ('color', "Color (posiciones 10 y 11)"),
    ('acabado', "Acabado (posiciones 15 y 16)"),
]
# Claves del catálogo con significado en el código: el rendimiento se
# calcula con la masa y el ancho del mismo documento.
CODE_MASS = 'masa'
CODE_WIDTH = 'ancho'


class FichaTecnicaCaracteristica(models.Model):
    """Característica del catálogo (unidad, método o norma y tipo de dato)."""
    _name = 'ficha.tecnica.caracteristica'
    _description = "Característica de ficha técnica (catálogo)"
    _order = 'sequence, name, id'

    sequence = fields.Integer(default=10, help="Orden en el que aparece en los catálogos.")
    code = fields.Char(string="Clave", required=True, index=True,
                       help="Clave corta y estable de la característica (p. ej. «masa», «ancho»). Con ella se "
                            "reconocen los renglones al pasar del proyecto de desarrollo a la ficha del artículo "
                            "y al calcular el rendimiento.")
    name = fields.Char(string="Característica", required=True,
                       help="Nombre con el que se ve en las tablas y en los PDF.")
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
                               help="Si la característica se calcula a partir de otras del mismo documento. El "
                                    "rendimiento (m/kg) sale de la masa (g/m²) y el ancho (m).")
    active = fields.Boolean(default=True, help="Las características archivadas no se proponen en documentos nuevos.")

    _code_uniq = models.Constraint(
        'unique(code)',
        "La clave de la característica debe ser única.",
    )

    @api.model
    def _by_code(self, code):
        return self.with_context(active_test=False).search([('code', '=', code)], limit=1)


class FichaTecnicaClaveCodigo(models.Model):
    """Clave de la regla de codificación de artículos de tejido y acabado (DAT P-D02-01)."""
    _name = 'ficha.tecnica.clave.codigo'
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

    @api.depends('code', 'name', 'kind', 'gauge', 'range_from', 'range_to')
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


class FichaTecnicaCaracteristicaMixin(models.AbstractModel):
    """Renglón de característica con dos juegos de límites: la especificación del cliente y el
    control interno (más cerrado, nunca sale al cliente)."""
    _name = 'ficha.tecnica.caracteristica.mixin'
    _description = "Renglón de característica con límites"

    sequence = fields.Integer(default=10, help="Orden del renglón en la tabla.")
    caracteristica_id = fields.Many2one('ficha.tecnica.caracteristica', string="Catálogo", index=True,
                                        ondelete='restrict',
                                        help="Característica del catálogo. Al elegirla se llenan nombre, unidad, "
                                             "método y tipo de dato; un renglón sin catálogo se escribe libre.")
    caracteristica_code = fields.Char(related='caracteristica_id.code', string="Clave", store=True)
    name = fields.Char(string="Característica", required=True, compute='_compute_from_caracteristica', store=True,
                       readonly=False, precompute=True,
                       help="Nombre de la característica (sale del catálogo; se puede ajustar).")
    # Sin default: un valor por omisión entraría en los vals del create y el
    # ORM dejaría de calcularlo desde el catálogo (CI 2026-10-06: «tacto» salía
    # numérica). El cálculo pone «num» cuando no hay catálogo.
    kind = fields.Selection(KINDS, string="Tipo de dato", compute='_compute_from_caracteristica', store=True,
                            readonly=False, precompute=True, required=True,
                            help="Numérica (valor y tolerancia), cualitativa (texto) o sí / no.")
    unit = fields.Char(string="Unidad", compute='_compute_from_caracteristica', store=True, readonly=False,
                       precompute=True, help="Unidad de medida del renglón.")
    method = fields.Char(string="Método / norma", compute='_compute_from_caracteristica', store=True,
                         readonly=False, precompute=True, help="Método de prueba o norma con la que se mide.")
    direction = fields.Selection(DIRECTIONS, default='na', string="Dirección", required=True,
                                 help="Largo o ancho cuando se mide en las dos direcciones.")
    position = fields.Selection(POSITIONS, default='na', string="Posición", required=True,
                                help="Izquierda, centro o derecha cuando se mide en tres puntos.")
    # --- Especificación del cliente --------------------------------------------
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
    # --- Control interno ---------------------------------------------------------
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
    # --- Documentos ---------------------------------------------------------------
    in_customer_spec = fields.Boolean(string="En especificación del cliente", default=True,
                                      help="El renglón aparece en las Especificaciones del producto que se entregan "
                                           "al cliente.")
    in_coa = fields.Boolean(string="En certificado",
                            help="El renglón aparece en el certificado de calidad del lote (siempre contra la "
                                 "especificación del cliente, nunca el control interno).")
    note = fields.Char(string="Observaciones", help="Texto libre; no capture aquí valores.")

    @api.depends('caracteristica_id')
    def _compute_from_caracteristica(self):
        for line in self:
            c = line.caracteristica_id
            if c:
                line.name = c.name
                line.kind = c.kind
                line.unit = c.unit
                line.method = c.method
            else:
                line.name = line.name or ''
                line.kind = line.kind or 'num'
                line.unit = line.unit or ''
                line.method = line.method or ''

    @staticmethod
    def _bounds(nominal, limit, tol_minus, tol_plus, pct):
        """(mín, máx) de un nominal con su tolerancia. Con «Máximo» el mínimo no
        aplica y con «Mínimo» el máximo no aplica (se devuelve None). El
        porcentaje se toma sobre |nominal| para que un nominal negativo
        (encogimiento) no invierta el margen."""
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

    def _limits(self, which):
        """(mín, máx) del cliente ('spec') o del control interno ('ctrl'), con None donde no aplica."""
        self.ensure_one()
        tol_minus, tol_plus = ((self.ctrl_tol_minus, self.ctrl_tol_plus) if which == 'ctrl' and self.ctrl_defined
                               else (self.spec_tol_minus, self.spec_tol_plus))
        return self._bounds(self.spec_nominal, self.spec_limit, tol_minus, tol_plus, self.spec_tol_pct)

    @staticmethod
    def _within(value, bounds):
        lo, hi = bounds
        return (lo is None or value >= lo - 1e-9) and (hi is None or value <= hi + 1e-9)

    def _has_spec(self):
        self.ensure_one()
        return bool(self.spec_nominal or self.spec_tol_minus or self.spec_tol_plus or self.spec_limit != 'tol')

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

    def _limit_vals(self):
        """Los valores del renglón que se copian de un documento a otro (del proyecto a la ficha)."""
        self.ensure_one()
        return {
            'caracteristica_id': self.caracteristica_id.id, 'name': self.name, 'kind': self.kind,
            'unit': self.unit, 'method': self.method, 'direction': self.direction, 'position': self.position,
            'sequence': self.sequence, 'spec_nominal': self.spec_nominal, 'spec_limit': self.spec_limit,
            'spec_tol_minus': self.spec_tol_minus, 'spec_tol_plus': self.spec_tol_plus,
            'spec_tol_pct': self.spec_tol_pct, 'spec_text': self.spec_text, 'spec_bool': self.spec_bool,
            'ctrl_tol_minus': self.ctrl_tol_minus, 'ctrl_tol_plus': self.ctrl_tol_plus,
            'in_customer_spec': self.in_customer_spec, 'in_coa': self.in_coa, 'note': self.note,
        }


class FichaTecnicaSpec(models.Model):
    """Renglón de característica de una ficha técnica de tejido o de acabado."""
    _name = 'ficha.tecnica.spec'
    _description = "Característica de la ficha técnica"
    _inherit = 'ficha.tecnica.caracteristica.mixin'
    _order = 'sequence, id'

    tejido_id = fields.Many2one('ficha.tecnica.tejido', string="Ficha de tejido", ondelete='cascade', index=True,
                                help="Ficha de tejido a la que pertenece el renglón.")
    acabado_id = fields.Many2one('ficha.tecnica.acabado', string="Ficha de acabado", ondelete='cascade', index=True,
                                 help="Ficha de acabado a la que pertenece el renglón.")

    @api.constrains('tejido_id', 'acabado_id')
    def _check_parent(self):
        for line in self:
            if bool(line.tejido_id) == bool(line.acabado_id):
                raise ValidationError("Cada característica pertenece a una ficha de tejido o a una de acabado.")

    @api.model_create_multi
    def create(self, vals_list):
        # El constraint solo corre sobre los campos presentes en el create: un
        # renglón sin ninguna ficha no lo dispararía (CI 2026-10-06).
        lines = super().create(vals_list)
        lines._check_parent()
        return lines


class FichaTecnicaTejidoSpecs(models.Model):
    _inherit = 'ficha.tecnica.tejido'

    spec_line_ids = fields.One2many('ficha.tecnica.spec', 'tejido_id', string="Características",
                                    help="Características del tejido con la especificación del cliente y el "
                                         "control interno.")


class FichaTecnicaAcabadoSpecs(models.Model):
    _inherit = 'ficha.tecnica.acabado'

    spec_line_ids = fields.One2many('ficha.tecnica.spec', 'acabado_id', string="Características",
                                    help="Características del producto acabado con la especificación del cliente "
                                         "y el control interno. El certificado y las Especificaciones del producto "
                                         "se imprimen contra la especificación del cliente.")
