# -*- coding: utf-8 -*-
"""Modelos de configuración: lo que el usuario llena en Odoo.

Todo lo que NO vive en modelos nativos (clasificación gerencial de cuentas,
mapeo cuenta→centro, factores de reparto, pesos por producto, ruteo, turnos)
se captura aquí. Los cálculos (vistas SQL y motor de costeo) leen estos
modelos + los registros nativos, así que "capturar en Odoo" basta para que
el modelo lo considere sin tocar código.
"""
import re

from odoo import api, fields, models
from odoo.exceptions import ValidationError

BUCKETS = [
    ('mp', 'Materia prima'),
    ('energia', 'Energía variable (luz/gas/agua)'),
    ('mod', 'Mano de obra directa'),
    ('overhead_fab', 'Overhead de fábrica'),
    ('depreciacion', 'Depreciación fábrica'),
    ('arrend_maquinaria', 'Arrendamiento de maquinaria'),
    ('operacion', 'Operación (admin / ventas)'),
    ('ventas', 'Ingresos (ventas)'),
    ('no_costeo', 'Fuera de costeo'),
]

DRIVERS = [
    ('peso', 'Peso (kg)'),
    ('largo', 'Largo (m)'),
    ('ventas', '% sobre ventas'),
    ('directo', 'Directo al centro'),
]

PRODUCT_BUCKETS = [
    ('tela', 'Tela (tejido + tintorería + acabado)'),
    ('entretela_tejida', 'Entretela tejida (tejido + tintorería + puntos)'),
    ('entretela_carda', 'Entretela carda / no tejida'),
    ('importado', 'Importado (solo inspección)'),
    ('subproducto', 'Subproducto / desperdicio'),
    ('servicio', 'Servicio / otro'),
]


class QbCosteoCentro(models.Model):
    _name = 'qb.costeo.centro'
    _description = 'Centro de costo / proceso productivo'
    _order = 'sequence, code'

    sequence = fields.Integer(default=10)
    code = fields.Char(required=True, index=True)
    name = fields.Char(required=True)
    active = fields.Boolean(default=True)
    company_id = fields.Many2one(
        'res.company', default=lambda self: self.env.company, required=True)
    nature = fields.Selection([
        ('fabril_directo', 'Fabril directo'),
        ('fabril_indirecto', 'Fabril indirecto'),
        ('admin', 'Administrativo'),
    ], required=True, default='fabril_directo')
    driver_principal = fields.Selection(
        [('peso', 'Peso (kg)'), ('largo', 'Largo (m)')],
        string='Driver principal', default='peso',
        help='Cómo absorbe este centro su gasto: por kg (tejido, tintorería) '
             'o por metro (acabado/rama, entretelas).')
    workcenter_ids = fields.Many2many(
        'mrp.workcenter', 'qb_centro_workcenter_rel', 'centro_id', 'workcenter_id',
        string='Centros de trabajo (Odoo)',
        help='Al ligar un mrp.workcenter nuevo aquí, entra solo a capacidad, '
             'balance y ociosidad — sin tocar código.')
    department_ids = fields.Many2many(
        'hr.department', 'qb_centro_department_rel', 'centro_id', 'department_id',
        string='Departamentos (RH)',
        help='Empleados de estos departamentos cuentan como dotación del centro.')
    output_uom_id = fields.Many2one('uom.uom', string='Unidad de salida')
    std_output_per_hour = fields.Float(
        string='Throughput nominal (unidades/hora/máquina)',
        help='kg/h por máquina de tejido, m/h por rama, kg/baño-hora en '
             'tintorería. Se usa cuando el workcenter no define capacity.')
    capacidad_normal = fields.Float(
        string='Capacidad normal (unidades/mes)',
        help='Capacidad NORMAL para costeo IAS 2 (costo fijo ÷ capacidad '
             'normal; la ociosidad va al P&L, no al producto). '
             '0 = derivar de calendario × throughput nominal.')
    renta_contractual_mxn = fields.Float(
        string='Renta contractual (MXN/mes)',
        help='Renta fija contractual del centro. Se usa en lugar del GL '
             'porque la renta se paga a saltos (un mes $0, el siguiente doble).')
    mo_name_pattern = fields.Char(
        string='Patrón de órdenes (fallback)',
        help="Patrón SQL LIKE sobre mrp.production.name (ej. 'TL/OP-ACA%') "
             'para atribuir producción cuando el centro aún NO tiene '
             'workcenters dados de alta. Al capturar workcenters reales, '
             'este patrón deja de ser necesario.')
    es_denominador_kg = fields.Boolean(
        string='Su producción define los kg del período',
        help='La producción de este centro entra al denominador de kg para '
             'el factor de fabricación por peso.')
    es_denominador_m = fields.Boolean(
        string='Su producción define los metros del período',
        help='La producción de este centro entra al denominador de metros '
             'para el factor de fabricación por largo.')
    notes = fields.Text()

    _sql_constraints = [
        ('code_company_uniq', 'unique(code, company_id)',
         'El código del centro debe ser único por compañía.'),
    ]


class QbCosteoCuentaClass(models.Model):
    _name = 'qb.costeo.cuenta.class'
    _description = 'Clasificación de cuentas contables para costeo'
    _order = 'bucket, code_pattern'

    name = fields.Char(compute='_compute_name', store=True)
    active = fields.Boolean(default=True)
    company_id = fields.Many2one(
        'res.company', default=lambda self: self.env.company, required=True)
    account_id = fields.Many2one(
        'account.account', string='Cuenta específica', ondelete='cascade',
        help='Clasificación explícita de una cuenta. Gana sobre cualquier patrón.')
    code_pattern = fields.Char(
        string='Patrón de código',
        help="Patrón SQL LIKE sobre el código de cuenta, ej. '501.06%'. "
             'Cuando un código matchea varios patrones, gana el más largo.')
    bucket = fields.Selection(BUCKETS, required=True)
    es_variable = fields.Boolean(
        string='Es variable',
        help='Variable = escala con el volumen (energía). Lo no variable es '
             'fijo y entra al costo de ociosidad.')
    centro_id = fields.Many2one(
        'qb.costeo.centro', string='Centro de costo',
        help='Asignación directa a un centro (ej. agua → Tintorería). '
             'Sin centro, el gasto se prorratea por el driver.')
    driver = fields.Selection(DRIVERS, help='Driver de reparto del gasto.')
    allocation_pct = fields.Float(
        string='% asignado', default=100.0,
        help='Porcentaje del gasto de la cuenta que entra a este bucket/centro.')
    notes = fields.Char()
    account_ids = fields.Many2many(
        'account.account', 'qb_cuenta_class_account_rel', 'class_id', 'account_id',
        string='Cuentas que matchean', readonly=True, copy=False,
        help='Resultado del matching (cuenta específica o patrón). Se '
             'refresca al guardar y con el cron nocturno, así una cuenta '
             'nueva en una familia ya clasificada entra sola.')

    @api.depends('account_id', 'code_pattern', 'bucket')
    def _compute_name(self):
        buckets = dict(BUCKETS)
        for rec in self:
            target = rec.account_id.display_name or rec.code_pattern or '?'
            rec.name = '%s → %s' % (target, buckets.get(rec.bucket, rec.bucket))

    @api.constrains('account_id', 'code_pattern')
    def _check_target(self):
        for rec in self:
            if not rec.account_id and not rec.code_pattern:
                raise ValidationError(
                    'Cada clasificación necesita una cuenta específica o un patrón de código.')

    @api.constrains('allocation_pct')
    def _check_pct(self):
        for rec in self:
            if not 0.0 <= rec.allocation_pct <= 100.0:
                raise ValidationError('% asignado debe estar entre 0 y 100.')

    def _recompute_matched_accounts(self):
        """Resuelve account_ids desde account_id / code_pattern.

        Se hace en Python (no en SQL) porque el código de cuenta en Odoo 19
        es company-dependent (code_store jsonb); el ORM lo resuelve bien.
        """
        Account = self.env['account.account'].with_context(active_test=False)
        for rec in self:
            accounts = Account.browse()
            if rec.account_id:
                accounts |= rec.account_id
            if rec.code_pattern:
                accounts |= Account.with_company(rec.company_id).search(
                    [('code', '=like', rec.code_pattern)])
            rec.account_ids = accounts

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        records._recompute_matched_accounts()
        return records

    def write(self, vals):
        res = super().write(vals)
        if any(f in vals for f in ('account_id', 'code_pattern', 'company_id', 'active')):
            self._recompute_matched_accounts()
        return res

    @api.model
    def cron_refresh_account_matching(self):
        """Cron nocturno: cuentas nuevas del plan entran solas al costeo."""
        self.with_context(active_test=False).search([])._recompute_matched_accounts()

    def action_refresh_accounts(self):
        self._recompute_matched_accounts()
        return True

    @api.model
    def action_unclassified_accounts(self):
        """Cuentas de resultados (4xx-7xx) sin clasificar — pendientes."""
        mapped = self.env['qb.costeo.cuenta.map'].search([]).mapped('account_id')
        candidates = self.env['account.account'].search([])
        pending = candidates.filtered(
            lambda a: a.code and a.code[:1] in '4567' and a not in mapped)
        return {
            'type': 'ir.actions.act_window',
            'name': 'Cuentas pendientes de clasificar',
            'res_model': 'account.account',
            'view_mode': 'list,form',
            'domain': [('id', 'in', pending.ids)],
        }


class QbCosteoFactorConfig(models.Model):
    _name = 'qb.costeo.factor.config'
    _description = 'Parámetros globales de costeo'
    _order = 'key'
    _rec_name = 'key'

    key = fields.Char(required=True, index=True)
    value = fields.Float(digits=(16, 6))
    value_text = fields.Char(
        help='Para parámetros de texto (ej. patrones regex).')
    descripcion = fields.Char()
    active = fields.Boolean(default=True)
    company_id = fields.Many2one(
        'res.company', default=lambda self: self.env.company, required=True)

    _sql_constraints = [
        ('key_company_uniq', 'unique(key, company_id)',
         'Cada parámetro es único por compañía.'),
    ]

    @api.model
    def get_param(self, key, default=0.0):
        rec = self.search([('key', '=', key)], limit=1)
        return rec.value if rec else default

    @api.model
    def get_param_text(self, key, default=''):
        rec = self.search([('key', '=', key)], limit=1)
        return rec.value_text if rec and rec.value_text else default


class QbProductoPeso(models.Model):
    _name = 'qb.producto.peso'
    _description = 'Peso por unidad y conversión kg↔m por producto'
    _order = 'product_id'

    product_id = fields.Many2one(
        'product.product', required=True, index=True, ondelete='cascade')
    kg_per_unit = fields.Float(
        string='kg por unidad', digits=(16, 6),
        help='Para tela vendida en metros: kg por metro (gramaje × ancho). '
             'Para productos en kg: 1.')
    m_per_kg = fields.Float(
        string='m por kg', digits=(16, 6),
        help='Conversión kg→metros para tela vendida por peso.')
    source = fields.Selection([
        ('manual', 'Manual (maestro de ingeniería)'),
        ('cvu', 'CVU (conversión medida en planta)'),
        ('ref_gramaje', 'Gramaje del ref'),
        ('bom', 'Peso por receta (BOM)'),
        ('odoo_weight', 'Peso de Odoo'),
        ('import_twin', 'Gemelo nacional'),
    ], default='manual', required=True,
        help='Prioridad al resolver: manual > cvu > ref_gramaje > bom > odoo_weight.')
    active = fields.Boolean(default=True)
    notes = fields.Char()

    _sql_constraints = [
        ('product_uniq', 'unique(product_id)',
         'Solo un registro de peso por producto (edítalo en lugar de duplicar).'),
    ]

    # Prioridad de fuentes: menor = gana.
    _SOURCE_PRIORITY = {
        'manual': 0, 'cvu': 1, 'ref_gramaje': 2,
        'bom': 3, 'odoo_weight': 4, 'import_twin': 5,
    }

    @api.model
    def resolve_kg_per_unit(self, product):
        """kg por unidad de venta, con la cadena de prioridad documentada.

        1. Registro en esta tabla (manual/cvu/etc.).
        2. UoM en kg → 1.0.
        3. Gramaje del ref: primer bloque numérico tras las letras con
           EXACTAMENTE 3 dígitos = g/m², × ancho (últimos dígitos /100).
           (Un bloque de 4 dígitos es código de resina — 4032/9032 — NO gramaje.)
        4. weight de Odoo solo si cae en rango creíble 0.01–1.5 kg/unidad.
        5. Gemelo nacional para importados (' I').
        """
        rec = self.search([('product_id', '=', product.id)], limit=1)
        if rec and rec.kg_per_unit:
            return rec.kg_per_unit
        uom_name = (product.uom_id.name or '').lower()
        if uom_name in ('kg', 'kgs', 'kilogramo', 'kilogramos'):
            return 1.0
        ref = product.default_code or ''
        if ref.endswith(' I'):
            twin = self.env['product.product'].search(
                [('default_code', '=', ref[:-2].strip())], limit=1)
            if twin:
                return self.resolve_kg_per_unit(twin)
        gramaje = self._gramaje_from_ref(ref)
        if gramaje:
            return gramaje
        weight = product.weight or 0.0
        if 0.01 <= weight <= 1.5:
            return weight
        return 0.0

    @api.model
    def _gramaje_from_ref(self, ref):
        """WJ045NT160 → 45 g/m² × 1.60 m = 0.072 kg/m. Solo bloques de
        exactamente 3 dígitos (4 dígitos = código de resina)."""
        m = re.match(r'^[A-Za-z]+(\d{3})(?!\d)', ref or '')
        if not m:
            return 0.0
        gramaje = int(m.group(1))
        ancho_m = re.search(r'(\d{2,3})\s*I?$', ref or '')
        ancho = int(ancho_m.group(1)) / 100.0 if ancho_m else 1.5
        if not 0.3 <= ancho <= 3.5:
            ancho = 1.5
        return gramaje / 1000.0 * ancho

    @api.model
    def resolve_m_per_kg(self, product):
        rec = self.search([('product_id', '=', product.id)], limit=1)
        if rec and rec.m_per_kg:
            return rec.m_per_kg
        kg = self.resolve_kg_per_unit(product)
        uom_name = (product.uom_id.name or '').lower()
        if kg and uom_name not in ('kg', 'kgs', 'kilogramo', 'kilogramos'):
            # Producto en metros: kg = kg/m → m/kg es su inverso.
            return 1.0 / kg
        return self.env['qb.costeo.factor.config'].get_param('m_per_kg_default', 8.0)


class QbProductoRuteo(models.Model):
    _name = 'qb.producto.ruteo'
    _description = 'Ruteo producto/familia → centros de proceso'
    _order = 'sequence, id'

    sequence = fields.Integer(default=10)
    active = fields.Boolean(default=True)
    product_id = fields.Many2one(
        'product.product', string='Producto', ondelete='cascade',
        help='Regla específica de un producto. Gana sobre categoría y patrones.')
    categ_id = fields.Many2one(
        'product.category', string='Categoría',
        help='Aplica a la categoría y sus hijas.')
    categ_pattern = fields.Char(
        string='Patrón de categoría',
        help="ILIKE sobre el nombre completo de la categoría, ej. '%Entretela%'.")
    name_pattern = fields.Char(
        string='Patrón de nombre/ref',
        help="Regex sobre nombre o referencia interna, ej. '^(SALDO|DESPERDICIO)' o ' I$'.")
    product_bucket = fields.Selection(
        PRODUCT_BUCKETS, required=True, string='Clasificación',
        help='Familia de costeo del producto (define qué factores carga).')
    centro_ids = fields.Many2many(
        'qb.costeo.centro', 'qb_ruteo_centro_rel', 'ruteo_id', 'centro_id',
        string='Centros por los que pasa',
        help='Cuando Odoo tenga rutas MRP por producto, leerlas de ahí; '
             'mientras, esta tabla suple el ruteo.')
    notes = fields.Char()

    @api.constrains('product_id', 'categ_id', 'categ_pattern', 'name_pattern')
    def _check_target(self):
        for rec in self:
            if not (rec.product_id or rec.categ_id or rec.categ_pattern or rec.name_pattern):
                raise ValidationError(
                    'Cada regla de ruteo necesita producto, categoría o patrón.')

    @api.model
    def resolve(self, product):
        """(product_bucket, centros) para un producto.

        Prioridad: regla por producto > patrón de nombre/ref (subproducto,
        importado ' I$' — señales más fuertes que la categoría) > categoría
        (child_of) > patrón de categoría. Dentro de cada nivel gana la
        primera regla por sequence. Si Odoo trae rutas MRP por producto en
        el futuro, este método es el único punto a cambiar.
        """
        rules = self.search([])
        ref = product.default_code or ''
        name = product.name or ''
        categ_name = product.categ_id.complete_name or ''
        # 1. Producto exacto
        for r in rules:
            if r.product_id and r.product_id == product:
                return r.product_bucket, r.centro_ids
        # 2. Patrón de nombre/ref
        for r in rules:
            if r.name_pattern and not (r.product_id or r.categ_id or r.categ_pattern):
                try:
                    if re.search(r.name_pattern, ref) or re.search(r.name_pattern, name):
                        return r.product_bucket, r.centro_ids
                except re.error:
                    continue
        # 3. Categoría (incluye hijas, vía parent_path)
        parent_ids = [int(x) for x in (product.categ_id.parent_path or '').split('/') if x]
        for r in rules:
            if r.categ_id and r.categ_id.id in parent_ids:
                return r.product_bucket, r.centro_ids
        # 4. Patrón de categoría (opcionalmente combinado con name_pattern)
        for r in rules:
            if r.categ_pattern and r.categ_pattern.strip('%').lower() in categ_name.lower():
                if r.name_pattern:
                    try:
                        if not (re.search(r.name_pattern, ref)
                                or re.search(r.name_pattern, name)):
                            continue
                    except re.error:
                        continue
                return r.product_bucket, r.centro_ids
        return 'tela', self.env['qb.costeo.centro'].browse()


class QbTurnoConfig(models.Model):
    _name = 'qb.turno.config'
    _description = 'Turnos / capacidad manual por centro (fallback de resource.calendar)'
    _order = 'centro_id'

    centro_id = fields.Many2one('qb.costeo.centro', required=True, ondelete='cascade')
    name = fields.Char(required=True)
    active = fields.Boolean(default=True)
    hours_per_week = fields.Float(
        string='Horas/semana',
        help='Horas de operación semanales del centro. Preferir capturar el '
             'resource.calendar del workcenter — esto es solo el fallback '
             'mientras el centro no tenga workcenters dados de alta.')
    machine_count = fields.Integer(
        string='Nº de máquinas/posiciones', default=1)
    dotacion_ajuste = fields.Float(
        string='Ajuste de dotación',
        help='Empleados extra (+) o descansando (−) que el calendario no expresa.')
    notes = fields.Char()

    def hours_per_month(self):
        self.ensure_one()
        weeks = self.env['qb.costeo.factor.config'].get_param('weeks_per_month', 4.33)
        return self.hours_per_week * weeks * max(self.machine_count, 1)
