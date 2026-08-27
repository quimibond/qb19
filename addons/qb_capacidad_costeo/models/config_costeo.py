# -*- coding: utf-8 -*-
"""Modelos de configuración: lo que el usuario llena en Odoo.

Todo lo que NO vive en modelos nativos (clasificación gerencial de cuentas,
mapeo cuenta→centro, factores de reparto, pesos por producto, ruteo, turnos)
se captura aquí. Los cálculos (vistas SQL y motor de costeo) leen estos
modelos + los registros nativos, así que "capturar en Odoo" basta para que
el modelo lo considere sin tocar código.
"""
import csv
import logging
import re

from odoo import api, fields, models, tools
from odoo.exceptions import ValidationError

_logger = logging.getLogger(__name__)

BUCKETS = [
    ('mp', 'Materia prima'),
    ('energia', 'Energía variable (luz/gas/agua)'),
    ('mod', 'Mano de obra directa'),
    ('overhead_fab', 'Overhead de fábrica'),
    ('depreciacion', 'Depreciación fábrica'),
    ('arrend_maquinaria', 'Arrendamiento de maquinaria'),
    ('importacion', 'Gastos e impuestos de importación'),
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

    _code_company_uniq = models.Constraint(
        'unique(code, company_id)',
        "El código del centro debe ser único por compañía.",
    )


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
    es_renta = fields.Boolean(
        string='Es renta de inmueble',
        help='La cuenta lleva renta o arrendamiento de inmueble. El motor la '
             'SACA del pool de fabricación y en su lugar usa la renta '
             'contractual capturada en cada centro — el GL de renta se paga '
             'a saltos (un mes $0, el siguiente doble) y el contrato es el '
             'número estable.\n\n'
             'Sin esta bandera la renta se contaría dos veces: una por el GL '
             'y otra por el contrato.')
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

    # Renta de inmueble: nombres con los que aparece en el plan contable, y
    # los que la descartan (el arrendamiento de MAQUINARIA sí es costo fabril
    # del GL y no tiene contrato capturado que lo sustituya).
    _RENTA_TOKENS = ('RENTA', 'ARRENDAMIENTO')
    _RENTA_EXCLUDE = ('MAQUINARIA', 'EQUIPO', 'TRANSPORTE', 'VEHIC')

    @api.model
    def _es_cuenta_de_renta(self, account):
        """¿La cuenta lleva renta de INMUEBLE? Se resuelve por nombre vía ORM
        (el nombre es traducible: en SQL sería un jsonb y no se puede filtrar
        con un LIKE simple)."""
        nombre = ((account.name or '') + ' ' + (account.code or '')).upper()
        if not any(t in nombre for t in self._RENTA_TOKENS):
            return False
        return not any(t in nombre for t in self._RENTA_EXCLUDE)

    @api.model
    def marcar_cuentas_de_renta(self):
        """Marca `es_renta` en las clasificaciones cuyas cuentas son renta de
        inmueble. Sin esta bandera el motor cuenta la renta dos veces: una por
        el GL y otra por el contrato del centro. Idempotente."""
        marcadas = self.browse()
        for rec in self.with_context(active_test=False).search(
                [('es_renta', '=', False)]):
            if any(self._es_cuenta_de_renta(a) for a in rec.account_ids):
                marcadas |= rec
        if marcadas:
            marcadas.es_renta = True
            _logger.info('qb_capacidad_costeo: %s clasificaciones marcadas '
                         'como renta de inmueble: %s', len(marcadas),
                         ', '.join(marcadas.mapped('name')))
        return marcadas

    def action_marcar_rentas(self):
        self.marcar_cuentas_de_renta()
        return True

    # Gastos e impuestos de importación: IGI (impuesto general de
    # importación), DTA (derecho de trámite aduanero), PRV (prevalidación) y
    # los gastos de agente aduanal/flete. Los tres acrónimos van con límite
    # de palabra: como subcadena matchearían cualquier cosa.
    _IMPORT_RE = re.compile(
        r'IMPORTACION|IMPORTACIÓN|\bIGI\b|\bDTA\b|\bPRV\b'
        r'|AGENTE ADUANAL|PEDIMENTO')

    # Cuentas de RESULTADOS: los reconocedores por nombre solo deben mover
    # cuentas de gasto. 'INVENTARIO DE MATERIA PRIMA' es un activo y matchea
    # el patrón de MP, pero no es consumo — meterlo al bucket falsearía la
    # conciliación.
    _EXPENSE_TYPES = ('expense', 'expense_depreciation', 'expense_direct_cost')

    @api.model
    def _es_cuenta_de_resultados(self, account):
        return account.account_type in self._EXPENSE_TYPES

    @api.model
    def _es_cuenta_de_importacion(self, account):
        """¿La cuenta lleva gasto o impuesto de importación? Se reparte sobre
        el valor de lo importado, no sobre las ventas."""
        if not self._es_cuenta_de_resultados(account):
            return False
        nombre = ((account.name or '') + ' ' + (account.code or '')).upper()
        if 'EXPORTACION' in nombre or 'EXPORTACIÓN' in nombre:
            return False
        return bool(self._IMPORT_RE.search(nombre))

    @api.model
    def reclasificar_cuentas_de_importacion(self):
        """Mueve al bucket `importacion` las cuentas de importación que hoy
        están FUERA de costeo.

        Solo toca `no_costeo` a propósito: esas cuentas hoy aportan cero a
        cualquier pool, así que moverlas no puede provocar doble conteo. Una
        cuenta de importación ya clasificada en otro bucket se deja quieta y
        se reporta — moverla sí cambiaría un reparto existente y esa decisión
        es del usuario. Idempotente."""
        movidas = self.browse()
        for rec in self.with_context(active_test=False).search(
                [('bucket', '=', 'no_costeo')]):
            if any(self._es_cuenta_de_importacion(a) for a in rec.account_ids):
                movidas |= rec
        if movidas:
            movidas.bucket = 'importacion'
            _logger.info('qb_capacidad_costeo: %s cuentas de importación '
                         'movidas de no_costeo a importacion: %s',
                         len(movidas), ', '.join(movidas.mapped('name')))
        return movidas

    # Materia prima realmente consumida: el costo primo del mayor más los
    # ajustes de inventario (la merma que la receta no lleva). Es el número
    # contra el que se concilia la MP de receta.
    _MP_RE = re.compile(
        r'COSTO PRIMO|MATERIA PRIMA|AJUSTES? A CANTIDAD'
        r'|DIFERENCIAS? POR CONTEO|AJUSTE DE INVENTARIO')

    @api.model
    def _es_cuenta_de_materia_prima(self, account):
        """¿La cuenta lleva el consumo real de materia prima?"""
        if not self._es_cuenta_de_resultados(account):
            return False
        nombre = ((account.name or '') + ' ' + (account.code or '')).upper()
        return bool(self._MP_RE.search(nombre))

    @api.model
    def reclasificar_cuentas_de_materia_prima(self):
        """Mueve al bucket `mp` las cuentas de costo primo que hoy están
        FUERA de costeo.

        Igual que con importación, solo toca `no_costeo`: hoy aportan cero.
        Y el bucket `mp` no se suma a ningún pool — es únicamente el número
        contra el que se concilia la MP de receta, así que moverlas ahí no
        puede inflar ningún costo; lo único que hace es destapar el ajuste.
        Idempotente."""
        movidas = self.browse()
        for rec in self.with_context(active_test=False).search(
                [('bucket', '=', 'no_costeo')]):
            if any(self._es_cuenta_de_materia_prima(a) for a in rec.account_ids):
                movidas |= rec
        if movidas:
            movidas.bucket = 'mp'
            _logger.info('qb_capacidad_costeo: %s cuentas de materia prima '
                         'movidas de no_costeo a mp: %s', len(movidas),
                         ', '.join(movidas.mapped('name')))
        return movidas

    @api.model
    def cuentas_de_importacion_mal_ubicadas(self):
        """Clasificaciones de importación que están en un bucket que las
        reparte por el driver equivocado (típicamente `operacion`, o sea
        prorrateadas sobre TODAS las ventas en vez de sobre lo importado)."""
        return self.with_context(active_test=False).search([
            ('bucket', 'not in', ('importacion', 'no_costeo')),
        ]).filtered(lambda c: any(
            self._es_cuenta_de_importacion(a) for a in c.account_ids))

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

    _key_company_uniq = models.Constraint(
        'unique(key, company_id)',
        "Cada parámetro es único por compañía.",
    )

    @api.model
    def _get_record(self, key):
        """Parámetro de la compañía activa; fallback a cualquier compañía
        (instalaciones mono-compañía seedean solo una)."""
        rec = self.search([('key', '=', key),
                           ('company_id', '=', self.env.company.id)], limit=1)
        return rec or self.search([('key', '=', key)], limit=1)

    @api.model
    def get_param(self, key, default=0.0):
        rec = self._get_record(key)
        return rec.value if rec else default

    @api.model
    def get_param_text(self, key, default=''):
        rec = self._get_record(key)
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
        ('kg_native', 'UoM en kg (= 1)'),
    ], default='manual', required=True,
        help='Prioridad al resolver: manual > cvu > ref_gramaje > bom > odoo_weight.')
    active = fields.Boolean(default=True)
    notes = fields.Char()

    _product_uniq = models.Constraint(
        'unique(product_id)',
        "Solo un registro de peso por producto (edítalo en lugar de duplicar).",
    )

    # Prioridad de fuentes: menor = gana.
    _SOURCE_PRIORITY = {
        'manual': 0, 'cvu': 1, 'ref_gramaje': 2,
        'bom': 3, 'odoo_weight': 4, 'import_twin': 5,
    }

    @api.model
    def resolve_kg_per_unit(self, product, cache=None):
        """kg por unidad de venta, con la cadena de prioridad documentada.

        1. Registro en esta tabla (manual/cvu/etc.).
        2. UoM en kg → 1.0.
        3. Gramaje del ref: primer bloque numérico tras las letras con
           EXACTAMENTE 3 dígitos = g/m², × ancho (últimos dígitos /100).
           (Un bloque de 4 dígitos es código de resina — 4032/9032 — NO gramaje.)
        4. weight de Odoo solo si cae en rango creíble 0.01–1.5 kg/unidad.
        5. Gemelo nacional para importados (' I').

        `cache` (dict opcional {product_id: kg}) evita re-resolver en loops
        grandes (motor de costeo): un search por producto no escala a 3k SKUs.
        """
        if cache is not None and product.id in cache:
            return cache[product.id]
        kg, _src = self._resolve_kg_source(product, cache)
        if cache is not None:
            cache[product.id] = kg
        return kg

    # Fuentes de peso que NO son medidas: son adivinanzas del código o
    # placeholders de Odoo. El motor las marca como 'peso_estimado' para que
    # el usuario sepa que hay que verificar ese peso (no fingir que es 'ok').
    PESO_SOURCES_ESTIMADAS = ('ref_gramaje', 'odoo_weight')

    @api.model
    def resolve_kg_source(self, product, cache=None):
        """Fuente del peso resuelto. Confiables: manual/cvu/kg_native/record/
        import_twin. ESTIMADAS (adivinanza): ref_gramaje, odoo_weight.
        Falta: sin_peso. Se usa para levantar la alerta 'peso_estimado'."""
        _kg, src = self._resolve_kg_source(product, cache)
        return src

    @api.model
    def _resolve_kg_source(self, product, cache=None):
        """Resuelve (kg, fuente) en una pasada. La fuente de un registro del
        maestro es su propio `source` (un registro con source='ref_gramaje'
        SIGUE siendo estimado — caso WD080)."""
        rec = self.search([('product_id', '=', product.id)], limit=1)
        if rec and rec.kg_per_unit:
            return rec.kg_per_unit, (rec.source or 'record')
        uom_name = (product.uom_id.name or '').lower()
        if uom_name in ('kg', 'kgs', 'kilogramo', 'kilogramos'):
            return 1.0, 'kg_native'
        ref = product.default_code or ''
        if ref.endswith(' I'):
            twin = self.env['product.product'].search(
                [('default_code', '=', ref[:-2].strip())], limit=1)
            if twin:
                kg, _src = self._resolve_kg_source(twin, cache)
                return kg, 'import_twin'
        gramaje = self._gramaje_from_ref(ref)
        if gramaje:
            return gramaje, 'ref_gramaje'
        weight = product.weight or 0.0
        if 0.01 <= weight <= 1.5:
            return weight, 'odoo_weight'
        return 0.0, 'sin_peso'

    @api.model
    def _resolve_kg_per_unit(self, product, cache=None):
        kg, _src = self._resolve_kg_source(product, cache)
        return kg

    @api.model
    def _gramaje_from_ref(self, ref):
        """WJ045NT160 → 45 g/m² × 1.60 m = 0.072 kg/m. Solo bloques de
        exactamente 3 dígitos (4 dígitos = código de resina)."""
        ref = ref or ''
        m = re.match(r'^[A-Za-z]+(\d{3})(?!\d)', ref)
        if not m:
            return 0.0
        gramaje = int(m.group(1))
        # El ancho debe ser un bloque DISTINTO del gramaje: se busca sólo en
        # lo que va DESPUÉS del gramaje. Así 'WD080' (sin ancho explícito) no
        # toma sus propios '080' como ancho 0.80 m (hallazgo #3).
        ancho_m = re.search(r'(\d{2,3})\s*I?$', ref[m.end():])
        ancho = int(ancho_m.group(1)) / 100.0 if ancho_m else 1.5
        if not 0.3 <= ancho <= 3.5:
            ancho = 1.5
        return gramaje / 1000.0 * ancho

    @api.model
    def resolve_m_per_kg(self, product, cache=None):
        rec = self.search([('product_id', '=', product.id)], limit=1)
        if rec and rec.m_per_kg:
            return rec.m_per_kg
        kg = self.resolve_kg_per_unit(product, cache)
        uom_name = (product.uom_id.name or '').lower()
        if kg and uom_name not in ('kg', 'kgs', 'kilogramo', 'kilogramos'):
            # Producto en metros: kg = kg/m → m/kg es su inverso.
            return 1.0 / kg
        return self.env['qb.costeo.factor.config'].get_param('m_per_kg_default', 8.0)

    # ------------------------------------------------------------------
    # Maestro de pesos NATIVO (sin Supabase)
    # ------------------------------------------------------------------
    @api.model
    def load_weight_master(self):
        """Carga los pesos MEDIDOS/de ingeniería desde el archivo nativo
        data/product_weights.csv, matcheando por código de producto
        (default_code — portable entre bases, no depende de ids de Supabase).

        Regla de no-pisado: sólo LLENA o CORRIGE pesos estimados
        (ref_gramaje/odoo_weight/bom) o faltantes; NUNCA pisa un peso ya
        autoritativo (manual/cvu) que alguien haya fijado en Odoo.
        Idempotente. Devuelve (creados, corregidos, sin_producto)."""
        Product = self.env['product.product']
        creados = corregidos = sin_producto = 0
        try:
            fh = tools.file_open('qb_capacidad_costeo/data/product_weights.csv')
        except Exception:
            _logger.warning(
                'No se pudo abrir data/product_weights.csv; el maestro de '
                'pesos nativo no se cargó (el motor sigue estimando el peso).')
            return creados, corregidos, sin_producto
        with fh as f:
            for row in csv.DictReader(f):
                ref = (row.get('ref') or '').strip()
                try:
                    kg = float(row.get('kg_per_unit') or 0.0)
                except (TypeError, ValueError):
                    continue
                if not ref or kg <= 0:
                    continue
                product = Product.with_context(active_test=False).search(
                    [('default_code', '=', ref)], limit=1)
                if not product:
                    sin_producto += 1
                    continue
                src = row.get('source') or 'manual'
                rec = self.with_context(active_test=False).search(
                    [('product_id', '=', product.id)], limit=1)
                if rec:
                    if rec.source in ('manual', 'cvu'):
                        continue  # ya autoritativo → respetar
                    rec.write({'kg_per_unit': kg, 'source': src})
                    corregidos += 1
                else:
                    self.create({'product_id': product.id,
                                 'kg_per_unit': kg, 'source': src})
                    creados += 1
        return creados, corregidos, sin_producto

    def action_load_weight_master(self):
        """Botón: (re)carga el maestro de pesos nativo y avisa el resultado."""
        creados, corregidos, sin_prod = self.load_weight_master()
        return {
            'type': 'ir.actions.client', 'tag': 'display_notification',
            'params': {
                'title': 'Maestro de pesos cargado',
                'message': '%s creados, %s corregidos, %s sin producto en '
                           'esta base.' % (creados, corregidos, sin_prod),
                'type': 'success', 'sticky': False,
            }}


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
    def resolve(self, product, rules=None):
        """(product_bucket, centros) para un producto.

        Prioridad: regla por producto > patrón de nombre/ref (subproducto,
        importado ' I$' — señales más fuertes que la categoría) > categoría
        (child_of) > patrón de categoría. Dentro de cada nivel gana la
        primera regla por sequence. Si Odoo trae rutas MRP por producto en
        el futuro, este método es el único punto a cambiar.

        `rules` (recordset opcional): prefetchear con search([]) una sola vez
        en loops grandes — el motor de costeo lo llama por cada nodo de BOM.
        """
        if rules is None:
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
