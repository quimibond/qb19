# -*- coding: utf-8 -*-
"""Familias de máquinas: la capacidad de un centro NO es fungible.

Un centro con 27 circulares y 197,529 kg/mes de capacidad suena holgado
cuando la planta teje 93,000. Pero el WJ044 de 235 cm solo sale en las diez
galga 18 diámetro 32, y esas van al 79%. El agregado del centro promedia
una familia saturada con otras vacías y contesta que sí a un pedido que la
planta no puede tejer.

Por eso la familia es una subdivisión de CAPACIDAD, no de costo: el pool de
gasto sigue siendo del centro y se absorbe sobre su capacidad completa
(costeo por ruta sigue bloqueado por la asignación del gasto fabril a
centros — ver COSTEO_REVISION §3.5). Lo que la familia arregla es lo que el
agregado contesta mal: dónde está el cuello y si un volumen cabe.

Las familias salen de la columna «Alternos» del formato de planta —qué
máquinas son intercambiables entre sí— y son componentes conexos de esa
relación, así que particionan el centro sin traslape. Un producto sí puede
caber en varias familias (el WJ047 de 112 cm sale en galga 18 de los dos
diámetros); ahí la carga se reparte entre las que pueden hacerlo.
"""
from odoo import api, fields, models
from odoo.exceptions import ValidationError

from .cuenta_map import cfg_sql, mo_qty_sql


class QbCosteoFamilia(models.Model):
    _name = 'qb.costeo.familia'
    _description = 'Familia de máquinas intercambiables dentro de un centro'
    _order = 'centro_id, sequence, code'

    sequence = fields.Integer(default=10)
    code = fields.Char(required=True, index=True)
    name = fields.Char(required=True)
    active = fields.Boolean(default=True)
    company_id = fields.Many2one(
        'res.company', default=lambda self: self.env.company, required=True)
    centro_id = fields.Many2one(
        'qb.costeo.centro', required=True, ondelete='cascade', index=True,
        string='Centro')
    machine_names = fields.Char(
        string='Máquinas instaladas',
        help='Nombres de las máquinas del grupo, como se llaman en Odoo y '
             'separados por coma (p.ej. "CIRCULAR 6, CIRCULAR 7"). Son TODAS '
             'las instaladas; cuántas se dotan va en el número de máquinas.')
    machine_count = fields.Integer(
        string='Máquinas dotadas', default=1,
        help='Las que de verdad se corren en el mes. Una máquina instalada '
             'y parada por falta de gente es ociosidad, no capacidad normal.')
    hours_per_week = fields.Float(
        string='Horas/semana',
        help='Horario del centro. Normalmente el mismo de sus turnos.')
    std_output_per_hour = fields.Float(
        string='Throughput por máquina-hora',
        help='Velocidad promedio de las máquinas DOTADAS del grupo, en la '
             'unidad del centro (kg/h en tejido, m/h en acabado).')
    capacidad_normal = fields.Float(
        string='Capacidad normal (unidades/mes)',
        help='Capacidad de la familia. Se valida contra horario × máquinas × '
             'velocidad, y la suma de las familias contra la del centro.')
    producto_ids = fields.One2many(
        'qb.familia.producto', 'familia_id', string='Qué puede producir')
    notes = fields.Text()

    _code_company_uniq = models.Constraint(
        'unique(code, company_id)',
        'El código de la familia debe ser único por compañía.',
    )

    @api.constrains('centro_id', 'company_id')
    def _check_company(self):
        for rec in self:
            if rec.centro_id.company_id != rec.company_id:
                raise ValidationError(
                    'La familia %s y su centro tienen que ser de la misma '
                    'compañía.' % rec.code)

    def capacidad_derivada(self):
        """Horario × máquinas dotadas × velocidad.

        Es contra esto que se valida `capacidad_normal`: el número capturado
        duplica un dato vivo (el horario de planta y la velocidad de la
        máquina) y sin contraste puede quedarse viejo sin que nada avise.
        """
        self.ensure_one()
        weeks = self.env['qb.costeo.factor.config'].get_param(
            'weeks_per_month', 4.33)
        return (self.hours_per_week * weeks * max(self.machine_count, 0)
                * self.std_output_per_hour)

    def workcenters(self):
        """Los `mrp.workcenter` que corresponden a `machine_names`.

        Sirve para contrastar contra Odoo: una máquina que la familia lista
        y Odoo no conoce (o al revés) es un catálogo desalineado.
        """
        self.ensure_one()
        nombres = [n.strip() for n in (self.machine_names or '').split(',')
                   if n.strip()]
        if not nombres:
            return self.env['mrp.workcenter']
        # El nombre en Odoo puede traer espacios dobles ('CIRCULAR  29')
        todos = self.env['mrp.workcenter'].search([])
        norm = {' '.join((wc.name or '').split()): wc for wc in todos}
        encontrados = self.env['mrp.workcenter']
        for n in nombres:
            wc = norm.get(' '.join(n.split()))
            if wc:
                encontrados |= wc
        return encontrados


class QbFamiliaProducto(models.Model):
    _name = 'qb.familia.producto'
    _description = 'Producto que una familia de máquinas puede fabricar'
    _order = 'familia_id, product_code'

    familia_id = fields.Many2one(
        'qb.costeo.familia', required=True, ondelete='cascade', index=True)
    product_code = fields.Char(
        string='Referencia interna', required=True, index=True,
        help='`default_code` del producto tal cual aparece en Odoo. Se '
             'captura por código y no por enlace para que el catálogo de '
             'planta se pueda cargar aunque el producto todavía no exista.')
    std_output_per_hour = fields.Float(
        string='Velocidad en esta familia',
        help='Velocidad de ESTE producto en ESTAS máquinas. El mismo '
             'artículo corre distinto según la galga: el WJ047 da 8.1 kg/h '
             'en la galga 18 Ø32 y 18.7 en la Ø30.')
    product_id = fields.Many2one(
        'product.product', string='Producto', compute='_compute_product_id',
        help='Se resuelve por referencia interna. Vacío = el código no '
             'existe en el catálogo de Odoo.')
    notes = fields.Char()

    _familia_code_uniq = models.Constraint(
        'unique(familia_id, product_code)',
        'El producto ya está dado de alta en esa familia.',
    )

    @api.depends('product_code')
    def _compute_product_id(self):
        codes = [r.product_code for r in self if r.product_code]
        por_code = {}
        if codes:
            for prod in self.env['product.product'].search(
                    [('default_code', 'in', codes)]):
                por_code.setdefault(prod.default_code, prod)
        for rec in self:
            rec.product_id = por_code.get(rec.product_code, False)

    @api.model
    def familias_de(self, product):
        """Familias que pueden fabricar el producto (vacío = sin catalogar)."""
        if not product or not product.default_code:
            return self.env['qb.costeo.familia']
        filas = self.search([('product_code', '=', product.default_code)])
        return filas.mapped('familia_id').filtered('active')


class QbFamiliaCarga(models.Model):
    _name = 'qb.familia.carga'
    _inherit = 'qb.sql.view'
    _description = 'Carga vs capacidad por familia de máquinas'
    _auto = False
    _order = 'utilization_pct DESC'

    familia_id = fields.Many2one('qb.costeo.familia', readonly=True)
    centro_id = fields.Many2one('qb.costeo.centro', readonly=True)
    name = fields.Char(related='familia_id.name', readonly=True)
    machine_count = fields.Integer(
        string='Máquinas dotadas', readonly=True)
    capacity_month_units = fields.Float(
        string='Capacidad/mes', readonly=True)
    load_month_units = fields.Float(
        string='Carga/mes', readonly=True,
        help='Producción real de los productos que esta familia puede '
             'hacer. Un producto que cabe en varias familias reparte su '
             'carga entre ellas.')
    utilization_pct = fields.Float(string='Utilización %', readonly=True)
    free_month_units = fields.Float(string='Disponible/mes', readonly=True)
    company_id = fields.Many2one('res.company', readonly=True)

    @property
    def _table_query(self):
        return """
            {cfg},
            alcance AS (
                SELECT fp.product_code, COUNT(DISTINCT fp.familia_id) AS n_fam
                FROM qb_familia_producto fp
                JOIN qb_costeo_familia f ON f.id = fp.familia_id AND f.active
                GROUP BY fp.product_code
            ),
            carga_prod AS (
                SELECT pp.default_code AS code,
                       mp.company_id,
                       SUM({qty}) / (SELECT window_months FROM cfg) AS qty_month
                FROM mrp_production mp
                JOIN product_product pp ON pp.id = mp.product_id
                JOIN cfg ON TRUE
                WHERE mp.state = 'done'
                  AND pp.default_code IS NOT NULL
                  AND mp.date_finished >= (date_trunc('month', CURRENT_DATE)
                        - make_interval(months => cfg.window_months::int))
                  AND mp.date_finished < date_trunc('month', CURRENT_DATE)
                GROUP BY pp.default_code, mp.company_id
            ),
            carga AS (
                SELECT fp.familia_id,
                       SUM(cp.qty_month / a.n_fam) AS qty_month
                FROM qb_familia_producto fp
                JOIN qb_costeo_familia f ON f.id = fp.familia_id
                JOIN carga_prod cp ON cp.code = fp.product_code
                                  AND cp.company_id = f.company_id
                JOIN alcance a ON a.product_code = fp.product_code
                GROUP BY fp.familia_id
            )
            SELECT f.id AS id,
                   f.id AS familia_id,
                   f.centro_id,
                   f.company_id,
                   f.machine_count,
                   f.capacidad_normal AS capacity_month_units,
                   COALESCE(carga.qty_month, 0) AS load_month_units,
                   CASE WHEN f.capacidad_normal > 0
                        THEN 100.0 * COALESCE(carga.qty_month, 0)
                             / f.capacidad_normal
                        ELSE 0 END AS utilization_pct,
                   GREATEST(f.capacidad_normal - COALESCE(carga.qty_month, 0),
                            0) AS free_month_units
            FROM qb_costeo_familia f
            LEFT JOIN carga ON carga.familia_id = f.id
            WHERE f.active
        """.replace('{qty}', mo_qty_sql(self.env)) \
            .replace('{cfg}', cfg_sql('window_months'))
