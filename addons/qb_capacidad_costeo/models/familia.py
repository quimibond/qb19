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
    machines_installed = fields.Integer(
        string='Máquinas instaladas (n)',
        compute='_compute_machines_installed', store=True, readonly=True,
        help='Se cuenta de la lista de máquinas: es un derivado, no un '
             'segundo número capturado que pueda quedarse viejo cuando la '
             'lista cambie. Sin lista, cae en las dotadas y el semáforo '
             'avisa del hueco.')
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

    @api.depends('machine_names', 'machine_count')
    def _compute_machines_installed(self):
        for rec in self:
            rec.machines_installed = (rec._maquinas_listadas()
                                      or max(rec.machine_count, 0))

    def _maquinas_listadas(self):
        """Cuántos nombres trae `machine_names` (0 = lista vacía)."""
        self.ensure_one()
        return len([n for n in (self.machine_names or '').split(',')
                    if n.strip()])

    @api.constrains('centro_id', 'company_id')
    def _check_company(self):
        for rec in self:
            if rec.centro_id.company_id != rec.company_id:
                raise ValidationError(
                    'La familia %s y su centro tienen que ser de la misma '
                    'compañía.' % rec.code)

    @api.constrains('machine_names', 'machine_count')
    def _check_dotadas_no_exceden_instaladas(self):
        """No se puede dotar una máquina que no existe.

        La lista de nombres es el inventario físico; dotar más de las que
        hay significa que una de las dos capturas está mal, y de las dos
        sale capacidad.
        """
        for rec in self:
            listadas = rec._maquinas_listadas()
            if listadas and rec.machine_count > listadas:
                raise ValidationError(
                    'La familia %s dota %s máquinas pero solo tiene %s '
                    'instaladas.' % (rec.code, rec.machine_count, listadas))

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

    def capacidad_instalada(self):
        """El techo físico: lo mismo, pero con TODAS las máquinas.

        No es capacidad normal —una máquina parada por falta de gente no
        absorbe costo, y meterla en el denominador de la absorción
        inventaría ociosidad que la planta no decidió tener—. Es la otra
        pregunta: cuánto podría correr si la dotara. La diferencia entre
        las dos es una decisión pendiente, no un residuo.

        Con dotación se escala la `capacidad_normal` capturada en vez de
        derivar un número aparte: así las dos cifras no se pueden
        contradecir. Sin dotación (una máquina parada, que es justo el
        caso interesante) no queda más que el horario por la velocidad.
        """
        self.ensure_one()
        instaladas = max(self.machines_installed, 0)
        if self.machine_count > 0:
            return self.capacidad_normal * instaladas / self.machine_count
        weeks = self.env['qb.costeo.factor.config'].get_param(
            'weeks_per_month', 4.33)
        return (self.hours_per_week * weeks * instaladas
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
    machines_installed = fields.Integer(
        string='Máquinas instaladas', readonly=True)
    activa = fields.Boolean(
        string='En operación', readonly=True,
        help='Una familia dada de baja sigue apareciendo con su capacidad '
             'instalada y carga cero: es capacidad que la planta tiene y '
             'no está usando, que es exactamente lo que hay que ver.')
    capacity_month_units = fields.Float(
        string='Capacidad dotada/mes', readonly=True)
    capacity_installed_units = fields.Float(
        string='Capacidad instalada/mes', readonly=True,
        help='Lo que darían TODAS las máquinas del grupo, dotadas o no. No '
             'absorbe costo —una máquina parada no lo absorbe— pero es el '
             'techo que la planta ya compró.')
    capacity_parked_units = fields.Float(
        string='Capacidad parada/mes', readonly=True,
        help='Instalada menos dotada: máquinas que existen y nadie corre. '
             'Se libera contratando o moviendo gente, no invirtiendo.')
    utilization_installed_pct = fields.Float(
        string='Utilización s/instalada %', readonly=True,
        help='Carga contra el techo físico. Siempre menor o igual que la '
             'utilización normal; la brecha entre las dos es la dotación.')
    load_month_units = fields.Float(
        string='Carga/mes', readonly=True,
        help='Producción real de los productos que esta familia puede '
             'hacer. Es una ASIGNACIÓN, no una medición: Odoo no registra '
             'en qué máquina corrió cada orden —las ramas y los jets ni '
             'siquiera existen como workcenter—, así que el reparto se '
             'modela. Primero lo cautivo (lo que solo esta familia puede '
             'hacer) y luego lo compartido, en proporción a la holgura que '
             'le queda a cada candidata.')
    utilization_pct = fields.Float(string='Utilización %', readonly=True)
    free_month_units = fields.Float(string='Disponible/mes', readonly=True)
    company_id = fields.Many2one('res.company', readonly=True)

    @property
    def _table_query(self):
        return """
            {cfg},
            cand AS (
                -- Familia x articulo que esa familia puede hacer.
                SELECT fp.familia_id, fp.product_code, f.company_id,
                       f.capacidad_normal
                FROM qb_familia_producto fp
                JOIN qb_costeo_familia f ON f.id = fp.familia_id AND f.active
            ),
            alcance AS (
                SELECT product_code, COUNT(DISTINCT familia_id) AS n_fam
                FROM cand GROUP BY product_code
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
            cautiva AS (
                -- Lo que SOLO esta familia puede hacer. No se reparte: no
                -- hay a donde.
                SELECT c.familia_id, SUM(cp.qty_month) AS qty_month
                FROM cand c
                JOIN alcance a ON a.product_code = c.product_code
                              AND a.n_fam = 1
                JOIN carga_prod cp ON cp.code = c.product_code
                                  AND cp.company_id = c.company_id
                GROUP BY c.familia_id
            ),
            holgura AS (
                -- Lo que le queda a cada familia despues de lo cautivo. Es
                -- la capacidad que de verdad puede ofrecer para lo demas.
                SELECT f.id AS familia_id,
                       GREATEST(f.capacidad_normal
                                - COALESCE(ct.qty_month, 0), 0) AS libre
                FROM qb_costeo_familia f
                LEFT JOIN cautiva ct ON ct.familia_id = f.id
                WHERE f.active
            ),
            repartible AS (
                SELECT c.product_code, SUM(h.libre) AS libre_total
                FROM cand c
                JOIN alcance a ON a.product_code = c.product_code
                              AND a.n_fam > 1
                JOIN holgura h ON h.familia_id = c.familia_id
                GROUP BY c.product_code
            ),
            compartida AS (
                -- Lo que cabe en varias maquinas va donde hay lugar, en
                -- proporcion a la holgura. Repartirlo en partes IGUALES
                -- —como se hacia— le manda carga a una maquina que ya no
                -- puede tomarla: en acabado dejaba a la UNITECH en 120
                -- por ciento y a la BRUCKNER en 48, con la planta corriendo
                -- de verdad. (Sin el signo de porcentaje a proposito: este
                -- SQL puede pasar por formateo estilo printf.)
                -- Si ninguna candidata tiene holgura se cae al reparto
                -- parejo, que es lo unico que queda cuando todas estan
                -- llenas.
                SELECT c.familia_id,
                       SUM(cp.qty_month
                           * CASE WHEN r.libre_total > 0
                                  THEN h.libre / r.libre_total
                                  ELSE 1.0 / a.n_fam END) AS qty_month
                FROM cand c
                JOIN alcance a ON a.product_code = c.product_code
                              AND a.n_fam > 1
                JOIN carga_prod cp ON cp.code = c.product_code
                                  AND cp.company_id = c.company_id
                JOIN holgura h ON h.familia_id = c.familia_id
                JOIN repartible r ON r.product_code = c.product_code
                GROUP BY c.familia_id
            ),
            carga AS (
                SELECT f.id AS familia_id,
                       COALESCE(ct.qty_month, 0)
                       + COALESCE(co.qty_month, 0) AS qty_month
                FROM qb_costeo_familia f
                LEFT JOIN cautiva ct ON ct.familia_id = f.id
                LEFT JOIN compartida co ON co.familia_id = f.id
            ),
            instalada AS (
                -- El techo fisico. Con dotacion se ESCALA la capacidad
                -- normal capturada (que ya esta validada contra horario x
                -- maquinas x velocidad) para que las dos cifras no se
                -- puedan contradecir; sin dotacion —la maquina parada, que
                -- es el caso que importa— no queda mas que derivarla.
                SELECT f.id AS familia_id,
                       COALESCE(CASE WHEN f.machine_count > 0
                            THEN f.capacidad_normal
                                 * GREATEST(f.machines_installed, 0)
                                 / f.machine_count
                            ELSE f.hours_per_week * cfg.weeks_per_month
                                 * GREATEST(f.machines_installed, 0)
                                 * f.std_output_per_hour
                       END, 0) AS units
                FROM qb_costeo_familia f
                JOIN cfg ON TRUE
            )
            SELECT f.id AS id,
                   f.id AS familia_id,
                   f.centro_id,
                   f.company_id,
                   f.active AS activa,
                   f.machine_count,
                   GREATEST(f.machines_installed, 0) AS machines_installed,
                   -- Una familia dada de baja no tiene capacidad DOTADA
                   -- aunque tenga el numero capturado: nadie la corre, asi
                   -- que no absorbe costo y toda su capacidad esta parada.
                   CASE WHEN f.active THEN f.capacidad_normal ELSE 0 END
                        AS capacity_month_units,
                   COALESCE(inst.units, 0) AS capacity_installed_units,
                   GREATEST(COALESCE(inst.units, 0)
                            - CASE WHEN f.active THEN f.capacidad_normal
                                   ELSE 0 END, 0)
                        AS capacity_parked_units,
                   COALESCE(carga.qty_month, 0) AS load_month_units,
                   CASE WHEN f.active AND f.capacidad_normal > 0
                        THEN 100.0 * COALESCE(carga.qty_month, 0)
                             / f.capacidad_normal
                        ELSE 0 END AS utilization_pct,
                   CASE WHEN COALESCE(inst.units, 0) > 0
                        THEN 100.0 * COALESCE(carga.qty_month, 0)
                             / inst.units
                        ELSE 0 END AS utilization_installed_pct,
                   GREATEST(CASE WHEN f.active THEN f.capacidad_normal
                                 ELSE 0 END
                            - COALESCE(carga.qty_month, 0), 0)
                        AS free_month_units
            FROM qb_costeo_familia f
            LEFT JOIN carga ON carga.familia_id = f.id
            LEFT JOIN instalada inst ON inst.familia_id = f.id
        """.replace('{qty}', mo_qty_sql(self.env)) \
            .replace('{cfg}', cfg_sql('window_months', 'weeks_per_month'))
