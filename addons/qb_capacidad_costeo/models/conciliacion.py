# -*- coding: utf-8 -*-
"""Conciliación mensual: costo del modelo vs. gasto real del mayor.

La pregunta que responde: **¿lo que le cobro a los productos es lo que de
verdad gasta la empresa?**

`qb.costo.producto` reparte costos con un MODELO (MP explotada de la receta
al último precio de compra, pools de gasto divididos entre kg y metros
producidos, operación como % de la venta). Ese modelo puede desviarse del
mayor por tres caminos, y ninguno se veía hasta ahora:

1. **Gasto que nunca llega a un producto.** Cuentas clasificadas
   `no_costeo` o sin clasificar: se pagan igual, pero no las carga nadie.
2. **Sobre o sub absorción.** Los factores $/kg y $/m se calculan con la
   producción del mes y se aplican a lo VENDIDO; si la mezcla vendida pesa
   distinto que la producida, la fabricación absorbida no cuadra con el
   pool. (`cobertura_fab_pct` ya lo medía, pero solo para fabricación.)
3. **MP modelada ≠ MP consumida.** La receta al último precio de compra no
   es el consumo real: no lleva merma, ni ajustes de inventario, ni
   variación de precio contra lo que efectivamente se pagó. La cuenta de
   costo primo del mayor es el número duro, y nada lo comparaba contra el
   modelo.

Read-only, en vivo, sin cron: es una vista SQL sobre el mayor y sobre las
filas ya calculadas de `qb.costo.producto`.

Convención de signos: todo se expresa en POSITIVO como gasto e ingreso
(el mayor guarda el ingreso en crédito, o sea negativo, y se invierte).
"""
from odoo import fields, models

from .cuenta_map import CUENTA_MAP_SQL


class QbCostoConciliacion(models.Model):
    _name = 'qb.costo.conciliacion'
    _description = 'Conciliación mensual: modelo de costeo vs. mayor'
    _auto = False
    _order = 'period DESC'
    _rec_name = 'period'

    period = fields.Date(string='Período', readonly=True)
    company_id = fields.Many2one('res.company', readonly=True)
    company_currency_id = fields.Many2one(
        'res.currency', related='company_id.currency_id', string='Moneda')

    # --- Ventas: modelo vs mayor -------------------------------------
    gl_ventas = fields.Float(
        string='Ventas (mayor)', readonly=True,
        help='Ingresos posteados del mes en cuentas de tipo "income". El '
             'número duro contra el que se compara todo.')
    modelo_ventas = fields.Float(
        string='Ventas (modelo)', readonly=True,
        help='Σ de "Ventas $" de Costo por producto. Debe empatar con el '
             'mayor: si no, hay facturas sin producto o productos fuera del '
             'alcance del cálculo.')
    ventas_dif = fields.Float(
        string='Δ Ventas', readonly=True)

    # --- Gasto real del mayor ----------------------------------------
    gl_costo_ventas = fields.Float(
        string='Costo de ventas (mayor)', readonly=True,
        help='Cuentas de tipo "costo directo" (501/502/504 en este plan): '
             'costo primo, nómina de fábrica, ajustes de inventario, '
             'impuestos y gastos de importación, overhead de planta.')
    gl_gastos_operacion = fields.Float(
        string='Gastos de operación (mayor)', readonly=True,
        help='Cuentas de gasto y depreciación (602/603): administración, '
             'ventas y corporativo.')
    gl_gasto_total = fields.Float(
        string='Gasto total (mayor)', readonly=True)

    # --- Costo que el modelo cargó a los productos --------------------
    modelo_mp = fields.Float(
        string='MP (modelo)', readonly=True)
    modelo_energia = fields.Float(
        string='Energía (modelo)', readonly=True)
    modelo_fab = fields.Float(
        string='Fabricación (modelo)', readonly=True)
    modelo_op = fields.Float(
        string='Operación (modelo)', readonly=True)
    modelo_costo_total = fields.Float(
        string='Costo total (modelo)', readonly=True,
        help='Σ del costo absorbido de lo vendido. Es lo que el modelo le '
             'cobra a los productos.')

    # --- El contraste que importa ------------------------------------
    gl_mp = fields.Float(
        string='MP consumida (mayor)', readonly=True,
        help='Cuentas clasificadas en el bucket "Materia prima" (costo '
             'primo y ajustes de inventario). Contra esto se compara la MP '
             'que el modelo explota de la receta. Sale en 0 si esas cuentas '
             'están clasificadas como "no_costeo".')
    gl_no_costeo = fields.Float(
        string='Gasto fuera de costeo', readonly=True,
        help='Cuentas clasificadas "no_costeo": gasto real que ningún '
             'producto carga. Parte es correcto (el costo primo se '
             'sustituye por la receta), parte es fuga (impuestos de '
             'importación, ajustes de inventario, renta excluida).')
    gl_sin_clasificar = fields.Float(
        string='Gasto sin clasificar', readonly=True,
        help='Cuentas de resultados que NADIE clasificó: no entran a ningún '
             'pool ni al costo. Debe ser 0 — cualquier monto aquí es gasto '
             'invisible para el costeo.')

    resultado_gl = fields.Float(
        string='Resultado de operación (mayor)', readonly=True,
        help='Ventas − costo de ventas − gastos de operación, del mayor.')
    resultado_modelo = fields.Float(
        string='Resultado (modelo)', readonly=True,
        help='Σ del margen neto total de Costo por producto.')
    ociosidad_ias2 = fields.Float(
        string='Ociosidad no absorbida', readonly=True,
        help='Costo fijo de la capacidad ociosa: bajo IAS 2 va al resultado '
             'del período y NO al costo del producto. Es una diferencia '
             'DELIBERADA entre el modelo y el gasto total — por eso se '
             'descuenta de la brecha para leer lo que de verdad falta '
             'explicar.')
    brecha = fields.Float(
        string='Brecha', readonly=True,
        help='Resultado del modelo − resultado del mayor. NEGATIVA = el '
             'modelo le cobra a los productos MÁS de lo que la empresa '
             'gasta (los pinta menos rentables de lo que son). POSITIVA = '
             'les cobra de menos.')
    brecha_neta = fields.Float(
        string='Brecha sin ociosidad', readonly=True,
        help='Brecha más la ociosidad no absorbida: lo que queda por explicar '
             'una vez descontada la capacidad ociosa, que a propósito no se '
             'le cobra al producto. ESTE es el número que debe tender a cero.')
    brecha_pct = fields.Float(
        string='Brecha % s/ventas', readonly=True,
        help='Brecha sin ociosidad ÷ ventas del mayor. Bajo ±2% el modelo es '
             'confiable para decidir precios; más allá, primero hay que '
             'cerrar la brecha.')
    cobertura_pct = fields.Float(
        string='Cobertura del gasto %', readonly=True,
        help='Costo total del modelo ÷ gasto total del mayor. 100% = el '
             'modelo reparte exactamente lo que se gasta.')

    @property
    def _table_query(self):
        company_id = int(self.env.company.id)
        # Sin ningún carácter de porcentaje en el SQL: _table_query pasa por
        # formateo estilo printf igual que las demás vistas del módulo.
        return f"""
            WITH cuenta_map AS ({CUENTA_MAP_SQL}),
            gl AS (
                SELECT date_trunc('month', aml.date)::date AS period,
                       aml.company_id,
                       SUM(CASE WHEN aa.account_type = 'income'
                                THEN -aml.balance ELSE 0 END) AS gl_ventas,
                       SUM(CASE WHEN aa.account_type = 'expense_direct_cost'
                                THEN aml.balance ELSE 0 END) AS gl_costo_ventas,
                       SUM(CASE WHEN aa.account_type IN
                                     ('expense', 'expense_depreciation')
                                THEN aml.balance ELSE 0 END) AS gl_operacion,
                       SUM(CASE WHEN m.bucket = 'mp'
                                THEN aml.balance * m.allocation_pct / 100.0
                                ELSE 0 END) AS gl_mp,
                       SUM(CASE WHEN m.bucket = 'no_costeo'
                                THEN aml.balance * m.allocation_pct / 100.0
                                ELSE 0 END) AS gl_no_costeo,
                       SUM(CASE WHEN m.bucket IS NULL
                                     AND aa.account_type != 'income'
                                THEN aml.balance ELSE 0 END)
                           AS gl_sin_clasificar
                FROM account_move_line aml
                JOIN account_account aa ON aa.id = aml.account_id
                LEFT JOIN cuenta_map m ON m.account_id = aml.account_id
                WHERE aml.parent_state = 'posted'
                  AND aml.company_id = {company_id}
                  AND aa.account_type IN ('income', 'expense_direct_cost',
                                          'expense', 'expense_depreciation')
                GROUP BY 1, 2
            ),
            factores AS (
                SELECT period, company_id,
                       SUM(fab_ocioso_month) AS ociosidad
                FROM qb_costo_factores
                WHERE company_id = {company_id}
                GROUP BY 1, 2
            ),
            modelo AS (
                SELECT period, company_id,
                       SUM(ventas_total) AS ventas,
                       SUM(mp_total) AS mp,
                       SUM(energia_total) AS energia,
                       SUM(fab_total) AS fab,
                       SUM(op_total) AS op,
                       SUM(costo_absorbido_total) AS costo,
                       SUM(margen_neto_total) AS resultado
                FROM qb_costo_producto
                WHERE company_id = {company_id}
                GROUP BY 1, 2
            )
            SELECT
                (EXTRACT(YEAR FROM gl.period)::int * 100
                 + EXTRACT(MONTH FROM gl.period)::int) AS id,
                gl.period,
                gl.company_id,
                gl.gl_ventas,
                COALESCE(mo.ventas, 0) AS modelo_ventas,
                COALESCE(mo.ventas, 0) - gl.gl_ventas AS ventas_dif,
                gl.gl_costo_ventas,
                gl.gl_operacion AS gl_gastos_operacion,
                gl.gl_costo_ventas + gl.gl_operacion AS gl_gasto_total,
                COALESCE(mo.mp, 0) AS modelo_mp,
                COALESCE(mo.energia, 0) AS modelo_energia,
                COALESCE(mo.fab, 0) AS modelo_fab,
                COALESCE(mo.op, 0) AS modelo_op,
                COALESCE(mo.costo, 0) AS modelo_costo_total,
                gl.gl_mp,
                gl.gl_no_costeo,
                gl.gl_sin_clasificar,
                gl.gl_ventas - gl.gl_costo_ventas - gl.gl_operacion
                    AS resultado_gl,
                COALESCE(mo.resultado, 0) AS resultado_modelo,
                COALESCE(fa.ociosidad, 0) AS ociosidad_ias2,
                COALESCE(mo.resultado, 0)
                    - (gl.gl_ventas - gl.gl_costo_ventas - gl.gl_operacion)
                    AS brecha,
                COALESCE(mo.resultado, 0)
                    - (gl.gl_ventas - gl.gl_costo_ventas - gl.gl_operacion)
                    + COALESCE(fa.ociosidad, 0) AS brecha_neta,
                CASE WHEN gl.gl_ventas > 0 THEN
                    100.0 * (COALESCE(mo.resultado, 0)
                             - (gl.gl_ventas - gl.gl_costo_ventas
                                - gl.gl_operacion)
                             + COALESCE(fa.ociosidad, 0)) / gl.gl_ventas
                     ELSE 0 END AS brecha_pct,
                CASE WHEN (gl.gl_costo_ventas + gl.gl_operacion) > 0 THEN
                    100.0 * COALESCE(mo.costo, 0)
                    / (gl.gl_costo_ventas + gl.gl_operacion)
                     ELSE 0 END AS cobertura_pct
            FROM gl
            LEFT JOIN modelo mo
                   ON mo.period = gl.period
                  AND mo.company_id = gl.company_id
            LEFT JOIN factores fa
                   ON fa.period = gl.period
                  AND fa.company_id = gl.company_id
        """
