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

from .cuenta_map import CUENTA_MAP_SQL, excluir_refs_sql


class QbCostoConciliacion(models.Model):
    _name = 'qb.costo.conciliacion'
    _inherit = 'qb.sql.view'
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
        string='Gasto total (mayor)', readonly=True,
        help='Costo de ventas + gastos de operación + el costeo que vive en '
             'otras cuentas. Es contra esto que se compara lo que el modelo '
             'le cobró a los productos.')

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
    gl_importacion = fields.Float(
        string='Aduana en el P&L', readonly=True,
        help='Gastos e impuestos de importación que quedaron en resultados '
             '(bucket «importacion»). Lo correcto es que casi nada llegue '
             'aquí: el pedimento se captura con el landed cost de Odoo sobre '
             'la recepción y se capitaliza al inventario de los productos que '
             'lo causaron. Lo que sí llega aquí es aduana que ningún producto '
             'está cargando.')
    importacion_absorbida = fields.Float(
        string='Aduana absorbida por el modelo', readonly=True,
        help='Lo que el prorrateo de aduana cargó a lo vendido. Con el driver '
             '«landed» (default) es 0 a propósito: el módulo no prorratea '
             'pedimentos, los espera capitalizados.')
    gl_no_costeo = fields.Float(
        string='Gasto fuera de costeo', readonly=True,
        help='Cuentas clasificadas "no_costeo": gasto real que ningún '
             'producto carga. Parte es correcto (el costo primo se '
             'sustituye por la receta), parte es fuga (impuestos de '
             'importación, ajustes de inventario, renta excluida).')
    gl_otros_costeo = fields.Float(
        string='Costeo en otras cuentas', readonly=True,
        help='Gasto que SÍ entra al costo del producto pero vive en cuentas '
             'de tipo «otros ingresos». Hoy es el arrendamiento de '
             'maquinaria (701.11), que el modelo le cobra al producto porque '
             'son las máquinas con las que se produce. Sin esta línea el '
             'mayor no lo contaba como gasto y la brecha salía baja por ese '
             'lado: $13,907,465 entre 2025 y 2026.')
    gl_resultado_integral = fields.Float(
        string='Resultado integral de financiamiento', readonly=True,
        help='Lo demás que vive en «otros ingresos»: pérdida y utilidad '
             'cambiaria, intereses, comisiones bancarias, utilidad en venta '
             'de activo fijo, otros ingresos. NO es costo de producto ni '
             'debe serlo — pero SÍ es resultado de la empresa, y sin él el '
             'resultado del mayor no era el de la empresa. Positivo = gasto '
             'neto.')
    gl_sin_clasificar = fields.Float(
        string='Gasto sin clasificar', readonly=True,
        help='Cuentas de resultados que NADIE clasificó: no entran a ningún '
             'pool ni al costo. Debe ser 0 — cualquier monto aquí es gasto '
             'invisible para el costeo.')

    resultado_gl = fields.Float(
        string='Resultado de operación (mayor)', readonly=True,
        help='Ventas − costo de ventas − gastos de operación, del mayor.')
    resultado_modelo = fields.Float(
        string='Margen de productos (modelo)', readonly=True,
        help='Σ del margen neto total de Costo por producto. NO es el '
             'resultado del período: los productos solo cargan la capacidad '
             'que usan, así que este margen se lee siempre en par con la '
             'ociosidad — margen − ociosidad = resultado.')
    resultado_par = fields.Float(
        string='Resultado del período (modelo)', readonly=True,
        help='Margen de productos − ociosidad no absorbida. El número que '
             'sí se compara contra el resultado de operación del mayor: '
             'los productos pueden dejar margen y aun así el mes salir '
             'tablas si la planta parada se lo come.')
    ociosidad_ias2 = fields.Float(
        string='Ociosidad no absorbida', readonly=True,
        help='Costo fijo de la capacidad ociosa: bajo IAS 2 va al resultado '
             'del período y NO al costo del producto. Es una diferencia '
             'DELIBERADA entre el modelo y el gasto total — por eso se '
             'descuenta de la brecha para leer lo que de verdad falta '
             'explicar.')
    brecha = fields.Float(
        string='Brecha', readonly=True,
        help='Resultado del modelo − resultado de operación del mayor. '
             'NEGATIVA = el modelo le cobra a los productos MÁS de lo que '
             'la empresa gasta (los pinta menos rentables de lo que son). '
             'POSITIVA = les cobra de menos. El lado del mayor es ventas − '
             'costo de ventas − gastos de operación − el costeo que vive en '
             'otras cuentas (arrendamiento de maquinaria, que el modelo SÍ '
             'cobra). El resultado integral de financiamiento queda fuera: '
             'el modelo no se lo cobra al producto.')
    brecha_neta = fields.Float(
        string='Brecha sin ociosidad', readonly=True,
        help='Brecha MENOS la ociosidad no absorbida. El modelo deja de '
             'cobrarle al producto la capacidad parada a propósito, así que '
             'la brecha bruta trae esa cantidad «sin explicar» por '
             'construcción; se descuenta y lo que queda es lo que de verdad '
             'falta explicar. ESTE es el número que debe tender a cero. '
             '(Hasta la v1.64 se SUMABA: la ociosidad entraba dos veces y la '
             'brecha del año salía en +28.8% cuando era −0.7%.)')
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
        # Resultado de operación del mayor y gasto total: la misma
        # expresión en brecha, brecha neta, % y cobertura, para que las
        # cuatro columnas hablen del mismo número.
        gasto_gl = ('(gl.gl_costo_ventas + gl.gl_operacion '
                    '+ gl.gl_otros_costeo)')
        res_op = f'(gl.gl_ventas - {gasto_gl})'
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
                       SUM(CASE WHEN m.bucket = 'importacion'
                                THEN aml.balance * m.allocation_pct / 100.0
                                ELSE 0 END) AS gl_importacion,
                       SUM(CASE WHEN m.bucket = 'no_costeo'
                                THEN aml.balance * m.allocation_pct / 100.0
                                ELSE 0 END) AS gl_no_costeo,
                       SUM(CASE WHEN m.bucket IS NULL
                                     AND aa.account_type NOT IN
                                         ('income', 'income_other')
                                THEN aml.balance ELSE 0 END)
                           AS gl_sin_clasificar,
                       -- `income_other` con bucket de COSTEO: hoy es el
                       -- arrendamiento de maquinaria (701.11), que el modelo
                       -- SÍ le cobra al producto. Sin esto el mayor no lo
                       -- contaba y la brecha salía baja por ese lado.
                       SUM(CASE WHEN aa.account_type = 'income_other'
                                     AND m.bucket IS NOT NULL
                                     AND m.bucket NOT IN ('ventas',
                                                          'no_costeo')
                                THEN aml.balance * m.allocation_pct / 100.0
                                ELSE 0 END) AS gl_otros_costeo,
                       -- El resto de `income_other`: resultado integral de
                       -- financiamiento (cambiaria, intereses, comisiones) y
                       -- otros ingresos. No es costo de producto ni debe
                       -- serlo, pero SÍ es resultado de la empresa.
                       SUM(CASE WHEN aa.account_type = 'income_other'
                                     AND m.bucket IS NULL
                                THEN aml.balance ELSE 0 END)
                           AS gl_resultado_integral
                FROM account_move_line aml
                JOIN account_move am ON am.id = aml.move_id
                JOIN account_account aa ON aa.id = aml.account_id
                LEFT JOIN cuenta_map m ON m.account_id = aml.account_id
                WHERE aml.parent_state = 'posted'
                  AND aml.company_id = {company_id}
                  AND aa.account_type IN ('income', 'income_other',
                                          'expense_direct_cost',
                                          'expense', 'expense_depreciation')
                  AND {excluir_refs_sql(self.env)}
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
                       SUM(importacion_total) AS importacion,
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
                gl.gl_otros_costeo,
                gl.gl_resultado_integral,
                gl.gl_costo_ventas + gl.gl_operacion + gl.gl_otros_costeo
                    AS gl_gasto_total,
                COALESCE(mo.mp, 0) AS modelo_mp,
                COALESCE(mo.energia, 0) AS modelo_energia,
                COALESCE(mo.fab, 0) AS modelo_fab,
                COALESCE(mo.op, 0) AS modelo_op,
                COALESCE(mo.costo, 0) AS modelo_costo_total,
                gl.gl_mp,
                gl.gl_importacion,
                COALESCE(mo.importacion, 0) AS importacion_absorbida,
                gl.gl_no_costeo,
                gl.gl_sin_clasificar,
                gl.gl_ventas - gl.gl_costo_ventas - gl.gl_operacion
                    - gl.gl_otros_costeo - gl.gl_resultado_integral
                    AS resultado_gl,
                COALESCE(mo.resultado, 0) AS resultado_modelo,
                COALESCE(fa.ociosidad, 0) AS ociosidad_ias2,
                COALESCE(mo.resultado, 0) - COALESCE(fa.ociosidad, 0)
                    AS resultado_par,
                -- La brecha compara al modelo contra lo que el modelo
                -- intenta repartir: el resultado de OPERACIÓN, con el
                -- arrendamiento de maquinaria (que sí se cobra al producto)
                -- y sin el resultado integral de financiamiento (que no).
                -- Y la ociosidad se RESTA: el modelo no se la cobra al
                -- producto a propósito, así que la brecha bruta la trae por
                -- construcción. Sumarla la metía dos veces (v1.14–v1.64).
                COALESCE(mo.resultado, 0) - {res_op} AS brecha,
                COALESCE(mo.resultado, 0) - {res_op}
                    - COALESCE(fa.ociosidad, 0) AS brecha_neta,
                CASE WHEN gl.gl_ventas > 0 THEN
                    100.0 * (COALESCE(mo.resultado, 0) - {res_op}
                             - COALESCE(fa.ociosidad, 0)) / gl.gl_ventas
                     ELSE 0 END AS brecha_pct,
                CASE WHEN {gasto_gl} > 0 THEN
                    100.0 * COALESCE(mo.costo, 0) / {gasto_gl}
                     ELSE 0 END AS cobertura_pct
            FROM gl
            LEFT JOIN modelo mo
                   ON mo.period = gl.period
                  AND mo.company_id = gl.company_id
            LEFT JOIN factores fa
                   ON fa.period = gl.period
                  AND fa.company_id = gl.company_id
        """
