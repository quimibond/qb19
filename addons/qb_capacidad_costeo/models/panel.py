# -*- coding: utf-8 -*-
"""Panel: puerta de entrada del módulo.

Semáforo de configuración (qué falta y qué número desbloquea) + KPIs del
mes. Es la respuesta a "¿por qué todo sale en cero?": cada prerequisito
se muestra con su estado y un botón directo para resolverlo.
"""
from dateutil.relativedelta import relativedelta

from odoo import api, fields, models

from .producto_reportes import money

OK = '✅'
WARN = '⚠️'
BAD = '❌'


class QbCosteoPanel(models.TransientModel):
    _name = 'qb.costeo.panel'
    _description = 'Panel de capacidad y costeo'

    negocio_html = fields.Html(compute='_compute_panel', sanitize=False)
    estado_html = fields.Html(compute='_compute_panel', sanitize=False)
    kpi_html = fields.Html(compute='_compute_panel', sanitize=False)

    def _compute_panel(self):
        for rec in self:
            rec.negocio_html = rec._build_negocio()
            rec.estado_html = rec._build_estado()
            rec.kpi_html = rec._build_kpis()

    # ------------------------------------------------------------------
    # ¿Cómo va el negocio? — lo primero que se ve al abrir: el mes en
    # curso en tarjetas, quién gana y quién pierde en 12 meses, y qué
    # necesita acción. La configuración vive abajo, colapsada: es de la
    # puesta a punto, no del día a día.
    # ------------------------------------------------------------------
    @staticmethod
    def _card(titulo, valor, sub='', color='#0d6efd'):
        return (
            '<div style="display:inline-block;min-width:180px;margin:4px;'
            'padding:10px 14px;border:1px solid #dee2e6;border-radius:8px;'
            'border-left:4px solid %s;vertical-align:top;">'
            '<div class="text-muted" style="font-size:11px;text-transform:'
            'uppercase;">%s</div>'
            '<div style="font-size:20px;font-weight:bold;">%s</div>'
            '<div class="text-muted" style="font-size:11px;">%s</div>'
            '</div>' % (color, titulo, valor, sub))

    @staticmethod
    def _mini_tabla(titulo, filas):
        """filas = [(icono, nombre, monto, pct)]"""
        cuerpo = ''.join(
            '<tr><td style="padding:2px 6px;">%s %s</td>'
            '<td style="padding:2px 6px;text-align:right;">%s</td>'
            '<td style="padding:2px 6px;text-align:right;">%+.1f&#37;</td>'
            '</tr>' % (icono, nombre, money(monto), pct)
            for icono, nombre, monto, pct in filas)
        return (
            '<div style="display:inline-block;vertical-align:top;'
            'min-width:300px;margin:4px 12px 4px 0;">'
            '<h6 style="margin-bottom:4px;">%s</h6>'
            '<table class="table table-sm" style="font-size:12px;">'
            '<tbody>%s</tbody></table></div>' % (titulo, cuerpo))

    def _build_negocio(self):
        env = self.env
        html = '<h5>¿Cómo va el negocio?</h5>'

        # El mes en curso, desde la conciliación (ventas del GL, margen
        # del modelo con todos los costos asignados)
        conc = env['qb.costo.conciliacion'].search(
            [], order='period desc', limit=1)
        if conc:
            # Con capacidad normal HONESTA el margen de productos ya no trae
            # la planta parada adentro: leerlo solo infla la foto (+11M de
            # productos con −13M de ociosidad al lado es un año en tablas).
            # El par va SIEMPRE junto: margen de productos − ociosidad =
            # resultado del mes.
            fac = env['qb.costo.factores'].search(
                [('period', '=', conc.period)], limit=1)
            ocioso = fac.fab_ocioso_month if fac else 0.0
            resultado = conc.resultado_modelo - ocioso
            pct = (100.0 * resultado / conc.gl_ventas
                   if conc.gl_ventas else 0.0)
            color = ('#dc3545' if pct < 0
                     else '#fd7e14' if pct < 5 else '#198754')
            html += self._card(
                'Ventas del mes', money(conc.gl_ventas), str(conc.period))
            html += self._card(
                'Margen de productos (mes)', money(conc.resultado_modelo),
                'lo que dejan los vendidos sobre la capacidad que SÍ usan')
            html += self._card(
                'Ociosidad del mes', money(-ocioso) if ocioso else money(0),
                'la capacidad parada la paga el período, no el producto',
                '#fd7e14')
            html += self._card(
                'Resultado del mes (modelo)', money(resultado),
                'margen de productos − ociosidad · %+.1f&#37; s/venta' % pct,
                color)

        # 12 meses: dónde se gana y dónde se pierde (clientes y productos)
        clientes = env['qb.cliente.rentabilidad'].search([])
        productos = env['qb.producto.rentabilidad'].search([])
        sem = lambda pct: ('🔴' if pct < 0 else '🟡' if pct < 5 else '🟢')
        if clientes:
            orden = clientes.sorted('margen_neto_12m')
            html += '<div>'
            html += self._mini_tabla(
                'Clientes que más DEJAN (12m, neto)',
                [(sem(c.margen_neto_pct), c.partner_id.name or '',
                  c.margen_neto_12m, c.margen_neto_pct)
                 for c in reversed(orden[-5:])])
            rojos = orden.filtered(lambda c: c.margen_neto_12m < 0)
            html += self._mini_tabla(
                'Clientes que más CUESTAN (12m, neto)',
                [(sem(c.margen_neto_pct), c.partner_id.name or '',
                  c.margen_neto_12m, c.margen_neto_pct)
                 for c in orden[:5] if c.margen_neto_12m < 0])
            html += '</div>'
        if productos:
            orden = productos.sorted('margen_neto_12m')
            html += '<div>'
            html += self._mini_tabla(
                'Productos que más DEJAN (12m, neto)',
                [(sem(p.margen_neto_pct),
                  p.product_id.default_code or p.product_id.name or '',
                  p.margen_neto_12m, p.margen_neto_pct)
                 for p in reversed(orden[-5:])])
            html += self._mini_tabla(
                'Productos que más CUESTAN (12m, neto)',
                [(sem(p.margen_neto_pct),
                  p.product_id.default_code or p.product_id.name or '',
                  p.margen_neto_12m, p.margen_neto_pct)
                 for p in orden[:5] if p.margen_neto_12m < 0])
            html += '</div>'

        # Cobertura de fijos del mes (la barra del CEO)
        html += self._build_breakeven()

        # ¿Qué necesita acción HOY?
        acciones = []
        if clientes:
            rojos = clientes.filtered(lambda c: c.margen_neto_12m < 0)
            if rojos:
                acciones.append(
                    '🔴 <b>%s clientes con margen neto negativo</b> que '
                    'suman %s/año — ábrelos en «Rentabilidad por cliente» '
                    'para ver su ficha y recotizar.' % (
                        len(rojos),
                        money(sum(rojos.mapped('margen_neto_12m')))))
        Aud = env['qb.peso.auditoria']
        n_pesos_mal = Aud.search_count(
            [('estado', 'in', ('critico', 'revisar'))])
        if n_pesos_mal:
            acciones.append(
                '⚖️ <b>%s productos con peso dudoso</b> en la Auditoría de '
                'pesos (Configuración) — un peso malo infla o esconde su '
                'costo.' % n_pesos_mal)
        hoy = fields.Date.today()
        por_vencer = env['qb.cotizacion'].search_count([
            ('state', 'in', ('draft', 'done')),
            ('validez_hasta', '!=', False),
            ('validez_hasta', '<=', hoy + relativedelta(days=15))])
        if por_vencer:
            acciones.append(
                '⏳ <b>%s cotizaciones vivas vencen en 15 días</b> — '
                'revisarlas en «Cotizaciones guardadas».' % por_vencer)
        if acciones:
            html += ('<h6 style="margin-top:10px;">Necesita acción</h6>'
                     '<ul style="font-size:13px;">%s</ul>'
                     % ''.join('<li>%s</li>' % a for a in acciones))
        return html

    # ------------------------------------------------------------------
    # Semáforo de configuración
    # ------------------------------------------------------------------
    def _build_estado(self):
        env = self.env
        checks = []

        # 1. Maestro de pesos medidos/ingeniería cargado (nativo, sin Supabase)
        n_pesos = env['qb.producto.peso'].search_count(
            [('source', 'in', ('manual', 'cvu'))])
        checks.append((OK if n_pesos else WARN,
                       'Pesos medidos capturados',
                       '%s productos con peso medido/ingeniería' % n_pesos
                       if n_pesos else
                       'corre "Cargar maestro de pesos" en Configuración '
                       '(sin esto el peso se estima del código)'))

        # 2. Workcenters ligados a centros
        total_wc = env['mrp.workcenter'].search_count([])
        linked_wc = len(env['qb.costeo.centro'].search([]).mapped('workcenter_ids'))
        icon = OK if linked_wc else (WARN if total_wc else BAD)
        checks.append((icon, 'Workcenters ligados a centros',
                       '%s de %s máquinas — sin esto el factor $/kg y la '
                       'capacidad de TEJIDO salen en 0. Se liga solo con '
                       '"Importar desde Supabase".' % (linked_wc, total_wc)))

        # 3. Centros fabriles sin fuente de capacidad
        sin_capacidad = env['qb.costeo.centro'].search([
            ('nature', '!=', 'admin'),
            ('workcenter_ids', '=', False),
        ]).filtered(lambda c: not env['qb.turno.config'].search_count(
            [('centro_id', '=', c.id)]))
        checks.append((OK if not sin_capacidad else WARN,
                       'Capacidad por centro',
                       'todos los centros tienen workcenters o turnos'
                       if not sin_capacidad else
                       'sin workcenters NI turnos: %s — captúralos en '
                       '"Turnos / capacidad manual"'
                       % ', '.join(sin_capacidad.mapped('code'))))

        # 4. Pesos por producto
        n_pesos = env['qb.producto.peso'].search_count([])
        checks.append((OK if n_pesos > 100 else WARN,
                       'Maestro de pesos',
                       '%s productos con peso — el import de Supabase trae '
                       '~2,758' % n_pesos))

        # 5. Cuentas sin clasificar
        mapped_accounts = env['qb.costeo.cuenta.map'].search([]).mapped('account_id')
        pending = env['account.account'].search([]).filtered(
            lambda a: a.code and a.code[:1] in '4567' and a not in mapped_accounts)
        checks.append((OK if len(pending) < 10 else WARN,
                       'Clasificación de cuentas',
                       '%s cuentas de resultados sin clasificar' % len(pending)))

        # 5.4 Régimen híbrido: un centro que Odoo ya capitaliza NO puede
        # seguir en el pool del módulo, y su cuenta de costos aplicados tiene
        # que estar clasificada para poder restarla. Las dos mitades del
        # doble conteo son silenciosas.
        Centro = env['qb.costeo.centro']
        Clase = env['qb.costeo.cuenta.class']
        hoy = fields.Date.today()
        absorbidos = Centro.absorbidos_en(hoy)
        n_absorcion = Clase.search_count([('bucket', '=', 'absorcion_odoo')])
        ultimo = env['qb.costo.factores'].search(
            [], order='period DESC', limit=1)
        if absorbidos and not n_absorcion:
            checks.append((
                BAD, 'Absorción por workcenter',
                '%s ya capitaliza por workcenter, pero NINGUNA cuenta está '
                'clasificada como «Absorbido por Odoo». Sin ella el módulo no '
                'puede restar lo capitalizado y ese costo se cuenta DOS '
                'veces: una en el AVCO del producto y otra en el pool.'
                % ', '.join(absorbidos.mapped('code'))))
        elif absorbidos and ultimo and not ultimo.absorcion_bruta_month:
            checks.append((
                WARN, 'Absorción por workcenter',
                '%s está marcado como absorbido y la cuenta está clasificada, '
                'pero el período %s no registra nada capitalizado. O la '
                'tarifa por hora sigue en 0, o la fecha de corte se adelantó.'
                % (', '.join(absorbidos.mapped('code')), ultimo.period)))
        elif (absorbidos and ultimo and ultimo.absorcion_bruta_month
                and not ultimo.absorcion_pool_month):
            checks.append((
                WARN, 'Absorción por workcenter',
                'Odoo capitalizó $%s/mes de %s, pero sus cuentas etiquetadas '
                'y su renta contractual ya sumaban $%s/mes fuera del pool: la '
                'resta neta queda en 0. La tarifa por hora absorbe menos que '
                'el costo que el centro ya tenía identificado — revisa la '
                'tarifa, o que sus cuentas no estén etiquetadas de más.'
                % (f'{ultimo.absorcion_bruta_month:,.0f}',
                   ', '.join(absorbidos.mapped('code')),
                   f'{ultimo.absorcion_ya_fuera_month:,.0f}')))
        elif absorbidos:
            checks.append((
                OK, 'Absorción por workcenter',
                '%s fuera del pool desde %s; Odoo capitalizó $%s/mes, de los '
                'que $%s ya estaban excluidos por centro y renta → se restan '
                '$%s/mes'
                % (', '.join(absorbidos.mapped('code')),
                   min(absorbidos.mapped('fecha_absorcion')),
                   f'{ultimo.absorcion_bruta_month:,.0f}' if ultimo else '0',
                   f'{ultimo.absorcion_ya_fuera_month:,.0f}' if ultimo else '0',
                   f'{ultimo.absorcion_pool_month:,.0f}' if ultimo else '0')))
        elif ultimo and ultimo.absorcion_bruta_month:
            checks.append((
                BAD, 'Absorción por workcenter',
                'Odoo capitalizó $%s/mes de costos fabriles aplicados pero '
                'ningún centro está marcado como absorbido. Marca su modo de '
                'costeo y su fecha de corte, o el pool seguirá arrastrando un '
                'gasto que ya viaja dentro del inventario.'
                % f'{ultimo.absorcion_bruta_month:,.0f}'))

        # 5.5 Renta: contractual vs. GL — el doble conteo es silencioso
        renta_contractual = sum(env['qb.costeo.centro'].search([
            ('nature', 'in', ('fabril_directo', 'fabril_indirecto')),
        ]).mapped('renta_contractual_mxn'))
        if renta_contractual:
            # Una clase de PATRÓN que abarca una cuenta de renta junto a
            # cuarenta que no lo son no se arregla marcándola: marcarla saca
            # las cuarenta del pool. Se arregla separando la cuenta de renta
            # en su propia clasificación, que gana por más específica.
            sin_marcar = Clase.clases_con_renta_mezclada()
            if sin_marcar:
                detalle = (
                    'la renta se está contando DOS VECES: $%s/mes por '
                    'contrato y además por una cuenta de renta que vive '
                    'dentro de %s, en un bucket fabril. NO marques esas '
                    'clases: abarcan mucho más que la renta y marcarlas '
                    'sacaría todo del pool. Dale a la cuenta de renta su '
                    'propia clasificación específica y marca esa.'
                    % (f'{renta_contractual:,.0f}',
                       ', '.join(sin_marcar.mapped('name'))))
            else:
                detalle = ('renta contractual $%s/mes en el pool; las cuentas '
                           'de renta del GL están marcadas y fuera'
                           % f'{renta_contractual:,.0f}')
            checks.append((BAD if sin_marcar else OK,
                           'Renta sin doble conteo', detalle))

        # 5.6 Aduana: ¿se está capitalizando con landed costs, o se queda en
        # resultados? El pedimento sabe a qué embarque pertenece; prorratearlo
        # con una fórmula le cobra al hilo el pedimento de una máquina.
        mal_ubicadas = Clase.cuentas_de_importacion_mal_ubicadas()
        n_import = Clase.search_count([('bucket', '=', 'importacion')])
        if mal_ubicadas:
            checks.append((
                WARN, 'Cuentas de aduana',
                'estas cuentas de aduana están en un bucket que las reparte '
                'por el driver equivocado (sobre TODAS las ventas, o fuera de '
                'costeo): %s. Muévelas al bucket «Gastos e impuestos de '
                'importación» para al menos poder medirlas.'
                % ', '.join(mal_ubicadas.mapped('name'))))
        elif not n_import:
            checks.append((
                WARN, 'Cuentas de aduana',
                'ninguna cuenta en el bucket «Gastos e impuestos de '
                'importación» — si importas, no hay forma de saber cuánta '
                'aduana se está quedando en resultados'))
        else:
            checks.append((OK, 'Cuentas de aduana',
                           '%s cuentas clasificadas' % n_import))

        if n_import:
            desde = fields.Date.today() - relativedelta(months=12)
            aduana = env['qb.costo.factores'].search(
                [], order='period DESC', limit=1).importacion_pool_month or 0.0
            capitalizado = sum(env['stock.landed.cost'].search([
                ('state', '=', 'done'), ('date', '>=', desde),
            ]).mapped('amount_total')) / 12.0
            if aduana and capitalizado < 0.25 * aduana:
                checks.append((
                    BAD, 'Aduana capitalizada (landed cost)',
                    '$%s/mes de aduana en resultados contra solo $%s/mes '
                    'capitalizado con landed costs. El pedimento sabe a qué '
                    'embarque pertenece: captúralo en la recepción y caerá en '
                    'los productos que lo causaron. Prorratearlo con una '
                    'fórmula le cobra al hilo el pedimento de una máquina.'
                    % (f'{aduana:,.0f}', f'{capitalizado:,.0f}')))
            else:
                checks.append((
                    OK, 'Aduana capitalizada (landed cost)',
                    '$%s/mes capitalizado con landed costs contra $%s/mes en '
                    'resultados' % (f'{capitalizado:,.0f}', f'{aduana:,.0f}')))

        # 5.7 MP: ¿hay contra qué conciliar la receta?
        n_mp = Clase.search_count([('bucket', '=', 'mp')])
        if not n_mp:
            checks.append((
                WARN, 'Conciliación de materia prima',
                'ninguna cuenta en el bucket «Materia prima» — la MP de '
                'receta no se está comparando contra el costo primo del '
                'mayor, así que la merma y la variación de precio no entran '
                'al costo'))
        else:
            ajuste = env['qb.costo.factores'].search(
                [], order='period DESC', limit=1).mp_ajuste or 1.0
            checks.append((
                OK, 'Conciliación de materia prima',
                '%s cuentas de costo primo; ajuste vigente ×%.3f '
                '(receta → consumo real)' % (n_mp, ajuste)))

        # 5.8 Capacidad normal: sin ella el producto carga la ociosidad
        denominadores = env['qb.costeo.centro'].search(
            ['|', ('es_denominador_kg', '=', True),
             ('es_denominador_m', '=', True)])
        caps = {o.centro_id.id: o.capacity_month_units
                for o in env['qb.ociosidad'].search(
                    [('centro_id', 'in', denominadores.ids)])}
        sin_normal = denominadores.filtered(lambda c: caps.get(c.id, 0.0) <= 0)
        if sin_normal:
            checks.append((
                WARN, 'Capacidad normal (IAS 2)',
                'estos centros definen el denominador y NO tienen capacidad '
                'normal derivable: %s. Su pool fijo se divide entre la '
                'producción real, así que un mes flojo encarece el producto. '
                'Captura su throughput nominal (o su capacidad normal) en '
                'Centros de costo.' % ', '.join(sin_normal.mapped('code'))))
        else:
            checks.append((
                OK, 'Capacidad normal (IAS 2)',
                'el pool fijo se divide entre capacidad normal; la ociosidad '
                'va al resultado del período, no al producto'))

        # 5.9 Cuello de botella: sin throughput el ranking apunta al centro
        # equivocado
        sin_throughput = env['qb.costeo.centro'].search([
            ('nature', '=', 'fabril_directo'),
            ('std_output_per_hour', '=', 0),
        ])
        if sin_throughput:
            checks.append((
                WARN, 'Throughput por centro',
                'sin throughput nominal: %s. La contribución por '
                'hora-máquina los ignora, así que el ranking mide el centro '
                'equivocado cuando el cuello real está en uno de ellos.'
                % ', '.join(sin_throughput.mapped('code'))))

        # 5.9.5 Ventana fabril corta tras un corte de absorción
        if ultimo and ultimo.fab_ventana_meses and \
                ultimo.fab_ventana_meses < 3 and absorbidos:
            checks.append((
                WARN, 'Ventana del pool fabril',
                'solo %s mes(es) desde el corte de %s. El pool no se puede '
                'promediar con meses del régimen anterior —llevaban el gasto '
                'del centro completo— así que arranca corto y ruidoso. Se '
                'estabiliza solo; mientras tanto, lee los factores del mes '
                'con esa reserva.'
                % (ultimo.fab_ventana_meses,
                   ', '.join(absorbidos.mapped('code')))))

        # 5.10 El ajuste de metros pierde su contrapeso si el estiramiento
        # se detiene: resta encogimiento y suma estiramiento, y se compensan.
        desde = fields.Date.today().replace(day=1) - relativedelta(months=6)
        sin_est = env['qb.costo.producto']._meses_sin_estiramiento(
            desde, fields.Date.today() + relativedelta(months=1))
        if sin_est:
            checks.append((
                WARN, 'Ajuste de metros (encogimiento/estiramiento)',
                'estos meses tienen encogimiento pero CERO estiramiento: %s. '
                'El ajuste resta metros sin su contrapeso, así que el costo '
                'por metro sale alto por una operación que dejó de hacerse, '
                'no porque la planta gaste más. Si el estiramiento se '
                'suspendió de verdad, revisa si el encogimiento debe seguir '
                'descontándose.'
                % ', '.join(str(m) for m in sin_est)))

        # 5.11 Períodos calculados ANTES del último cambio de pesos. El
        # caso WD3846NT163m2: se corrigió su peso y los meses recalculados
        # antes de la captura se quedaron con el criterio viejo — la misma
        # ventana de 12 meses mezclaba dos pesos y el producto "perdía"
        # $1M que no perdía. Un período abierto más viejo que el maestro
        # de pesos es un reporte mintiendo en silencio.
        ultimo_peso = env['qb.producto.peso'].search(
            [], order='write_date desc', limit=1)
        if ultimo_peso:
            desfasados = env['qb.costo.factores'].search([
                ('state', '=', 'borrador'),
                ('write_date', '<', ultimo_peso.write_date)])
            if desfasados:
                periodos = ', '.join(
                    str(p) for p in sorted(desfasados.mapped('period'))[:8])
                if len(desfasados) > 8:
                    periodos += '…'
                checks.append((
                    WARN, 'Períodos vs maestro de pesos',
                    '%s período(s) abiertos calculados ANTES del último '
                    'cambio de pesos (%s): su costo usa el peso viejo y la '
                    'ventana de 12 meses mezcla criterios. Corre '
                    '«Recalcular costeo (rango de meses)» sobre: %s'
                    % (len(desfasados),
                       ultimo_peso.write_date.strftime('%Y-%m-%d %H:%M'),
                       periodos)))
            else:
                checks.append((
                    OK, 'Períodos vs maestro de pesos',
                    'todos los períodos abiertos son posteriores al último '
                    'cambio de pesos'))

        # 5.12 Cola de recálculo atorada: el cron diferido murió una vez
        # dejando 6 meses de 2024 pendientes por 2 días — nadie lo vio
        # porque el cron simplemente dejó de correr.
        Config = env['qb.costeo.factor.config']
        cola = Config.search([('key', '=', 'recalculo_pendiente')], limit=1)
        pendientes_cola = [p for p in (cola.value_text or '').split(',') if p]
        if pendientes_cola:
            cron = env.ref('qb_capacidad_costeo.cron_recalculo_pendientes',
                           raise_if_not_found=False)
            if not cron or not cron.active:
                checks.append((
                    BAD, 'Cola de recálculo diferido',
                    '%s período(s) esperando en la cola y el cron está '
                    'APAGADO — la cola quedó atorada. Actívalo en Ajustes '
                    '→ Técnico → Acciones planificadas («Recálculo '
                    'diferido de históricos») o corre el recálculo por '
                    'rango.' % len(pendientes_cola)))
            else:
                checks.append((
                    OK, 'Cola de recálculo diferido',
                    '%s período(s) en cola, cron activo — convergiendo'
                    % len(pendientes_cola)))

        # 5.13 AVCO de importados vs su compra real. El costo del ' I' es su
        # AVCO (compra del IT + gastos de la OP de conversión) y nadie lo
        # validaba contra la fuente: el KP2032T11GO152 I traía 9.39 —
        # calcado del gemelo nacional — cuando su IT real (KP4032T11GO152
        # IT) se compró a ~6.10. +54% de MP fantasma en $511K de venta.
        if ultimo:
            Costo = env['qb.costo.producto']
            imp_rows = Costo.search([
                ('period', '=', ultimo.period),
                ('product_bucket', '=', 'importado'),
                ('qty_vendida', '>', 0)])
            divergentes = []
            for row in imp_rows:
                it = Costo._it_twin(row.product_id)
                if not it:
                    continue
                compra = Costo._last_purchase_cost(it)
                if compra <= 0 or row.mp_unit <= 0:
                    continue
                delta = (row.mp_unit - compra) / compra
                if abs(delta) > 0.35:
                    divergentes.append((abs(delta), delta, row, compra))
            if divergentes:
                divergentes.sort(key=lambda d: d[0], reverse=True)
                det = '; '.join(
                    '%s: modelo $%.2f vs compra IT $%.2f (%+.0f%%)'
                    % (row.default_code, row.mp_unit, compra, delta * 100)
                    for _absd, delta, row, compra in divergentes[:6])
                if len(divergentes) > 6:
                    det += '…'
                checks.append((
                    WARN, 'AVCO de importados vs compra IT',
                    '%s importado(s) vendidos con MP a más de ±35%% de la '
                    'última compra de su gemelo IT: %s. Puede ser AVCO '
                    'seteado a mano, capas viejas de inventario o un precio '
                    'nuevo — revisa el costo del producto en Odoo.'
                    % (len(divergentes), det)))
            elif imp_rows:
                checks.append((
                    OK, 'AVCO de importados vs compra IT',
                    'los %s importados vendidos del período están a ±35%% '
                    'de su última compra IT' % len(imp_rows)))

        # 5.14 Consumo de BOM vs consumo real de las OPs. El caso X140
        # (ago-2026): la BOM de acabado decía 0.2674 kg de tejido por
        # metro y las OPs done consumían 0.2474 — 8% de hilo fantasma
        # que le cargaba $1/m de MP a un producto que "perdía" $72K/mes
        # sin perderlos (y +13% en los SCRIM). La receta duplica un dato
        # vivo (lo que las OPs consumieron) y nadie los comparaba.
        uom_m = env.ref('uom.product_uom_meter', raise_if_not_found=False)
        uom_kg = env.ref('uom.product_uom_kgm', raise_if_not_found=False)
        if uom_m and uom_kg:
            hace_12m = fields.Date.today() - relativedelta(months=12)
            env.cr.execute("""
                WITH cons AS (
                    SELECT sm.raw_material_production_id AS mo_id,
                           sm.product_id AS comp_id,
                           SUM(sm.quantity) AS consumed
                      FROM stock_move sm
                     WHERE sm.state = 'done'
                       AND sm.raw_material_production_id IS NOT NULL
                       AND sm.date >= %s
                     GROUP BY 1, 2
                ),
                reales AS (
                    SELECT mp.product_id AS out_id, c.comp_id,
                           SUM(c.consumed) AS consumed,
                           SUM(mp.product_qty) AS produced
                      FROM cons c
                      JOIN mrp_production mp ON mp.id = c.mo_id
                     WHERE mp.state = 'done'
                       AND mp.product_uom_id = %s
                       AND mp.company_id = %s
                     GROUP BY 1, 2
                    HAVING SUM(mp.product_qty) >= %s
                ),
                lineas AS (
                    SELECT pp.id AS out_id, bl.product_id AS comp_id,
                           MIN(bl.product_qty) AS q_min,
                           MAX(bl.product_qty) AS q_max
                      FROM mrp_bom_line bl
                      JOIN mrp_bom b ON b.id = bl.bom_id
                      JOIN product_product pp
                        ON pp.product_tmpl_id = b.product_tmpl_id
                     WHERE b.active
                       AND bl.product_uom_id = %s
                       AND bl.product_qty >= 0.02
                     GROUP BY 1, 2
                )
                SELECT r.out_id, r.comp_id, r.consumed, r.produced,
                       l.q_min, l.q_max
                  FROM reales r
                  JOIN lineas l ON l.out_id = r.out_id
                              AND l.comp_id = r.comp_id
            """, (hace_12m, uom_m.id, env.company.id, 50000.0, uom_kg.id))
            desviadas = []
            for out_id, comp_id, _cons, produced, q_min, q_max in \
                    env.cr.fetchall():
                real = _cons / produced if produced else 0.0
                if real <= 0:
                    continue
                # Con recetas alternativas basta que UNA esté calibrada:
                # manda la BOM más cercana al consumo real.
                cerca = min((q_min, q_max), key=lambda q: abs(q - real))
                delta = (cerca - real) / real
                if abs(delta) > 0.05:
                    desviadas.append(
                        (abs(delta), delta, out_id, comp_id, cerca, real))
            if desviadas:
                desviadas.sort(reverse=True)
                Prod = env['product.product']
                det = '; '.join(
                    '%s ← %s: BOM %.4f vs real %.4f (%+.0f%%)'
                    % (Prod.browse(o).default_code or Prod.browse(o).name,
                       Prod.browse(c).default_code or Prod.browse(c).name,
                       q, real, delta * 100)
                    for _a, delta, o, c, q, real in desviadas[:6])
                if len(desviadas) > 6:
                    det += '…'
                checks.append((
                    WARN, 'Consumo de BOM vs OPs reales',
                    '%s receta(s) kg→m con consumo a más de ±5%% de lo '
                    'que las OPs terminadas consumieron en 12 meses: %s. '
                    'La MP del costeo sale de la receta — valida el factor '
                    'con producción, corrige la BOM y recalcula.'
                    % (len(desviadas), det)))
            else:
                checks.append((
                    OK, 'Consumo de BOM vs OPs reales',
                    'las recetas kg→m con volumen (≥50,000 m/12m) están a '
                    '±5% del consumo real de las OPs'))

        # 5.15 Producción arriba de la capacidad normal: el caso Acabado
        # (952K m reales vs 915,733 capturados, una rama nueva sin
        # reflejar). El modelo topaba la utilización en 100 y ponía el
        # ocioso en cero — escondía el error en vez de señalarlo. Ahora el
        # período usa la producción real como denominador (IAS 2) y este
        # check exige actualizar la capacidad del centro.
        #
        # Mira una VENTANA, no el último período: con capacidad de Acabado
        # en 915,733, enero–mayo de 2026 la superaron y junio–agosto no, así
        # que el check se pintaba verde mirando agosto mientras cinco meses
        # del mismo año seguían rojos. Un mes flojo no arregla la capacidad
        # mal capturada; solo la esconde.
        meses_cap = int(Config.get_param('capacidad_superada_meses', 12.0))
        recientes = env['qb.costo.factores'].search(
            [('company_id', '=', env.company.id)],
            order='period DESC', limit=max(meses_cap, 1))
        superados = recientes.filtered(
            lambda f: f.capacidad_superada_m or f.capacidad_superada_kg)
        if superados:
            en_m = superados.filtered('capacidad_superada_m')
            en_kg = superados.filtered('capacidad_superada_kg')
            lados = []
            if en_m:
                lados.append('METROS (hasta %s m/mes producidos — la '
                             'capacidad capturada de los centros '
                             'es_denominador_m quedó abajo)'
                             % '{:,.0f}'.format(
                                 max(en_m.mapped('m_produccion_month'))))
            if en_kg:
                lados.append('KG (hasta %s kg/mes producidos)'
                             % '{:,.0f}'.format(
                                 max(en_kg.mapped('kg_produccion_month'))))
            periodos = ', '.join(
                str(p) for p in sorted(superados.mapped('period'))[:6])
            if len(superados) > 6:
                periodos += '…'
            checks.append((
                BAD, 'Capacidad normal vs producción real',
                '%s de los últimos %s períodos SUPERAN la capacidad normal '
                'capturada en %s: hay máquinas/ramas sin reflejar (%s). El '
                'costeo ya usa la producción real como denominador (IAS 2) '
                'para no sobre-absorber, pero la ociosidad de ese lado '
                'sale en cero por construcción — actualiza la capacidad '
                'del centro en Configuración → Centros de costo.'
                % (len(superados), len(recientes), ' y '.join(lados),
                   periodos)))
        elif recientes:
            checks.append((
                OK, 'Capacidad normal vs producción real',
                'la producción de los últimos %s períodos cabe dentro de la '
                'capacidad normal capturada en ambos lados (kg y m)'
                % len(recientes)))

        # 5.16 La capacidad capturada duplica un dato vivo — horario de
        # planta × velocidad de máquina — y hasta hoy nadie los comparaba:
        # `capacidad_normal`, cuando está capturada, GANA sobre el cálculo
        # de turnos y lo deja mudo. Así fue como Acabado se quedó en
        # 915,733 m/mes mientras sus dos ramas daban 1.18M: el número vivió
        # dos años sin que nada lo contradijera. Misma regla que ya cubre
        # pesos (5.11), AVCO de importados (5.13) y consumo de BOM (5.14).
        tol_cap = Config.get_param('capacidad_capturada_tol_pct', 10.0)
        Turno = env['qb.turno.config']
        divergentes, sin_base = [], []
        for centro in env['qb.costeo.centro'].search(
                [('nature', '!=', 'admin'), ('capacidad_normal', '>', 0),
                 ('company_id', '=', env.company.id)]):
            turnos = Turno.search([('centro_id', '=', centro.id),
                                   ('active', '=', True)])
            horas = sum(t.hours_per_month() for t in turnos)
            derivada = horas * (centro.std_output_per_hour or 0.0)
            if not derivada:
                sin_base.append(centro.code)
                continue
            delta = (centro.capacidad_normal - derivada) / derivada
            if abs(delta) * 100.0 > tol_cap:
                divergentes.append((abs(delta), centro.code,
                                    centro.capacidad_normal, derivada, delta))
        if divergentes:
            divergentes.sort(reverse=True)
            det = '; '.join(
                '%s: capturada %s vs horario×velocidad %s (%+.0f%%)'
                % (code, '{:,.0f}'.format(cap), '{:,.0f}'.format(der),
                   delta * 100)
                for _a, code, cap, der, delta in divergentes)
            checks.append((
                WARN, 'Capacidad capturada vs horario × velocidad',
                '%s centro(s) con la capacidad capturada a más de ±%.0f%% de '
                'lo que dan sus turnos por su throughput nominal: %s. Uno de '
                'los dos está viejo — el horario, la velocidad o el número '
                'capturado.' % (len(divergentes), tol_cap, det)))
        elif sin_base:
            checks.append((
                WARN, 'Capacidad capturada vs horario × velocidad',
                'sin turnos o sin throughput nominal para contrastar: %s. '
                'Captúralos en "Turnos / capacidad manual" y en el centro; '
                'sin ellos la capacidad capturada no tiene contra qué '
                'validarse.' % ', '.join(sorted(sin_base))))
        else:
            checks.append((
                OK, 'Capacidad capturada vs horario × velocidad',
                'la capacidad capturada de cada centro cuadra a ±%.0f%% con '
                'sus turnos por su throughput nominal' % tol_cap))

        # 5.17 La capacidad de un centro NO es fungible. Tejido tiene 27
        # circulares y 197,529 kg/mes, pero el WJ044 de 235 cm solo sale en
        # las diez galga 18 Ø32 — y esas van al 79% mientras la planta va al
        # 44%. Dos cosas que se pueden validar solas: que la suma de las
        # familias cuadre con su centro, y que la producción del centro esté
        # catalogada en alguna familia (lo que no está, no se puede rutear).
        Familia = env['qb.costeo.familia']
        familias = Familia.search([('company_id', '=', env.company.id)])
        if familias:
            descuadre, saturadas = [], []
            for centro in familias.mapped('centro_id'):
                suma = sum(familias.filtered(
                    lambda f: f.centro_id == centro).mapped('capacidad_normal'))
                if not (centro.capacidad_normal and suma):
                    continue
                delta = (suma - centro.capacidad_normal) / centro.capacidad_normal
                if abs(delta) > 0.10:
                    descuadre.append((centro.code, suma,
                                      centro.capacidad_normal, delta))
            for fila in env['qb.familia.carga'].search([]):
                if fila.utilization_pct >= 75.0:
                    saturadas.append((fila.utilization_pct,
                                      fila.familia_id.code))
            if descuadre:
                det = '; '.join(
                    '%s: familias %s vs centro %s (%+.0f%%)'
                    % (code, '{:,.0f}'.format(suma),
                       '{:,.0f}'.format(centro_cap), delta * 100)
                    for code, suma, centro_cap, delta in descuadre)
                checks.append((
                    WARN, 'Familias de máquinas vs capacidad del centro',
                    'la capacidad de las familias no suma la del centro: %s. '
                    'Una de las dos está vieja — o falta dar de alta una '
                    'familia.' % det))
            elif saturadas:
                saturadas.sort(reverse=True)
                checks.append((
                    WARN, 'Familias de máquinas',
                    'la capacidad del centro cuadra con sus familias, pero '
                    '%s de ellas van arriba del 75%%: %s. El agregado del '
                    'centro NO las ve — para decidir si cabe un pedido, mira '
                    'la familia que lo puede hacer.'
                    % (len(saturadas),
                       ', '.join('%s (%.0f%%)' % (c, u) for u, c in saturadas[:5]))))
            else:
                checks.append((
                    OK, 'Familias de máquinas',
                    '%s familias dadas de alta, su capacidad cuadra con la de '
                    'sus centros y ninguna pasa del 75%% de carga'
                    % len(familias)))
        else:
            checks.append((
                WARN, 'Familias de máquinas',
                'ninguna familia dada de alta: la capacidad de cada centro se '
                'lee como si cualquier máquina hiciera cualquier producto, y '
                'un centro medio vacío puede tener su familia clave '
                'saturada.'))

        # 6. Factores calculados
        factores = env['qb.costo.factores'].search([], order='period DESC', limit=1)
        if not factores:
            checks.append((BAD, 'Factores de costeo',
                           'nunca calculados — corre "Recalcular costeo"'))
        else:
            detail = ('período %s: fab $%.2f/kg + $%.2f/m, energía $%.2f/kg, '
                      'op %.1f%%, cobertura %.0f%%'
                      % (factores.period, factores.factor_fab_kg,
                         factores.factor_fab_m, factores.energia_por_kg,
                         factores.op_pct * 100, factores.cobertura_fab_pct))
            icon = OK if factores.factor_fab_kg and factores.op_pct else WARN
            checks.append((icon, 'Factores de costeo', detail))

        # 7. Costos por producto
        n_costos = env['qb.costo.producto'].search_count(
            [('period', '=', factores.period)]) if factores else 0
        checks.append((OK if n_costos else WARN, 'Costo por producto',
                       '%s productos costeados en el último período' % n_costos))

        rows = ''.join(
            '<tr><td style="padding:4px 8px;">%s</td>'
            '<td style="padding:4px 8px;"><b>%s</b></td>'
            '<td style="padding:4px 8px;">%s</td></tr>' % c for c in checks)
        table = '<table class="table table-sm"><tbody>%s</tbody></table>' % rows
        # Siempre colapsado: la configuración es de la puesta a punto, no
        # del día a día. El resumen dice si hay algo que atender.
        pendientes = [c for c in checks if c[0] != OK]
        if not pendientes:
            resumen = '%s <b>Configuración completa</b>' % OK
        else:
            resumen = ('%s <b>Configuración: %s punto(s) por revisar</b>'
                       % (WARN if all(c[0] == WARN for c in pendientes)
                          else BAD, len(pendientes)))
        return ('<details><summary style="cursor:pointer;">%s — clic para '
                'el detalle</summary>%s</details>' % (resumen, table))

    # ------------------------------------------------------------------
    # KPIs del mes
    # ------------------------------------------------------------------
    def _build_kpis(self):
        env = self.env
        balance = env['qb.balance'].search([])
        if not balance or not any(b.capacity_equiv_m for b in balance):
            return ('<p class="text-muted">Sin datos de capacidad todavía — '
                    'completa el semáforo de arriba.</p>')
        cuello = balance.filtered('is_bottleneck')[:1]
        idle = sum(env['qb.ociosidad'].search([]).mapped('idle_cost_month'))
        cards = []
        for b in balance.sorted('capacity_equiv_m'):
            color = ('#dc3545' if b.is_bottleneck
                     else '#198754' if b.utilization_pct < 70 else '#fd7e14')
            cards.append(
                '<div style="display:inline-block;min-width:170px;margin:4px;'
                'padding:10px;border:1px solid #dee2e6;border-radius:8px;'
                'border-left:4px solid %s;">'
                '<div style="font-weight:bold;">%s%s</div>'
                '<div>Utilización: %.0f%%</div>'
                '<div class="text-muted" style="font-size:12px;">'
                'cap. %s m-equiv/mes</div></div>'
                % (color, b.centro_id.code,
                   ' 🔒 CUELLO' if b.is_bottleneck else '',
                   b.utilization_pct, f'{b.capacity_equiv_m:,.0f}'))
        header = (
            '<p><b>Cuello de botella:</b> %s — techo de planta. &nbsp; '
            '<b>Costo ocioso del mes:</b> $%s</p>'
            % (cuello.centro_id.code if cuello else 'n/d', f'{idle:,.0f}'))
        # El breakeven vive arriba, en «¿Cómo va el negocio?» — aquí solo
        # la capacidad.
        return header + ''.join(cards) + self._build_tendencia()

    def _build_breakeven(self):
        """La pregunta del CEO: ¿la contribución del mes ya cubrió los
        fijos? (fabricación + entretelas + operación). Arriba del 100%,
        cada peso de contribución adicional es utilidad."""
        factores = self.env['qb.costo.factores'].search(
            [], order='period DESC', limit=1)
        if not factores:
            return ''
        contrib = sum(self.env['qb.costo.producto'].search([
            ('period', '=', factores.period)]).mapped('contrib_total'))
        fijos = (factores.fab_pool_month + factores.entretela_pool_month
                 + factores.op_pool_month)
        if not fijos:
            return ''
        pct = 100.0 * contrib / fijos
        color = '#198754' if pct >= 100 else (
            '#fd7e14' if pct >= 80 else '#dc3545')
        return (
            '<h5 style="margin-top:12px;">Cobertura de fijos — %s</h5>'
            '<p>Contribución del mes <b>$%s</b> vs fijos <b>$%s</b> '
            '(fabricación + entretelas + operación):</p>'
            '<div style="max-width:420px;background:#e9ecef;'
            'border-radius:6px;"><div style="width:%s%%;background:%s;'
            'border-radius:6px;padding:3px 8px;color:white;'
            'white-space:nowrap;"><b>%.0f%%</b>%s</div></div>'
            % (factores.period, f'{contrib:,.0f}', f'{fijos:,.0f}',
               min(pct, 100), color, pct,
               ' — arriba de aquí todo es utilidad' if pct >= 100 else
               ' — faltan $%s' % f'{max(fijos - contrib, 0):,.0f}'))

    def _build_tendencia(self):
        """Mini-tendencia de los últimos snapshots mensuales (utilización
        promedio y costo ocioso). El detalle vive en Histórico → Tendencia."""
        snapshots = self.env['qb.costeo.snapshot'].search(
            [], order='period DESC', limit=6)
        if not snapshots:
            return ''
        rows = ''
        for snap in reversed(snapshots):
            lines = snap.line_ids
            util = (sum(lines.mapped('utilization_pct')) / len(lines)
                    if lines else 0.0)
            idle = sum(lines.mapped('idle_cost_month'))
            rows += (
                '<tr><td style="padding:2px 8px;">%s</td>'
                '<td style="padding:2px 8px;text-align:right;">%.0f%%</td>'
                '<td style="padding:2px 8px;text-align:right;">$%s</td></tr>'
                % (snap.period.strftime('%Y-%m'), util, f'{idle:,.0f}'))
        return ('<h5 style="margin-top:12px;">Tendencia (snapshots)</h5>'
                '<table class="table table-sm" style="max-width:420px;">'
                '<thead><tr><th>Mes</th><th style="text-align:right;">'
                'Utilización prom.</th><th style="text-align:right;">'
                'Costo ocioso</th></tr></thead><tbody>%s</tbody></table>' % rows)

    # ------------------------------------------------------------------
    # Botones de acción directa
    # ------------------------------------------------------------------
    def _open(self, xmlid):
        action = self.env['ir.actions.act_window']._for_xml_id(
            'qb_capacidad_costeo.%s' % xmlid)
        return action

    def action_cargar_pesos(self):
        """Carga el maestro de pesos nativo (sin Supabase) y abre la lista."""
        self.env['qb.producto.peso'].load_weight_master()
        return self._open('producto_peso_action')

    def action_recalcular(self):
        self.env['qb.costo.producto'].action_recompute_period()
        return self._open('costo_factores_action')

    def action_cuentas_pendientes(self):
        return self.env['qb.costeo.cuenta.class'].action_unclassified_accounts()

    def action_turnos(self):
        return self._open('turno_config_action')

    def action_balance(self):
        return self._open('balance_action')

    def action_cotizar(self):
        return self._open('cotizador_wizard_action')

    def action_ranking(self):
        return self._open('costo_ranking_action')

    def action_rentabilidad_clientes(self):
        return self._open('cliente_rentabilidad_action')

    def action_rentabilidad_productos(self):
        return self._open('producto_rentabilidad_action')
