# -*- coding: utf-8 -*-
"""Panel: puerta de entrada del módulo.

Semáforo de configuración (qué falta y qué número desbloquea) + KPIs del
mes. Es la respuesta a "¿por qué todo sale en cero?": cada prerequisito
se muestra con su estado y un botón directo para resolverlo.
"""
from dateutil.relativedelta import relativedelta

from odoo import api, fields, models

OK = '✅'
WARN = '⚠️'
BAD = '❌'


class QbCosteoPanel(models.TransientModel):
    _name = 'qb.costeo.panel'
    _description = 'Panel de capacidad y costeo'

    estado_html = fields.Html(compute='_compute_panel', sanitize=False)
    kpi_html = fields.Html(compute='_compute_panel', sanitize=False)

    def _compute_panel(self):
        for rec in self:
            rec.estado_html = rec._build_estado()
            rec.kpi_html = rec._build_kpis()

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
        if all(c[0] == OK for c in checks):
            # Todo en verde: colapsar el detalle para dejar el foco en KPIs
            return ('<details><summary style="cursor:pointer;">%s '
                    '<b>Configuración completa</b> — clic para el detalle'
                    '</summary>%s</details>' % (OK, table))
        return table

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
        return (header + ''.join(cards) + self._build_breakeven()
                + self._build_tendencia())

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
