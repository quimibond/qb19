# -*- coding: utf-8 -*-
"""Panel: puerta de entrada del módulo.

Semáforo de configuración (qué falta y qué número desbloquea) + KPIs del
mes. Es la respuesta a "¿por qué todo sale en cero?": cada prerequisito
se muestra con su estado y un botón directo para resolverlo.
"""
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

        # 1. Credenciales Supabase (para el import automático)
        icp = env['ir.config_parameter'].sudo()
        has_creds = bool(icp.get_param('quimibond_intelligence.supabase_url')
                         and icp.get_param('quimibond_intelligence.supabase_service_key'))
        checks.append((OK if has_creds else WARN,
                       'Credenciales de Supabase',
                       'listas (import automático disponible)' if has_creds
                       else 'faltan los ir.config_parameter del sync'))

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
        return ('<table class="table table-sm"><tbody>%s</tbody></table>' % rows)

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
        return header + ''.join(cards)

    # ------------------------------------------------------------------
    # Botones de acción directa
    # ------------------------------------------------------------------
    def _open(self, xmlid):
        action = self.env['ir.actions.act_window']._for_xml_id(
            'qb_capacidad_costeo.%s' % xmlid)
        return action

    def action_importar_supabase(self):
        return self._open('supabase_import_action')

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
