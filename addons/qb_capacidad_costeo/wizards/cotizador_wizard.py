# -*- coding: utf-8 -*-
"""Cotizador: calculadora viva de capacidad y costo.

Los resultados (costo por capa, pisos, contribución, capacidad) son campos
computados que se recalculan EN VIVO al cambiar producto, volumen, precio o
margen — sensación de hoja de cálculo, pero con datos que entran solos de
Odoo (último costo de BOM, factores del GL, horas-máquina libres). El botón
solo GUARDA el escenario elegido como qb.cotizacion con sus supuestos.
"""
from odoo import api, fields, models
from odoo.exceptions import UserError

KG_UOM_NAMES = ('kg', 'kgs', 'kilogramo', 'kilogramos')


class QbCotizadorWizard(models.TransientModel):
    _name = 'qb.cotizador.wizard'
    _description = 'Cotizador de capacidad y costo (calculadora viva)'

    # ------------------------------------------------------------------
    # Inputs
    # ------------------------------------------------------------------
    partner_id = fields.Many2one('res.partner', string='Cliente')
    product_id = fields.Many2one(
        'product.product', string='Producto existente',
        domain=[('sale_ok', '=', True)])
    spec_mode = fields.Boolean(
        string='Especificación nueva',
        help='Cotizar un producto que aún no existe en Odoo, con gramaje/'
             'ancho/receta tentativa.')
    spec_descripcion = fields.Char(string='Descripción')
    spec_gramaje = fields.Float(string='Gramaje (g/m²)')
    spec_ancho = fields.Float(string='Ancho (m)', default=1.5)
    spec_galga = fields.Char(string='Galga')
    spec_bucket = fields.Selection([
        ('tela', 'Tela'),
        ('entretela_tejida', 'Entretela tejida'),
        ('entretela_carda', 'Entretela carda'),
        ('importado', 'Importado'),
    ], string='Familia', default='tela')
    spec_mp_unit = fields.Float(
        string='MP estimada $/u',
        help='Costo de MP por unidad de la receta tentativa (a último costo).')
    spec_centro_ids = fields.Many2many(
        'qb.costeo.centro', string='Ruta (centros)',
        help='Centros por los que pasaría. Vacío = según familia.')
    volumen = fields.Float(string='Volumen (unidades/mes)')
    precio_objetivo = fields.Float(string='Precio objetivo $/u')
    target_margin = fields.Float(
        string='Margen meta %',
        help='0 = usar el target_margin de configuración.')
    fx_rate = fields.Float(string='FX (MXN/USD)')

    # ------------------------------------------------------------------
    # Resultados EN VIVO (computados, se refrescan al teclear)
    # ------------------------------------------------------------------
    factores_id = fields.Many2one(
        'qb.costo.factores', compute='_compute_cotizacion',
        string='Factores usados')
    factores_info = fields.Char(
        compute='_compute_cotizacion', string='Base de cálculo')
    product_bucket = fields.Char(
        compute='_compute_cotizacion', string='Familia detectada')
    kg_per_unit = fields.Float(
        compute='_compute_cotizacion', string='Peso (kg/u)', digits=(16, 4))
    mp_unit = fields.Float(
        compute='_compute_cotizacion', string='MP $/u', digits=(16, 4))
    energia_unit = fields.Float(
        compute='_compute_cotizacion', string='Energía $/u', digits=(16, 4))
    fab_unit = fields.Float(
        compute='_compute_cotizacion', string='Fabricación $/u', digits=(16, 4))
    costo_variable = fields.Float(
        compute='_compute_cotizacion', string='Costo variable $/u', digits=(16, 4))
    op_pct_display = fields.Float(
        compute='_compute_cotizacion', string='Operación % s/venta')
    precio_sugerido = fields.Float(
        compute='_compute_cotizacion', string='Precio sugerido $/u', digits=(16, 4))
    piso_ocioso = fields.Float(
        compute='_compute_cotizacion', string='Piso con capacidad ociosa $/u',
        digits=(16, 4),
        help='= costo variable. Con capacidad ociosa, todo precio arriba de '
             'esto APORTA a fijos.')
    piso_lleno = fields.Float(
        compute='_compute_cotizacion', string='Piso a planta llena $/u',
        digits=(16, 4),
        help='= (variable + fab) ÷ (1 − op%): margen cero absorbiendo todo.')
    margen_contribucion = fields.Float(
        compute='_compute_cotizacion', string='Contribución $/u', digits=(16, 4))
    margen_contribucion_pct = fields.Float(
        compute='_compute_cotizacion', string='Contribución %')
    contrib_hora_maquina = fields.Float(
        compute='_compute_cotizacion', string='Contribución $/hora-máquina')
    capacity_ok = fields.Boolean(
        compute='_compute_cotizacion', string='¿Cabe en capacidad?')
    capacity_detail = fields.Text(
        compute='_compute_cotizacion', string='Detalle de capacidad')

    @api.model
    def default_get(self, fields_list):
        """Prefill desde el contexto: lanzado desde una cotización de venta
        (sale.order) toma el cliente automáticamente."""
        res = super().default_get(fields_list)
        if (self.env.context.get('active_model') == 'sale.order'
                and self.env.context.get('active_id')
                and 'partner_id' in fields_list and not res.get('partner_id')):
            order = self.env['sale.order'].browse(
                self.env.context['active_id']).exists()
            if order:
                res['partner_id'] = order.partner_id.id
        return res

    # ------------------------------------------------------------------
    # Cálculo (compartido entre el compute vivo y el guardado)
    # ------------------------------------------------------------------
    def _calc(self):
        """Corre el motor para los inputs actuales. Devuelve dict o None si
        aún no hay qué calcular. NO escribe nada (seguro para compute)."""
        self.ensure_one()
        if not self.product_id and not self.spec_mode:
            return None
        Costo = self.env['qb.costo.producto']
        Config = self.env['qb.costeo.factor.config']
        factores = self.env['qb.costo.factores'].search(
            [], order='period DESC', limit=1)
        if not factores:
            return {'error': 'Aún no hay factores calculados: corre '
                             '"Recalcular costeo (mes anterior)" en '
                             'Configuración una primera vez.'}

        if self.product_id and not self.spec_mode:
            product = self.product_id
            bucket, centros = self.env['qb.producto.ruteo'].resolve(product)
            kg = self.env['qb.producto.peso'].resolve_kg_per_unit(product)
            m_per_kg = self.env['qb.producto.peso'].resolve_m_per_kg(product)
            is_kg = (product.uom_id.name or '').lower() in KG_UOM_NAMES
            mp = Costo._mp_cost_unit(product)
            uom_name = product.uom_id.name
            name = 'COT %s' % (product.default_code or product.name)
        else:
            bucket = self.spec_bucket
            centros = self.spec_centro_ids
            kg = (self.spec_gramaje / 1000.0) * (self.spec_ancho or 1.5)
            m_per_kg = 1.0 / kg if kg else Config.get_param('m_per_kg_default', 8.0)
            is_kg = False  # especificación nueva se cotiza por metro
            mp = self.spec_mp_unit
            uom_name = 'm'
            name = 'COT %s' % (self.spec_descripcion or 'especificación nueva')
        if not centros:
            centros = self._default_centros(bucket)

        energia = 0.0 if bucket in ('importado', 'subproducto') \
            else factores.energia_por_kg * kg
        fab = Costo._fab_unit(bucket, is_kg, kg, m_per_kg, factores)
        variable = mp + energia
        op_pct = factores.op_pct
        target = self.target_margin / 100.0 if self.target_margin \
            else Config.get_param('target_margin', 0.30)

        denom = 1.0 - op_pct - target
        precio_sugerido = (variable + fab) / denom if denom > 0 else 0.0
        piso_ocioso = variable
        piso_lleno = (variable + fab) / (1.0 - op_pct) if op_pct < 1 else 0.0
        precio_ref = self.precio_objetivo or precio_sugerido
        contrib = precio_ref - variable
        hours_per_unit = Costo._hours_per_unit(centros, is_kg, kg, m_per_kg)
        contrib_hora = contrib / hours_per_unit if hours_per_unit else 0.0

        capacity_ok, capacity_detail = self._check_capacity(
            centros, is_kg, kg, m_per_kg, self.volumen)

        return {
            'name': name, 'bucket': bucket, 'centros': centros,
            'factores': factores, 'kg': kg, 'm_per_kg': m_per_kg,
            'uom_name': uom_name, 'mp': mp, 'energia': energia, 'fab': fab,
            'variable': variable, 'op_pct': op_pct, 'target': target,
            'precio_sugerido': precio_sugerido, 'piso_ocioso': piso_ocioso,
            'piso_lleno': piso_lleno, 'precio_ref': precio_ref,
            'contrib': contrib, 'contrib_hora': contrib_hora,
            'capacity_ok': capacity_ok, 'capacity_detail': capacity_detail,
        }

    @api.depends('product_id', 'spec_mode', 'spec_gramaje', 'spec_ancho',
                 'spec_bucket', 'spec_mp_unit', 'spec_centro_ids',
                 'volumen', 'precio_objetivo', 'target_margin')
    def _compute_cotizacion(self):
        for wiz in self:
            zero = dict.fromkeys([
                'kg_per_unit', 'mp_unit', 'energia_unit', 'fab_unit',
                'costo_variable', 'op_pct_display', 'precio_sugerido',
                'piso_ocioso', 'piso_lleno', 'margen_contribucion',
                'margen_contribucion_pct', 'contrib_hora_maquina'], 0.0)
            try:
                res = wiz._calc()
            except Exception as exc:  # un dato roto no debe romper el form
                wiz.update(dict(zero, factores_id=False, product_bucket=False,
                                factores_info=False, capacity_ok=False,
                                capacity_detail='Error al calcular: %s' % exc))
                continue
            if not res:
                wiz.update(dict(zero, factores_id=False, product_bucket=False,
                                factores_info=False, capacity_ok=False,
                                capacity_detail=False))
                continue
            if res.get('error'):
                wiz.update(dict(zero, factores_id=False, product_bucket=False,
                                factores_info=res['error'], capacity_ok=False,
                                capacity_detail=res['error']))
                continue
            factores = res['factores']
            precio_ref = res['precio_ref']
            wiz.update({
                'factores_id': factores.id,
                'factores_info': 'Factores %s (ventana %sm) · fab $%.2f/kg + '
                                 '$%.2f/m · energía $%.2f/kg · op %.1f%%' % (
                                     factores.period, factores.window_months,
                                     factores.factor_fab_kg,
                                     factores.factor_fab_m,
                                     factores.energia_por_kg,
                                     res['op_pct'] * 100.0),
                'product_bucket': res['bucket'],
                'kg_per_unit': res['kg'],
                'mp_unit': res['mp'],
                'energia_unit': res['energia'],
                'fab_unit': res['fab'],
                'costo_variable': res['variable'],
                'op_pct_display': res['op_pct'] * 100.0,
                'precio_sugerido': res['precio_sugerido'],
                'piso_ocioso': res['piso_ocioso'],
                'piso_lleno': res['piso_lleno'],
                'margen_contribucion': res['contrib'],
                'margen_contribucion_pct':
                    100.0 * res['contrib'] / precio_ref if precio_ref else 0.0,
                'contrib_hora_maquina': res['contrib_hora'],
                'capacity_ok': res['capacity_ok'],
                'capacity_detail': res['capacity_detail'],
            })

    # ------------------------------------------------------------------
    # Guardar el escenario
    # ------------------------------------------------------------------
    def action_cotizar(self):
        self.ensure_one()
        if not self.volumen:
            raise UserError('Captura el volumen mensual para guardar la cotización.')
        res = self._calc()
        if not res:
            raise UserError('Elige un producto existente o captura una '
                            'especificación nueva.')
        if res.get('error'):
            raise UserError(res['error'])

        factores = res['factores']
        supuestos = (
            'Factores del período %s (ventana %s meses).\n'
            'Pool fabricación $%s/mes; denominadores: %s kg/mes, %s m/mes.\n'
            'Factor peso $%.2f/kg; factor largo $%.2f/m; energía $%.2f/kg; '
            'operación %.1f%% sobre venta.\n'
            'Peso usado: %.4f kg/u (%s). FX supuesto: %s.'
        ) % (factores.period, factores.window_months,
             f'{factores.fab_pool_month:,.0f}',
             f'{factores.kg_denom_month:,.0f}',
             f'{factores.m_denom_month:,.0f}',
             factores.factor_fab_kg, factores.factor_fab_m,
             factores.energia_por_kg, res['op_pct'] * 100.0,
             res['kg'], res['bucket'], self.fx_rate or 'FX de cada compra')

        cotizacion = self.env['qb.cotizacion'].create({
            'name': res['name'],
            'partner_id': self.partner_id.id,
            'product_id': self.product_id.id,
            'spec_descripcion': self.spec_descripcion,
            'spec_gramaje': self.spec_gramaje,
            'spec_ancho': self.spec_ancho,
            'spec_galga': self.spec_galga,
            'volumen': self.volumen,
            'uom_name': res['uom_name'],
            'fx_rate': self.fx_rate,
            'mp_unit': res['mp'],
            'energia_unit': res['energia'],
            'fab_unit': res['fab'],
            'op_pct': res['op_pct'] * 100.0,
            'costo_variable': res['variable'],
            'costo_absorbido_sin_op': res['variable'] + res['fab'],
            'target_margin': res['target'] * 100.0,
            'precio_objetivo': self.precio_objetivo,
            'precio_sugerido': res['precio_sugerido'],
            'piso_ocioso': res['piso_ocioso'],
            'piso_lleno': res['piso_lleno'],
            'margen_contribucion': res['contrib'],
            'margen_contribucion_pct':
                100.0 * res['contrib'] / res['precio_ref']
                if res['precio_ref'] else 0.0,
            'contrib_hora_maquina': res['contrib_hora'],
            'capacity_ok': res['capacity_ok'],
            'capacity_detail': res['capacity_detail'],
            'factores_id': factores.id,
            'supuestos': supuestos,
        })
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'qb.cotizacion',
            'res_id': cotizacion.id,
            'view_mode': 'form',
            'target': 'current',
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _default_centros(self, bucket):
        Centro = self.env['qb.costeo.centro']
        if bucket in ('importado', 'subproducto', 'servicio'):
            return Centro.browse()
        if bucket == 'entretela_carda':
            return Centro.search([('code', 'ilike', 'ENTRETELA')])
        return Centro.search([('nature', '=', 'fabril_directo')])

    def _check_capacity(self, centros, is_kg, kg, m_per_kg, volumen):
        """Horas requeridas vs libres por centro; máquinas/turnos faltantes."""
        if not centros:
            return True, ('Sin ruta de fabricación (importado/servicio): '
                          'no consume capacidad.')
        if not volumen:
            return True, 'Captura el volumen mensual para validar capacidad.'
        lines = []
        ok = True
        capacidad = self.env['qb.capacidad'].search(
            [('centro_id', 'in', centros.ids)])
        free_by_centro = {}
        hours_wc_by_centro = {}
        for cap in capacidad:
            free_by_centro[cap.centro_id.id] = \
                free_by_centro.get(cap.centro_id.id, 0.0) + cap.free_hours_month
            hours_wc_by_centro[cap.centro_id.id] = \
                hours_wc_by_centro.get(cap.centro_id.id, 0.0) + cap.hours_month_available
        for centro in centros:
            std = centro.std_output_per_hour
            if not std:
                lines.append('%s: sin throughput nominal configurado — no se '
                             'puede validar capacidad.' % centro.code)
                continue
            if centro.driver_principal == 'peso':
                units = volumen * (1.0 if is_kg else kg)
            else:
                units = volumen * (m_per_kg if is_kg else 1.0)
            hours_needed = units / std
            free = free_by_centro.get(centro.id)
            if free is None:
                # Centro sin workcenters: capacidad desde turnos config
                turnos = self.env['qb.turno.config'].search(
                    [('centro_id', '=', centro.id)])
                total_hours = sum(t.hours_per_month() for t in turnos)
                balance = self.env['qb.balance'].search(
                    [('centro_id', '=', centro.id)], limit=1)
                used_pct = balance.utilization_pct if balance else 0.0
                free = total_hours * (1.0 - used_pct / 100.0)
            if hours_needed <= free:
                lines.append('%s: requiere %.0f h/mes, libres %.0f h/mes — OK.'
                             % (centro.code, hours_needed, free))
            else:
                ok = False
                deficit = hours_needed - free
                n_wc = len(centro.workcenter_ids) or 1
                hours_per_machine = (
                    hours_wc_by_centro.get(centro.id, 0.0) / n_wc
                ) or 200.0
                machines_needed = deficit / hours_per_machine
                lines.append(
                    '%s: requiere %.0f h/mes, libres %.0f h/mes — FALTAN '
                    '%.0f h (≈ %.1f máquinas o turnos equivalentes).'
                    % (centro.code, hours_needed, free, deficit, machines_needed))
        return ok, '\n'.join(lines)
