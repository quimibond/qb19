# -*- coding: utf-8 -*-
"""Cotizador: producto existente o especificación nueva → costo por capa,
precio sugerido, pisos, contribución por hora-máquina y chequeo de capacidad.
"""
from odoo import api, fields, models
from odoo.exceptions import UserError

KG_UOM_NAMES = ('kg', 'kgs', 'kilogramo', 'kilogramos')


class QbCotizadorWizard(models.TransientModel):
    _name = 'qb.cotizador.wizard'
    _description = 'Cotizador de capacidad y costo'

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

    volumen = fields.Float(string='Volumen (unidades/mes)', required=True)
    precio_objetivo = fields.Float(string='Precio objetivo $/u')
    target_margin = fields.Float(
        string='Margen meta %',
        help='0 = usar el target_margin de configuración.')
    fx_rate = fields.Float(string='FX (MXN/USD)')

    def action_cotizar(self):
        self.ensure_one()
        if not self.product_id and not self.spec_mode:
            raise UserError('Elige un producto existente o captura una especificación nueva.')

        Costo = self.env['qb.costo.producto']
        Config = self.env['qb.costeo.factor.config']
        factores = self.env['qb.costo.factores'].search([], order='period DESC', limit=1)
        if not factores:
            period = fields.Date.today().replace(day=1)
            factores = Costo._compute_factores(period)

        if self.product_id:
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

        # Precio sugerido: cubre op% (que va sobre venta) y deja el margen meta
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
             factores.energia_por_kg, op_pct * 100.0,
             kg, bucket, self.fx_rate or 'FX de cada compra')

        cotizacion = self.env['qb.cotizacion'].create({
            'name': name,
            'partner_id': self.partner_id.id,
            'product_id': self.product_id.id,
            'spec_descripcion': self.spec_descripcion,
            'spec_gramaje': self.spec_gramaje,
            'spec_ancho': self.spec_ancho,
            'spec_galga': self.spec_galga,
            'volumen': self.volumen,
            'uom_name': uom_name,
            'fx_rate': self.fx_rate,
            'mp_unit': mp,
            'energia_unit': energia,
            'fab_unit': fab,
            'op_pct': op_pct * 100.0,
            'costo_variable': variable,
            'costo_absorbido_sin_op': variable + fab,
            'target_margin': target * 100.0,
            'precio_objetivo': self.precio_objetivo,
            'precio_sugerido': precio_sugerido,
            'piso_ocioso': piso_ocioso,
            'piso_lleno': piso_lleno,
            'margen_contribucion': contrib,
            'margen_contribucion_pct':
                100.0 * contrib / precio_ref if precio_ref else 0.0,
            'contrib_hora_maquina': contrib_hora,
            'capacity_ok': capacity_ok,
            'capacity_detail': capacity_detail,
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
            return True, 'Sin ruta de fabricación (importado/servicio): no consume capacidad.'
        lines = []
        ok = True
        capacidad = {c.centro_id.id: c for c in self.env['qb.capacidad'].search(
            [('centro_id', 'in', centros.ids)])}
        free_by_centro = {}
        hours_wc_by_centro = {}
        for cap in capacidad.values():
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
                # Horas por máquina promedio del centro para dimensionar el faltante
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
