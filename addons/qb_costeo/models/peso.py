# -*- coding: utf-8 -*-
"""Peso por unidad de venta (kg/u): lo que convierte metros en kilos.

Orden de precedencia (spec §8.6): `pesaje` (rollos pesados en el centro de
trabajo) > `manual` > `ficha` (ficha técnica de Consolti, si está
instalada) > `nomenclatura` (peso g/m² × ancho del código DAT P-D02-01).
Un producto vendido en kg pesa 1. (El modelo se llama `qb.producto.kg` porque
`qb.producto.peso` es del módulo anterior y conviven en la misma base.) La fuente queda en la fila; las que no
son medidas (`nomenclatura`) pintan el costo como estimado en ese renglón.
"""
import re

from odoo import api, fields, models

NOMEN_RE = re.compile(r'^I?([A-Z])([A-Z])(\d{3})([QBR])(\d{2})([HIJ])([A-Z]{2})(\d{3})')

FUENTES = [
    ('pesaje', 'Rollos pesados'),
    ('manual', 'Capturado'),
    ('ficha', 'Ficha técnica'),
    ('legado', 'Importado del módulo anterior'),
    ('kg', 'Se vende en kg (= 1)'),
    ('nomenclatura', 'Peso × ancho del código (estimado)'),
    ('ninguna', 'Sin peso'),
]
ESTIMADAS = ('nomenclatura', 'ninguna')


class QbProductoPeso(models.Model):
    _name = 'qb.producto.kg'
    _description = 'Peso por unidad de venta'
    _order = 'product_id'
    _rec_name = 'product_id'

    product_id = fields.Many2one('product.product', required=True,
                                 index=True, ondelete='cascade')
    default_code = fields.Char(related='product_id.default_code', store=True)
    company_id = fields.Many2one(
        'res.company', required=True,
        default=lambda self: self.env.company)
    kg_por_unidad = fields.Float(digits=(16, 6), string='kg por unidad')
    fuente = fields.Selection(FUENTES, default='ninguna', required=True)
    kg_manual = fields.Float(digits=(16, 6), string='kg capturados')
    manual_motivo = fields.Char(string='Motivo')
    detalle = fields.Char()
    calculado_el = fields.Datetime(readonly=True)

    _product_uniq = models.Constraint(
        'unique(product_id, company_id)', 'Ya hay un peso para ese producto.')

    @api.model
    def _de_nomenclatura(self, ref):
        m = NOMEN_RE.match((ref or '').replace(' ', ''))
        if not m:
            return 0.0
        gramaje = int(m.group(3))
        ancho = int(m.group(8)) / 100.0
        if not 0.3 <= ancho <= 3.5:
            return 0.0
        return gramaje / 1000.0 * ancho

    @api.model
    def _de_ficha(self, product):
        """Ficha técnica de acabado de Consolti, si el módulo está."""
        if 'ficha.tecnica.acabado' not in self.env:
            return 0.0, ''
        Ficha = self.env['ficha.tecnica.acabado']
        ficha = Ficha.search([('product_acabado_id', '=', product.id)],
                             limit=1, order='id desc')
        if not ficha:
            return 0.0, ''
        rend = getattr(ficha, 'rendimiento_tela_acabada', 0.0) or 0.0
        if rend:
            return 1.0 / rend, ficha.display_name
        peso = getattr(ficha, 'peso_acabado', 0.0) or 0.0
        ancho = getattr(ficha, 'ancho_acabado', 0.0) or 0.0
        if peso and ancho:
            return peso / 1000.0 * (ancho / 100.0 if ancho > 10 else ancho), \
                ficha.display_name
        return 0.0, ''

    @api.model
    def _de_pesaje(self, product, meses=12):
        """kg por unidad desde los lotes pesados en producción: Σ peso de
        los lotes ÷ Σ unidades, cuando el pesaje deja el peso en el lote
        (`pesaje_rollos_tejido`: un lote por rollo)."""
        Lot = self.env['stock.lot']
        if 'weight' not in Lot._fields and 'peso' not in Lot._fields:
            return 0.0, 0
        campo = 'weight' if 'weight' in Lot._fields else 'peso'
        if product.uom_id and (product.uom_id.name or '').lower().startswith('kg'):
            return 0.0, 0
        self.env.flush_all()
        self.env.cr.execute("""
            SELECT COALESCE(SUM(l.%s), 0), COALESCE(SUM(sml.quantity), 0), COUNT(*)
            FROM stock_move_line sml
            JOIN stock_lot l ON l.id = sml.lot_id
            JOIN stock_move sm ON sm.id = sml.move_id
            WHERE sml.product_id = %%s AND sml.state = 'done'
              AND sm.production_id IS NOT NULL
              AND sml.date >= (CURRENT_DATE - INTERVAL '%%s months')
              AND l.%s > 0
        """ % (campo, campo), (product.id, meses))
        kg, qty, n = self.env.cr.fetchone()
        if kg and qty:
            return kg / qty, n
        return 0.0, 0

    @api.model
    def resolver(self, product):
        """(kg_por_unidad, fuente, detalle) sin guardar."""
        rec = self.search([('product_id', '=', product.id),
                           ('company_id', '=', self.env.company.id)], limit=1)
        kg, n = self._de_pesaje(product)
        if kg:
            return kg, 'pesaje', '%d rollos pesados' % n
        if rec and rec.kg_manual:
            return rec.kg_manual, 'manual', rec.manual_motivo or ''
        kg, det = self._de_ficha(product)
        if kg:
            return kg, 'ficha', det
        if rec and rec.fuente == 'legado' and rec.kg_por_unidad:
            return rec.kg_por_unidad, 'legado', rec.detalle or ''
        if (product.uom_id.name or '').lower().startswith('kg'):
            return 1.0, 'kg', ''
        kg = self._de_nomenclatura(product.default_code)
        if kg:
            return kg, 'nomenclatura', 'peso × ancho del código'
        return 0.0, 'ninguna', ''

    @api.model
    def recalcular(self, products=None):
        company = self.env.company
        if products is None:
            products = self.env['qb.producto.horas']._productos_a_calcular()
        existentes = {r.product_id.id: r for r in self.search(
            [('product_id', 'in', products.ids), ('company_id', '=', company.id)])}
        ahora = fields.Datetime.now()
        filas = self.browse()
        for p in products:
            kg, fuente, det = self.resolver(p)
            vals = {'kg_por_unidad': kg, 'fuente': fuente, 'detalle': det,
                    'calculado_el': ahora}
            rec = existentes.get(p.id)
            if rec:
                rec.write(vals)
            else:
                rec = self.create(dict(vals, product_id=p.id, company_id=company.id))
            filas |= rec
        return filas

    @api.model
    def kg_de(self, product):
        rec = self.search([('product_id', '=', product.id),
                           ('company_id', '=', self.env.company.id)], limit=1)
        if rec and rec.kg_por_unidad:
            return rec.kg_por_unidad, rec.fuente
        kg, fuente, _d = self.resolver(product)
        return kg, fuente

    @api.model
    def cron_recalcular(self):
        for company in self.env['res.company'].search([]):
            self.with_company(company).recalcular()

    def action_recalcular_todo(self):
        self.recalcular()
        return {
            'type': 'ir.actions.act_window',
            'name': 'Peso por unidad',
            'res_model': 'qb.producto.kg',
            'view_mode': 'list',
        }

    @api.model
    def importar_legados(self):
        """Pesos medidos del módulo anterior (`qb_producto_peso` viejo):
        solo las fuentes medidas (manual, cvu, op_consumo, bom), nunca las
        adivinadas (ref_gramaje, odoo_weight). Idempotente."""
        self.env.cr.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'qb_producto_peso' AND column_name = 'kg_per_unit'
        """)
        if not self.env.cr.fetchone():
            return 0
        self.env.cr.execute("""
            SELECT product_id, kg_per_unit, source, notes FROM qb_producto_peso
            WHERE COALESCE(kg_per_unit, 0) > 0 AND active
              AND source IN ('manual', 'cvu', 'op_consumo', 'bom')
        """)
        company = self.env.company
        n = 0
        for pid, kg, source, notes in self.env.cr.fetchall():
            rec = self.search([('product_id', '=', pid),
                               ('company_id', '=', company.id)], limit=1)
            if rec and rec.fuente in ('pesaje', 'manual'):
                continue
            vals = {'kg_por_unidad': kg, 'fuente': 'legado',
                    'detalle': 'qb_capacidad_costeo: %s%s' % (
                        source, ' — %s' % notes if notes else '')}
            if source == 'manual':
                vals.update(kg_manual=kg, fuente='manual',
                            manual_motivo=notes or 'maestro de ingeniería (módulo anterior)')
            if rec:
                rec.write(vals)
            else:
                self.create(dict(vals, product_id=pid, company_id=company.id))
            n += 1
        return n
