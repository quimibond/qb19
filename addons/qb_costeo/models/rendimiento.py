# -*- coding: utf-8 -*-
"""Rendimiento vendible por producto: de cada 100 unidades que salen de
producción, cuántas llegan a primera calidad.

Reglas (heredadas del módulo anterior, memo del 31-ago-2026):

- Se lee `stock.move.line` (el `stock.move` apunta a la ubicación padre y
  dejaba millones de metros sin clasificar).
- Vendible = lo que entra a PT primera calidad. Merma = FE + segundas +
  desperdicio. El FE cuenta como merma: no tiene canal de venta directo.
- Se excluyen los tipos de operación de conversión de unidades y cambio de
  artículo (reetiquetan material ya clasificado).
- Solo entradas desde producción, inspección o liberación: los reingresos
  ya se clasificaron antes.
- Ventana de 12 meses; por producto si clasificó al menos el umbral de
  unidades; abajo del umbral, tasa de planta.
- Lo capturado a mano manda mientras esté vigente (un lote de desarrollo
  no es la producción normal).
"""
from datetime import date

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models
from odoo.exceptions import UserError

FUENTES = [
    ('manual', 'Capturado con motivo'),
    ('producto', 'Historia propia'),
    ('planta', 'Promedio de planta'),
    ('ninguna', 'Sin dato'),
]


class QbProductoRendimiento(models.Model):
    _name = 'qb.producto.rendimiento'
    _description = 'Rendimiento vendible por producto'
    _order = 'product_id'
    _rec_name = 'product_id'

    product_id = fields.Many2one('product.product', required=True,
                                 index=True, ondelete='cascade')
    default_code = fields.Char(related='product_id.default_code', store=True)
    company_id = fields.Many2one(
        'res.company', required=True,
        default=lambda self: self.env.company)
    rendimiento = fields.Float(
        digits=(6, 4), string='Rendimiento vigente',
        help='Fracción de primera calidad que usa el costeo (0.88 = 88 %).')
    fuente = fields.Selection(FUENTES, default='ninguna', required=True)
    rendimiento_calculado = fields.Float(
        digits=(6, 4), help='El que sale de los movimientos de almacén, '
        'aunque mande el manual.')
    unidades_clasificadas = fields.Float(
        digits=(16, 2), help='Unidades que entraron a primera o merma en la '
        'ventana; abajo del umbral se usa la tasa de planta.')
    unidades_vendibles = fields.Float(digits=(16, 2))
    calculado_el = fields.Datetime(readonly=True)
    # Captura manual
    rendimiento_manual = fields.Float(
        digits=(6, 4), string='Rendimiento capturado',
        help='Manda sobre el calculado mientras esté vigente.')
    manual_motivo = fields.Char(string='Motivo')
    manual_vigente_hasta = fields.Date(
        string='Vigente hasta',
        help='Vacío = sin caducidad. Al vencer, el recálculo vuelve al '
             'calculado.')

    _product_uniq = models.Constraint(
        'unique(product_id, company_id)',
        'Ya hay un rendimiento para ese producto.')
    _rango = models.Constraint(
        'CHECK(rendimiento_manual >= 0 AND rendimiento_manual <= 1)',
        'El rendimiento capturado es una fracción entre 0 y 1.')

    @api.constrains('rendimiento_manual', 'manual_motivo')
    def _check_manual(self):
        for rec in self:
            if rec.rendimiento_manual and not (rec.manual_motivo or '').strip():
                raise UserError('Escriba el motivo del rendimiento capturado.')

    # ------------------------------------------------------------------
    @api.model
    def _ids_param(self, key, default):
        return [int(x) for x in self.env['qb.parametro'].get_list(key, default)
                if x.strip().isdigit()]

    @api.model
    def medir(self, hasta=None, meses=None):
        """{product_id: (vendible, total)} en la ventana que termina en
        `hasta` (exclusivo), más la tasa de planta."""
        P = self.env['qb.parametro']
        vendible = self._ids_param('calidad_locs_vendible', '40')
        merma = self._ids_param('calidad_locs_merma', '41,42,43')
        origen = self._ids_param('calidad_locs_origen', '15,36,246')
        excluir = self._ids_param('calidad_picking_excluir', '77,147') or [0]
        if not (vendible and merma and origen):
            return {}, 1.0
        meses = meses or int(P.get_float('horas_historia_meses', 12)) or 12
        hasta = hasta or (date.today() + relativedelta(days=1))
        desde = hasta - relativedelta(months=meses)
        self.env.flush_all()
        self.env.cr.execute("""
            SELECT sml.product_id,
                   SUM(CASE WHEN sml.location_dest_id IN %s
                            THEN sml.quantity ELSE 0 END) AS vend,
                   SUM(sml.quantity) AS total
              FROM stock_move_line sml
              JOIN stock_move sm ON sm.id = sml.move_id
             WHERE sml.state = 'done'
               AND sml.date >= %s AND sml.date < %s
               AND sml.location_dest_id IN %s
               AND sml.location_id IN %s
               AND (sm.picking_type_id IS NULL
                    OR sm.picking_type_id NOT IN %s)
               AND sm.company_id = %s
             GROUP BY sml.product_id
        """, (tuple(vendible), desde, hasta, tuple(vendible + merma),
              tuple(origen), tuple(excluir), self.env.company.id))
        datos = {pid: (v or 0.0, t or 0.0) for pid, v, t in self.env.cr.fetchall()}
        tot_v = sum(v for v, _t in datos.values())
        tot_t = sum(t for _v, t in datos.values())
        return datos, (tot_v / tot_t if tot_t else 1.0)

    @api.model
    def recalcular(self, products=None, hasta=None):
        """Recalcula el rendimiento de `products` (default: los que tienen
        historia o captura manual). Devuelve las filas."""
        P = self.env['qb.parametro']
        company = self.env.company
        min_u = P.get_float('rendimiento_min_unidades', 10000.0)
        datos, planta = self.medir(hasta)
        hoy = fields.Date.today()
        existentes = {r.product_id.id: r for r in self.search(
            [('company_id', '=', company.id)])}
        if products is None:
            ids = set(datos) | set(existentes)
        else:
            ids = set(products.ids)
        ahora = fields.Datetime.now()
        filas = self.browse()
        for pid in ids:
            rec = existentes.get(pid)
            v, t = datos.get(pid, (0.0, 0.0))
            if t >= min_u:
                calc, fuente = (v / t if t else 1.0), 'producto'
            elif t > 0:
                calc, fuente = planta, 'planta'
            else:
                calc, fuente = planta, 'planta'
            manual = rec and rec.rendimiento_manual and (
                not rec.manual_vigente_hasta or rec.manual_vigente_hasta >= hoy)
            vals = {
                'rendimiento_calculado': calc,
                'rendimiento': rec.rendimiento_manual if manual else calc,
                'fuente': 'manual' if manual else fuente,
                'unidades_clasificadas': t, 'unidades_vendibles': v,
                'calculado_el': ahora,
            }
            if rec:
                rec.write(vals)
            else:
                rec = self.create(dict(vals, product_id=pid, company_id=company.id))
            filas |= rec
        return filas

    @api.model
    def planta(self, hasta=None):
        return self.medir(hasta)[1]

    @api.model
    def para(self, product, hasta=None):
        """Rendimiento de un producto para costear: la fila vigente; sin
        fila, la tasa de planta. Devuelve (fracción, fuente)."""
        rec = self.search([('product_id', '=', product.id),
                           ('company_id', '=', self.env.company.id)], limit=1)
        if rec and rec.fuente != 'ninguna':
            return rec.rendimiento or 1.0, rec.fuente
        return self.planta(hasta), 'planta'

    @api.model
    def cron_recalcular(self):
        for company in self.env['res.company'].search([]):
            self.with_company(company).recalcular()

    def action_recalcular_todo(self):
        self.recalcular()
        return {
            'type': 'ir.actions.act_window',
            'name': 'Rendimiento vendible',
            'res_model': 'qb.producto.rendimiento',
            'view_mode': 'list,form',
        }

    @api.model
    def importar_manuales_legados(self):
        """Trae los rendimientos capturados en `qb.producto.peso` del módulo
        anterior (misma base). Idempotente."""
        self.env.cr.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_name = 'qb_producto_peso'
        """)
        if not self.env.cr.fetchone():
            return 0
        self.env.cr.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'qb_producto_peso'
              AND column_name = 'rendimiento_manual'
        """)
        if not self.env.cr.fetchone():
            return 0
        self.env.cr.execute("""
            SELECT product_id, rendimiento_manual, rendimiento_motivo
            FROM qb_producto_peso
            WHERE COALESCE(rendimiento_manual, 0) > 0
        """)
        n = 0
        company = self.env.company
        for pid, rend, motivo in self.env.cr.fetchall():
            rec = self.search([('product_id', '=', pid),
                               ('company_id', '=', company.id)], limit=1)
            if rec and rec.rendimiento_manual:
                continue
            vals = {'rendimiento_manual': rend,
                    'manual_motivo': motivo or 'importado de qb_capacidad_costeo',
                    'rendimiento': rend, 'fuente': 'manual'}
            if rec:
                rec.write(vals)
            else:
                self.create(dict(vals, product_id=pid, company_id=company.id))
            n += 1
        return n
