# -*- coding: utf-8 -*-
"""Materia prima por unidad: la receta vigente explotada hasta las hojas
compradas, cada hoja a su precio.

Precio de una hoja comprada: la última compra confirmada **conocida al
corte** (fin del período que se costea), en moneda de la compañía y en la
unidad del producto. Costear marzo con el hilo de agosto pinta márgenes que
nunca existieron. Sin compras antes del corte, la primera compra conocida;
sin compras, el costo promedio de Odoo acotado a 0 (un AVCO negativo no es
un costo). Sin corte (cotizar) se usa la última compra de hoy: cotizar es a
reposición.

Un componente con receta propia se explota; una validación abierta en una
hoja (precio 0, fuera de banda) o en la receta marca la MP como dudosa.
"""
from datetime import date

from dateutil.relativedelta import relativedelta

from odoo import api, fields, models


class QbProductoMp(models.Model):
    _name = 'qb.producto.mp'
    _description = 'Materia prima por unidad de producto'
    _order = 'product_id'
    _rec_name = 'product_id'

    product_id = fields.Many2one('product.product', required=True,
                                 index=True, ondelete='cascade')
    default_code = fields.Char(related='product_id.default_code', store=True)
    company_id = fields.Many2one(
        'res.company', required=True,
        default=lambda self: self.env.company)
    mp_unit = fields.Float(
        digits=(16, 4), string='MP $/u',
        help='Σ cantidad × precio de cada hoja comprada de la receta '
             'vigente, por unidad del producto, a reposición de hoy.')
    bom_id = fields.Many2one('mrp.bom', string='Receta vigente')
    n_hojas = fields.Integer(string='Hojas compradas')
    hojas_dudosas = fields.Integer(
        help='Hojas o recetas de la explosión con una validación abierta.')
    calidad = fields.Selection([
        ('alta', 'Sin datos dudosos'),
        ('dudosa', 'Con validaciones abiertas'),
        ('ninguna', 'Sin receta')], default='ninguna')
    detalle = fields.Text(help='Las hojas que más pesan, con su precio.')
    calculado_el = fields.Datetime(readonly=True)

    _product_uniq = models.Constraint(
        'unique(product_id, company_id)',
        'Ya hay una MP para ese producto.')

    # ------------------------------------------------------------------
    # Precios de hoja
    # ------------------------------------------------------------------
    @api.model
    def precios_compra(self, product_ids, cutoff=None):
        """{product_id: precio en moneda de la compañía y unidad del
        producto} desde la última compra confirmada antes de `cutoff`
        (o la primera conocida si no hubo antes; o la última de hoy sin
        corte)."""
        if not product_ids:
            return {}
        self.env.flush_all()
        params = [list(product_ids), self.env.company.id]
        date_filter = ''
        if cutoff:
            date_filter = 'AND po.date_order < %s'
            params.append(cutoff)
        self.env.cr.execute("""
            SELECT DISTINCT ON (pol.product_id) pol.product_id, pol.price_unit,
                   COALESCE(pol.discount, 0), COALESCE(NULLIF(po.currency_rate, 0), 1),
                   pol.product_uom_id
            FROM purchase_order_line pol
            JOIN purchase_order po ON po.id = pol.order_id
            WHERE po.state IN ('purchase', 'done') AND pol.price_unit > 0
              AND pol.product_id = ANY(%s) AND po.company_id = %s
              """ + date_filter + """
            ORDER BY pol.product_id, po.date_order DESC, pol.id DESC
        """, params)
        filas = self.env.cr.fetchall()
        if cutoff:
            faltan = set(product_ids) - {r[0] for r in filas}
            if faltan:
                self.env.cr.execute("""
                    SELECT DISTINCT ON (pol.product_id) pol.product_id, pol.price_unit,
                           COALESCE(pol.discount, 0),
                           COALESCE(NULLIF(po.currency_rate, 0), 1), pol.product_uom_id
                    FROM purchase_order_line pol
                    JOIN purchase_order po ON po.id = pol.order_id
                    WHERE po.state IN ('purchase', 'done') AND pol.price_unit > 0
                      AND pol.product_id = ANY(%s) AND po.company_id = %s
                    ORDER BY pol.product_id, po.date_order ASC, pol.id ASC
                """, (list(faltan), self.env.company.id))
                filas += self.env.cr.fetchall()
        Prod = self.env['product.product']
        Uom = self.env['uom.uom']
        out = {}
        for pid, precio, desc, rate, uom_id in filas:
            precio = precio * (1 - desc / 100.0) / rate
            prod = Prod.browse(pid)
            uom = Uom.browse(uom_id) if uom_id else prod.uom_id
            if uom and prod.uom_id and uom != prod.uom_id and \
                    uom._has_common_reference(prod.uom_id):
                precio = uom._compute_price(precio, prod.uom_id)
            out[pid] = precio
        return out

    # ------------------------------------------------------------------
    # Explosión
    # ------------------------------------------------------------------
    @api.model
    def explotar(self, product, cutoff=None, _ctx=None):
        """(mp_por_unidad, hojas) donde hojas = {leaf_id: (qty_por_unidad,
        precio)}. Memo en `_ctx` para una corrida completa."""
        ctx = _ctx if _ctx is not None else {
            'memo': {}, 'precios': {}, 'pila': set(), 'dudosos': set()}
        if product.id in ctx['memo']:
            return ctx['memo'][product.id]
        if product.id in ctx['pila']:
            return 0.0, {}
        Horas = self.env['qb.producto.horas']
        bom = Horas._bom_vigente(product, cutoff)
        if not bom or not bom.bom_line_ids:
            ctx['memo'][product.id] = (None, {})
            return None, {}
        ctx['pila'].add(product.id)
        qty_bom = Horas._qty_bom_en_uom_producto(bom, product)
        total = 0.0
        hojas = {}
        for line in bom.bom_line_ids:
            if line._skip_bom_line(product):
                continue
            comp = line.product_id
            if comp.type == 'service':
                continue
            qty = line.product_uom_id._compute_quantity(
                line.product_qty, comp.uom_id, round=False,
                raise_if_failure=False) / qty_bom
            sub, sub_hojas = self.explotar(comp, cutoff, ctx)
            if sub is not None:
                total += qty * sub
                for lid, (q, p) in sub_hojas.items():
                    q0, _p0 = hojas.get(lid, (0.0, p))
                    hojas[lid] = (q0 + q * qty, p)
            else:
                if comp.id not in ctx['precios']:
                    ctx['precios'].update(self.precios_compra([comp.id], cutoff))
                precio = ctx['precios'].get(comp.id)
                if precio is None:
                    precio = max(comp.standard_price or 0.0, 0.0)
                total += qty * precio
                q0, _p0 = hojas.get(comp.id, (0.0, precio))
                hojas[comp.id] = (q0 + qty, precio)
        ctx['pila'].discard(product.id)
        ctx['memo'][product.id] = (total, hojas)
        return total, hojas

    @api.model
    def mp_para(self, product, cutoff=None):
        """MP por unidad para costear, sin guardar. None = sin receta."""
        return self.explotar(product, cutoff)[0]

    # ------------------------------------------------------------------
    @api.model
    def recalcular(self, products=None):
        company = self.env.company
        Horas = self.env['qb.producto.horas']
        if products is None:
            products = Horas._productos_a_calcular()
        Val = self.env['qb.producto.validacion']
        dudosos = set(Val.search([('estado', '=', 'abierta'),
                                  ('company_id', '=', company.id)])
                      .mapped('product_id').ids)
        comps_dudosos = set(Val.search([('estado', '=', 'abierta'),
                                        ('company_id', '=', company.id),
                                        ('componente_id', '!=', False)])
                            .mapped('componente_id').ids)
        existentes = {r.product_id.id: r for r in self.search(
            [('product_id', 'in', products.ids), ('company_id', '=', company.id)])}
        ctx = {'memo': {}, 'precios': {}, 'pila': set(), 'dudosos': set()}
        # Precios de todas las hojas en una consulta.
        self.env.cr.execute(
            'SELECT DISTINCT product_id FROM mrp_bom_line WHERE product_id IS NOT NULL')
        ctx['precios'] = self.precios_compra(
            [r[0] for r in self.env.cr.fetchall()])
        ahora = fields.Datetime.now()
        filas = self.browse()
        Prod = self.env['product.product']
        for p in products:
            total, hojas = self.explotar(p, None, ctx)
            if total is None:
                rec = existentes.get(p.id)
                if rec:
                    rec.unlink()
                continue
            bom = Horas._bom_vigente(p)
            n_dud = len([lid for lid in hojas if lid in comps_dudosos or lid in dudosos])
            if p.id in dudosos:
                n_dud += 1
            top = sorted(hojas.items(), key=lambda kv: -kv[1][0] * kv[1][1])[:6]
            detalle = '\n'.join(
                '%s: %.4f × $%.2f = $%.4f' % (
                    Prod.browse(lid).default_code or Prod.browse(lid).name,
                    q, pr, q * pr) for lid, (q, pr) in top)
            vals = {
                'mp_unit': total, 'bom_id': bom.id if bom else False,
                'n_hojas': len(hojas), 'hojas_dudosas': n_dud,
                'calidad': 'dudosa' if n_dud else 'alta',
                'detalle': detalle, 'calculado_el': ahora,
            }
            rec = existentes.get(p.id)
            if rec:
                rec.write(vals)
            else:
                rec = self.create(dict(vals, product_id=p.id, company_id=company.id))
            filas |= rec
        return filas

    @api.model
    def cron_recalcular(self):
        for company in self.env['res.company'].search([]):
            self.with_company(company).recalcular()

    def action_recalcular_todo(self):
        self.recalcular()
        return {
            'type': 'ir.actions.act_window',
            'name': 'Materia prima por producto',
            'res_model': 'qb.producto.mp',
            'view_mode': 'list,form',
        }

    @api.model
    def _cutoff_periodo(self, period):
        return date(period.year, period.month, 1) + relativedelta(months=1)
