"""Sync push: products, stock locations/moves, account entries (stock valuation),
orderpoints, BOMs, UoMs."""
import logging
from datetime import datetime, timedelta

from odoo import api, fields, models

from .supabase_client import SupabaseClient

_logger = logging.getLogger(__name__)


class QuimibondSyncInventory(models.TransientModel):
    _inherit = 'quimibond.sync'

    def _push_products(self, client: SupabaseClient, last_sync=None) -> int:
        Product = self.env['product.product'].sudo()
        # Traemos TODOS los productos (active + inactive) y propagamos el
        # flag real. Antes el filtro active=True no incluía archivados, y
        # cuando se archivaba un producto en Odoo, Supabase quedaba con la
        # última versión active=true (audit detectó 447 fantasmas
        # 2026-04-20). Con active_test=False + sin filtro active, cada
        # upsert refresca el flag real.
        ProductAll = Product.with_context(active_test=False)
        domain = []
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        products = ProductAll.search(domain)

        # Pre-fetch all reorder rules in one query (avoids N+1)
        orderpoint_map = {}  # product_id -> {min, max}
        try:
            Orderpoint = self.env['stock.warehouse.orderpoint'].sudo()
            all_orderpoints = Orderpoint.search([
                ('product_id', 'in', products.ids),
            ])
            for op in all_orderpoints:
                pid = op.product_id.id
                if pid not in orderpoint_map:
                    orderpoint_map[pid] = {
                        'min': op.product_min_qty,
                        'max': op.product_max_qty,
                    }
        except Exception:
            pass

        rows = []
        for p in products:
            # Use computed fields from product.product which aggregate stock.quant
            # These are more reliable than manual quant queries and handle
            # warehouse contexts correctly.
            stock_qty = 0.0
            reserved_qty = 0.0
            try:
                # qty_available = on hand, virtual_available = forecasted
                # outgoing_qty = reserved for outgoing
                stock_qty = p.qty_available or 0.0
                reserved_qty = (p.qty_available or 0.0) - (p.free_qty or 0.0)
            except Exception:
                # Fallback: try stock.quant directly
                try:
                    Quant = self.env['stock.quant'].sudo()
                    quants = Quant.search([
                        ('product_id', '=', p.id),
                        ('location_id.usage', '=', 'internal'),
                    ])
                    for q in quants:
                        stock_qty += q.quantity
                        reserved_qty += getattr(q, 'reserved_quantity', 0.0)
                except Exception:
                    pass

            # Determine product type string
            ptype = getattr(p, 'detailed_type', None) or getattr(p, 'type', 'consu')

            # Get reorder rules from pre-fetched map
            reorder_min = reorder_max = 0.0
            if p.id in orderpoint_map:
                reorder_min = orderpoint_map[p.id]['min']
                reorder_max = orderpoint_map[p.id]['max']

            # Get full category path for better classification
            category = ''
            try:
                if p.categ_id:
                    category = p.categ_id.complete_name or p.categ_id.name or ''
            except Exception:
                category = p.categ_id.name if p.categ_id else ''

            rows.append({
                'odoo_product_id': p.id,
                'name': p.name,
                'internal_ref': (p.default_code or '').strip() or None,
                'category': category,
                'uom': p.uom_id.name if p.uom_id else '',
                'uom_id': p.uom_id.id if p.uom_id else None,
                'product_type': ptype,
                'stock_qty': round(stock_qty, 2),
                'reserved_qty': round(reserved_qty, 2),
                'available_qty': round(stock_qty - reserved_qty, 2),
                'reorder_min': round(reorder_min, 2),
                'reorder_max': round(reorder_max, 2),
                'standard_price': round(p.standard_price, 2),
                'list_price': round(p.lst_price, 2),
                'avg_cost': round(p.avg_cost, 2) if hasattr(p, 'avg_cost') and p.avg_cost else None,
                'weight': round(p.weight, 4) if hasattr(p, 'weight') and p.weight else None,
                'active': p.active,
                'odoo_company_id': p.company_id.id if p.company_id else None,
                'updated_at': datetime.now().isoformat(),
            })

        return client.upsert('odoo_products', rows, on_conflict='odoo_product_id', batch_size=100)

    # ── Order Lines (Sale + Purchase, ALL history) ────────────────────

    def _push_orderpoints(self, client: SupabaseClient, last_sync=None) -> int:
        """Push stock.warehouse.orderpoint → odoo_orderpoints table.
        Critical for desabasto (stockout) detection."""
        try:
            Orderpoint = self.env['stock.warehouse.orderpoint'].sudo()
        except KeyError:
            _logger.info('stock.warehouse.orderpoint not available, skipping')
            return 0

        cids = self._get_company_ids()
        domain = [('active', '=', True), ('company_id', 'in', cids)]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        orderpoints = Orderpoint.search(domain, limit=5000)

        rows = []
        for op in orderpoints:
            product = op.product_id
            qty_on_hand = 0.0
            qty_forecast = 0.0
            try:
                qty_on_hand = product.qty_available or 0.0
                qty_forecast = product.virtual_available or 0.0
            except Exception:
                pass

            rows.append({
                'odoo_orderpoint_id': op.id,
                'odoo_product_id': product.id if product else None,
                'product_name': product.name if product else '',
                'warehouse_name': op.warehouse_id.name if op.warehouse_id else '',
                'location_name': op.location_id.complete_name if op.location_id else '',
                'product_min_qty': round(op.product_min_qty, 2),
                'product_max_qty': round(op.product_max_qty, 2),
                'qty_to_order': round(getattr(op, 'qty_to_order', 0) or 0, 2),
                'qty_on_hand': round(qty_on_hand, 2),
                'qty_forecast': round(qty_forecast, 2),
                'trigger_type': getattr(op, 'trigger', 'auto'),
                'active': op.active,
                'odoo_company_id': op.company_id.id if op.company_id else None,
            })

        return client.upsert('odoo_orderpoints', rows,
                              on_conflict='odoo_orderpoint_id', batch_size=200)

    # ── Account Payments (real payment records) ─────────────────────────

    def _push_boms(self, client: SupabaseClient, last_sync=None) -> int:
        """Push mrp.bom + mrp.bom.line → mrp_boms / mrp_bom_lines tables.

        BOMs unlock real manufacturing cost: instead of relying on the
        cached standard_price (often stale or zero for finished goods),
        we can roll down each BOM to sum component standard_prices and
        derive the actual unit cost of each manufactured product.

        Returns the number of bom headers pushed (0 if mrp not installed).
        """
        try:
            Bom = self.env['mrp.bom'].sudo()
        except KeyError:
            _logger.info('mrp.bom not available, skipping')
            return 0

        cids = self._get_company_ids()
        domain = [
            ('active', '=', True),
            '|', ('company_id', 'in', cids), ('company_id', '=', False),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))

        boms = Bom.search(domain)
        if not boms:
            return 0

        bom_rows = []
        line_rows = []
        for bom in boms:
            # Resolve product (variant) and template
            tmpl = bom.product_tmpl_id
            variant = bom.product_id  # may be empty (BOM applies to all variants)
            display_product = variant if variant else (tmpl.product_variant_id if tmpl else False)

            bom_rows.append({
                'odoo_bom_id': bom.id,
                'odoo_product_tmpl_id': tmpl.id if tmpl else None,
                'odoo_product_id': display_product.id if display_product else None,
                'product_name': display_product.name if display_product else (tmpl.name if tmpl else ''),
                'product_ref': ((display_product.default_code or '').strip() or None) if display_product else None,
                'product_qty': float(bom.product_qty or 1.0),
                'product_uom': bom.product_uom_id.name if bom.product_uom_id else '',
                'code': bom.code or '',
                'bom_type': bom.type or 'normal',
                'active': bool(bom.active),
                'odoo_company_id': bom.company_id.id if bom.company_id else None,
                'synced_at': datetime.now().isoformat(),
            })

            for line in bom.bom_line_ids:
                comp = line.product_id
                line_rows.append({
                    'odoo_bom_line_id': line.id,
                    'odoo_bom_id': bom.id,
                    'odoo_product_id': comp.id if comp else None,
                    'product_name': comp.name if comp else '',
                    'product_ref': ((comp.default_code or '').strip() or None) if comp else None,
                    'product_qty': float(line.product_qty or 0.0),
                    'product_uom': line.product_uom_id.name if line.product_uom_id else '',
                    'synced_at': datetime.now().isoformat(),
                })

        # Push headers first, then lines (FK soft via odoo_bom_id)
        client.upsert('mrp_boms', bom_rows,
                      on_conflict='odoo_bom_id', batch_size=200)
        if line_rows:
            client.upsert('mrp_bom_lines', line_rows,
                          on_conflict='odoo_bom_line_id', batch_size=500)
        return len(bom_rows)

    # ── UoMs (uom.uom master table) ──────────────────────────────────────

    def _push_uoms(self, client: SupabaseClient, last_sync=None) -> int:
        """Push uom.uom -> odoo_uoms table.

        Sprint 13e: needed to convert sale/invoice line quantities back
        to the product's canonical UoM when they differ. Conversion is
        within a UoM category (length, weight, volume); cross-category
        conversion is product-dependent and only flagged downstream.

        Odoo convention for `factor`: ratio relative to the category
        reference UoM. A SMALLER unit has factor > 1 (e.g. cm.factor =
        100 if m is the reference). Conversion math:
            qty_in_target = qty * (target.factor / source.factor)
        when both share the same category_id.
        """
        try:
            Uom = self.env['uom.uom'].sudo()
        except KeyError:
            _logger.info('uom.uom not available, skipping')
            return 0

        uoms = Uom.search([])
        rows = []
        for u in uoms:
            try:
                cat = getattr(u, 'category_id', None)
                rows.append({
                    'odoo_uom_id': u.id,
                    'name': u.name or '',
                    'category_id': cat.id if cat else None,
                    'category_name': cat.name if cat else None,
                    'factor': float(u.factor) if hasattr(u, 'factor') else None,
                    'factor_inv': float(u.factor_inv) if hasattr(u, 'factor_inv') else None,
                    'uom_type': getattr(u, 'uom_type', None),
                    'active': bool(getattr(u, 'active', True)),
                    'rounding': float(getattr(u, 'rounding', 0) or 0),
                    'synced_at': datetime.now().isoformat(),
                })
            except Exception as exc:
                _logger.warning('uom %s: %s', u.id, exc)

        if not rows:
            return 0
        return client.upsert('odoo_uoms', rows,
                             on_conflict='odoo_uom_id', batch_size=200)

    # ── SP11: Stock vs Accounting Reconciliation ─────────────────────────

    def _push_stock_locations(self, client: SupabaseClient, last_sync=None) -> int:
        """Push stock.location → odoo_stock_locations.
        Catálogo pequeño (~50 rows). Necesario para clasificar moves
        entrada/salida/transferencia/ajuste por usage."""
        try:
            Loc = self.env['stock.location'].sudo()
        except KeyError:
            return 0
        cids = self._get_company_ids()
        locs = Loc.search([
            ('active', '=', True),
            '|', ('company_id', 'in', cids), ('company_id', '=', False),
        ])
        rows = []
        for l in locs:
            rows.append({
                'odoo_location_id': l.id,
                'odoo_company_id': l.company_id.id if l.company_id else None,
                'name': l.name or '',
                'complete_name': l.complete_name or l.name or '',
                'usage': l.usage or 'internal',
                'warehouse_name': l.warehouse_id.name if hasattr(l, 'warehouse_id') and l.warehouse_id else None,
                'active': bool(l.active),
            })
        if not rows:
            return 0
        return client.upsert('odoo_stock_locations', rows,
                             on_conflict='odoo_location_id', batch_size=500)

    def _push_stock_moves(self, client: SupabaseClient, last_sync=None) -> int:
        """Push stock.move → odoo_stock_moves.
        Cada movimiento físico de inventario con su valor monetario y los
        account.move generados (account_move_ids). Base para invariants
        inventory.move_without_accounting + valuation_drift.

        SP11.3 (2026-04-22): removed limit=20000. All history requested.
        Chunked per 500 ids with browse + invalidate + try/except, mirroring
        the _push_invoices pattern, so memory stays bounded and a single
        chunk failure does not abort the whole push."""
        try:
            Move = self.env['stock.move'].sudo()
        except KeyError:
            return 0
        cids = self._get_company_ids()
        domain = [
            ('company_id', 'in', cids),
            ('state', '=', 'done'),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))

        move_ids = Move.search(domain, order='date desc, id desc').ids
        total = len(move_ids)
        if not total:
            return 0

        BATCH = 500
        ok = 0
        for chunk_start in range(0, total, BATCH):
            chunk_ids = move_ids[chunk_start:chunk_start + BATCH]
            try:
                moves = Move.browse(chunk_ids)
                rows = []
                for m in moves:
                    try:
                        qty_done = getattr(m, 'quantity', None)
                        if qty_done is None:
                            qty_done = getattr(m, 'quantity_done', None)
                        val = None
                        price_u = None
                        acct_ids = []
                        try:
                            val = float(m.value) if hasattr(m, 'value') and m.value is not None else None
                        except Exception:
                            pass
                        try:
                            price_u = float(m.price_unit) if hasattr(m, 'price_unit') else None
                        except Exception:
                            pass
                        # SP11.6 (2026-04-23): real field per env['stock.move'].fields_get
                        # is account_move_id (many2one to account.move, singular),
                        # NOT the plural M2M that SP11.4 assumed. stock.valuation.layer
                        # does not exist in Odoo 19 Quimibond.
                        try:
                            am = m.account_move_id if hasattr(m, 'account_move_id') else False
                            if am and am.id:
                                acct_ids = [am.id]
                        except Exception:
                            pass
                        rows.append({
                            'odoo_move_id': m.id,
                            'odoo_company_id': m.company_id.id if m.company_id else None,
                            'picking_id': m.picking_id.id if m.picking_id else None,
                            'picking_name': m.picking_id.name if m.picking_id else None,
                            'product_id': m.product_id.id if m.product_id else None,
                            'product_ref': (m.product_id.default_code or None) if m.product_id else None,
                            'product_uom_qty': float(m.product_uom_qty or 0),
                            'quantity': float(qty_done or 0),
                            'state': m.state,
                            'date': m.date.isoformat() if m.date else None,
                            'date_deadline': m.date_deadline.isoformat() if getattr(m, 'date_deadline', None) else None,
                            'location_id': m.location_id.id if m.location_id else None,
                            'location_dest_id': m.location_dest_id.id if m.location_dest_id else None,
                            'location_usage': m.location_id.usage if m.location_id else None,
                            'location_dest_usage': m.location_dest_id.usage if m.location_dest_id else None,
                            'reference': m.reference or None,
                            'origin': getattr(m, 'origin', None),
                            'is_inventory': bool(getattr(m, 'is_inventory', False)),
                            'is_in': bool(getattr(m, 'is_in', False)),
                            'is_out': bool(getattr(m, 'is_out', False)),
                            'is_dropship': bool(getattr(m, 'is_dropship', False)),
                            'value': val,
                            'price_unit': price_u,
                            'has_account_move': bool(acct_ids),
                            'account_move_ids': acct_ids,
                            # SP12.1 (2026-04-23): ligar stock.move → mrp.production
                            'production_id':              m.production_id.id if getattr(m, 'production_id', False) else None,
                            'raw_material_production_id': m.raw_material_production_id.id if getattr(m, 'raw_material_production_id', False) else None,
                        })
                    except Exception as exc:
                        _logger.warning('stock_move %s: %s', m.id, exc)

                if rows:
                    self.env.cr.commit()
                    self.env.invalidate_all()
                    ok += client.upsert('odoo_stock_moves', rows,
                                        on_conflict='odoo_move_id', batch_size=500) or 0
            except Exception as exc:
                _logger.exception('stock_moves chunk %s failed: %s', chunk_start, exc)
        return ok

    def _push_account_entries_stock(self, client: SupabaseClient, last_sync=None) -> int:
        """Push account.moves con líneas en cuentas inventario (115%), COGS (501%) o compras (504%).
        Quimibond verified codes (2026-04-23):
          Inventory: 115% (Inventory, Raw materials, Production in progress, Variación)
          COGS:      501% (Cost of sales, COSTO PRIMO, VARIACIÓN INVENTARIO, mano de obra)
          Purchase:  504% (costo compras, vendor bill counterpart)
        NOTE: 116.003 es Cuenta Transitoria BBVA (bancaria) — NO incluir 116%.

        SP11.8 (2026-04-23): evidencia empírica (sample 5+5 ene-feb) mostró que
        el link real stock.move ↔ account.move vive principalmente en:
          - PURCHASE: journal FACTU, move_type=in_invoice, cuenta 504.01.* (Δ≈+1d)
          - CUSTOMER: journal C, move_type=out_invoice, cuentas 115.04.*+501.01.* (Δ≈0)
          - Internal: journal STJ, move_type=entry, cuentas 115.*+501.* (Δ≈0)
        Ampliamos move_type IN (entry,in_invoice,out_invoice,in_refund,out_refund)
        y agregamos cuenta 504% para capturar los asientos de compra.
        Se captura JSONB lines_stock con {account_code, product_id, product_ref,
        debit, credit, name, partner_id} para matching SQL en Supabase (producto +
        monto ± 0.01 + fecha window).
        """
        try:
            Account = self.env['account.account'].sudo()
            Move = self.env['account.move'].sudo()
        except KeyError:
            return 0
        cids = self._get_company_ids()

        # Odoo 19: account.account uses company_ids (M2M), not company_id.
        # Try multi-company filter; fall back to no filter (single company).
        try:
            inv_account_ids = Account.search([
                ('company_ids', 'in', cids),
                '|', '|', ('code', '=like', '115%'), ('code', '=like', '501%'), ('code', '=like', '504%'),
            ]).ids
        except Exception:
            inv_account_ids = Account.search([
                '|', '|', ('code', '=like', '115%'), ('code', '=like', '501%'), ('code', '=like', '504%'),
            ]).ids
        if not inv_account_ids:
            return 0

        domain = [
            ('move_type', 'in', ('entry', 'in_invoice', 'out_invoice', 'in_refund', 'out_refund')),
            ('state', '=', 'posted'),
            ('line_ids.account_id', 'in', inv_account_ids),
            ('company_id', 'in', cids),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))

        entry_ids = Move.search(domain, order='date desc, id desc').ids
        total = len(entry_ids)
        if not total:
            return 0

        inv_codes = {}
        for a in Account.browse(inv_account_ids):
            inv_codes[a.id] = a.code or ''

        BATCH = 500
        ok = 0
        for chunk_start in range(0, total, BATCH):
            chunk_ids = entry_ids[chunk_start:chunk_start + BATCH]
            try:
                moves = Move.browse(chunk_ids)
                rows = []
                for m in moves:
                    try:
                        inv_lines_codes = []
                        cogs_lines_codes = []
                        purchase_lines_codes = []
                        lines_stock = []
                        for l in m.line_ids:
                            code = inv_codes.get(l.account_id.id)
                            if not code:
                                continue
                            if code.startswith('115'):
                                inv_lines_codes.append(code)
                            elif code.startswith('501'):
                                cogs_lines_codes.append(code)
                            elif code.startswith('504'):
                                purchase_lines_codes.append(code)
                            lines_stock.append({
                                'account_code': code,
                                'product_id': l.product_id.id if l.product_id else None,
                                'product_ref': (l.product_id.default_code or None) if l.product_id else None,
                                'debit':  float(l.debit or 0),
                                'credit': float(l.credit or 0),
                                'name':   (l.name or '')[:200],
                                'partner_id': l.partner_id.id if l.partner_id else None,
                            })
                        stock_ids = []
                        try:
                            if m.stock_move_ids:
                                stock_ids = [sm.id for sm in m.stock_move_ids]
                        except Exception:
                            pass
                        landed_ids = []
                        try:
                            if hasattr(m, 'landed_costs_ids') and m.landed_costs_ids:
                                landed_ids = [lc.id for lc in m.landed_costs_ids]
                        except Exception:
                            pass
                        wip_prod_ids = []
                        try:
                            if hasattr(m, 'wip_production_ids') and m.wip_production_ids:
                                wip_prod_ids = [p.id for p in m.wip_production_ids]
                        except Exception:
                            pass
                        asset_id_val = None
                        try:
                            if hasattr(m, 'asset_id') and m.asset_id:
                                asset_id_val = m.asset_id.id
                        except Exception:
                            pass
                        rows.append({
                            'odoo_move_id': m.id,
                            'odoo_company_id': m.company_id.id if m.company_id else None,
                            'date': m.date.isoformat() if m.date else None,
                            'name': m.name or None,
                            'ref': m.ref or None,
                            'journal_name': m.journal_id.name if m.journal_id else None,
                            'journal_type': m.journal_id.type if m.journal_id else None,
                            'move_type': m.move_type,
                            'amount_total': float(m.amount_total or 0),
                            'stock_move_ids': stock_ids,
                            'landed_costs_ids': landed_ids,
                            'wip_production_ids': wip_prod_ids,
                            'asset_id': asset_id_val,
                            'has_inventory_account': bool(inv_lines_codes),
                            'has_cogs_account': bool(cogs_lines_codes),
                            'has_purchase_account': bool(purchase_lines_codes),
                            'inventory_account_codes': sorted(set(inv_lines_codes))[:10],
                            'cogs_account_codes': sorted(set(cogs_lines_codes))[:10],
                            'purchase_account_codes': sorted(set(purchase_lines_codes))[:10],
                            'lines_stock': lines_stock,
                            'state': m.state,
                        })
                    except Exception as exc:
                        _logger.warning('account_entry_stock %s: %s', m.id, exc)

                if rows:
                    self.env.cr.commit()
                    self.env.invalidate_all()
                    ok += client.upsert('odoo_account_entries_stock', rows,
                                        on_conflict='odoo_move_id', batch_size=500) or 0
            except Exception as exc:
                _logger.exception('account_entries_stock chunk %s failed: %s', chunk_start, exc)
        return ok
