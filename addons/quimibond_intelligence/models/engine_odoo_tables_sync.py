"""
Engine — Sync Odoo operational tables to Supabase

Sincroniza datos estructurados de Odoo a tablas dedicadas en Supabase:
- odoo_products: catálogo con stock en tiempo real
- odoo_order_lines: detalle de ventas/compras por producto
- odoo_users: equipo con actividades pendientes

Corre como parte del enrichment (cada 6h) o manualmente.
"""
import json
import logging
import time
from datetime import datetime, timedelta

from odoo import api, fields, models

from .intelligence_config import acquire_lock, release_lock

_logger = logging.getLogger(__name__)


class IntelligenceEngine(models.Model):
    _inherit = 'intelligence.engine'

    @api.model
    def run_sync_odoo_tables(self):
        """Sync productos, líneas de orden, y usuarios a Supabase."""
        lock = 'quimibond_intelligence.odoo_tables_sync_running'
        if not acquire_lock(self.env, lock):
            return
        start = time.time()

        try:
            cfg = self._load_config()
            if not cfg:
                return

            from ..services.supabase_service import SupabaseService

            with SupabaseService(cfg['supabase_url'], cfg['supabase_key']) as supa:
                products = self._sync_products(supa)
                lines = self._sync_order_lines(supa)
                users = self._sync_users(supa)

                _logger.info(
                    '✓ Odoo tables sync: %d products, %d order lines, '
                    '%d users (%.1fs)',
                    products, lines, users, time.time() - start,
                )
        except Exception as exc:
            _logger.error('run_sync_odoo_tables: %s', exc, exc_info=True)
        finally:
            release_lock(self.env, lock)

    # ── Products ──────────────────────────────────────────────────────────────

    def _sync_products(self, supa) -> int:
        """Sync product catalog with real-time stock."""
        try:
            Product = self.env['product.product'].sudo()
        except KeyError:
            return 0

        products = Product.search([
            ('active', '=', True),
            ('type', '!=', 'service'),
        ], limit=2000)

        if not products:
            return 0

        # Cargar stock.quant y orderpoints en batch
        try:
            Quant = self.env['stock.quant'].sudo()
            Orderpoint = self.env['stock.warehouse.orderpoint'].sudo()
        except KeyError:
            Quant = None
            Orderpoint = None

        batch = []
        for p in products:
            stock_qty = 0
            reserved_qty = 0
            reorder_min = 0
            reorder_max = 0

            if Quant:
                quants = Quant.search([
                    ('product_id', '=', p.id),
                    ('location_id.usage', '=', 'internal'),
                ])
                stock_qty = sum(q.quantity for q in quants)
                reserved_qty = sum(q.reserved_quantity for q in quants)

            if Orderpoint:
                op = Orderpoint.search([
                    ('product_id', '=', p.id),
                ], limit=1)
                if op:
                    reorder_min = op.product_min_qty
                    reorder_max = op.product_max_qty

            batch.append({
                'odoo_product_id': p.id,
                'name': p.name,
                'internal_ref': p.default_code or '',
                'category': p.categ_id.name if p.categ_id else '',
                'category_id': p.categ_id.id if p.categ_id else None,
                'uom': p.uom_id.name if p.uom_id else 'Unidad',
                'stock_qty': round(stock_qty, 2),
                'reserved_qty': round(reserved_qty, 2),
                'reorder_min': round(reorder_min, 2),
                'reorder_max': round(reorder_max, 2),
                'standard_price': round(p.standard_price or 0, 2),
                'list_price': round(p.lst_price or 0, 2),
                'active': p.active,
                'product_type': p.type,
                'barcode': p.barcode or '',
                'weight': round(p.weight or 0, 3),
                'updated_at': datetime.now().isoformat(),
            })

        # Upsert en batches de 100
        synced = 0
        for i in range(0, len(batch), 100):
            chunk = batch[i:i + 100]
            try:
                supa._request(
                    '/rest/v1/odoo_products?on_conflict=odoo_product_id',
                    'POST', chunk,
                    extra_headers={
                        'Prefer': 'resolution=merge-duplicates',
                    },
                )
                synced += len(chunk)
            except Exception as exc:
                _logger.debug('sync_products batch: %s', exc)

        return synced

    # ── Order Lines ───────────────────────────────────────────────────────────

    def _sync_order_lines(self, supa) -> int:
        """Sync sale/purchase order lines (last 12 months)."""
        cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        synced = 0

        # Sale order lines
        try:
            SOLine = self.env['sale.order.line'].sudo()
            lines = SOLine.search([
                ('order_id.date_order', '>=', cutoff),
                ('order_id.state', 'in', ['sale', 'done']),
                ('display_type', '=', False),
            ], limit=5000)

            batch = []
            for line in lines:
                order = line.order_id
                cpid = (order.partner_id.commercial_partner_id.id
                        if order.partner_id.commercial_partner_id
                        else order.partner_id.id)
                batch.append({
                    'odoo_line_id': line.id,
                    'odoo_order_id': order.id,
                    'odoo_partner_id': cpid,
                    'order_name': order.name,
                    'order_date': (order.date_order.strftime('%Y-%m-%d')
                                   if order.date_order else None),
                    'order_type': 'sale',
                    'order_state': order.state,
                    'product_name': line.product_id.name if line.product_id else '',
                    'odoo_product_id': line.product_id.id if line.product_id else None,
                    'qty': round(line.product_uom_qty or 0, 2),
                    'price_unit': round(line.price_unit or 0, 2),
                    'discount': round(line.discount or 0, 2),
                    'subtotal': round(line.price_subtotal or 0, 2),
                    'currency': order.currency_id.name if order.currency_id else 'MXN',
                })

            for i in range(0, len(batch), 200):
                chunk = batch[i:i + 200]
                try:
                    supa._request(
                        '/rest/v1/odoo_order_lines?on_conflict=odoo_line_id',
                        'POST', chunk,
                        extra_headers={
                            'Prefer': 'resolution=merge-duplicates',
                        },
                    )
                    synced += len(chunk)
                except Exception as exc:
                    _logger.debug('sync sale lines batch: %s', exc)

        except Exception as exc:
            _logger.debug('sync sale lines: %s', exc)

        # Purchase order lines
        try:
            POLine = self.env['purchase.order.line'].sudo()
            po_lines = POLine.search([
                ('order_id.date_order', '>=', cutoff),
                ('order_id.state', 'in', ['purchase', 'done']),
                ('display_type', '=', False),
            ], limit=5000)

            batch = []
            for line in po_lines:
                order = line.order_id
                cpid = (order.partner_id.commercial_partner_id.id
                        if order.partner_id.commercial_partner_id
                        else order.partner_id.id)
                # Use negative IDs for PO lines to avoid collision with SO lines
                batch.append({
                    'odoo_line_id': -line.id,
                    'odoo_order_id': order.id,
                    'odoo_partner_id': cpid,
                    'order_name': order.name,
                    'order_date': (order.date_order.strftime('%Y-%m-%d')
                                   if order.date_order else None),
                    'order_type': 'purchase',
                    'order_state': order.state,
                    'product_name': line.product_id.name if line.product_id else '',
                    'odoo_product_id': line.product_id.id if line.product_id else None,
                    'qty': round(line.product_qty or 0, 2),
                    'price_unit': round(line.price_unit or 0, 2),
                    'discount': 0,
                    'subtotal': round(line.price_subtotal or 0, 2),
                    'currency': order.currency_id.name if order.currency_id else 'MXN',
                })

            for i in range(0, len(batch), 200):
                chunk = batch[i:i + 200]
                try:
                    supa._request(
                        '/rest/v1/odoo_order_lines?on_conflict=odoo_line_id',
                        'POST', chunk,
                        extra_headers={
                            'Prefer': 'resolution=merge-duplicates',
                        },
                    )
                    synced += len(chunk)
                except Exception as exc:
                    _logger.debug('sync purchase lines batch: %s', exc)

        except Exception as exc:
            _logger.debug('sync purchase lines: %s', exc)

        return synced

    # ── Users / Team ──────────────────────────────────────────────────────────

    def _sync_users(self, supa) -> int:
        """Sync active internal users with their pending activities."""
        User = self.env['res.users'].sudo()
        users = User.search([
            ('active', '=', True),
            ('share', '=', False),  # Only internal users
        ], limit=200)

        if not users:
            return 0

        today = fields.Date.today()
        try:
            Activity = self.env['mail.activity'].sudo()
        except KeyError:
            Activity = None

        batch = []
        for u in users:
            pending = 0
            overdue = 0
            activities = []

            if Activity:
                acts = Activity.search([
                    ('user_id', '=', u.id),
                ], limit=20, order='date_deadline asc')
                pending = len(acts)
                overdue = len([a for a in acts if a.date_deadline < today])
                activities = [{
                    'type': (a.activity_type_id.name
                             if a.activity_type_id else 'Tarea'),
                    'summary': a.summary or '',
                    'deadline': a.date_deadline.strftime('%Y-%m-%d'),
                    'overdue': a.date_deadline < today,
                    'model': a.res_model or '',
                } for a in acts[:10]]

            dept = ''
            if hasattr(u, 'department_id') and u.department_id:
                dept = u.department_id.name
            job = ''
            if hasattr(u, 'job_title'):
                job = u.job_title or ''

            batch.append({
                'odoo_user_id': u.id,
                'name': u.name,
                'email': u.email or u.login,
                'department': dept,
                'job_title': job,
                'pending_activities_count': pending,
                'overdue_activities_count': overdue,
                'activities_json': activities,
                'updated_at': datetime.now().isoformat(),
            })

        try:
            supa._request(
                '/rest/v1/odoo_users?on_conflict=odoo_user_id',
                'POST', batch,
                extra_headers={
                    'Prefer': 'resolution=merge-duplicates',
                },
            )
            return len(batch)
        except Exception as exc:
            _logger.debug('sync_users: %s', exc)
            return 0
