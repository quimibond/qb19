"""Sync push: manufacturing orders, workcenters, workorders."""
import logging
from datetime import datetime, timedelta

from odoo import models

from .supabase_client import SupabaseClient

_logger = logging.getLogger(__name__)


class QuimibondSyncManufacturing(models.TransientModel):
    _inherit = 'quimibond.sync'

    def _push_manufacturing(self, client: SupabaseClient, last_sync=None) -> int:
        """Push mrp.production → odoo_manufacturing table."""
        try:
            MO = self.env['mrp.production'].sudo()
        except KeyError:
            _logger.info('mrp.production not available, skipping manufacturing sync')
            return 0

        cids = self._get_company_ids()
        cutoff = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        domain = [
            ('company_id', 'in', cids),
            '|',
            ('state', 'not in', ['done', 'cancel']),
            ('date_start', '>=', cutoff),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        productions = MO.search(domain)

        rows = []
        for mo in productions:
            # SP12.1: bom/cost/wip links para invariants manufacturing cost audit.
            # qty_produced es computed non-stored — se deriva.
            wip_ids = []
            try:
                if hasattr(mo, 'wip_move_ids') and mo.wip_move_ids:
                    wip_ids = [w.id for w in mo.wip_move_ids]
            except Exception:
                pass
            workorder_ids = []
            try:
                if hasattr(mo, 'workorder_ids') and mo.workorder_ids:
                    workorder_ids = [w.id for w in mo.workorder_ids]
            except Exception:
                pass
            rows.append({
                'odoo_production_id': mo.id,
                'name': mo.name,
                'product_name': mo.product_id.name if mo.product_id else '',
                'odoo_product_id': mo.product_id.id if mo.product_id else None,
                'qty_planned': round(mo.product_qty, 2),
                'qty_produced': round(getattr(mo, 'qty_produced', 0) or 0, 2),
                'qty_producing': round(getattr(mo, 'qty_producing', 0) or 0, 2),
                'state': mo.state,
                'date_start': mo.date_start.isoformat() if mo.date_start else None,
                'date_finished': mo.date_finished.isoformat() if mo.date_finished else None,
                'create_date': mo.create_date.strftime('%Y-%m-%d') if mo.create_date else None,
                'assigned_user': mo.user_id.name if mo.user_id else '',
                'origin': mo.origin or '',
                'odoo_company_id': mo.company_id.id if mo.company_id else None,
                # SP12.1 (2026-04-23): manufacturing cost audit fields
                'bom_id':          mo.bom_id.id if mo.bom_id else None,
                'sale_line_id':    mo.sale_line_id.id if getattr(mo, 'sale_line_id', False) else None,
                'extra_cost':      float(getattr(mo, 'extra_cost', 0) or 0),
                'wip_move_ids':    wip_ids,
                'workorder_ids':   workorder_ids,
                'location_src_id':  mo.location_src_id.id if mo.location_src_id else None,
                'location_dest_id': mo.location_dest_id.id if mo.location_dest_id else None,
            })

        return client.upsert('odoo_manufacturing', rows,
                              on_conflict='odoo_production_id', batch_size=200)

    # ── HR Employees ─────────────────────────────────────────────────────

    def _push_workcenters(self, client: SupabaseClient, last_sync=None) -> int:
        """Push mrp.workcenter → odoo_workcenters.
        Catálogo chico (~10-20 rows). Necesario para calcular labor cost
        (workorder.duration × workcenter.costs_hour)."""
        try:
            WC = self.env['mrp.workcenter'].sudo()
        except KeyError:
            return 0
        cids = self._get_company_ids()
        wcs = WC.search([
            ('active', '=', True),
            '|', ('company_id', 'in', cids), ('company_id', '=', False),
        ])
        rows = []
        for w in wcs:
            rows.append({
                'odoo_workcenter_id':     w.id,
                'odoo_company_id':        w.company_id.id if w.company_id else None,
                'name':                   w.name or '',
                'code':                   w.code or None,
                'active':                 bool(w.active),
                'costs_hour':             float(w.costs_hour or 0),
                'employee_costs_hour':    float(getattr(w, 'employee_costs_hour', 0) or 0),
                'time_efficiency':        float(w.time_efficiency or 0),
                'time_start':             float(w.time_start or 0),
                'time_stop':              float(w.time_stop or 0),
                'oee_target':             float(getattr(w, 'oee_target', 0) or 0),
                'expense_account_id':     w.expense_account_id.id if getattr(w, 'expense_account_id', False) else None,
            })
        if not rows:
            return 0
        return client.upsert('odoo_workcenters', rows,
                             on_conflict='odoo_workcenter_id', batch_size=200)

    def _push_workorders(self, client: SupabaseClient, last_sync=None) -> int:
        """Push mrp.workorder → odoo_workorders.
        Cada WO tiene duration_expected (BOM estándar) + duration (real),
        workcenter_id (para costs_hour), production_id. Base para calcular
        labor cost real por MO.
        Scope: workorders con production_id.date_finished en últimos 180d
        + todos los no-done para visibilidad de MOs en progreso."""
        try:
            WO = self.env['mrp.workorder'].sudo()
        except KeyError:
            return 0
        cids = self._get_company_ids()
        domain = [
            ('production_id.company_id', 'in', cids),
            '|',
              ('state', 'not in', ['done', 'cancel']),
              ('production_id.date_finished', '>=',
                 (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d %H:%M:%S')),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        workorder_ids = WO.search(domain, order='id desc').ids
        total = len(workorder_ids)
        if not total:
            return 0
        BATCH = 500
        ok = 0
        for chunk_start in range(0, total, BATCH):
            chunk_ids = workorder_ids[chunk_start:chunk_start + BATCH]
            try:
                wos = WO.browse(chunk_ids)
                rows = []
                for wo in wos:
                    try:
                        rows.append({
                            'odoo_workorder_id':      wo.id,
                            'odoo_production_id':     wo.production_id.id if wo.production_id else None,
                            'odoo_workcenter_id':     wo.workcenter_id.id if wo.workcenter_id else None,
                            'name':                   wo.name or '',
                            'state':                  wo.state,
                            'duration':               float(wo.duration or 0),
                            'duration_expected':      float(wo.duration_expected or 0),
                            'qty_produced':           float(getattr(wo, 'qty_produced', 0) or 0),
                            'qty_remaining':          float(getattr(wo, 'qty_remaining', 0) or 0),
                            'date_start':             wo.date_start.isoformat() if getattr(wo, 'date_start', False) else None,
                            'date_finished':          wo.date_finished.isoformat() if getattr(wo, 'date_finished', False) else None,
                        })
                    except Exception as exc:
                        _logger.warning('workorder %s: %s', wo.id, exc)
                if rows:
                    self.env.cr.commit()
                    self.env.invalidate_all()
                    ok += client.upsert('odoo_workorders', rows,
                                        on_conflict='odoo_workorder_id', batch_size=500) or 0
            except Exception as exc:
                _logger.exception('workorders chunk %s failed: %s', chunk_start, exc)
        return ok
