# -*- coding: utf-8 -*-
"""
Backfill module — operaciones one-shot que el cron incremental no cubre.

Sprint 5 (audit 2026-04-14): el cron `_push_invoice_lines` usa
`write_date >= last_sync` como watermark, por lo que las facturas
históricas que nunca fueron tocadas en Odoo desde que empezó el sync
NUNCA se pushean. Resultado: 26,353 facturas posteadas (97% del
histórico) sin líneas en Supabase, lo que rompe customer_margin_analysis,
product_margin_analysis, invoice_line_margins y todo cálculo de margen.

Este módulo extiende el modelo `quimibond.sync.push` con un método
`manual_backfill_invoice_lines()` que se invoca manualmente desde el
shell de Odoo.

Uso:
    env['quimibond.sync.push'].manual_backfill_invoice_lines()
    env['quimibond.sync.push'].manual_backfill_invoice_lines(max_batches=10)
    env['quimibond.sync.push'].manual_backfill_invoice_lines(reset_cursor=True)

El cursor (último id procesado) se persiste en ir.config_parameter para
poder reanudar si la corrida se interrumpe.
"""

import logging
from datetime import datetime

from odoo import models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

# Tipos de movimiento que llevan líneas de producto reales (excluye asientos)
_INVOICE_MOVE_TYPES = ['out_invoice', 'out_refund', 'in_invoice', 'in_refund']

# Display types de account.move.line que NO son líneas de producto
_NON_PRODUCT_LINE_TYPES = ('line_section', 'line_note', 'payment_term',
                           'tax', 'rounding')


def _commercial_partner_id(partner):
    """Resolve commercial partner ID (parent company). Mirror del helper
    en sync_push.py para evitar dependencia cruzada de imports privados."""
    cp = partner.commercial_partner_id
    return cp.id if cp else partner.id


def _get_supabase_client(env):
    """Construye el SupabaseClient desde ir.config_parameter.

    Mirror local del helper privado en sync_push.py para mantener este
    módulo aislado y no romper si sync_push.py es refactoreado.
    """
    from .supabase_client import SupabaseClient
    get = lambda k: env['ir.config_parameter'].sudo().get_param(k) or ''
    url = get('quimibond_intelligence.supabase_url')
    key = get('quimibond_intelligence.supabase_service_key')
    if not url or not key:
        _logger.error('Supabase URL/service key no configurado')
        return None
    return SupabaseClient(url, key)


def _build_invoice_line_rows(invoices):
    """Construye payload para upsert en odoo_invoice_lines desde un
    recordset de account.move. Replica la lógica de
    `_push_invoice_lines` para no acoplar este módulo a su firma privada.
    """
    rows = []
    for inv in invoices:
        pid = _commercial_partner_id(inv.partner_id)
        if not pid:
            continue

        inv_date = inv.invoice_date.strftime('%Y-%m-%d') if inv.invoice_date else None

        # Razón de conversión a MXN (computada una vez por invoice)
        inv_currency = inv.currency_id.name if inv.currency_id else 'MXN'
        mxn_ratio = 1.0
        amt_signed = getattr(inv, 'amount_total_signed', None)
        if amt_signed is not None and inv.amount_total:
            mxn_ratio = abs(amt_signed) / inv.amount_total

        for line in inv.invoice_line_ids:
            if line.display_type in _NON_PRODUCT_LINE_TYPES:
                continue

            rows.append({
                'odoo_line_id': line.id,
                'odoo_move_id': inv.id,
                'odoo_partner_id': pid,
                'move_name': inv.name,
                'move_type': inv.move_type,
                'invoice_date': inv_date,
                'odoo_product_id': line.product_id.id if line.product_id else None,
                'product_name': (
                    line.product_id.name if line.product_id
                    else (line.name or '')[:200]
                ),
                'product_ref': (
                    line.product_id.default_code or '' if line.product_id else ''
                ),
                'quantity': round(line.quantity, 2),
                'price_unit': round(line.price_unit, 2),
                'discount': round(line.discount, 2),
                'price_subtotal': round(line.price_subtotal, 2),
                'price_total': round(line.price_total, 2),
                'currency': inv_currency,
                'price_subtotal_mxn': round(line.price_subtotal * mxn_ratio, 2),
                'price_total_mxn': round(line.price_total * mxn_ratio, 2),
                'odoo_company_id': inv.company_id.id if inv.company_id else None,
            })
    return rows


class QuimibondSyncBackfill(models.Model):
    """Inherit del modelo principal para agregar operaciones one-shot."""
    _inherit = 'quimibond.sync.push'

    def manual_backfill_invoice_lines(self, batch_size=500, max_batches=None,
                                      reset_cursor=False):
        """One-shot backfill de odoo_invoice_lines para facturas históricas.

        Itera por TODAS las account.move posteadas en orden ascendente de
        id, en chunks de `batch_size`, y envía sus líneas al upsert
        idempotente. El cursor (último id procesado) se persiste en
        ir.config_parameter para poder reanudar si se interrumpe.

        Args:
            batch_size: cantidad de account.move por batch (default 500).
                        Cada move puede tener 1-N líneas.
            max_batches: límite de batches a procesar en esta corrida
                         (None = sin límite, hasta terminar).
            reset_cursor: si True, ignora el cursor guardado y empieza
                          desde id 0.

        Returns:
            dict con summary: batches_run, invoices_processed,
                              lines_pushed, last_id_processed,
                              finished, elapsed_seconds.

        Uso desde shell:
            env['quimibond.sync.push'].manual_backfill_invoice_lines()
            env['quimibond.sync.push'].manual_backfill_invoice_lines(max_batches=10)
            env['quimibond.sync.push'].manual_backfill_invoice_lines(reset_cursor=True)
        """
        client = _get_supabase_client(self.env)
        if not client:
            raise UserError('Supabase client no configurado')

        ICP = self.env['ir.config_parameter'].sudo()
        cursor_key = 'quimibond_intelligence.invoice_lines_backfill_cursor'

        if reset_cursor:
            ICP.set_param(cursor_key, '0')
            _logger.info('[backfill_invoice_lines] cursor reseteado a 0')

        try:
            cursor = int(ICP.get_param(cursor_key, '0') or '0')
        except (ValueError, TypeError):
            cursor = 0

        Move = self.env['account.move'].sudo()
        cid = self._get_company_id()
        base_domain = [
            ('company_id', '=', cid),
            ('move_type', 'in', _INVOICE_MOVE_TYPES),
            ('state', '=', 'posted'),
        ]

        total_remaining = Move.search_count(base_domain + [('id', '>', cursor)])
        _logger.info(
            '[backfill_invoice_lines] cursor=%s total_remaining=%s '
            'batch_size=%s max_batches=%s',
            cursor, total_remaining, batch_size, max_batches,
        )

        start_ts = datetime.now()
        batches_run = 0
        invoices_processed = 0
        lines_pushed = 0
        last_id = cursor
        finished = False

        while True:
            if max_batches is not None and batches_run >= max_batches:
                _logger.info(
                    '[backfill_invoice_lines] max_batches alcanzado: %s',
                    max_batches,
                )
                break

            domain = base_domain + [('id', '>', last_id)]
            invoices = Move.search(domain, order='id asc', limit=batch_size)
            if not invoices:
                finished = True
                _logger.info('[backfill_invoice_lines] sin más facturas, terminado')
                break

            rows = _build_invoice_line_rows(invoices)
            if rows:
                pushed = client.upsert(
                    'odoo_invoice_lines', rows,
                    on_conflict='odoo_line_id', batch_size=200,
                )
                lines_pushed += pushed

            invoices_processed += len(invoices)
            last_id = invoices[-1].id
            batches_run += 1

            # Persistir cursor cada batch para poder reanudar si se interrumpe
            ICP.set_param(cursor_key, str(last_id))

            _logger.info(
                '[backfill_invoice_lines] batch %s: %s facturas (%s líneas), '
                'last_id=%s',
                batches_run, len(invoices), len(rows), last_id,
            )

        elapsed = (datetime.now() - start_ts).total_seconds()

        # Log summary a sync_log para visibilidad en la UI de Odoo
        try:
            self.env['quimibond.sync.log'].sudo().create({
                'name': f'Backfill invoice_lines ({batches_run} batches)',
                'direction': 'push',
                'status': 'success' if finished else 'partial',
                'summary': (
                    f'invoices={invoices_processed} '
                    f'lines={lines_pushed} '
                    f'last_id={last_id} '
                    f'finished={finished}'
                ),
                'duration_seconds': round(elapsed, 1),
            })
        except Exception as exc:
            _logger.warning('No se pudo crear sync_log: %s', exc)

        result = {
            'batches_run': batches_run,
            'invoices_processed': invoices_processed,
            'lines_pushed': lines_pushed,
            'last_id_processed': last_id,
            'finished': finished,
            'elapsed_seconds': round(elapsed, 1),
        }
        _logger.info('[backfill_invoice_lines] SUMMARY: %s', result)
        return result
