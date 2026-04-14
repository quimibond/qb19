# -*- coding: utf-8 -*-
"""
Backfill module — operaciones one-shot que el cron incremental no cubre.

Sprint 5 (audit 2026-04-14): el cron `_push_invoice_lines` usa
`write_date >= last_sync` como watermark, por lo que las facturas
históricas que nunca fueron tocadas en Odoo desde que empezó el sync
NUNCA se pushean. Resultado: 26,353 facturas posteadas (97% del
histórico) sin líneas en Supabase, lo que rompe customer_margin_analysis,
product_margin_analysis, invoice_line_margins y todo cálculo de margen.

Sprint 6: el mismo cron tampoco pusheó nunca cfdi_state ni edi_state
sobre odoo_invoices → 14,490 facturas posteadas con cfdi_state=NULL
y compliance SAT offline. manual_backfill_cfdi_states() lo recupera.

Sprint 7: _push_account_payments tiene `limit=5000` hardcodeado en el
primer full-sync → cobertura 36% de los pagos reales. El cron incremental
captura los nuevos vía write_date, pero los históricos > 5K nunca se
recuperan. manual_backfill_account_payments() los completa.

Este módulo extiende el modelo `quimibond.sync.push` con métodos
one-shot que se invocan manualmente desde el shell de Odoo:

    env['quimibond.sync.push'].manual_backfill_invoice_lines()
    env['quimibond.sync.push'].manual_backfill_cfdi_states()
    env['quimibond.sync.push'].manual_backfill_account_payments()

Cada método persiste un cursor independiente en ir.config_parameter
para poder reanudar si la corrida se interrumpe.
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

# Tipos de movimiento out (facturas a clientes) — para backfill CFDI
_OUT_INVOICE_MOVE_TYPES = ['out_invoice', 'out_refund']


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


def _build_cfdi_state_map(env, invoice_ids):
    """Build {invoice_id: {uuid, sat_state, doc_state}} from
    l10n_mx_edi.document via ORM. Mirror del helper en sync_push.py
    extendido para incluir el state del documento (no sólo uuid + sat_state).

    El UUID y el sat_state ya se pushean en _push_invoices via _build_cfdi_map.
    Pero `state` (= estado del workflow CFDI: 'sent', 'cancel', etc) y
    `account.move.edi_state` nunca se pushearon → 14,490 facturas con
    cfdi_state=NULL.
    """
    if not invoice_ids:
        return {}

    result = {}
    try:
        Document = env['l10n_mx_edi.document'].sudo()
        docs = Document.search([
            ('invoice_ids', 'in', invoice_ids),
        ], order='id desc')
        for doc in docs:
            uuid = getattr(doc, 'attachment_uuid', None)
            sat = getattr(doc, 'sat_state', None)
            doc_state = getattr(doc, 'state', None)
            for inv_id in doc.invoice_ids.ids:
                if inv_id not in result and inv_id in invoice_ids:
                    result[inv_id] = {
                        'uuid': uuid,
                        'sat': sat or None,
                        'doc_state': doc_state or None,
                    }
    except Exception as exc:
        _logger.warning('CFDI state map build failed: %s', exc)

    return result


def _build_account_payment_rows(payments):
    """Construye payload para upsert en odoo_account_payments desde un
    recordset de account.payment. Mirror de la lógica de
    `_push_account_payments` para mantener este módulo aislado.
    """
    rows = []
    for p in payments:
        try:
            pid = _commercial_partner_id(p.partner_id) if p.partner_id else None

            journal_name = None
            try:
                journal_name = p.journal_id.name if p.journal_id else None
            except Exception:
                pass

            payment_method = None
            try:
                if hasattr(p, 'payment_method_line_id') and p.payment_method_line_id:
                    payment_method = p.payment_method_line_id.name
                elif hasattr(p, 'payment_method_id') and p.payment_method_id:
                    payment_method = p.payment_method_id.name
            except Exception:
                pass

            amount_signed = None
            if hasattr(p, 'amount_company_currency_signed'):
                acs = p.amount_company_currency_signed
                if acs:
                    amount_signed = round(acs, 2)

            rows.append({
                'odoo_payment_id': p.id,
                'odoo_partner_id': pid,
                'name': p.name or '',
                'payment_type': p.payment_type or '',
                'partner_type': p.partner_type or '',
                'amount': round(p.amount or 0, 2),
                'amount_signed': amount_signed,
                'currency': p.currency_id.name if p.currency_id else 'MXN',
                'date': p.date.strftime('%Y-%m-%d') if p.date else None,
                'ref': (p.ref or '') if hasattr(p, 'ref') else '',
                'journal_name': journal_name,
                'payment_method': payment_method,
                'state': p.state or '',
                'is_matched': bool(getattr(p, 'is_matched', False)),
                'is_reconciled': bool(getattr(p, 'is_reconciled', False)),
                'reconciled_invoices_count': int(
                    getattr(p, 'reconciled_invoices_count', 0) or 0
                ),
                'odoo_company_id': p.company_id.id if p.company_id else None,
            })
        except Exception as exc:
            _logger.warning('account_payment %s: %s', p.id, exc)
    return rows


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

    def manual_backfill_cfdi_states(self, batch_size=500, max_batches=None,
                                    reset_cursor=False):
        """One-shot backfill de cfdi_state, edi_state, cfdi_uuid y
        cfdi_sat_state sobre odoo_invoices.

        El cron _push_invoices nunca pusheó cfdi_state ni edi_state →
        14,490 facturas posteadas (100% de las out_invoice) sin cumplimiento
        SAT visible en Supabase.

        Esta función itera por TODAS las account.move out (out_invoice +
        out_refund) en orden ascendente de id, lee uuid/sat_state/doc_state
        de l10n_mx_edi.document y edi_state directamente de la factura,
        y llama a la RPC update_invoice_cfdi_states_bulk en Supabase con
        un payload por batch.

        La RPC sólo actualiza filas existentes (no inserta), y usa
        COALESCE para no sobreescribir datos válidos con NULL.

        Args:
            batch_size: cantidad de account.move por batch (default 500)
            max_batches: límite de batches (None = sin límite)
            reset_cursor: si True, ignora el cursor guardado y empieza desde 0

        Returns:
            dict con summary: batches_run, invoices_processed,
                              rows_updated, last_id_processed,
                              finished, elapsed_seconds.

        Uso desde shell:
            env['quimibond.sync.push'].manual_backfill_cfdi_states(max_batches=5)
            env['quimibond.sync.push'].manual_backfill_cfdi_states()
            env['quimibond.sync.push'].manual_backfill_cfdi_states(reset_cursor=True)
        """
        client = _get_supabase_client(self.env)
        if not client:
            raise UserError('Supabase client no configurado')

        ICP = self.env['ir.config_parameter'].sudo()
        cursor_key = 'quimibond_intelligence.cfdi_states_backfill_cursor'

        if reset_cursor:
            ICP.set_param(cursor_key, '0')
            _logger.info('[backfill_cfdi_states] cursor reseteado a 0')

        try:
            cursor = int(ICP.get_param(cursor_key, '0') or '0')
        except (ValueError, TypeError):
            cursor = 0

        Move = self.env['account.move'].sudo()
        cid = self._get_company_id()
        base_domain = [
            ('company_id', '=', cid),
            ('move_type', 'in', _OUT_INVOICE_MOVE_TYPES),
            ('state', '=', 'posted'),
        ]

        total_remaining = Move.search_count(base_domain + [('id', '>', cursor)])
        _logger.info(
            '[backfill_cfdi_states] cursor=%s total_remaining=%s '
            'batch_size=%s max_batches=%s',
            cursor, total_remaining, batch_size, max_batches,
        )

        start_ts = datetime.now()
        batches_run = 0
        invoices_processed = 0
        rows_updated = 0
        last_id = cursor
        finished = False

        while True:
            if max_batches is not None and batches_run >= max_batches:
                _logger.info(
                    '[backfill_cfdi_states] max_batches alcanzado: %s',
                    max_batches,
                )
                break

            domain = base_domain + [('id', '>', last_id)]
            invoices = Move.search(domain, order='id asc', limit=batch_size)
            if not invoices:
                finished = True
                _logger.info('[backfill_cfdi_states] sin más facturas, terminado')
                break

            cfdi_map = _build_cfdi_state_map(self.env, invoices.ids)

            payload = []
            for inv in invoices:
                pid = _commercial_partner_id(inv.partner_id)
                if not pid or not inv.name:
                    continue
                cfdi = cfdi_map.get(inv.id, {})
                edi_state = getattr(inv, 'edi_state', None)

                # Skip si no hay nada nuevo que pushear
                if not any([
                    cfdi.get('uuid'),
                    cfdi.get('sat'),
                    cfdi.get('doc_state'),
                    edi_state,
                ]):
                    continue

                payload.append({
                    'odoo_partner_id': pid,
                    'name': inv.name,
                    'cfdi_uuid': cfdi.get('uuid'),
                    'cfdi_sat_state': cfdi.get('sat'),
                    'cfdi_state': cfdi.get('doc_state'),
                    'edi_state': edi_state,
                })

            if payload:
                try:
                    rpc_result = client.rpc(
                        'update_invoice_cfdi_states_bulk',
                        {'p_data': payload},
                    )
                    if isinstance(rpc_result, dict):
                        rows_updated += int(rpc_result.get('rows_updated', 0))
                    elif isinstance(rpc_result, list) and rpc_result:
                        rows_updated += int(
                            (rpc_result[0] or {}).get('rows_updated', 0)
                        )
                except Exception as exc:
                    _logger.warning(
                        '[backfill_cfdi_states] RPC error en batch %s: %s',
                        batches_run + 1, exc,
                    )

            invoices_processed += len(invoices)
            last_id = invoices[-1].id
            batches_run += 1

            ICP.set_param(cursor_key, str(last_id))

            _logger.info(
                '[backfill_cfdi_states] batch %s: %s facturas (%s payload), '
                'last_id=%s',
                batches_run, len(invoices), len(payload), last_id,
            )

        elapsed = (datetime.now() - start_ts).total_seconds()

        try:
            self.env['quimibond.sync.log'].sudo().create({
                'name': f'Backfill cfdi_states ({batches_run} batches)',
                'direction': 'push',
                'status': 'success' if finished else 'partial',
                'summary': (
                    f'invoices={invoices_processed} '
                    f'updated={rows_updated} '
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
            'rows_updated': rows_updated,
            'last_id_processed': last_id,
            'finished': finished,
            'elapsed_seconds': round(elapsed, 1),
        }
        _logger.info('[backfill_cfdi_states] SUMMARY: %s', result)
        return result

    def manual_backfill_account_payments(self, batch_size=500, max_batches=None,
                                          reset_cursor=False):
        """One-shot backfill de odoo_account_payments para pagos históricos.

        El cron _push_account_payments en sync_push.py tiene `limit=5000`
        hardcodeado en la primera corrida full-sync, por lo que sólo cubre
        los primeros 5,000 account.payment records (por orden default de
        Odoo). Audit 2026-04-14: 5,000 pagos sincronizados de 13,868
        facturas pagadas → cobertura ~36%.

        Esta función itera por TODOS los account.payment en orden ascendente
        de id, en chunks de batch_size, y los empuja al upsert idempotente.
        Mismo patrón que manual_backfill_invoice_lines (Sprint 5).

        Args:
            batch_size: cantidad de account.payment por batch (default 500)
            max_batches: límite de batches (None = sin límite)
            reset_cursor: si True, ignora el cursor guardado y empieza desde 0

        Returns:
            dict con summary: batches_run, payments_processed, rows_pushed,
                              last_id_processed, finished, elapsed_seconds.

        Uso desde shell:
            env['quimibond.sync.push'].manual_backfill_account_payments(max_batches=5)
            env['quimibond.sync.push'].manual_backfill_account_payments()
            env['quimibond.sync.push'].manual_backfill_account_payments(reset_cursor=True)
        """
        client = _get_supabase_client(self.env)
        if not client:
            raise UserError('Supabase client no configurado')

        try:
            Payment = self.env['account.payment'].sudo()
        except KeyError:
            raise UserError('account.payment no disponible en este Odoo')

        ICP = self.env['ir.config_parameter'].sudo()
        cursor_key = 'quimibond_intelligence.account_payments_backfill_cursor'

        if reset_cursor:
            ICP.set_param(cursor_key, '0')
            _logger.info('[backfill_account_payments] cursor reseteado a 0')

        try:
            cursor = int(ICP.get_param(cursor_key, '0') or '0')
        except (ValueError, TypeError):
            cursor = 0

        cid = self._get_company_id()
        base_domain = [('company_id', '=', cid)]

        total_remaining = Payment.search_count(
            base_domain + [('id', '>', cursor)]
        )
        _logger.info(
            '[backfill_account_payments] cursor=%s total_remaining=%s '
            'batch_size=%s max_batches=%s',
            cursor, total_remaining, batch_size, max_batches,
        )

        start_ts = datetime.now()
        batches_run = 0
        payments_processed = 0
        rows_pushed = 0
        last_id = cursor
        finished = False

        while True:
            if max_batches is not None and batches_run >= max_batches:
                _logger.info(
                    '[backfill_account_payments] max_batches alcanzado: %s',
                    max_batches,
                )
                break

            domain = base_domain + [('id', '>', last_id)]
            payments = Payment.search(domain, order='id asc', limit=batch_size)
            if not payments:
                finished = True
                _logger.info('[backfill_account_payments] sin más pagos, terminado')
                break

            rows = _build_account_payment_rows(payments)
            if rows:
                pushed = client.upsert(
                    'odoo_account_payments', rows,
                    on_conflict='odoo_payment_id', batch_size=200,
                )
                rows_pushed += pushed

            payments_processed += len(payments)
            last_id = payments[-1].id
            batches_run += 1

            ICP.set_param(cursor_key, str(last_id))

            _logger.info(
                '[backfill_account_payments] batch %s: %s pagos (%s rows), '
                'last_id=%s',
                batches_run, len(payments), len(rows), last_id,
            )

        elapsed = (datetime.now() - start_ts).total_seconds()

        try:
            self.env['quimibond.sync.log'].sudo().create({
                'name': f'Backfill account_payments ({batches_run} batches)',
                'direction': 'push',
                'status': 'success' if finished else 'partial',
                'summary': (
                    f'payments={payments_processed} '
                    f'pushed={rows_pushed} '
                    f'last_id={last_id} '
                    f'finished={finished}'
                ),
                'duration_seconds': round(elapsed, 1),
            })
        except Exception as exc:
            _logger.warning('No se pudo crear sync_log: %s', exc)

        result = {
            'batches_run': batches_run,
            'payments_processed': payments_processed,
            'rows_pushed': rows_pushed,
            'last_id_processed': last_id,
            'finished': finished,
            'elapsed_seconds': round(elapsed, 1),
        }
        _logger.info('[backfill_account_payments] SUMMARY: %s', result)
        return result
