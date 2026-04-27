"""Sync push: invoices, invoice lines, payments, accounts, balances, FX rates.

Includes CFDI helpers (_read_cfdi_uuid, _maybe_warn_stale_uuid, _serialize_invoice,
_compute_invoice_fx_ratio) used only by the invoice push.
"""
import logging
from datetime import datetime, timedelta

from odoo import models

from .ingestion_core import IngestionCore
from .supabase_client import SupabaseClient
from .sync_push import (
    _commercial_partner_id,
    _build_cfdi_map,
    _build_payment_date_map,
    STALE_UUID_THRESHOLD_MINUTES,
)

_logger = logging.getLogger(__name__)


class QuimibondSyncFinance(models.TransientModel):
    _inherit = 'quimibond.sync'

    # ── CFDI UUID capture helpers (Task 3 — 2026-04-24) ─────────────────
    #
    # Odoo 19 background: `move.l10n_mx_edi_cfdi_uuid` is a stored computed
    # field that was left stale after the 17→19 migration — `.read()` returns
    # NULL for moves written before the recompute was triggered. The bulk sync
    # uses `_build_cfdi_map(env, ids)` (queries `l10n_mx_edi.document` directly,
    # SAT/posted/id scoring) to bypass that.
    #
    # `_read_cfdi_uuid(move, cfdi_map=None)` has TWO trust modes:
    #
    # 1) BULK path (cfdi_map is not None): `cfdi_map` is the SP10.4 authority.
    #    It already picked ONE winner per duplicate UUID across the chunk to
    #    avoid hitting `uq_odoo_invoices_cfdi_uuid` during upsert. A missing
    #    entry for move.id means "this move LOST the winner scoring — do not
    #    claim a UUID." We MUST NOT fall back to the stored field or a fresh
    #    per-move `_build_cfdi_map` call, because either would let a loser
    #    re-claim a UUID already assigned to the winner and reopen the
    #    UNIQUE-violation window SP10.4 closed.
    #
    # 2) SINGLE-INVOICE path (cfdi_map is None): used by `_serialize_invoice`
    #    from ad-hoc callers (tests, debugging, one-off pushes). No contention
    #    with the chunk-level winner-scoring, so falling back to the stored
    #    field and then an on-demand `_build_cfdi_map([move.id])` is safe.
    #
    # Both paths emit the stale-timbrado WARNING when a posted move older
    # than STALE_UUID_THRESHOLD_MINUTES comes back without a UUID.
    def _read_cfdi_uuid(self, move, cfdi_map: dict | None = None) -> str | None:
        """Resolve the CFDI UUID for a posted account.move.

        Args:
            move: browse record on `account.move`.
            cfdi_map: pre-built {move_id: {uuid, sat}} dict from
                `_build_cfdi_map`. Passed during bulk `_push_invoices` to
                avoid N+1 queries AND to enforce SP10.4 winner scoring. When
                provided, it is the EXCLUSIVE source of truth for this move;
                no stored-field or on-demand fallback runs.

        Returns: lowercase UUID string, or None if not yet timbrado / lost
        the winner scoring.
        """
        # BULK path — cfdi_map is the SP10.4 authority.
        if cfdi_map is not None:
            entry = cfdi_map.get(move.id)
            uuid = (entry.get('uuid') if entry else None)
            if uuid:
                # Normalize to lowercase per SP10.6 (Odoo stores supplier XML
                # UUIDs uppercased; SAT/Syntage uses lowercase).
                return uuid.lower()
            self._maybe_warn_stale_uuid(move)
            return None

        # SINGLE-INVOICE path — no winner-scoring contention.
        uuid = None

        # a) Stored computed field (Odoo 19 canonical name on account.move).
        #    May be stale post-migration; only useful as a fallback.
        try:
            uuid = getattr(move, 'l10n_mx_edi_cfdi_uuid', None) or None
        except Exception:
            uuid = None

        # b) Last resort: query l10n_mx_edi.document directly for this one move.
        if not uuid:
            try:
                fresh = _build_cfdi_map(self.env, [move.id])
                entry = fresh.get(move.id) if fresh else None
                if entry:
                    uuid = entry.get('uuid')
            except Exception as exc:
                _logger.debug(
                    'single-move _build_cfdi_map failed for move id=%s: %s',
                    move.id, exc,
                )

        if uuid:
            return uuid.lower()

        self._maybe_warn_stale_uuid(move)
        return None

    def _maybe_warn_stale_uuid(self, move) -> None:
        """Log a WARNING when `move` is posted, older than
        STALE_UUID_THRESHOLD_MINUTES, and has no CFDI UUID yet. Operational
        signal that l10n_mx_edi timbrado may have failed.
        """
        try:
            if move.state == 'posted' and move.create_date:
                age_minutes = (
                    (fields.Datetime.now() - move.create_date).total_seconds()
                    / 60.0
                )
                if age_minutes > STALE_UUID_THRESHOLD_MINUTES:
                    _logger.warning(
                        'account.move id=%s name=%s posted %.1f min ago without '
                        'cfdi_uuid (l10n_mx_edi timbrado may have failed)',
                        move.id, move.name, age_minutes,
                    )
        except Exception as exc:
            _logger.debug(
                'stale-uuid check failed for move id=%s: %s', move.id, exc,
            )

    def _serialize_invoice(self, move, cfdi_map: dict | None = None) -> dict:
        """Thin per-move serializer that routes CFDI UUID through
        `_read_cfdi_uuid` so single-invoice callers (tests, debugging
        helpers) share the same UUID-capture path as `_push_invoices`.

        The bulk `_push_invoices` loop builds its full payload inline for
        performance; this helper intentionally returns only the UUID-relevant
        slice plus identifiers so the test suite can target UUID capture
        without coupling to the full 40-field payload shape.
        """
        return {
            'odoo_invoice_id': move.id,
            'cfdi_uuid': self._read_cfdi_uuid(move, cfdi_map=cfdi_map),
        }

    # ── Invoices (ALL history) ──────────────────────────────────────────

    def _push_invoices(self, client: SupabaseClient, last_sync=None) -> int:
        core = IngestionCore(client)
        run_id, core_watermark = core.start_run(
            source='odoo',
            table='odoo_invoices',
            run_type='full' if not last_sync else 'incremental',
            triggered_by='cron',
        )
        effective_watermark = core_watermark or (last_sync.isoformat() if last_sync else None)
        status = 'success'
        final_watermark = effective_watermark
        ok = 0
        try:
            Move = self.env['account.move'].sudo()
            cids = self._get_company_ids()
            domain = [
                ('company_id', 'in', cids),
                ('move_type', 'in', [
                    'out_invoice', 'out_refund',
                    'in_invoice', 'in_refund',
                ]),
                ('state', '=', 'posted'),
            ]
            if last_sync:
                domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
            # Batch loop — antes cargábamos las 26k+ facturas en memoria junto
            # con cfdi_map y payment_date_map (cada uno por move_id), + rows
            # dicts. El worker de Odoo.sh era OOM-killed durante full-sync
            # (2026-04-20). Ahora procesamos en chunks y vaciamos ORM caches
            # entre chunks.
            #
            # SP5.5 (2026-04-22): BATCH 2000 → 1000 + commit/invalidate se mueve
            # ANTES del upsert (antes estaba después), para que el cursor de
            # Odoo no quede "idle-in-transaction" durante los 10+ POSTs HTTP
            # secuenciales del chunk. Eso era el origen del "connection already
            # closed after 800+s": Postgres mataba la sesión por idle-in-tx.
            # Adicionalmente cada chunk se ejecuta en try/except propio — si
            # un chunk falla se loggea y seguimos con el siguiente, en vez de
            # tumbar el sync entero + arrastrar sale_orders/account_balances.
            invoice_ids = Move.search(domain).ids
            # SP9 T6 (2026-04-23): BATCH 1000 -> 500 para acortar la ventana de
            # upserts HTTP por chunk (antes ~10-14 min secuenciales); chunks mas
            # cortos reducen la probabilidad de que idle_session_timeout de
            # Odoo.sh cierre la conexion PG entre el commit local y el siguiente
            # Move.browse. Combinado con el ping post-upsert (ver mas abajo).
            BATCH = 500
            today = datetime.now().date()
            total = len(invoice_ids)
            num_chunks = (total + BATCH - 1) // BATCH if total else 0
            all_failed: list = []
            total_attempted = 0
            chunks_failed = 0
            # Cap the per-chunk individual failure reports — un sub-batch de
            # 200 rows con 502 disparaba 200 RPCs secuenciales a
            # ingestion_report_failure, amplificando la indisponibilidad.
            REPORT_FAILURE_CAP = 25
            # SP5.5 (2026-04-22): global cfdi_uuid dedup across chunks.
            # 11 UUIDs in Odoo's l10n_mx_edi.document are shared across 1,700+
            # moves (SP0 residue). Without cross-chunk dedup, a UUID could win
            # in chunk 1 AND again in chunk 5 → Supabase UNIQUE violation.
            seen_cfdi_uuids: set = set()
            chunk_idx = 0
            for chunk_start in range(0, total, BATCH):
                chunk_idx += 1
                chunk_ids = invoice_ids[chunk_start:chunk_start + BATCH]
                t_chunk_start = datetime.now()
                try:
                    invoices = Move.browse(chunk_ids)

                    # CFDI UUID + SAT state: bypasses the stored computed field on
                    # account.move which is stale for post-migration invoices (Jul 2025+).
                    cfdi_map = _build_cfdi_map(self.env, chunk_ids, seen_cfdi_uuids)

                    # Payment dates from account.partial.reconcile (real payment date)
                    payment_date_map = _build_payment_date_map(self.env, chunk_ids)

                    rows = []
                    for inv in invoices:
                        pid = _commercial_partner_id(inv.partner_id)
                        if not pid:
                            continue

                        days_overdue = 0
                        if inv.payment_state in ('not_paid', 'partial') and inv.invoice_date_due:
                            if inv.invoice_date_due < today:
                                days_overdue = (today - inv.invoice_date_due).days

                        # Payment term
                        pay_term = None
                        try:
                            if inv.invoice_payment_term_id:
                                pay_term = inv.invoice_payment_term_id.name
                        except Exception:
                            pass

                        # CFDI fields from pre-read map.
                        # Task 3 (2026-04-24): route UUID capture through
                        # `_read_cfdi_uuid` so the stale-timbrado WARNING
                        # fires here too. The cfdi_map is still the preferred
                        # source (bulk-built, SP10.4/SP10.6 scoring); the
                        # helper just adds null-safe fallback + ops logging.
                        cfdi = cfdi_map.get(inv.id, {})
                        cfdi_uuid = self._read_cfdi_uuid(inv, cfdi_map=cfdi_map)
                        cfdi_sat = cfdi.get('sat')

                        # Payment date and days_to_pay from reconciliation
                        pay_date = payment_date_map.get(inv.id)
                        pay_date_str = pay_date.strftime('%Y-%m-%d') if pay_date else None
                        days_to_pay = None
                        if pay_date and inv.invoice_date:
                            delta = (pay_date - inv.invoice_date).days
                            days_to_pay = max(delta, 0)

                        # MXN amounts: amount_total_signed is always in company
                        # currency (MXN).  For MXN invoices the value equals
                        # amount_total; for USD/EUR it is the converted amount.
                        # sign: out_invoice positive, in_invoice negative by Odoo
                        # convention — we store absolute value so sums make sense.
                        amt_signed = getattr(inv, 'amount_total_signed', None)
                        amount_total_mxn = round(abs(amt_signed), 2) if amt_signed is not None else None
                        # amount_untaxed_signed doesn't exist, derive from ratio
                        if amount_total_mxn and inv.amount_total:
                            ratio = abs(amt_signed) / inv.amount_total if inv.amount_total else 1.0
                            amount_untaxed_mxn = round(inv.amount_untaxed * ratio, 2)
                            amount_residual_mxn = round(inv.amount_residual * ratio, 2)
                        else:
                            amount_untaxed_mxn = round(inv.amount_untaxed, 2)
                            amount_residual_mxn = round(inv.amount_residual, 2)

                        # Salesperson: from linked sale order or invoice's user
                        salesperson_name = None
                        salesperson_user_id = None
                        try:
                            # Prefer the invoice's own user_id (commercial responsible)
                            if inv.invoice_user_id:
                                salesperson_name = inv.invoice_user_id.name
                                salesperson_user_id = inv.invoice_user_id.id
                            elif inv.user_id:
                                salesperson_name = inv.user_id.name
                                salesperson_user_id = inv.user_id.id
                        except Exception:
                            pass

                        rows.append({
                            'odoo_invoice_id': inv.id,
                            'odoo_partner_id': pid,
                            'name': inv.name,
                            'move_type': inv.move_type,
                            'amount_total': round(inv.amount_total, 2),
                            'amount_residual': round(inv.amount_residual, 2),
                            'amount_tax': round(inv.amount_tax, 2) if hasattr(inv, 'amount_tax') else None,
                            'amount_untaxed': round(inv.amount_untaxed, 2) if hasattr(inv, 'amount_untaxed') else None,
                            'amount_paid': round(inv.amount_total - inv.amount_residual, 2),
                            'amount_total_mxn': amount_total_mxn,
                            'amount_untaxed_mxn': amount_untaxed_mxn,
                            'amount_residual_mxn': amount_residual_mxn,
                            'currency': inv.currency_id.name if inv.currency_id else 'MXN',
                            'invoice_date': inv.invoice_date.strftime('%Y-%m-%d') if inv.invoice_date else None,
                            'due_date': inv.invoice_date_due.strftime('%Y-%m-%d') if inv.invoice_date_due else None,
                            'state': inv.state,
                            'payment_state': inv.payment_state,
                            'days_overdue': days_overdue,
                            'days_to_pay': days_to_pay,
                            'payment_date': pay_date_str,
                            'payment_term': pay_term,
                            'cfdi_uuid': cfdi_uuid,
                            'cfdi_sat_state': cfdi_sat,
                            'salesperson_name': salesperson_name,
                            'salesperson_user_id': salesperson_user_id,
                            'ref': inv.ref or '',
                            'write_date': inv.write_date.strftime('%Y-%m-%dT%H:%M:%S') if inv.write_date else None,
                            'odoo_company_id': inv.company_id.id if inv.company_id else None,
                            # SP5 §14.3 (2026-04-21): reversed_entry_id for canonical_credit_notes linkage
                            'reversed_entry_id': inv.reversed_entry_id.id if inv.reversed_entry_id else None,
                        })

                    # Deduplicate within chunk por odoo_invoice_id (el PK natural
                    # de Odoo, único cross-company). Antes usábamos (partner_id,
                    # name) pero con multi-company colisionaba: companies 2/3/4
                    # pueden facturar al mismo partner con el mismo nombre de
                    # factura → 409 Conflict en el upsert → 99% de invoices no
                    # company-1 perdidos. 2026-04-20.
                    seen = {}
                    for row in rows:
                        seen[row['odoo_invoice_id']] = row
                    rows = list(seen.values())

                    # SP5.5: liberamos recordset y commiteamos ANTES del upsert.
                    # El ORM cache ya no se necesita (rows son dicts planos) y
                    # mantener el cursor PG "idle-in-transaction" durante los
                    # 5+ POSTs HTTP secuenciales dispara idle_in_transaction_
                    # session_timeout → "connection already closed".
                    invoices.invalidate_recordset()
                    self.env.cr.commit()

                    ok_batch, failed_batch = client.upsert_with_details(
                        'odoo_invoices', rows, on_conflict='odoo_invoice_id', batch_size=200
                    )
                    # SP9 T6 (2026-04-23): ping la conexion PG post-upsert.
                    # Despues de 200-500s de POSTs HTTP secuenciales la sesion
                    # queda idle -> Odoo.sh la cierra -> el siguiente chunk
                    # explota con 'connection already closed' y cascadea a
                    # _push_invoice_lines/sale_orders/etc. Un SELECT 1 basta
                    # para que la conexion se re-valide antes de continuar.
                    try:
                        self.env.cr.execute('SELECT 1')
                    except Exception as _ping_exc:
                        _logger.warning(
                            'cursor ping failed after invoices chunk %d/%d: %s',
                            chunk_idx, num_chunks, _ping_exc,
                        )
                    ok += ok_batch
                    total_attempted += len(rows)
                    all_failed.extend(failed_batch)

                    # Best-effort telemetry. Si el RPC mismo devuelve 5xx (p.e.
                    # cuando Supabase está degradado) NO queremos que arrastre
                    # el chunk entero — loggea y continúa.
                    try:
                        core.report_batch(
                            run_id,
                            attempted=len(rows),
                            succeeded=ok_batch,
                            failed=len(failed_batch),
                        )
                    except Exception as exc:
                        _logger.warning(
                            'report_batch RPC failed for invoices chunk %d/%d: %s',
                            chunk_idx, num_chunks, exc,
                        )

                    # Cap per-chunk individual failure reports. Un sub-batch
                    # de 200 con 502 generaba 200 RPCs secuenciales a
                    # ingestion_report_failure, amplificando la indisponibilidad
                    # a varios minutos. Reportamos hasta REPORT_FAILURE_CAP y
                    # un summary para el resto.
                    fail_reports_sent = 0
                    for row, err in failed_batch[:REPORT_FAILURE_CAP]:
                        try:
                            core.report_failure(
                                run_id=run_id,
                                entity_id=str(row.get('name') or row.get('odoo_partner_id') or ''),
                                error_code=err['code'],
                                error_detail=err['detail'],
                                payload=row,
                            )
                            fail_reports_sent += 1
                        except Exception as exc:
                            _logger.warning(
                                'report_failure RPC failed for invoice chunk %d/%d: %s',
                                chunk_idx, num_chunks, exc,
                            )
                            break  # Supabase degradado — aborta fan-out
                    if len(failed_batch) > fail_reports_sent:
                        _logger.warning(
                            '_push_invoices chunk %d/%d: %d rows failed, '
                            'only %d individual reports sent (cap %d).',
                            chunk_idx, num_chunks, len(failed_batch),
                            fail_reports_sent, REPORT_FAILURE_CAP,
                        )

                    if rows:
                        batch_watermark = max(
                            (r.get('write_date') for r in rows if r.get('write_date')),
                            default=None,
                        )
                        if batch_watermark and (final_watermark is None or batch_watermark > final_watermark):
                            final_watermark = batch_watermark

                    elapsed = (datetime.now() - t_chunk_start).total_seconds()
                    try:
                        client.insert('pipeline_logs', [{
                            'level': 'warning' if failed_batch else 'info',
                            'phase': 'odoo_push',
                            'message': (
                                f'[invoices] chunk {chunk_idx}/{num_chunks}: '
                                f'{ok_batch}/{len(rows)} pushed, '
                                f'{len(failed_batch)} failed in {elapsed:.1f}s'
                            ),
                            'details': {
                                'chunk': chunk_idx,
                                'total_chunks': num_chunks,
                                'ok': ok_batch,
                                'failed': len(failed_batch),
                                'elapsed_s': round(elapsed, 1),
                            },
                        }])
                    except Exception:
                        pass
                except Exception as chunk_exc:
                    # SP5.5: un chunk que explota NO debe tumbar el sync
                    # entero — el arrastre a sale_orders/account_balances fue
                    # la raíz del "connection already closed" cascade en
                    # 2026-04-21/22.
                    chunks_failed += 1
                    elapsed = (datetime.now() - t_chunk_start).total_seconds()
                    _logger.exception(
                        'push_invoices chunk %d/%d failed after %.1fs: %s',
                        chunk_idx, num_chunks, elapsed, chunk_exc,
                    )
                    try:
                        self.env.cr.rollback()
                    except Exception:
                        pass
                    try:
                        client.insert('pipeline_logs', [{
                            'level': 'error',
                            'phase': 'odoo_push',
                            'message': (
                                f'[invoices] chunk {chunk_idx}/{num_chunks} '
                                f'FAILED after {elapsed:.1f}s: {str(chunk_exc)[:300]}'
                            ),
                            'details': {
                                'chunk': chunk_idx,
                                'total_chunks': num_chunks,
                                'error': str(chunk_exc)[:500],
                            },
                        }])
                    except Exception:
                        pass
                    # Continúa con el siguiente chunk.
                    continue

            if chunks_failed:
                status = 'partial'
            elif all_failed:
                status = 'partial'
        except Exception as e:
            status = 'failed'
            _logger.exception('push_invoices failed: %s', e)
            try:
                core.complete_run(run_id, status=status, high_watermark=effective_watermark)
            except Exception as exc:
                _logger.warning('complete_run (failure path) RPC failed: %s', exc)
            raise
        try:
            core.complete_run(run_id, status=status, high_watermark=final_watermark)
        except Exception as exc:
            _logger.warning('complete_run RPC failed: %s', exc)
        return ok

    # ── Invoice Lines (ALL history) ──────────────────────────────────────

    def _compute_invoice_fx_ratio(self, inv) -> float:
        """Resuelve MXN-per-native-unit para una factura.

        Fallback chain (H12):
          1. amount_total_signed / amount_total (sanity-checked)
          2. currency_id._convert() a company currency en invoice_date
          3. res.currency.rate.rate en la fecha de la factura
          4. 1.0 (MXN-native o no resoluble)

        Extraído como helper para ser reutilizable + testeable.
        """
        inv_currency = inv.currency_id.name if inv.currency_id else 'MXN'
        mxn_ratio = 1.0

        amt_signed = getattr(inv, 'amount_total_signed', None)
        if amt_signed is not None and inv.amount_total:
            ratio_from_signed = abs(amt_signed) / inv.amount_total
            # Sanity: en non-MXN un ratio ≈1.0 es FX no aplicada, forzar fallback.
            if ratio_from_signed > 0 and not (
                inv_currency != 'MXN' and abs(ratio_from_signed - 1.0) < 0.001
            ):
                mxn_ratio = ratio_from_signed

        if mxn_ratio == 1.0 and inv_currency != 'MXN' and inv.currency_id:
            try:
                company = inv.company_id or self.env.company
                target = company.currency_id
                on_date = inv.invoice_date or inv.date or datetime.now().date()
                converted = inv.currency_id._convert(
                    1.0, target, company, on_date, round=False,
                )
                if converted and converted > 0:
                    mxn_ratio = float(converted)
            except Exception as exc:
                _logger.debug('FX _convert failed for %s: %s', inv.name, exc)

        if mxn_ratio == 1.0 and inv_currency != 'MXN':
            try:
                Rate = self.env['res.currency.rate'].sudo()
                on_date = inv.invoice_date or datetime.now().date()
                rate_row = Rate.search(
                    [
                        ('currency_id', '=', inv.currency_id.id),
                        ('name', '<=', on_date.strftime('%Y-%m-%d')),
                    ],
                    order='name desc',
                    limit=1,
                )
                if rate_row and rate_row.rate:
                    mxn_ratio = 1.0 / float(rate_row.rate)
            except Exception as exc:
                _logger.debug(
                    'FX res.currency.rate fallback failed for %s: %s',
                    inv.name, exc,
                )

        return mxn_ratio

    def _push_invoice_lines(self, client: SupabaseClient, last_sync=None) -> int:
        """Push account.move.line → odoo_invoice_lines table.

        H11 refactor (2026-04-17): antes iteraba invoice por invoice y accedía
        `inv.invoice_line_ids` lazy, causando N+1 queries ORM. Para 14,520
        facturas esto rebasaba el timeout del cron → 97% de invoices sin
        lines pushed. Ahora:
          1. Search invoices + precompute FX ratios (UNA vez)
          2. Bulk search de TODAS las lines con move_id IN (...)
          3. Single pass sobre lines usando inv_map precomputado
        """
        Move = self.env['account.move'].sudo()
        cids = self._get_company_ids()

        domain = [
            ('company_id', 'in', cids),
            ('move_type', 'in', [
                'out_invoice', 'out_refund', 'in_invoice', 'in_refund',
            ]),
            ('state', '=', 'posted'),
        ]
        if last_sync:
            domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
        invoice_ids = Move.search(domain).ids

        if not invoice_ids:
            return 0

        # Batching — antes cargábamos las 14k+ invoices + account.move.line
        # bulk-fetched todas juntas (10k+ lines) en memoria, OOM-killing al
        # worker de Odoo.sh durante full-sync (2026-04-20).
        # Ahora procesamos 2000 invoices a la vez, con sus lines, upsert, y
        # libera ORM caches.
        BATCH = 2000
        Line = self.env['account.move.line'].sudo()
        total_ok = 0
        total_rows = 0
        for chunk_start in range(0, len(invoice_ids), BATCH):
            chunk_ids = invoice_ids[chunk_start:chunk_start + BATCH]
            invoices = Move.browse(chunk_ids)

            # Precompute metadata + FX ratios para el chunk.
            inv_map: dict[int, dict] = {}
            ratios: dict[int, float] = {}
            for inv in invoices:
                pid = _commercial_partner_id(inv.partner_id)
                if not pid:
                    continue
                inv_map[inv.id] = {
                    'pid': pid,
                    'name': inv.name,
                    'move_type': inv.move_type,
                    'invoice_date': (
                        inv.invoice_date.strftime('%Y-%m-%d') if inv.invoice_date else None
                    ),
                    'currency': inv.currency_id.name if inv.currency_id else 'MXN',
                    'company_id': inv.company_id.id if inv.company_id else None,
                }
                ratios[inv.id] = self._compute_invoice_fx_ratio(inv)

            if not inv_map:
                continue

            # Bulk fetch de lines sólo para el chunk actual.
            lines = Line.search([
                ('move_id', 'in', list(inv_map.keys())),
                ('display_type', 'not in',
                 ['line_section', 'line_note', 'payment_term', 'tax', 'rounding']),
            ])

            rows = []
            for line in lines:
                mv_id = line.move_id.id
                ctx = inv_map.get(mv_id)
                if not ctx:
                    continue
                ratio = ratios.get(mv_id, 1.0)
                line_uom_obj = getattr(line, 'product_uom_id', None)
                rows.append({
                    'odoo_line_id': line.id,
                    'odoo_move_id': mv_id,
                    'odoo_partner_id': ctx['pid'],
                    'move_name': ctx['name'],
                    'move_type': ctx['move_type'],
                    'invoice_date': ctx['invoice_date'],
                    'odoo_product_id': line.product_id.id if line.product_id else None,
                    'product_name': (
                        line.product_id.name if line.product_id else (line.name or '')[:200]
                    ),
                    'product_ref': ((line.product_id.default_code or '').strip() or None) if line.product_id else None,
                    # price_unit y quantity con 6 decimales — Odoo internamente
                    # usa Product Price precision (6 por default), que al
                    # redondear a 2 en items con cantidad enorme (millones) causa
                    # drift de miles $$ vs price_subtotal (el "oficial" de Odoo).
                    # Fase 2 fix — audit invariant invoice_lines.price_recompute
                    # detectó 31,883 líneas con drift por este redondeo.
                    'quantity': round(line.quantity, 6),
                    'price_unit': round(line.price_unit, 6),
                    'discount': round(line.discount, 2),
                    'price_subtotal': round(line.price_subtotal, 2),
                    'price_total': round(line.price_total, 2),
                    'currency': ctx['currency'],
                    'price_subtotal_mxn': round(line.price_subtotal * ratio, 2),
                    'price_total_mxn': round(line.price_total * ratio, 2),
                    'line_uom': line_uom_obj.name if line_uom_obj else None,
                    'line_uom_id': line_uom_obj.id if line_uom_obj else None,
                    'odoo_company_id': ctx['company_id'],
                })

            total_rows += len(rows)
            total_ok += client.upsert('odoo_invoice_lines', rows,
                                       on_conflict='odoo_line_id', batch_size=200)

            # Liberar caches + commit entre chunks.
            invoices.invalidate_recordset()
            lines.invalidate_recordset()
            self.env.cr.commit()

        _logger.info(
            '_push_invoice_lines: upserted %d rows from %d invoices (batched)',
            total_rows, len(invoice_ids),
        )
        return total_ok

    # ── Deliveries (pending + last 90 days) ──────────────────────────────

    def _push_account_payments(self, client: SupabaseClient, last_sync=None) -> int:
        """Push account.payment → odoo_account_payments table.

        Real payment records from Odoo (not proxy from invoices).
        Includes payment method, journal, bank reconciliation status.
        """
        try:
            Payment = self.env['account.payment'].sudo()
        except KeyError:
            _logger.info('account.payment not available, skipping')
            return 0

        try:
            cids = self._get_company_ids()
            domain = [('company_id', 'in', cids)]
            # Skip incremental filter on first run (table may be empty)
            if last_sync:
                existing = client.fetch('odoo_account_payments', {'limit': '1', 'select': 'id'})
                if existing:
                    domain.append(('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S')))
            payments = Payment.search(domain, limit=5000)
            _logger.info('account_payments: found %d records', len(payments))

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

                    rows.append({
                        'odoo_payment_id': p.id,
                        'odoo_partner_id': pid,
                        'name': p.name or '',
                        'payment_type': p.payment_type or '',
                        'partner_type': p.partner_type or '',
                        'amount': round(p.amount or 0, 2),
                        'amount_signed': round(p.amount_company_currency_signed, 2) if hasattr(p, 'amount_company_currency_signed') and p.amount_company_currency_signed else None,
                        'currency': p.currency_id.name if p.currency_id else 'MXN',
                        'date': p.date.strftime('%Y-%m-%d') if p.date else None,
                        'ref': (p.ref or '') if hasattr(p, 'ref') else '',
                        'journal_name': journal_name,
                        'payment_method': payment_method,
                        'state': p.state or '',
                        'is_matched': bool(getattr(p, 'is_matched', False)),
                        'is_reconciled': bool(getattr(p, 'is_reconciled', False)),
                        'reconciled_invoices_count': int(getattr(p, 'reconciled_invoices_count', 0) or 0),
                        'odoo_company_id': p.company_id.id if p.company_id else None,
                    })
                except Exception as exc:
                    _logger.warning('account_payment %s: %s', p.id, exc)

            _logger.info('account_payments: pushing %d rows', len(rows))
            return client.upsert('odoo_account_payments', rows,
                                  on_conflict='odoo_payment_id', batch_size=200)
        except Exception as exc:
            _logger.error('_push_account_payments failed: %s', exc)
            return 0

    def _push_payment_invoice_links(self, client: SupabaseClient, last_sync=None) -> int:
        """Push account.payment.reconciled_invoice_ids → odoo_payment_invoice_links.

        Expone la relación payment↔invoice que Odoo mantiene en el m2m
        `reconciled_invoice_ids`. Habilita matching Syntage↔Odoo via CFDI UUID:
            Syntage doctos_relacionados[].uuid_docto
              → odoo_invoices.cfdi_uuid
              → odoo_payment_invoice_links.odoo_invoice_id
              → odoo_payment_invoice_links.odoo_payment_id
              → odoo_account_payments
        """
        try:
            Payment = self.env['account.payment'].sudo()
        except KeyError:
            _logger.info('account.payment not available, skipping payment_invoice_links')
            return 0

        try:
            cids = self._get_company_ids()
            domain = [
                ('company_id', 'in', cids),
                ('reconciled_invoice_ids', '!=', False),
            ]
            # Incremental por write_date del payment (si una reconciliación cambia,
            # Odoo actualiza el payment). Skip si la tabla está vacía.
            if last_sync:
                existing = client.fetch(
                    'odoo_payment_invoice_links', {'limit': '1', 'select': 'id'}
                )
                if existing:
                    domain.append(
                        ('write_date', '>=', last_sync.strftime('%Y-%m-%d %H:%M:%S'))
                    )

            payments = Payment.search(domain, limit=5000)
            _logger.info('payment_invoice_links: scanning %d payments', len(payments))

            rows = []
            touched_payment_ids = []
            for p in payments:
                try:
                    invoices = p.reconciled_invoice_ids
                    if not invoices:
                        continue
                    touched_payment_ids.append(p.id)
                    comp_id = p.company_id.id if p.company_id else None
                    for inv in invoices:
                        rows.append({
                            'odoo_payment_id': p.id,
                            'odoo_invoice_id': inv.id,
                            'odoo_company_id': comp_id,
                        })
                except Exception as exc:
                    _logger.warning('payment_invoice_links %s: %s', p.id, exc)

            # Full replace por payment_id tocado: los m2m pueden perder filas
            # (reconciliaciones deshechas). Borrar + re-insertar garantiza
            # consistencia sin correr full scan.
            if touched_payment_ids:
                # Supabase REST DELETE con filter IN. Batching para URL length.
                batch = 500
                for i in range(0, len(touched_payment_ids), batch):
                    chunk = touched_payment_ids[i:i + batch]
                    try:
                        client.delete(
                            'odoo_payment_invoice_links',
                            {'odoo_payment_id': f'in.({",".join(str(x) for x in chunk)})'},
                        )
                    except Exception as exc:
                        _logger.warning('payment_invoice_links delete chunk failed: %s', exc)

            _logger.info('payment_invoice_links: pushing %d link rows', len(rows))
            if not rows:
                return 0
            return client.upsert(
                'odoo_payment_invoice_links', rows,
                on_conflict='odoo_payment_id,odoo_invoice_id',
                batch_size=500,
            )
        except Exception as exc:
            _logger.error('_push_payment_invoice_links failed: %s', exc)
            return 0

    # ── Chart of Accounts ───────────────────────────────────────────────

    def _push_chart_of_accounts(self, client: SupabaseClient, last_sync=None) -> int:
        """Push account.account → odoo_chart_of_accounts table.

        The chart of accounts is the foundation for P&L and Balance Sheet.
        Always full sync (small table, ~100 rows).
        """
        try:
            Account = self.env['account.account'].sudo()
        except KeyError:
            _logger.info('account.account not available, skipping')
            return 0

        try:
            # Odoo 17+: account.account.code es computed per-company via
            # code_store_ids. Para Quimibond MX con multi-company (12+ orgs
            # con catálogos SAT propios), iteramos por compañía y resolvemos
            # el code en el contexto de cada una. Antes se usaba solo
            # self._get_company_id() (company 1), dejando 1,044 cuentas de
            # las otras 11 companies con code='' (audit expuso 2026-04-20).
            Account = Account.with_context(active_test=False)
            Company = self.env['res.company'].sudo()
            companies = Company.search([])

            rows = []
            seen_acc_ids = set()
            for company in companies:
                company_cid = company.id
                # Try the direct company_id filter first; if empty (Odoo 17+
                # shared chart mode) or if it raises, fall back to all.
                try:
                    accounts = Account.search([('company_id', '=', company_cid)])
                    if not accounts:
                        # Shared chart: todas las cuentas accesibles por esta
                        # company via company_ids many2many (o all).
                        accounts = Account.search([])
                except Exception:
                    accounts = Account.search([])

                accounts_ctx = accounts.with_company(company_cid)
                for acc in accounts_ctx:
                    try:
                        code = acc.code or ''
                        # Fallback adicional: leer directamente code_store_ids
                        if not code and hasattr(acc, 'code_store_ids'):
                            mapping = acc.code_store_ids.filtered(
                                lambda m: m.company_id.id == company_cid
                            )
                            if mapping:
                                code = mapping[0].code or ''

                        # Skip si la cuenta no tiene code en este contexto
                        # (significa que no "pertenece" a esta company).
                        if not code and acc.id in seen_acc_ids:
                            continue

                        acc_type = getattr(acc, 'account_type', None) or ''
                        rows.append({
                            'odoo_account_id': acc.id,
                            'code': code,
                            'name': acc.name or '',
                            'account_type': acc_type,
                            'reconcile': bool(acc.reconcile) if hasattr(acc, 'reconcile') else False,
                            'deprecated': bool(getattr(acc, 'deprecated', False)),
                            'active': bool(getattr(acc, 'active', True)),
                            'odoo_company_id': company_cid,
                        })
                        seen_acc_ids.add(acc.id)
                    except Exception as exc:
                        _logger.warning('chart_of_accounts %s: %s', acc.id, exc)

            _logger.info('chart_of_accounts: pushing %d rows across %d companies',
                         len(rows), len(companies))
            return client.upsert('odoo_chart_of_accounts', rows,
                                  on_conflict='odoo_account_id', batch_size=200)
        except Exception as exc:
            _logger.error('_push_chart_of_accounts failed: %s', exc)
            return 0

    # ── Account Balances (monthly, for P&L) ─────────────────────────────

    def _push_account_balances(self, client: SupabaseClient, last_sync=None) -> int:
        """Push monthly account balances → odoo_account_balances table.

        Aggregates account.move.line by account + month for P&L and
        Balance Sheet reporting. Only posted entries.
        """
        try:
            Line = self.env['account.move.line'].sudo()
        except KeyError:
            _logger.info('account.move.line not available, skipping')
            return 0

        # Use read_group for efficient aggregation in Odoo
        # Filter to operating company to avoid mixing P&L from 8 companies
        cids = self._get_company_ids()
        try:
            groups = Line.read_group(
                domain=[
                    ('parent_state', '=', 'posted'),
                    ('display_type', 'not in', ['line_section', 'line_note']),
                    ('company_id', 'in', cids),
                ],
                fields=['account_id', 'debit:sum', 'credit:sum', 'balance:sum'],
                groupby=['account_id', 'date:month', 'company_id'],
                lazy=False,
            )
        except Exception as exc:
            _logger.warning('read_group account balances failed: %s', exc)
            return 0

        # Build account cache for names/codes. Mismo patrón multi-company
        # que _push_chart_of_accounts: iteramos res.company para que el
        # compute de `code` (Odoo 17+ code_store_ids) se resuelva
        # correctamente. Antes un solo with_company(cid) dejaba el cache
        # vacío (si company_id filter raised) o con codes incompletos,
        # causando account_code='' en 100% de odoo_account_balances.
        account_cache = {}
        try:
            Account = self.env['account.account'].sudo()
            Company = self.env['res.company'].sudo()
            for company in Company.search([]):
                company_cid = company.id
                try:
                    accounts = Account.search([('company_id', '=', company_cid)])
                    if not accounts:
                        accounts = Account.search([])
                except Exception:
                    accounts = Account.search([])
                for acc in accounts.with_company(company_cid):
                    code = acc.code or ''
                    if not code and hasattr(acc, 'code_store_ids'):
                        mapping = acc.code_store_ids.filtered(
                            lambda m: m.company_id.id == company_cid
                        )
                        if mapping:
                            code = mapping[0].code or ''
                    # Solo registrar si tenemos code (cuenta "pertenece" a
                    # esta company) o si no hay entry previa para la acc.
                    if code or acc.id not in account_cache:
                        account_cache[acc.id] = {
                            'code': code,
                            'name': acc.name or '',
                            'account_type': getattr(acc, 'account_type', '') or '',
                        }
        except Exception as exc:
            _logger.warning('account_balances cache build failed: %s', exc)

        # Month name → number mapping for Spanish locale (Odoo read_group
        # returns localized month names like "enero 2026", "febrero 2026").
        _MONTH_ES = {
            'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
            'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
            'septiembre': '09', 'octubre': '10', 'noviembre': '11',
            'diciembre': '12',
        }
        # English fallback
        _MONTH_EN = {
            'january': '01', 'february': '02', 'march': '03', 'april': '04',
            'may': '05', 'june': '06', 'july': '07', 'august': '08',
            'september': '09', 'october': '10', 'november': '11',
            'december': '12',
        }

        def _normalize_period(raw: str) -> str:
            """Convert 'abril 2026' or 'April 2026' → '2026-04'."""
            if not raw:
                return raw
            parts = raw.strip().lower().split()
            if len(parts) == 2:
                month_name, year = parts
                month_num = _MONTH_ES.get(month_name) or _MONTH_EN.get(month_name)
                if month_num and year.isdigit():
                    return f'{year}-{month_num}'
            return raw  # fallback: return as-is

        rows = []
        for g in groups:
            acc_id = g['account_id'][0] if g['account_id'] else None
            if not acc_id:
                continue

            acc_info = account_cache.get(acc_id, {})
            # date:month returns 'abril 2026' format — normalize to '2026-04'
            month_str = _normalize_period(g.get('date:month', ''))

            # Nota: groupby incluye 'company_id' para que read_group devuelva
            # UNA fila por (account, month, company). La tabla
            # odoo_account_balances no tiene columna odoo_company_id todavía
            # (TODO: ALTER TABLE), por eso no la embebemos en el row. El
            # company_id lo resuelve la view v_audit_account_balances_buckets
            # via JOIN con odoo_chart_of_accounts.odoo_company_id.
            rows.append({
                'odoo_account_id': acc_id,
                'account_code': acc_info.get('code', ''),
                'account_name': acc_info.get('name', ''),
                'account_type': acc_info.get('account_type', ''),
                'period': month_str,
                'debit': round(g.get('debit', 0) or 0, 2),
                'credit': round(g.get('credit', 0) or 0, 2),
                'balance': round(g.get('balance', 0) or 0, 2),
            })

        # ── SP5 §14.2 (2026-04-21): synthetic equity_unaffected rows ─────────
        # equity_unaffected (utilidad del ejercicio / current year earnings) is
        # a computed balance in Odoo — no actual account.move.line rows carry
        # that account_type, so read_group above returns zero rows for it.
        # Gold layer gold_balance_sheet needs it to reconcile
        # (unbalanced_amount = assets - liabilities - equity, including
        # equity_unaffected = net income = sum(income) - sum(expense) per period).
        # We synthesise one row per (period) as the net income balance and tag
        # it with the equity_unaffected account from chart_of_accounts (if any),
        # or use a sentinel odoo_account_id=0 if not found.
        try:
            # Find the equity_unaffected account id(s) in our chart
            eq_acc = self.env['account.account'].sudo().search(
                [('account_type', '=', 'equity_unaffected')], limit=1
            )
            eq_acc_id = eq_acc.id if eq_acc else 0
            eq_acc_code = eq_acc.code or 'equity_unaffected' if eq_acc else 'equity_unaffected'
            eq_acc_name = eq_acc.name or 'Current Year Earnings' if eq_acc else 'Current Year Earnings'

            # Aggregate net income per period from existing rows
            _INCOME_TYPES = {'income', 'income_other'}
            _EXPENSE_TYPES = {'expense', 'expense_depreciation', 'expense_direct_cost'}
            net_by_period: dict = {}
            for r in rows:
                at = r.get('account_type', '')
                period = r.get('period', '')
                if not period:
                    continue
                if at in _INCOME_TYPES:
                    # Income: credit-normal; balance is negative of net income
                    # read_group balance = debit - credit (negative for income)
                    net_by_period.setdefault(period, 0.0)
                    net_by_period[period] -= r.get('balance', 0.0)
                elif at in _EXPENSE_TYPES:
                    # Expense: debit-normal; balance positive = cost
                    net_by_period.setdefault(period, 0.0)
                    net_by_period[period] -= r.get('balance', 0.0)

            for period, net_income in net_by_period.items():
                if net_income == 0.0:
                    continue
                rows.append({
                    'odoo_account_id': eq_acc_id,
                    'account_code': eq_acc_code,
                    'account_name': eq_acc_name,
                    'account_type': 'equity_unaffected',
                    'period': period,
                    'debit': round(max(net_income, 0), 2),
                    'credit': round(max(-net_income, 0), 2),
                    'balance': round(-net_income, 2),
                })
        except Exception as exc:
            _logger.warning('_push_account_balances equity_unaffected synthesis failed: %s', exc)

        # Full refresh (balances change as entries are posted)
        if rows:
            client.delete_all('odoo_account_balances')
            return client.insert('odoo_account_balances', rows, batch_size=500)
        return 0

    # ── Retry Failures ─────────────────────────────────────────────────

    @api.model
    def _push_bank_balances(self, client: SupabaseClient, last_sync=None) -> int:
        """Push bank journal balances → odoo_bank_balances table.

        Shows current cash position from bank-type journals.
        """
        try:
            Journal = self.env['account.journal'].sudo()
        except KeyError:
            _logger.info('account.journal not available, skipping')
            return 0

        # Only sync journals from the operating company
        cids = self._get_company_ids()
        journals = Journal.search([
            ('type', 'in', ['bank', 'cash']),
            ('company_id', 'in', cids),
        ])

        rows = []
        for j in journals:
            # Compute both company-currency balance (MXN) and foreign-currency
            # native balance from the same read_group.
            #
            # account.move.line.balance        = debit - credit (company ccy, MXN)
            # account.move.line.amount_currency = native foreign-currency amount
            #
            # For USD/EUR journals we want BOTH:
            #   - current_balance_mxn -> MXN ledger value (used for aggregations)
            #   - current_balance     -> native foreign value (for display)
            balance_mxn = 0.0
            balance_native = 0.0
            try:
                if hasattr(j, 'default_account_id') and j.default_account_id:
                    Line = self.env['account.move.line'].sudo()
                    result = Line.read_group(
                        domain=[
                            ('account_id', '=', j.default_account_id.id),
                            ('parent_state', '=', 'posted'),
                        ],
                        fields=['balance:sum', 'amount_currency:sum'],
                        groupby=[],
                    )
                    if result:
                        balance_mxn = result[0].get('balance', 0) or 0
                        balance_native = result[0].get('amount_currency', 0) or 0
            except Exception as exc:
                _logger.warning('Bank balance for %s: %s', j.name, exc)

            bank_account = None
            try:
                if hasattr(j, 'bank_account_id') and j.bank_account_id:
                    bank_account = j.bank_account_id.acc_number
                elif hasattr(j, 'bank_acc_number'):
                    bank_account = j.bank_acc_number
            except Exception:
                pass

            company_currency = (
                j.company_id.currency_id.name
                if j.company_id and j.company_id.currency_id else 'MXN'
            )
            journal_currency = j.currency_id.name if j.currency_id else company_currency

            # If journal operates in a foreign currency AND has amount_currency
            # data, current_balance = native foreign value. Otherwise,
            # current_balance = MXN ledger balance (same as _mxn).
            if journal_currency != company_currency and balance_native:
                current_balance = round(balance_native, 2)
            else:
                current_balance = round(balance_mxn, 2)

            # Detect credit card journals. Odoo's account.journal.type only
            # has 'bank' and 'cash' — credit cards are configured as type='bank'
            # with a default_account_id whose account_type is
            # 'liability_credit_card'. Downstream dashboards need to distinguish
            # them (display as "Tarjeta" and classify as cc_debt in cash
            # bucketing), so we override journal_type='credit' when detected.
            effective_type = j.type
            try:
                acc = j.default_account_id
                if acc and getattr(acc, 'account_type', None) == 'liability_credit_card':
                    effective_type = 'credit'
            except Exception:
                pass

            rows.append({
                'odoo_journal_id': j.id,
                'name': j.name,
                'journal_type': effective_type,  # bank / cash / credit
                'currency': journal_currency,
                'bank_account': bank_account,
                'current_balance': current_balance,
                'current_balance_mxn': round(balance_mxn, 2),
                'odoo_company_id': j.company_id.id if j.company_id else None,
                'company_name': j.company_id.name if j.company_id else None,
                'updated_at': datetime.now().isoformat(),
            })

        # Full refresh (small table, balances change)
        if rows:
            client.delete_all('odoo_bank_balances')
            return client.insert('odoo_bank_balances', rows, batch_size=50)
        return 0

    # ── Currency Rates ───────────────────────────────────────────────────

    def _push_currency_rates(self, client: SupabaseClient, last_sync=None) -> int:
        """Push res.currency.rate → odoo_currency_rates table.

        Pushes the latest rate for each active foreign currency so Supabase
        views can convert USD/EUR amounts to MXN using real Odoo rates
        instead of hardcoded values.
        """
        try:
            Rate = self.env['res.currency.rate'].sudo()
        except KeyError:
            _logger.info('res.currency.rate not available, skipping')
            return 0

        cids = self._get_company_ids()
        # Get all currencies that have rates defined
        Currency = self.env['res.currency'].sudo()
        currencies = Currency.search([('active', '=', True)])
        Company = self.env['res.company'].sudo()

        rows = []
        # Iterate per company — cada una puede tener currency base distinta
        # y rate entries propias (company_id IS NULL son rates compartidas).
        for cid in cids:
            company = Company.browse(cid)
            company_currency = company.currency_id.name if company.currency_id else 'MXN'

            for cur in currencies:
                if cur.name == company_currency:
                    continue  # Skip base→base

                # Get the latest rate for this currency in the company
                rates = Rate.search([
                    ('currency_id', '=', cur.id),
                    ('company_id', 'in', [cid, False]),
                ], order='name desc', limit=30)  # last 30 rate entries

                for r in rates:
                    # Odoo stores inverse rate: 1 / (foreign per base)
                    # e.g. if 1 USD = 19.5 MXN, Odoo stores rate = 1/19.5 = 0.05128
                    inverse_rate = r.rate or 0
                    if inverse_rate > 0:
                        mxn_rate = round(1.0 / inverse_rate, 6)
                    else:
                        continue

                    rows.append({
                        'currency': cur.name,
                        'rate': mxn_rate,
                        'inverse_rate': round(inverse_rate, 10),
                        'rate_date': r.name.strftime('%Y-%m-%d') if r.name else None,
                        'odoo_company_id': cid,
                    })

        if rows:
            return client.upsert('odoo_currency_rates', rows,
                                  on_conflict='currency,rate_date,odoo_company_id',
                                  batch_size=200)
        return 0
