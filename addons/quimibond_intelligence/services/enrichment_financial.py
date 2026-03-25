"""
Quimibond Intelligence — Financial Mixin
Payment behavior analysis for OdooEnrichmentService.
"""
import logging
from datetime import datetime, timedelta

_logger = logging.getLogger(__name__)


class FinancialMixin:
    """Payment behavior intelligence."""

    def _analyze_payment_behavior(self, pid, models, today):
        """Analyze payment behavior: agreed terms vs actual payment dates.

        Looks at paid invoices (last 12 months) to calculate:
        - Compliance score (0-100): % of invoices paid on time or early
        - Average days late/early vs due date
        - Trend: comparing recent 6m behavior vs previous 6m
        - Payment term info from the partner
        - Per-invoice detail for the most recent ones

        Returns dict with compliance_score, avg_days_late, trend, details.
        """
        AM = models['account_move']
        date_12m = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        date_6m = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')

        # Get paid invoices with both due date and payment date
        paid_invoices = AM.search([
            ('partner_id', 'child_of', pid),
            ('move_type', '=', 'out_invoice'),
            ('state', '=', 'posted'),
            ('payment_state', 'in', ['paid', 'in_payment']),
            ('invoice_date', '>=', date_12m),
            ('invoice_date_due', '!=', False),
        ], order='invoice_date desc', limit=50)

        if not paid_invoices:
            return {'invoices_analyzed': 0}

        # Analyze each invoice: days between due date and actual payment
        invoice_details = []
        recent_delays = []   # last 6 months
        previous_delays = []  # 6-12 months ago

        for inv in paid_invoices:
            due_date = inv.invoice_date_due
            # Find actual payment date from reconciled payments
            payment_date = self._get_invoice_payment_date(inv)
            if not payment_date or not due_date:
                continue

            days_diff = (payment_date - due_date).days  # positive = late
            invoice_date_str = (
                inv.invoice_date.strftime('%Y-%m-%d')
                if inv.invoice_date else ''
            )

            detail = {
                'invoice': inv.name,
                'amount': inv.amount_total,
                'invoice_date': invoice_date_str,
                'due_date': due_date.strftime('%Y-%m-%d'),
                'payment_date': payment_date.strftime('%Y-%m-%d'),
                'days_diff': days_diff,
                'status': (
                    'early' if days_diff < 0
                    else 'on_time' if days_diff <= 3
                    else 'late'
                ),
            }
            invoice_details.append(detail)

            if invoice_date_str >= date_6m:
                recent_delays.append(days_diff)
            else:
                previous_delays.append(days_diff)

        if not invoice_details:
            return {'invoices_analyzed': 0}

        # Compliance score: % paid on time (within 3 day grace period)
        on_time_count = sum(
            1 for d in invoice_details if d['days_diff'] <= 3
        )
        compliance_score = round(on_time_count / len(invoice_details) * 100)

        # Average days late (negative = early)
        all_delays = [d['days_diff'] for d in invoice_details]
        avg_days_late = round(sum(all_delays) / len(all_delays), 1)

        # Trend: compare recent vs previous average delay
        trend = 'stable'
        recent_avg = None
        previous_avg = None
        if recent_delays and previous_delays:
            recent_avg = round(
                sum(recent_delays) / len(recent_delays), 1)
            previous_avg = round(
                sum(previous_delays) / len(previous_delays), 1)
            diff = recent_avg - previous_avg
            if diff <= -3:
                trend = 'improving'
            elif diff >= 3:
                trend = 'worsening'

        # Payment term from the partner
        Partner = models['partner']
        partner = Partner.browse(pid)
        payment_term_name = ''
        payment_term_days = None
        if hasattr(partner, 'property_payment_term_id') and \
                partner.property_payment_term_id:
            pt = partner.property_payment_term_id
            payment_term_name = pt.name or ''
            # Estimate days from the term lines
            try:
                if hasattr(pt, 'line_ids') and pt.line_ids:
                    max_days = max(
                        line.nb_days for line in pt.line_ids
                        if hasattr(line, 'nb_days')
                    )
                    payment_term_days = max_days
            except (ValueError, AttributeError):
                pass

        # Worst offenders (most late invoices)
        worst = sorted(
            invoice_details, key=lambda x: x['days_diff'], reverse=True,
        )[:3]

        return {
            'invoices_analyzed': len(invoice_details),
            'compliance_score': compliance_score,
            'avg_days_late': avg_days_late,
            'median_days_late': sorted(all_delays)[len(all_delays) // 2],
            'max_days_late': max(all_delays),
            'min_days_late': min(all_delays),
            'on_time_count': on_time_count,
            'late_count': len(invoice_details) - on_time_count,
            'trend': trend,
            'recent_6m_avg': recent_avg,
            'previous_6m_avg': previous_avg,
            'payment_term': payment_term_name,
            'payment_term_days': payment_term_days,
            'recent_invoices': invoice_details[:10],
            'worst_offenders': worst,
        }

    @staticmethod
    def _get_invoice_payment_date(invoice):
        """Get the actual payment date for a paid invoice.

        Tries reconciled payment first, falls back to write_date.
        """
        try:
            # Try to find reconciled payments via the invoice's
            # reconciled move lines
            for partial in (invoice._get_reconciled_payments() or []):
                if hasattr(partial, 'date') and partial.date:
                    return partial.date
            # Fallback: if payment_state is paid, use the last write_date
            # as an approximation
            if invoice.payment_state in ('paid', 'in_payment'):
                # Use invoice_date_due + a small buffer as conservative
                # estimate, or write_date
                if hasattr(invoice, 'write_date') and invoice.write_date:
                    return invoice.write_date.date()
        except Exception:
            pass
        # Final fallback
        if hasattr(invoice, 'write_date') and invoice.write_date:
            return invoice.write_date.date()
        return None
