"""
Quimibond Intelligence — Enrichment Helpers
Pure functions and constants used by enrichment mixins.
"""
import logging

_logger = logging.getLogger(__name__)


def _safe_sum_aggregate(model, domain, field_name):
    """Read an aggregate SUM compatible with Odoo 17+ and 19.

    Odoo 17+ deprecated read_group in favor of _read_group.
    Odoo 19 may have removed read_group entirely.
    Returns the numeric total or 0.
    """
    # Try new API first (_read_group, Odoo 17+)
    if hasattr(model, '_read_group'):
        try:
            result = model._read_group(
                domain, aggregates=[f'{field_name}:sum'],
            )
            # _read_group returns [(val,)] when no groupby
            if result and len(result) > 0:
                row = result[0]
                if isinstance(row, (list, tuple)):
                    return row[0] or 0
                # Some versions return dict
                if isinstance(row, dict):
                    return row.get(f'{field_name}', 0) or 0
            return 0
        except Exception as exc:
            _logger.debug('_read_group fallback: %s', exc)

    # Fallback: old API (Odoo 16 and earlier)
    try:
        rows = model.read_group(domain, [field_name], [])
        return rows[0][field_name] if rows else 0
    except Exception as exc:
        _logger.warning('read_group failed for %s: %s', field_name, exc)

    # Final fallback: brute-force search + sum
    try:
        records = model.search(domain)
        return sum(getattr(r, field_name, 0) or 0 for r in records)
    except Exception:
        return 0

# ── Dominios genéricos (Gmail, Outlook, etc.) ────────────────────────────────

GENERIC_DOMAINS = frozenset({
    'gmail.com', 'googlemail.com', 'outlook.com', 'hotmail.com',
    'yahoo.com', 'yahoo.com.mx', 'live.com', 'live.com.mx',
    'icloud.com', 'aol.com', 'protonmail.com', 'proton.me',
    'msn.com', 'mail.com', 'zoho.com', 'yandex.com',
    'google.com', 'vercel.com', 'github.com',
})

# Prefixes that indicate automated/non-human senders
AUTOMATED_PREFIXES = (
    'noreply', 'no-reply', 'no-responder', 'donotreply',
    'notifications', 'notification', 'alerts', 'alert',
    'calendar-notification', 'mailer-daemon', 'postmaster',
    'bounce', 'system', 'daemon', 'auto', 'robot',
)

# Domains of SaaS/services that are never real business contacts
SERVICE_DOMAINS = frozenset({
    # Dev/infra tools
    'github.com', 'vercel.com', 'supabase.com', 'n8n.io',
    'ngrok.com', 'anthropic.com', 'mail.anthropic.com',
    'paddle.com', 'stripe.com', 'mongodb.com', 'voyage.mongodb.com',
    'incident.io', 'status.incident.io', 'transactional.n8n.io',
    'info.n8n.io',
    # Google services
    'accounts.google.com', 'docs.google.com',
    # Job boards
    'indeed.com', 'indeedemail.com', 'acciontrabajo.com',
    'mex.acciontrabajo.com',
    # Social/newsletters
    'quora.com', 'ccsend.com', 'shared1.ccsend.com',
    'constantcontact.com', 'smergers.net',
    # Banking notifications (not business contacts)
    'bbva.mx', 'mifel.com.mx',
    # Logistics tracking (automated)
    'one-line.com', 'customer.cmacgm-group.com',
    # Consumer services
    'uber.com', 'amazon.com', 'amazon.com.mx', 'eg.expedia.com',
    'expedia.com',
    # Government automated
    'sat.gob.mx', 'buengobierno.gob.mx',
    # Odoo notifications
    'mail.odoo.com',
    # Read.ai, Spaceti, etc.
    'e.read.ai', 'spaceti.cloud',
})


def is_automated_sender(email: str) -> bool:
    """Return True if email is from an automated/non-human sender."""
    if not email:
        return False
    email = email.lower().strip()
    local = email.split('@')[0] if '@' in email else ''
    domain = email.split('@')[1] if '@' in email else ''

    # Check prefix
    for prefix in AUTOMATED_PREFIXES:
        if local == prefix or local.startswith(prefix + '-') or local.startswith(prefix + '+'):
            return True

    # Check domain (exact or subdomain)
    if domain in SERVICE_DOMAINS:
        return True
    # Check parent domain (e.g., status.incident.io → incident.io)
    parts = domain.split('.')
    for i in range(1, len(parts)):
        parent = '.'.join(parts[i:])
        if parent in SERVICE_DOMAINS:
            return True

    return False


def is_generic_domain(domain: str) -> bool:
    """Retorna True si el dominio es genérico (Gmail, Outlook, etc.)."""
    return domain.lower() in GENERIC_DOMAINS
