#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Valida el flujo de efectivo NIF B-2 contra los saldos contables via JSON-2.

Para cada periodo compara:

* el efectivo final que calcula el reporte (``cash.flow.config.compute_summary``:
  efectivo inicial + incremento neto + efecto cambiario), contra
* la suma de saldos de las cuentas de efectivo al cierre, obtenida con
  ``account.move.line`` ``formatted_read_group`` (o ``read_group``) usando la
  misma lista de cuentas de la configuracion (``get_cash_account_ids``),

e imprime la diferencia. Tambien reporta la diferencia entre metodos y los
renglones "Sin clasificar" / "Otros".

Uso::

    export ODOO_URL=https://quimibond.odoo.com
    export ODOO_API_KEY=xxxxxxxx          # Ajustes > Usuario > Seguridad > API keys
    export ODOO_DB=quimibond-prod          # opcional (solo si hay varias bases)
    python3 scripts/validate_vs_odoo.py [--company 1] [--period 2026-01-01:2026-08-31] [--period 2025-01-01:2025-12-31]

Sin dependencias externas: solo ``urllib``.
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.request

DEFAULT_PERIODS = ['2026-01-01:2026-08-31', '2025-01-01:2025-12-31']


class Json2Client:
    """Cliente minimo de la API JSON-2 de Odoo 19
    (``POST /json/2/<model>/<method>`` con ``Authorization: Bearer <api_key>``)."""

    def __init__(self, url, api_key, db=None, company_id=None):
        self.url = url.rstrip('/')
        self.api_key = api_key
        self.db = db
        self.company_id = company_id

    def call(self, model, method, ids=None, **kwargs):
        payload = dict(kwargs)
        if ids:
            payload['ids'] = list(ids)
        context = dict(payload.pop('context', {}) or {})
        if self.company_id:
            context.setdefault('allowed_company_ids', [self.company_id])
        if context:
            payload['context'] = context
        data = json.dumps(payload).encode('utf-8')
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer %s' % self.api_key,
        }
        if self.db:
            headers['X-Odoo-Database'] = self.db
        request = urllib.request.Request('%s/json/2/%s/%s' % (self.url, model, method), data=data, headers=headers, method='POST')
        try:
            with urllib.request.urlopen(request, timeout=300) as response:
                return json.loads(response.read().decode('utf-8'))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode('utf-8', errors='replace')
            raise SystemExit('HTTP %s en %s.%s: %s' % (exc.code, model, method, body[:2000]))


def cash_balance(client, company_id, cash_account_ids, date_to):
    """Suma de ``balance`` de las cuentas de efectivo al cierre de ``date_to``."""
    domain = [
        ['company_id', '=', company_id],
        ['parent_state', '=', 'posted'],
        ['account_id', 'in', cash_account_ids],
        ['date', '<=', date_to],
    ]
    try:
        groups = client.call('account.move.line', 'formatted_read_group', domain=domain, groupby=[], aggregates=['balance:sum'])
        return float(groups[0]['balance:sum'] or 0.0) if groups else 0.0
    except SystemExit:
        groups = client.call('account.move.line', 'read_group', domain=domain, fields=['balance:sum'], groupby=[], lazy=False)
        return float(groups[0]['balance'] or 0.0) if groups else 0.0


def cash_balance_by_account(client, company_id, cash_account_ids, date_to):
    domain = [
        ['company_id', '=', company_id],
        ['parent_state', '=', 'posted'],
        ['account_id', 'in', cash_account_ids],
        ['date', '<=', date_to],
    ]
    try:
        groups = client.call('account.move.line', 'formatted_read_group', domain=domain, groupby=['account_id'], aggregates=['balance:sum'])
        return [(g['account_id'], g['balance:sum']) for g in groups]
    except SystemExit:
        groups = client.call('account.move.line', 'read_group', domain=domain, fields=['balance:sum'], groupby=['account_id'], lazy=False)
        return [(g['account_id'], g['balance']) for g in groups]


def fmt(value):
    return '{:,.2f}'.format(value)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--url', default=os.environ.get('ODOO_URL'), help='URL base de Odoo (o ODOO_URL)')
    parser.add_argument('--api-key', default=os.environ.get('ODOO_API_KEY'), help='API key (o ODOO_API_KEY)')
    parser.add_argument('--db', default=os.environ.get('ODOO_DB'), help='Base de datos (o ODOO_DB)')
    parser.add_argument('--company', type=int, default=int(os.environ.get('ODOO_COMPANY_ID', '1')))
    parser.add_argument('--period', action='append', help='date_from:date_to (repetible)')
    parser.add_argument('--by-account', action='store_true', help='Desglosa el saldo de efectivo por cuenta')
    args = parser.parse_args(argv)
    if not args.url or not args.api_key:
        parser.error('Faltan --url/ODOO_URL o --api-key/ODOO_API_KEY')

    client = Json2Client(args.url, args.api_key, args.db, args.company)
    configs = client.call('cash.flow.config', 'search_read', domain=[['company_id', '=', args.company]], fields=['id', 'company_id'], limit=1)
    if not configs:
        raise SystemExit('La compañía %s no tiene configuración de flujo de efectivo (cash.flow.config).' % args.company)
    config_id = configs[0]['id']
    cash_ids = client.call('cash.flow.config', 'get_cash_account_ids', ids=[config_id])
    print('Compañía %s · configuración %s · %d cuentas de efectivo' % (args.company, config_id, len(cash_ids)))

    exit_code = 0
    for period in args.period or DEFAULT_PERIODS:
        date_from, date_to = period.split(':')
        summary = client.call('cash.flow.config', 'compute_summary', ids=[config_id], date_from=date_from, date_to=date_to)
        book_closing = cash_balance(client, args.company, cash_ids, date_to)
        book_opening = cash_balance(client, args.company, cash_ids, _day_before(date_from))
        diff = summary['closing_cash_calc'] - book_closing
        print()
        print('=== %s → %s ===' % (date_from, date_to))
        print('  Efectivo inicial (reporte)         %18s' % fmt(summary['opening_cash']))
        print('  Efectivo inicial (saldos contables)%18s' % fmt(book_opening))
        print('  Operación (indirecto)              %18s' % fmt(summary['indirect']['operating']))
        print('  Inversión                          %18s' % fmt(summary['indirect']['investing']))
        print('  Financiamiento                     %18s' % fmt(summary['indirect']['financing']))
        print('  Incremento neto (indirecto)        %18s' % fmt(summary['indirect']['net_increase']))
        print('  Incremento neto (directo)          %18s' % fmt(summary['direct']['net_increase']))
        print('  Efecto cambiario                   %18s' % fmt(summary['indirect']['fx_effect']))
        print('  Efectivo final calculado           %18s' % fmt(summary['closing_cash_calc']))
        print('  Efectivo final (saldos contables)  %18s' % fmt(book_closing))
        print('  DIFERENCIA                         %18s' % fmt(diff))
        print('  Diferencia entre métodos           %18s' % fmt(summary['methods_difference']))
        print('  Sin clasificar (indirecto)         %18s' % fmt(summary['lines'].get('unclassified', 0.0)))
        print('  Otros (directo)                    %18s' % fmt(summary['lines'].get('d_other', 0.0)))
        for row in summary.get('unclassified', []):
            print('      sin clasificar: %-14s %-40s %18s' % (row['code'], row['name'][:40], fmt(row['amount'])))
        for row in summary.get('other', []):
            print('      otros:          %-14s %-40s %18s' % (row['code'], row['name'][:40], fmt(row['amount'])))
        if args.by_account:
            for account, balance in cash_balance_by_account(client, args.company, cash_ids, date_to):
                label = account[1] if isinstance(account, (list, tuple)) else account
                print('      saldo: %-60s %18s' % (str(label)[:60], fmt(balance or 0.0)))
        if abs(diff) > 0.01 or abs(summary['methods_difference']) > 0.01:
            exit_code = 1
    return exit_code


def _day_before(date_str):
    from datetime import date, timedelta
    y, m, d = (int(x) for x in date_str.split('-'))
    return (date(y, m, d) - timedelta(days=1)).isoformat()


if __name__ == '__main__':
    sys.exit(main())
