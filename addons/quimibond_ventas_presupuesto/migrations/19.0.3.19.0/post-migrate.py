# -*- coding: utf-8 -*-
"""3.19.0: condiciones de entrega por cliente y planta.

Una ficha por cada perfil de release (cliente y planta), con 3 semanas congeladas (Jose, 19-nov: «al
igual que con Shawmut o Zwisstex o cualquier otro cliente, cualquier aumento se ve reflejado en la 4
semana»). Si la base trae la 3.18.0 (solo `main`), las semanas congeladas, los embarques y el rollo del
perfil, y el rollo de cada parte del cliente, pasan a la ficha; las columnas viejas se quedan sin uso."""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def _columns(cr, table):
    cr.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table,))
    return {row[0] for row in cr.fetchall()}


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {'active_test': False})
    Terms = env['qb.delivery.terms']
    profile_cols = _columns(cr, 'qb_release_profile')
    old = {}
    wanted = [c for c in ('frozen_weeks', 'group_weeks', 'group_start', 'roll_qty', 'roll_uom')
              if c in profile_cols]
    if wanted:
        cr.execute("SELECT id, %s FROM qb_release_profile" % ", ".join(wanted))
        for row in cr.dictfetchall():
            old[row.pop('id')] = row
    created = 0
    for profile in env['qb.release.profile'].search([]):
        try:
            with cr.savepoint():
                partner = profile.partner_id.commercial_partner_id
                terms = Terms.search([
                    ('company_id', '=', profile.company_id.id), ('partner_id', '=', partner.id),
                    ('ship_to_id', '=', profile.ship_to_id.id or False),
                    ('plant', '=', (profile.plant or False) if not profile.ship_to_id else False)], limit=1)
                if terms:
                    continue
                vals = {'company_id': profile.company_id.id, 'partner_id': partner.id,
                        'ship_to_id': profile.ship_to_id.id or False,
                        'plant': (profile.plant or False) if not profile.ship_to_id else False,
                        'frozen_weeks': 3}
                prev = old.get(profile.id, {})
                for key in ('frozen_weeks', 'group_weeks', 'group_start', 'roll_qty', 'roll_uom'):
                    if prev.get(key) not in (None, False):
                        vals[key] = prev[key]
                terms = Terms.create(vals)
                created += 1
        except Exception as exc:  # noqa: BLE001 - un perfil no detiene la actualización
            _logger.warning("3.19.0: el perfil %s no dio condiciones de entrega: %s", profile.id, exc)
    lines = 0
    if {'roll_qty', 'roll_uom'} <= _columns(cr, 'qb_customer_part'):
        cr.execute("SELECT id, roll_qty, roll_uom FROM qb_customer_part WHERE roll_qty > 0")
        for part_id, roll_qty, roll_uom in cr.fetchall():
            part = env['qb.customer.part'].browse(part_id)
            if not part.product_id:
                continue
            terms = Terms._qb_for(part.partner_id, part.ship_to_id, None, part.company_id)
            if not terms or terms.line_ids.filtered(lambda l: l.product_id == part.product_id):
                continue
            terms.write({'line_ids': [(0, 0, {'product_id': part.product_id.id, 'customer_part': part.customer_part,
                                              'roll_qty': roll_qty, 'roll_uom': roll_uom or 'yd'})]})
            lines += 1
    _logger.info("3.19.0: %d condiciones de entrega nuevas y %d presentaciones por producto", created, lines)
