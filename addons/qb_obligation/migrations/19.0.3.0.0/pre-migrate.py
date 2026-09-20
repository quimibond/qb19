# -*- coding: utf-8 -*-
"""3.0.0: la cobranza sale del módulo (las facturas vencidas ya viven en
Contabilidad). Se borran las obligaciones del piloto de cobranza y sus
mensajes; los tipos dejan de existir en el modelo."""
TYPES = ('collection.overdue_invoice', 'collection.apply_payment')


def migrate(cr, version):
    cr.execute("SELECT id FROM qb_obligation WHERE obligation_type IN %s", (TYPES,))
    ids = [r[0] for r in cr.fetchall()]
    if not ids:
        return
    for table, column in (('mail_message', 'model'), ('mail_followers', 'res_model'), ('mail_activity', 'res_model')):
        cr.execute("DELETE FROM %s WHERE %s = 'qb.obligation' AND res_id IN %%s" % (table, column), (tuple(ids),))
    cr.execute("DELETE FROM qb_obligation WHERE id IN %s", (tuple(ids),))
