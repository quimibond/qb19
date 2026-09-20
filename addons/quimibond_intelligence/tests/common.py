# -*- coding: utf-8 -*-
"""Base común de los tests de señales: fecha fija, partner y helpers de documentos."""
from datetime import date, timedelta

from odoo.tests import TransactionCase


class SenalesCommon(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.hoy = date.today()
        cls.cliente = cls.env['res.partner'].create({'name': 'Cliente Señal', 'customer_rank': 1, 'vat': 'ABC010101XYZ'})
        cls.proveedor = cls.env['res.partner'].create({'name': 'Proveedor Señal', 'supplier_rank': 1})
        cls.cfg = {'umbrales': {}}

    def hace(self, dias):
        return self.hoy - timedelta(days=dias)

    def filas(self, senal, cfg=None):
        from ..models.senales import base
        return base.REGISTRO[senal](self.env, cfg or self.cfg)

    def fila_de(self, filas, clave):
        for f in filas:
            if f['clave'] == clave:
                return f
        self.fail('no hay fila %s en %s' % (clave, [f['clave'] for f in filas]))
