# -*- coding: utf-8 -*-
"""57.65.0 — modelo de Odoo en los entregables evidentes (post-migrate): solo
lo vacío, respeta lo capturado y lo que ya mide, idempotente."""
import importlib.util
import os

from odoo.tests import TransactionCase, tagged

from ..models.sgi_deliverable_models import SGI_DELIVERABLE_MODELS

_MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@tagged('post_install', '-at_install')
class TestEntregablesModelo(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.company = cls.env.company
        Deliverable = cls.env['sgi.deliverable']
        cls.d_empty = Deliverable.create({'name': 'ZD Tarjeta', 'code': 'ZD-TARJETA',
                                          'company_id': cls.company.id})
        cls.d_set = Deliverable.create({
            'name': 'ZD Con modelo', 'code': 'ZD-CON', 'company_id': cls.company.id,
            'odoo_model_id': cls.env['ir.model']._get('sale.order').id})
        # Su campo de fecha no existe en la orden de producción: se salta.
        cls.d_date = Deliverable.create({
            'name': 'ZD Fecha', 'code': 'ZD-FECHA', 'company_id': cls.company.id,
            'measure_date_field': 'date_order'})
        cls.table = (
            ('ZD-TARJETA', 'mrp.production'),
            ('ZD-CON', 'mrp.production'),
            ('ZD-FECHA', 'mrp.production'),
            ('ZD-NO-EXISTE', 'mrp.production'),
            ('ZD-TARJETA', 'modelo.que.no.existe'),
        )

    def test_01_solo_lo_vacio_e_idempotente(self):
        Deliverable = self.env['sgi.deliverable']
        written = Deliverable._sgi_fill_evident_models(self.table, company=self.company)
        self.assertEqual(written, {'ZD-TARJETA': 'mrp.production'})
        self.assertEqual(self.d_empty.odoo_model_id.model, 'mrp.production')
        self.assertEqual(self.d_set.odoo_model_id.model, 'sale.order', "Lo capturado se respeta.")
        self.assertFalse(self.d_date.odoo_model_id, "Campo de fecha ajeno al modelo: se salta.")
        self.assertEqual(Deliverable._sgi_fill_evident_models(self.table, company=self.company), {})

    def test_02_tabla_real(self):
        codes = [code for code, _model in SGI_DELIVERABLE_MODELS]
        self.assertEqual(len(codes), len(set(codes)), "Código repetido en la tabla.")
        for code, model_name in SGI_DELIVERABLE_MODELS:
            self.assertIn(model_name, self.env, "%s: modelo %s." % (code, model_name))

    def test_03_post_migrate_corre(self):
        path = os.path.join(_MODULE_DIR, 'migrations', '19.0.57.65.0', 'post-migrate.py')
        spec = importlib.util.spec_from_file_location('sgi_mig_57_65_0', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.migrate(self.env.cr, '19.0.57.64.0')
        self.assertEqual(self.env['sgi.deliverable']._sgi_fill_evident_models(), {})
