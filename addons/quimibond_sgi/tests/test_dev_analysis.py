# -*- coding: utf-8 -*-
"""57.120.0 (C1, bloque 4): parecidos, pruebas de laboratorio y factibilidad."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, new_test_user, tagged

from ..models.sgi_dev_analysis import parse_article_code


@tagged('post_install', '-at_install')
class TestDevAnalysis(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Project = cls.env['project.project']
        cls.Product = cls.env['product.product']
        cls.Clave = cls.env['ficha.tecnica.clave.codigo']
        cls.partner = cls.env['res.partner'].create({'name': 'Cliente análisis', 'is_company': True})
        cls.dev = cls.Project.create({
            'name': 'x', 'sgi_is_ft': True, 'partner_id': cls.partner.id, 'sgi_dev_product_name': 'Jersey',
            'sgi_dev_code_composicion_id': cls.Clave.search([('kind', '=', 'composicion'), ('code', '=', 'W')]).id,
            'sgi_dev_code_dibujo_id': cls.Clave.search([('kind', '=', 'dibujo'), ('code', '=', 'J')]).id,
        })
        cls.dev.action_sgi_dev_load_lines()
        lines = cls.dev.sgi_dev_line_ids
        lines.filtered(lambda l: l.caracteristica_code == 'masa').write(
            {'spec_nominal': 53, 'spec_tol_minus': 3, 'spec_tol_plus': 3})
        lines.filtered(lambda l: l.caracteristica_code == 'ancho').write(
            {'spec_nominal': 1.60, 'spec_tol_minus': 0.05, 'spec_tol_plus': 0.05})
        lines.filtered(lambda l: l.caracteristica_code == 'galga').write({'spec_nominal': 18})

    def test_01_parse_and_similar(self):
        self.assertEqual(parse_article_code('WJ053Q22JNT160')['peso'], 53)
        self.assertEqual(parse_article_code('WJ053Q22JNT160AF')['acab'], 'AF')
        self.assertIsNone(parse_article_code('WJ053Q22JNT135M2'))
        self.assertIsNone(parse_article_code('MUESTRA PILOTO TEJIDO'))
        exact = self.Product.create({'name': 'Igual', 'default_code': 'WJ054Q22JNT162', 'sale_ok': True})
        close = self.Product.create({'name': 'Cerca', 'default_code': 'WJ060Q22JNT160', 'sale_ok': True})
        other_dib = self.Product.create({'name': 'Otro dibujo', 'default_code': 'WK053Q22JNT160', 'sale_ok': True})
        far = self.Product.create({'name': 'Lejos', 'default_code': 'WJ300Q46JNG165', 'sale_ok': True})
        self.Product.create({'name': 'Crudo', 'default_code': 'WJ053Q22HNT185', 'sale_ok': False})
        rows = self.dev._sgi_dev_similar_candidates()
        products = [r['product_id'] for r in rows]
        self.assertEqual(products[0], exact.id, "El más cercano va primero.")
        self.assertIn(close.id, products)
        self.assertIn(other_dib.id, products)
        self.assertNotIn(far.id, products, "Más de 60 % de diferencia no se propone.")
        by_id = {r['product_id']: r for r in rows}
        self.assertTrue(by_id[exact.id]['within_tolerance'])
        self.assertFalse(by_id[close.id]['within_tolerance'])
        self.assertIn('dibujo K', by_id[other_dib.id]['note'])
        self.assertLess(by_id[close.id]['score'], by_id[other_dib.id]['score'])
        action = self.dev.action_sgi_dev_find_similar()
        wizard = self.env['sgi.dev.similar'].browse(action['res_id'])
        self.assertEqual(wizard.peso, 53)
        wizard.line_ids.filtered(lambda l: l.product_id == exact).action_use_as_base()
        self.assertEqual((self.dev.sgi_dev_base_product_id, self.dev.sgi_dev_analysis_result), (exact, 'linea'))
        wizard.line_ids.filtered(lambda l: l.product_id == close).action_use_as_base()
        self.assertEqual(self.dev.sgi_dev_analysis_result, 'nuevo')

    def test_02_lab_request_flow(self):
        Param = self.env['ir.config_parameter'].sudo()
        # Como producción: el nombre que contiene «Coordinador de Laboratorio» vive en es_MX.
        self.env['res.lang']._activate_lang('es_MX')
        job = self.env['hr.job'].create({'name': 'Lab Coordinator (prueba)'})
        job.with_context(lang='es_MX').name = 'Coordinador de Laboratorio y MP (prueba)'
        self.assertFalse(self.env['hr.job'].search([('name', 'ilike', 'Coordinador de Laboratorio'), ('id', '=', job.id)]),
                         "Sin idioma en el contexto no se encuentra: así quedó vacío el parámetro en producción.")
        lab_user = new_test_user(self.env, login='lab_coord', groups='base.group_user')
        self.env['hr.employee'].create({'name': 'Ariadna prueba', 'job_id': job.id, 'user_id': lab_user.id})
        other_user = new_test_user(self.env, login='otro_user', groups='base.group_user')
        Request = self.env['sgi.dev.lab.request']
        Param.set_param('quimibond_sgi.dev_lab_authorizer_job_id', '')
        self.assertEqual(Request._authorizer_job(), job, "Sin parámetro, el puesto se busca por nombre.")
        self.assertEqual(Request._sgi_dev_set_lab_job_default(), job)
        self.assertEqual(Param.get_param('quimibond_sgi.dev_lab_authorizer_job_id'), str(job.id))
        self.assertFalse(Request._sgi_dev_set_lab_job_default(), "No pisa un valor capturado.")
        self.dev.sgi_dev_line_ids.write({'lab_requested': False})
        masa = self.dev.sgi_dev_line_ids.filtered(lambda l: l.caracteristica_code == 'masa')
        ancho = self.dev.sgi_dev_line_ids.filtered(lambda l: l.caracteristica_code == 'ancho')
        (masa | ancho).write({'lab_requested': True})
        action = self.dev.action_sgi_dev_request_lab_tests()
        request = self.env['sgi.dev.lab.request'].browse(action['res_id'])
        self.assertEqual((request.state, request.kind, len(request.line_ids)), ('solicitada', 'muestra_cliente', 2))
        self.assertEqual(request.pending_count, 2)
        self.assertTrue(request.activity_ids.filtered(lambda a: a.user_id == lab_user))
        with self.assertRaises(UserError):
            self.dev.action_sgi_dev_request_lab_tests()  # ya están en una solicitud en curso
        with self.assertRaises(UserError):
            request.with_user(other_user).action_authorize()
        request.with_user(lab_user).action_authorize()
        self.assertEqual((request.state, request.authorized_by_id), ('autorizada', lab_user))
        masa.write({'sample_value': 54})
        self.assertEqual((request.state, request.pending_count), ('autorizada', 1))
        ancho.write({'sample_value': 1.61})
        self.assertEqual((request.state, request.pending_count), ('medida', 0))
        self.assertTrue(request.date_measured)
        self.assertEqual(masa.sample_result, 'cumple', "El laboratorio mide; el resultado se calcula.")
        self.assertFalse(masa.verdict, "El dictamen lo pone Diseño, no el laboratorio.")
        self.assertEqual(self.dev.sgi_dev_lab_request_count, 1)
        self.assertEqual(self.dev.sgi_dev_lab_open_count, 0)

    def test_03_feasibility_and_single_review(self):
        Item = self.env['sgi.dev.feasibility.item']
        self.assertFalse(Item.search([]), "El catálogo nace vacío: nadie definió los recursos.")
        manual = Item.create({'line': 'tejido_circular', 'name': 'Máquina disponible', 'auto': 'none'})
        auto = Item.create({'line': 'tejido_circular', 'name': 'Materia prima', 'auto': 'materia_prima'})
        Item.create({'line': 'entretelas', 'name': 'Carda', 'auto': 'none'})
        self.dev.action_sgi_dev_load_feasibility()
        lines = self.dev.sgi_dev_feasibility_ids
        self.assertEqual(set(lines.mapped('item_id')), {manual, auto}, "Solo la línea del proyecto.")
        self.assertEqual(lines.filtered(lambda l: l.item_id == auto).answer, 'pendiente', "Sin lista de materiales.")
        self.assertEqual(self.dev.sgi_dev_feasibility_pending, 2)
        with self.assertRaises(UserError):
            self.dev.action_sgi_dev_review_approve()  # falta el resultado del análisis
        self.dev.write({'sgi_dev_analysis_result': 'nuevo'})
        with self.assertRaises(UserError):
            self.dev.action_sgi_dev_review_approve()  # faltan respuestas
        # Materia prima con lista de materiales: la contesta Odoo.
        yarn = self.Product.create({'name': 'Hilo', 'type': 'consu', 'is_storable': True})
        finished = self.Product.create({'name': 'Acabado', 'type': 'consu', 'is_storable': True})
        self.env['mrp.bom'].create({'product_tmpl_id': finished.product_tmpl_id.id, 'product_qty': 1,
                                    'bom_line_ids': [(0, 0, {'product_id': yarn.id, 'product_qty': 1})]})
        self.dev.write({'sgi_dev_base_product_id': finished.id})
        self.dev.action_sgi_dev_load_feasibility()
        self.assertEqual(lines.filtered(lambda l: l.item_id == auto).answer, 'no', "Sin existencia del hilo.")
        self.env['stock.quant']._update_available_quantity(yarn, self.env.ref('stock.stock_location_stock'), 10)
        self.dev.action_sgi_dev_load_feasibility()
        self.assertEqual(lines.filtered(lambda l: l.item_id == auto).answer, 'si')
        lines.filtered(lambda l: l.item_id == manual).write({'answer': 'si', 'observation': 'Circular 7'})
        self.assertEqual(lines.filtered(lambda l: l.item_id == manual).answered_by_id, self.env.user)
        self.assertEqual(self.dev.sgi_dev_feasibility_pending, 0)
        self.dev.write({'sgi_dev_review_note': 'Falta el precio objetivo'})
        self.dev.action_sgi_dev_review_return()
        self.assertEqual(self.dev.sgi_dev_review_state, 'regresado')
        self.dev.action_sgi_dev_review_approve()
        self.assertEqual((self.dev.sgi_dev_review_state, self.dev.sgi_dev_reviewed_by_id), ('aprobado', self.env.user))
        self.dev.write({'stage_id': self.env.ref('quimibond_sgi.sgi_dev_stage_cotizacion').id})
        self.assertEqual(self.dev.sgi_dev_stage_key, 'cotizacion')
