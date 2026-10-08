# -*- coding: utf-8 -*-
"""Corre en el build de Odoo.sh (`--test-tags /qb_costeo_sgi`): el CI no instala el SGI."""
from odoo.exceptions import UserError
from odoo.tests import tagged

from odoo.addons.qb_cotizador.tests.common import CotizadorCase


@tagged('post_install', '-at_install')
class TestBridge(CotizadorCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.project = cls.env['project.project'].create({
            'name': 'Desarrollo prueba cotizador', 'sgi_is_ft': True, 'partner_id': cls.partner.id,
            'sgi_dev_product_name': 'Jersey 150 prueba', 'sgi_dev_volume': 2500, 'sgi_dev_volume_uom': 'kg',
            'sgi_dev_target_price': 33.0})
        Car = cls.env['ficha.tecnica.caracteristica']
        for code, val in (('masa', 150.0), ('ancho', 1.7)):
            car = Car.search([('code', '=', code)], limit=1)
            cls.env['sgi.dev.characteristic'].create({'project_id': cls.project.id, 'caracteristica_id': car.id,
                                                      'spec_nominal': val})
        galga = Car.search([('code', '=', 'galga')], limit=1)
        if galga:
            cls.env['sgi.dev.characteristic'].create({'project_id': cls.project.id, 'caracteristica_id': galga.id,
                                                      'spec_nominal': 28})

    def test_01_toma_datos_del_proyecto(self):
        cot = self.env['qb.cotizador.cotizacion'].with_user(self.vendedor).create({'project_id': self.project.id})
        self.assertEqual(cot.partner_id, self.partner)
        self.assertEqual(cot.spec_descripcion, 'Jersey 150 prueba')
        self.assertEqual(cot.spec_gramaje, 150.0)
        self.assertEqual(cot.spec_ancho, 1.7)
        self.assertEqual(cot.volumen, 2500)
        self.assertEqual(cot.volumen_uom, 'kg')
        self.assertEqual(cot.precio_objetivo, 33.0)
        self.assertEqual(self.project.qb_cotizacion_count, 1)
        self.assertEqual(self.project.action_qb_cotizaciones()['domain'], [('project_id', '=', self.project.id)])

    def test_02_revision_recalcula_y_detiene(self):
        cot = self._presentada(project_id=self.project.id, product_id=self.tela.id)
        self.assertEqual(cot.project_revision, 0)
        self.project.action_sgi_dev_new_revision()
        self.assertEqual(cot.state, 'presentada', 'Mismo costo: pasa')
        self.assertFalse(self.project.qb_costo_bloqueado)
        self.costo.write({'mp_unit': 9.0, 'costo_variable': 10.0, 'costo_produccion': 12.0})
        self.project.action_sgi_dev_new_revision()
        self.assertEqual(cot.state, 'por_aprobar', 'El costo cambió: espera aprobación')
        self.assertTrue(self.project.qb_costo_bloqueado)
        otra_etapa = self.env['project.project.stage'].search([('id', '!=', self.project.stage_id.id)], limit=1)
        if otra_etapa:
            with self.assertRaises(UserError):
                self.project.write({'stage_id': otra_etapa.id})
        cot.with_user(self.finanzas).action_aprobar()
        self.assertFalse(self.project.qb_costo_bloqueado)
        self.assertEqual(cot.project_revision, 2)

    def test_03_entregables_c1_apuntan_a_la_cotizacion(self):
        self.env['qb.cotizador.cotizacion']._qb_sgi_apuntar_entregables()
        for code in ('C1-COSTO', 'C1-COTIZACION'):
            d = self.env['sgi.deliverable'].search([('code', '=', code)], limit=1)
            if d:
                self.assertEqual(d.odoo_model_id.model, 'qb.cotizador.cotizacion')

    def test_04_aprobacion_en_aprobaciones(self):
        Cot = self.env['qb.cotizador.cotizacion']
        category, subject = Cot._qb_sgi_approval_subject()
        cot = self._cot(project_id=self.project.id)
        cot.action_calcular()
        cot.action_enviar_aprobacion()
        if not category:
            self.assertFalse(cot.approval_request_id, "Sin rol de C1.05 como solicitud aprueba el puesto")
            return
        req = cot.approval_request_id
        self.assertTrue(req)
        self.assertEqual(req.category_id, category)
        self.assertEqual(req.sgi_subject_id, subject)
        self.assertEqual(req.request_status, 'pending')
        self.assertFalse(cot.with_user(self.finanzas).puede_aprobar, "Mientras la solicitud vive no se aprueba aquí")
        with self.assertRaises(UserError):
            cot.with_user(self.finanzas).action_aprobar()
        approver = req.approver_ids[:1]
        if not approver:
            return
        req.sudo().action_approve(approver=approver)
        if req.request_status == 'approved':
            self.assertEqual(cot.state, 'presentada', "Aprobar en Aprobaciones presenta la cotización")
            self.assertTrue(cot.approved_by_id)
        otra = self._cot(project_id=self.project.id)
        otra.action_calcular()
        otra.action_enviar_aprobacion()
        otra.approval_request_id.sudo().action_cancel()
        self.assertEqual(otra.state, 'borrador', "Cancelar la solicitud regresa la cotización")
        self.assertEqual(otra.regreso_motivo_id, self.env.ref('qb_cotizador.motivo_regreso_otro'))
