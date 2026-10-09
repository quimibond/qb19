# -*- coding: utf-8 -*-
"""Corre en el build de Odoo.sh (`--test-tags /qb_costeo_sgi`): el CI no instala el SGI."""
from odoo import fields
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

    def test_01b_articulo_base_es_el_hermano(self):
        # 1.4.0: el artículo base pasa como producto hermano; sin artículo generado, fuente «hermano».
        self.project.write({'sgi_dev_base_product_id': self.hermana.id})
        cot = self.env['qb.cotizador.cotizacion'].with_user(self.vendedor).create({'project_id': self.project.id})
        self.assertEqual(cot.hermano_product_id, self.hermana)
        self.assertEqual(cot.costo_fuente, 'hermano')
        cot.action_calcular()
        self.assertEqual(cot.costo_id.product_id, self.hermana)
        # Renglones «Por capturar» en una lista del desarrollo: no se manda a aprobar.
        self.tela.product_tmpl_id.write({'sgi_dev_project_id': self.project.id})
        bom = self.env['mrp.bom'].create({'product_tmpl_id': self.tela.product_tmpl_id.id, 'product_qty': 1,
                                          'bom_line_ids': [(0, 0, {'product_id': self.hermana.id, 'product_qty': 1,
                                                                   'sgi_dev_pending': True})]})
        self.assertEqual(self.project.sgi_dev_bom_pending_count, 1)
        cot.write({'precio_objetivo': 20.0, 'volumen': 1000})
        with self.assertRaises(UserError, msg="Con renglones por capturar no se aprueba"):
            cot.action_enviar_aprobacion()
        bom.bom_line_ids.write({'sgi_dev_pending': False})
        self.project.invalidate_recordset(['sgi_dev_bom_pending_count'])
        cot.action_enviar_aprobacion()
        self.assertEqual(cot.state, 'por_aprobar')

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
        for code in ('C1-COSTO', 'C1-COTIZACION', 'C1-ARTICULO'):
            d = self.env['sgi.deliverable'].search([('code', '=', code)], limit=1)
            if d:
                self.assertEqual(d.odoo_model_id.model, 'qb.cotizador.cotizacion')
                if code == 'C1-ARTICULO':
                    self.assertEqual(d.measure_date_field, 'tarifa_fecha', '1.3.0: C1.17 se mide con la tarifa')

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

    def test_04b_vendedor_sin_grupos_de_aprobaciones(self):
        """1.5.0: el vendedor (solo Ventas / Usuario, sin grupos de Aprobaciones) manda a aprobar,
        retira, vuelve a mandar y registra la aprobación del cliente sin error de acceso."""
        Cot = self.env['qb.cotizador.cotizacion']
        category, _subject = Cot._qb_sgi_approval_subject()
        self.assertFalse(self.vendedor.has_group('approvals.group_approval_user'))
        cot = self._cot(project_id=self.project.id).with_user(self.vendedor)
        cot.action_calcular()
        cot.action_enviar_aprobacion()
        self.assertEqual(cot.state, 'por_aprobar')
        if category:
            req = cot.approval_request_id.sudo()
            self.assertTrue(req, "La solicitud se crea aunque el vendedor no tenga grupos de Aprobaciones")
            self.assertEqual(req.request_owner_id, self.vendedor, "Y en Aprobaciones se ve quién la pidió")
            self.assertEqual(req.request_status, 'pending')
            # Retirar cancela la solicitud; volver a enviar crea otra.
            cot.action_volver_a_borrador()
            self.assertEqual(cot.state, 'borrador')
            self.assertEqual(req.request_status, 'cancel')
            cot.action_enviar_aprobacion()
            self.assertNotEqual(cot.approval_request_id, req)
            self.assertEqual(cot.sudo().approval_request_id.request_status, 'pending')
            cot.action_volver_a_borrador()
        else:
            cot.action_volver_a_borrador()
            cot.action_enviar_aprobacion()
            cot.action_volver_a_borrador()
        # Aprobación del cliente la registra el vendedor (la tarifa va con sudo en qb_cotizador).
        cot.write({'cliente_aprobo': True, 'cliente_medio': 'correo', 'cliente_fecha': fields.Date.today()})
        self.assertTrue(cot.cliente_aprobo)

    def test_04c_quien_aprueba_no_manda_su_propia_cotizacion(self):
        """1.5.1 / SGI 57.143.0: si quien manda a aprobar es el puesto que aprueba
        C1.05 y el rol no tiene suplente, la solicitud no se confirma y el mensaje
        lo dice; con suplente nombrado, la aprueba el suplente."""
        Cot = self.env['qb.cotizador.cotizacion']
        role = Cot._qb_sgi_approval_role()
        if not role:
            self.skipTest("C1.05 sin aprobación como solicitud en esta base.")
        approver = role.sudo()._sgi_approver_users()[:1]
        if not approver:
            self.skipTest("El rol de C1.05 no tiene personas.")
        cot = self._cot(user=approver, project_id=self.project.id).with_user(approver)
        cot.action_calcular()
        role.sudo().substitute_job_id = False
        with self.assertRaises(UserError) as cm:
            cot.action_enviar_aprobacion()
        self.assertIn('suplente', str(cm.exception))
        job_sup = self.env['hr.job'].create({'name': 'SUPLENTE C1.05 PRUEBA'})
        suplente = self.env['res.users'].with_context(no_reset_password=True).create({
            'name': 'Suplente C1.05 prueba', 'login': 'qbcot_sup_c105',
            'group_ids': [(6, 0, [self.env.ref('approvals.group_approval_user').id])]})
        self.env['hr.employee'].create({'name': 'Suplente C1.05 prueba', 'user_id': suplente.id, 'job_id': job_sup.id})
        role.sudo().substitute_job_id = job_sup
        cot.action_enviar_aprobacion()
        req = cot.sudo().approval_request_id
        self.assertEqual(req.request_status, 'pending')
        self.assertEqual(req.approver_ids.user_id, suplente, "La aprueba el suplente, no quien la pidió.")

    def test_05_precio_en_tarifa_al_ganar_con_el_articulo_del_desarrollo(self):
        """1.3.0 (Jose 5.6): la cotización nace sin artículo; la aprobación de la muestra llega del
        envío del SGI; al generar el artículo, la ganada pone su precio en la tarifa sola."""
        cot = self._cot(project_id=self.project.id, product_id=False)
        self.assertFalse(cot.product_id)
        cot._marcar_ganada()
        self.assertEqual(cot.state, 'ganada')
        self.assertFalse(cot.pricelist_item_id, 'Ganada sin aprobación del cliente: sin tarifa')
        self.project._qb_sgi_cliente_aprobo('oc', fields.Date.today(), ref='OC-PRUEBA')
        self.assertTrue(cot.cliente_aprobo)
        self.assertEqual(cot.cliente_medio, 'oc')
        self.assertFalse(cot.pricelist_item_id, 'Sin artículo todavía: el precio espera')
        self.project.write({'sgi_dev_product_id': self.tela.id})
        self.project._qb_sgi_ligar_articulo()
        self.assertEqual(cot.product_id, self.tela, 'El artículo del desarrollo entra a la cotización')
        self.assertTrue(cot.pricelist_item_id, 'Ganada + aprobación + artículo ⇒ precio en tarifa')
        self.assertEqual(cot.pricelist_item_id.product_tmpl_id, self.tela.product_tmpl_id)
        self.assertEqual(cot.pricelist_item_id.fixed_price, 14.0)
        self.assertTrue(cot.tarifa_fecha)
        self.assertEqual(self.partner.property_product_pricelist, cot.pricelist_item_id.pricelist_id)
        # Segunda aprobación (otro envío) no vuelve a escribir ni duplica
        self.project._qb_sgi_cliente_aprobo('correo', fields.Date.today())
        self.assertEqual(cot.cliente_medio, 'oc')
        self.assertEqual(len(self.env['product.pricelist.item'].search(
            [('pricelist_id', '=', cot.pricelist_item_id.pricelist_id.id),
             ('product_tmpl_id', '=', self.tela.product_tmpl_id.id)])), 1)

    def test_06_liberado_sin_tarifa_lo_dice(self):
        Project = self.env['project.project']
        dev = Project.create({'name': 'Desarrollo sin tarifa', 'sgi_is_ft': True, 'partner_id': self.partner.id,
                              'sgi_dev_product_name': 'Jersey sin tarifa'})
        self._presentada(project_id=dev.id, product_id=self.tela.id)
        liberado = Project._sgi_dev_stage('liberado')
        if not liberado:
            self.skipTest('Sin etapa Liberado en la base')
        # Como una migración (sin la compuerta de aprobación del cliente); el aviso se dispara igual al
        # pasar a Liberado por la ficha, aquí se llama directo.
        dev.with_context(sgi_dev_migration=True).write({'stage_id': liberado.id})
        dev._qb_sgi_avisar_sin_tarifa()
        bodies = ' '.join(dev.message_ids.mapped('body'))
        self.assertIn('Liberado sin precio en la tarifa', bodies)
        self.assertIn('ninguna cotización se ha marcado ganada', bodies)
