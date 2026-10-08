# -*- coding: utf-8 -*-
"""57.137.0 (C1, Jessica 2026-10-08): aprobación para iniciar con folio, solicitud de modificación con
5 porqués y firma de Dirección, ruta a la vista y expediente 8.3 / APQP."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestDevChange(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Project = cls.env['project.project']
        cls.Request = cls.env['sgi.dev.change.request']
        m = cls.env.ref('uom.product_uom_meter')
        cls.partner = cls.env['res.partner'].create({'name': 'CLIENTE MODIFICACION PRUEBA', 'is_company': True})
        cls.contact = cls.env['res.partner'].create({'name': 'Contacto modif', 'parent_id': cls.partner.id,
                                                     'email': 'contacto.modif@example.com'})
        cls.tela = cls.env['product.product'].create({'name': 'tela modif', 'default_code': 'WJ150Q28JNT171M',
                                                      'type': 'consu', 'is_storable': True, 'uom_id': m.id})

    def _dev(self, stage='cotizacion', product=True):
        vals = {'name': 'x', 'sgi_is_ft': True, 'partner_id': self.partner.id, 'sgi_dev_product_name': 'Jersey modif',
                'sgi_dev_contact_id': self.contact.id, 'sgi_dev_use': 'Forro de calzado'}
        if product:
            vals['sgi_dev_product_id'] = self.tela.id
        dev = self.Project.create(vals)
        if product:
            self.tela.product_tmpl_id.write({'sgi_dev_project_id': dev.id, 'sgi_dev_state': 'desarrollo'})
        dev.with_context(sgi_dev_migration=True).write({'stage_id': self.Project._sgi_dev_stage(stage).id})
        return dev

    def _request(self, dev, **vals):
        return self.Request.create(dict({
            'project_id': dev.id, 'origin': 'resultado', 'description': 'Bajar el gramaje a 140',
            'cause': 'La tela salió más pesada', 'why_1': 'Más hilo', 'why_2': 'Galga equivocada',
            'root_cause': 'Galga', 'solution': 'Cambiar galga a 28',
            'stage_ids': [(6, 0, [self.Project._sgi_dev_stage('muestra').id])]}, **vals))

    # ---- Aprobación para iniciar ----
    def test_01_aprobacion_para_iniciar_asigna_folio_y_guarda_pdf(self):
        dev = self._dev(stage='cotizacion', product=False)
        self.assertFalse(dev.sgi_ft_folio)
        dev.action_sgi_dev_start_approval()
        self.assertTrue(dev.sgi_ft_folio, "El documento lleva el folio FT: se asigna al generarlo")
        self.assertTrue(dev.sgi_dev_start_approval_attachment_id)
        self.assertEqual(dev.sgi_dev_start_approval_by_id, self.env.user)
        self.assertEqual(dev.sgi_dev_stage_key, 'cotizacion', "Generar el documento no mueve la etapa")
        folio = dev.sgi_ft_folio
        dev.action_sgi_dev_start_approval()
        self.assertEqual(dev.sgi_ft_folio, folio, "Volver a generar no cambia el folio")
        action = dev.action_sgi_dev_start_approval_mail()
        self.assertEqual(action['res_model'], 'mail.compose.message')
        self.assertIn(dev.sgi_dev_start_approval_attachment_id.id, action['context']['default_attachment_ids'])
        self.assertEqual(action['context']['default_partner_ids'], [self.contact.id])
        dev.with_context(sgi_dev_start_approval_notify=True).message_post(body='enviado', message_type='comment')
        self.assertTrue(dev.sgi_dev_start_approval_sent_at)
        self.assertEqual(dev.sgi_dev_start_approval_sent_by_id, self.env.user)

    def test_02_aprobacion_para_iniciar_exige_cliente_y_no_aplica_a_linea(self):
        dev = self._dev(stage='cotizacion', product=False)
        dev.write({'partner_id': False})
        with self.assertRaises(UserError, msg="Sin cliente no hay documento"):
            dev.action_sgi_dev_start_approval()
        dev.write({'partner_id': self.partner.id, 'sgi_dev_analysis_result': 'linea'})
        with self.assertRaises(UserError, msg="Producto de línea: no abre proyecto"):
            dev.action_sgi_dev_start_approval()

    # ---- Solicitud de modificación ----
    def test_03_solicitud_firmada_abre_revision_y_regresa_a_muestra(self):
        dev = self._dev(stage='respuesta_cliente')
        rev = dev.sgi_dev_revision
        req = self._request(dev)
        self.assertEqual(req.sequence_number, 1)
        self.assertEqual(req.state, 'borrador')
        self.assertIn(self.Project._sgi_dev_stage('muestra').id, req.dev_stage_ids.ids)
        req.action_approve()
        self.assertEqual(req.state, 'firmada')
        self.assertEqual(req.approved_by_id, self.env.user)
        self.assertEqual(dev.sgi_dev_revision, rev + 1, "Firmada: abre la revisión siguiente")
        self.assertEqual(req.revision, rev + 1)
        self.assertEqual(dev.sgi_dev_stage_key, 'muestra', "Y el proyecto vuelve a Muestra")
        self.assertTrue(req.attachment_id, "Queda el PDF firmado")
        last = dev.sgi_dev_revision_ids.sorted('id')[-1]
        self.assertIn('Bajar el gramaje', last.note)
        self.assertEqual(last.requested_by, 'interno')
        with self.assertRaises(UserError, msg="Firmada no se firma dos veces"):
            req.action_approve()
        with self.assertRaises(UserError, msg="Firmada no se borra"):
            req.unlink()
        req.action_apply()
        self.assertEqual(req.state, 'aplicada')
        self.assertEqual(req.applied_by_id, self.env.user)
        segunda = self._request(dev)
        self.assertEqual(segunda.sequence_number, 2, "Consecutivo por desarrollo")

    def test_04_solicitud_incompleta_no_se_firma(self):
        dev = self._dev(stage='muestra')
        req = self._request(dev, cause=False, why_1=False, why_2=False)
        with self.assertRaises(UserError, msg="Sin causa ni porqués no se firma"):
            req.action_approve()
        req.write({'cause': 'Peso alto'})
        with self.assertRaises(UserError, msg="Al menos un porqué"):
            req.action_approve()
        req.write({'why_1': 'Galga'})
        req.write({'stage_ids': [(5, 0, 0)]})
        with self.assertRaises(UserError, msg="Fases afectadas obligatorias"):
            req.action_approve()
        req.write({'stage_ids': [(6, 0, [self.Project._sgi_dev_stage('muestra').id])]})
        req.action_approve()
        self.assertEqual(req.state, 'firmada')
        # Cambio que pide el cliente: la causa y los porqués no se exigen.
        cli = self._request(dev, origin='cliente', cause=False, why_1=False, why_2=False, root_cause=False)
        cli.action_approve()
        self.assertEqual(cli.state, 'firmada')
        self.assertEqual(dev.sgi_dev_revision_ids.sorted('id')[-1].requested_by, 'cliente')

    def test_05_respuesta_pide_cambios_deja_solicitud_en_borrador(self):
        dev = self._dev(stage='muestra')
        self.env['sgi.dev.lab.request'].create({'project_id': dev.id, 'kind': 'corrida'}).write({
            'state': 'medida', 'date_measured': '2026-10-08 10:00:00', 'verdict_by_id': self.env.uid,
            'date_verdict': '2026-10-08 10:00:00'})
        ship = self.env['sgi.dev.shipment'].create({
            'project_id': dev.id, 'medium': 'recoge_cliente',
            'roll_ids': [(0, 0, {'length_m': 10.0, 'width_m': 1.7, 'weight_kg': 3.0})]})
        ship.action_ship()
        ship.write({'response': 'cambios', 'response_date': '2026-10-09', 'response_medium': 'whatsapp',
                    'response_note': 'Más suave', 'response_file': b'b2s=', 'response_filename': 'r.pdf'})
        ship.action_register_response()
        req = self.Request.search([('shipment_id', '=', ship.id)])
        self.assertEqual(len(req), 1, "Pide cambios: una solicitud en borrador")
        self.assertEqual(req.state, 'borrador')
        self.assertEqual(req.origin, 'cliente')
        self.assertEqual(req.description, 'Más suave')
        self.assertEqual(req.revision, dev.sgi_dev_revision, "Con la revisión que abrió la respuesta")
        rev = dev.sgi_dev_revision
        req.write({'solution': 'Suavizante', 'stage_ids': [(6, 0, [self.Project._sgi_dev_stage('muestra').id])]})
        req.action_approve()
        self.assertEqual(dev.sgi_dev_revision, rev, "La revisión ya estaba abierta: no sube otra vez")

    # ---- Ruta y expediente ----
    def test_06_ruta_y_expediente(self):
        dev = self._dev(stage='muestra')
        self.assertEqual(dev.sgi_dev_route_count, 0)
        self.assertIn('Sin ruta', dev.sgi_dev_route_html)
        action = dev.action_sgi_dev_open_route()
        self.assertEqual(action['res_model'], 'mrp.bom')
        self.assertEqual(action['context']['default_product_tmpl_id'], self.tela.product_tmpl_id.id)
        wc = self.env['mrp.workcenter'].create({'name': 'Tejido prueba'})
        bom = self.env['mrp.bom'].create({'product_tmpl_id': self.tela.product_tmpl_id.id, 'product_qty': 1,
                                          'operation_ids': [(0, 0, {'name': 'Tejer', 'workcenter_id': wc.id})]})
        dev.invalidate_recordset(['sgi_dev_route_html', 'sgi_dev_route_count'])
        self.assertEqual(dev.sgi_dev_route_count, 1)
        self.assertIn('Tejer', dev.sgi_dev_route_html)
        self.assertIn('Tejido prueba', dev.sgi_dev_route_html)
        self.assertEqual(dev.action_sgi_dev_open_route()['res_id'], bom.id)
        rows = dev._sgi_dev_dossier_rows()
        clauses = {r[0] for r in rows}
        self.assertEqual(clauses, {'8.3.2', '8.3.3', '8.3.4', '8.3.5', '8.3.6'})
        by_label = {r[1]: r for r in rows}
        self.assertTrue(by_label["Salidas: ruta con centros de trabajo"][2])
        self.assertTrue(by_label["Salidas: artículos del desarrollo dados de alta"][2])
        self.assertFalse(by_label["Controles: aprobación del cliente para iniciar registrada con evidencia"][2])
        self.assertIsNone(by_label["Cambios: solicitudes de modificación firmadas por Dirección"][2],
                          "Sin solicitudes: no aplica todavía")
        self.assertTrue(dev.sgi_dev_dossier_total > 0)
        self.assertLessEqual(dev.sgi_dev_dossier_done, dev.sgi_dev_dossier_total)
        self.assertIn('8.3.3', dev.sgi_dev_dossier_html)
        pdf, _kind = self.env['ir.actions.report']._render_qweb_pdf('quimibond_sgi.report_dev_dossier_document', res_ids=dev.ids)
        self.assertTrue(pdf, "El expediente se imprime")
        sin_producto = self._dev(stage='cotizacion', product=False)
        with self.assertRaises(UserError, msg="Sin artículo no hay ruta que editar"):
            sin_producto.action_sgi_dev_open_route()
