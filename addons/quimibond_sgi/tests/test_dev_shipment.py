# -*- coding: utf-8 -*-
"""57.129.0 (C1, Jose 3.3): envío de la muestra y respuesta del cliente como registro propio."""
import base64
from datetime import timedelta

from odoo import fields
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged

from ..models.sgi_dev_shipment import PARAM_FOLLOWUP_DAYS, PARAM_NOTIFY_JOB, PARAM_OUT_PICKING_TYPE


@tagged('post_install', '-at_install')
class TestDevShipment(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Project = cls.env['project.project']
        m = cls.env.ref('uom.product_uom_meter')
        cls.partner = cls.env['res.partner'].create({'name': 'CLIENTE ENVIO PRUEBA', 'is_company': True})
        cls.contact = cls.env['res.partner'].create({'name': 'Contacto envío', 'parent_id': cls.partner.id,
                                                     'email': 'contacto.envio@example.com'})
        cls.tela = cls.env['product.product'].create({'name': 'tela envío', 'default_code': 'WJ150Q28JNT171X',
                                                      'type': 'consu', 'is_storable': True, 'uom_id': m.id})
        wh = cls.env['stock.warehouse'].search([('company_id', '=', cls.env.company.id)], limit=1)
        cls.location = cls.env['stock.location'].create({'name': '31 DESARROLLOS envío', 'usage': 'internal',
                                                         'location_id': wh.lot_stock_id.id})
        cls.out_type = cls.env['stock.picking.type'].create({
            'name': 'Baja de Muestras prueba', 'code': 'outgoing', 'sequence_code': 'BMP',
            'warehouse_id': wh.id, 'company_id': cls.env.company.id,
            'default_location_src_id': cls.location.id,
            'default_location_dest_id': cls.env.ref('stock.stock_location_customers').id})
        cls.job = cls.env['hr.job'].create({'name': 'ADMINISTRADOR DE VENTAS (prueba)'})
        cls.ventas = cls.env['res.users'].with_context(no_reset_password=True).create({
            'name': 'Ventas prueba', 'login': 'sgi_dev_ship_ventas',
            'group_ids': [(6, 0, [cls.env.ref('base.group_user').id])]})
        cls.env['hr.employee'].create({'name': 'Ventas prueba', 'job_id': cls.job.id, 'user_id': cls.ventas.id})
        Param = cls.env['ir.config_parameter'].sudo()
        Param.set_param(PARAM_OUT_PICKING_TYPE, str(cls.out_type.id))
        Param.set_param(PARAM_NOTIFY_JOB, str(cls.job.id))
        Param.set_param(PARAM_FOLLOWUP_DAYS, '')
        Option = cls.env['sgi.dev.option']
        cls.carrier = Option.create({'kind': 'paqueteria', 'name': 'Paquetería prueba'})
        cls.reason = Option.create({'kind': 'motivo_rechazo_cliente', 'name': 'No cumple el tacto (prueba)'})
        cls.evidence = {'response_file': base64.b64encode(b'ok'), 'response_filename': 'respuesta.pdf'}

    def _dev(self):
        dev = self.Project.create({'name': 'x', 'sgi_is_ft': True, 'partner_id': self.partner.id,
                                   'sgi_dev_product_name': 'Jersey envío', 'sgi_dev_product_id': self.tela.id,
                                   'sgi_dev_contact_id': self.contact.id})
        self.tela.product_tmpl_id.write({'sgi_dev_project_id': dev.id, 'sgi_dev_state': 'desarrollo'})
        muestra = self.env.ref('quimibond_sgi.sgi_dev_stage_muestra')
        dev.with_context(sgi_dev_migration=True).write({'stage_id': muestra.id})
        return dev

    def _verdict(self, dev):
        req = self.env['sgi.dev.lab.request'].create({'project_id': dev.id, 'kind': 'corrida'})
        req.write({'state': 'medida', 'date_measured': fields.Datetime.now(),
                   'verdict_by_id': self.env.uid, 'date_verdict': fields.Datetime.now()})
        return req

    def _shipment(self, dev, **vals):
        return self.env['sgi.dev.shipment'].create(dict({
            'project_id': dev.id, 'medium': 'paqueteria', 'carrier_id': self.carrier.id, 'tracking_ref': 'G-1',
            'roll_ids': [(0, 0, {'length_m': 35.0, 'width_m': 1.7, 'weight_kg': 9.0}),
                         (0, 0, {'length_m': 25.0, 'width_m': 1.7, 'weight_kg': 6.5})]}, **vals))

    def test_01_sin_dictamen_no_hay_envio(self):
        dev = self._dev()
        ship = self._shipment(dev)
        self.assertEqual(ship.product_id, self.tela)
        self.assertEqual(ship.roll_count, 2)
        self.assertAlmostEqual(ship.total_m, 60.0)
        with self.assertRaises(UserError, msg="Sin dictamen de Diseño (C1.11) no hay envío"):
            ship.action_ship()
        with self.assertRaises(UserError, msg="Ni baja"):
            ship.action_create_picking()
        self._verdict(dev)
        ship.write({'carrier_id': False})
        with self.assertRaises(UserError, msg="Paquetería exige paquetería y guía"):
            ship.action_ship()
        ship.write({'carrier_id': self.carrier.id, 'roll_ids': [(5, 0, 0)]})
        with self.assertRaises(UserError, msg="Sin rollos no hay envío"):
            ship.action_ship()

    def test_02_envio_manual_correo_y_aviso(self):
        dev = self._dev()
        self._verdict(dev)
        ship = self._shipment(dev, medium='recoge_cliente', carrier_id=False, tracking_ref=False)
        ship.action_ship()
        self.assertEqual(ship.state, 'enviada')
        self.assertEqual(ship.shipped_by_id, self.env.user)
        self.assertEqual(ship.date_shipped, fields.Date.context_today(ship))
        self.assertEqual(dev.sgi_dev_stage_key, 'respuesta_cliente', "Con el envío el proyecto espera respuesta")
        act = ship.activity_ids.filtered(lambda a: a.user_id == self.ventas)
        self.assertTrue(act, "Actividad al puesto que avisa al cliente")
        action = ship.action_open_mail()
        self.assertEqual(action['res_model'], 'mail.compose.message')
        self.assertEqual(action['context']['default_partner_ids'], [self.contact.id])
        self.assertTrue(action['context']['sgi_dev_shipment_notify'])
        template = self.env.ref('quimibond_sgi.mail_template_sgi_dev_shipment')
        body = template._render_template(template.body_html, 'sgi.dev.shipment', ship.ids, engine='qweb')[ship.id]
        self.assertIn('35.00', body)
        self.assertIn('Recoge el cliente', body)
        ship.with_context(sgi_dev_shipment_notify=True).message_post(body='aviso', message_type='comment',
                                                                     subtype_xmlid='mail.mt_comment')
        self.assertTrue(ship.notified_date)
        self.assertEqual(ship.notified_by_id, self.env.user)
        self.assertFalse(ship.activity_ids.filtered(lambda a: a.user_id == self.ventas), "El aviso cierra la actividad")
        with self.assertRaises(UserError, msg="No se registra dos veces"):
            ship.action_ship()

    def test_03_baja_de_almacen_registra_el_envio(self):
        dev = self._dev()
        self._verdict(dev)
        ship = self._shipment(dev)
        action = ship.action_create_picking()
        picking = self.env['stock.picking'].browse(action['res_id'])
        self.assertEqual(ship.picking_id, picking)
        self.assertEqual(picking.picking_type_id, self.out_type)
        self.assertEqual(picking.partner_id, self.partner)
        self.assertEqual(picking.location_id, self.location)
        self.assertEqual(picking.sgi_dev_shipment_id, ship)
        move = picking.move_ids
        self.assertEqual(move.product_id, self.tela)
        self.assertAlmostEqual(move.product_uom_qty, 60.0, msg="Metros de los rollos en la unidad del artículo")
        with self.assertRaises(UserError, msg="Con baja ligada, el envío se registra al validarla"):
            ship.action_ship()
        picking.action_confirm()
        move.write({'quantity': 60.0, 'picked': True})
        picking.button_validate()
        self.assertEqual(picking.state, 'done')
        self.assertEqual(ship.state, 'enviada', "La baja validada registra el envío")
        self.assertEqual(ship.shipped_at, picking.date_done)
        self.assertEqual(dev.sgi_dev_stage_key, 'respuesta_cliente')

    def test_04_respuesta_aprueba(self):
        dev = self._dev()
        masa = self.env['ficha.tecnica.caracteristica'].search([('code', '=', 'masa')], limit=1)
        line = self.env['sgi.dev.characteristic'].create({'project_id': dev.id, 'caracteristica_id': masa.id,
                                                          'spec_nominal': 150.0, 'in_customer_spec': True})
        self.assertFalse(line.customer_approved)
        self._verdict(dev)
        ship = self._shipment(dev)
        with self.assertRaises(UserError, msg="Antes del envío no hay respuesta"):
            ship.action_register_response()
        ship.action_ship()
        ship.write({'response': 'aprueba', 'response_date': '2026-10-09', 'response_medium': 'correo'})
        with self.assertRaises(UserError, msg="Sin evidencia no se registra"):
            ship.action_register_response()
        ship.write(dict(self.evidence, response_contact_id=self.contact.id, response_ref='RE: muestra'))
        ship.action_register_response()
        self.assertEqual(ship.state, 'respondida')
        self.assertEqual(ship.response_registered_by_id, self.env.user)
        self.assertEqual(dev.sgi_dev_stage_key, 'pilotaje', "Aprueba: el desarrollo pasa a pilotaje")
        self.assertTrue(line.customer_approved, "Y las características de la especificación quedan aprobadas por él")
        self.assertEqual(self.tela.sgi_dev_state, 'pilotaje', "Y el artículo se vende con aviso")

    def test_05_respuesta_cambios_y_rechazo(self):
        dev = self._dev()
        self._verdict(dev)
        ship = self._shipment(dev)
        ship.action_ship()
        rev = dev.sgi_dev_revision
        ship.write(dict(self.evidence, response='cambios', response_date='2026-10-09', response_medium='whatsapp',
                        response_note='Más suave'))
        ship.action_register_response()
        self.assertEqual(dev.sgi_dev_revision, rev + 1, "Pide cambios: sube revisión")
        self.assertEqual(dev.sgi_dev_stage_key, 'muestra', "Y vuelve a Muestra")
        self.assertIn('Más suave', dev.sgi_dev_revision_ids.sorted('id')[-1].note)
        otro = self._shipment(dev)
        otro.action_ship()
        otro.write(dict(self.evidence, response='rechaza', response_date='2026-10-10', response_medium='correo'))
        with self.assertRaises(UserError, msg="Rechazo sin motivo de la lista"):
            otro.action_register_response()
        otro.write({'reject_reason_id': self.reason.id})
        otro.action_register_response()
        self.assertEqual(dev.sgi_dev_stage_key, 'cerrado_sin_producto', "Rechaza: cierra sin producto")
        self.assertEqual(self.tela.sgi_dev_state, 'archivado')

    def test_06_seguimiento_si_no_contesta(self):
        dev = self._dev()
        self._verdict(dev)
        ship = self._shipment(dev)
        ship.action_ship()
        Shipment = self.env['sgi.dev.shipment']
        self.assertEqual(Shipment.cron_sgi_dev_shipment_followup(), 0, "Parámetro vacío: sin seguimiento")
        self.env['ir.config_parameter'].sudo().set_param(PARAM_FOLLOWUP_DAYS, '3')
        self.assertEqual(Shipment.cron_sgi_dev_shipment_followup(), 0, "Sin aviso al cliente no corre el plazo")
        ship.write({'notified_date': fields.Datetime.now() - timedelta(days=4), 'notified_by_id': self.ventas.id})
        self.assertEqual(Shipment.cron_sgi_dev_shipment_followup(), 1)
        self.assertTrue(ship.followup_date)
        self.assertTrue(ship.activity_ids.filtered(lambda a: a.user_id == self.ventas and 'Seguimiento' in a.summary))
        self.assertEqual(Shipment.cron_sgi_dev_shipment_followup(), 0, "No repite hasta que pase otro plazo")
