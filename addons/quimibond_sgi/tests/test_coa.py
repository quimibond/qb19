# -*- coding: utf-8 -*-
"""COA ligado a la entrega y al pedido (fase 1)."""
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged

from ..models.sgi_coa import sgi_parse_coa_filename


@tagged('post_install', '-at_install')
class TestCoa(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Param = cls.env['ir.config_parameter'].sudo()
        cls.Param.set_param('quimibond_sgi.coa_block_validation', 'False')
        cls.company = cls.env.company
        cls.wh = cls.env['stock.warehouse'].search(
            [('company_id', '=', cls.company.id)], limit=1)
        cls.customers = cls.env.ref('stock.stock_location_customers')
        cls.product = cls.env['product.product'].create({
            'name': 'Tela COA', 'default_code': 'COATEST160', 'type': 'consu'})
        cls.customer = cls.env['res.partner'].create({
            'name': 'Cliente COA', 'is_company': True, 'email': 'calidad@cliente.test'})
        cls.customer.sgi_requires_coa = True
        cls.plant = cls.env['res.partner'].create({
            'name': 'Planta León', 'type': 'delivery', 'parent_id': cls.customer.id})
        cls.plain = cls.env['res.partner'].create({
            'name': 'Cliente sin COA', 'is_company': True})

    def _picking(self, partner, sale=None):
        picking = self.env['stock.picking'].create({
            'picking_type_id': self.wh.out_type_id.id,
            'partner_id': partner.id,
            'location_id': self.wh.lot_stock_id.id,
            'location_dest_id': self.customers.id,
            'sale_id': sale.id if sale else False,
            'move_ids': [(0, 0, {
                'product_id': self.product.id, 'product_uom_qty': 5.0,
                'product_uom': self.product.uom_id.id,
                'location_id': self.wh.lot_stock_id.id,
                'location_dest_id': self.customers.id})],
        })
        picking.action_confirm()
        picking.move_ids.write({'quantity': 5.0, 'picked': True})
        return picking

    def _pdf(self, name='COATEST160 INV-2040-06-0001.pdf'):
        return self.env['ir.attachment'].create({
            'name': name, 'raw': b'%PDF-1.4 coa', 'mimetype': 'application/pdf'})

    def test_01_no_aplica_valida_normal(self):
        picking = self._picking(self.plain)
        self.assertFalse(picking.sgi_requires_coa)
        self.assertEqual(picking.sgi_coa_status, 'no_aplica')
        picking.button_validate()
        self.assertEqual(picking.state, 'done')

    def test_02_requerido_sin_coa_valida_y_queda_pendiente(self):
        picking = self._picking(self.customer)
        self.assertTrue(picking.sgi_requires_coa)
        self.assertEqual(picking.sgi_coa_status, 'pendiente')
        picking.button_validate()
        self.assertEqual(picking.state, 'done', "Sin el parámetro, solo avisa.")
        self.assertEqual(picking.sgi_coa_status, 'pendiente')

    def test_03_adjuntar_y_enviar(self):
        picking = self._picking(self.customer)
        pdf = self._pdf()
        wizard = self.env['sgi.coa.attach.wizard'].with_context(
            default_picking_id=picking.id).create({
                'attachment_ids': [(6, 0, pdf.ids)], 'send': False})
        self.assertEqual(wizard.recipient_ids, self.customer,
                         "Sin destinatarios propios, el contacto de la entrega.")
        wizard.action_confirm()
        self.assertEqual(picking.sgi_coa_status, 'adjunto')
        self.assertTrue(picking.sgi_coa_date)
        self.assertEqual(picking.sgi_coa_uid, self.env.user)
        self.assertEqual((pdf.res_model, pdf.res_id), ('stock.picking', picking.id))
        send = self.env['sgi.coa.attach.wizard'].with_context(
            default_picking_id=picking.id).create({
                'attachment_ids': [(6, 0, self._pdf('COATEST160 extra.pdf').ids)],
                'send': True})
        send.action_confirm()
        self.assertEqual(picking.sgi_coa_status, 'enviado')
        self.assertTrue(picking.sgi_coa_sent_date)
        self.assertTrue(any('COA Cliente COA' in (m.subject or '')
                            for m in picking.message_ids),
                        "El correo con la plantilla queda en el chatter.")

    def test_03b_destinatarios_del_cliente(self):
        contact = self.env['res.partner'].create({
            'name': 'Jefa de calidad cliente', 'parent_id': self.customer.id,
            'email': 'jefa@cliente.test'})
        self.customer.sgi_coa_recipient_ids = [(6, 0, contact.ids)]
        picking = self._picking(self.plant)
        self.assertEqual(picking._sgi_coa_recipients(), contact)

    def test_04_direccion_hija_hereda(self):
        picking = self._picking(self.plant)
        self.assertTrue(picking.sgi_requires_coa,
                        "La planta hereda el requisito de su empresa.")

    def test_04b_marcar_cliente_actualiza_salidas_abiertas(self):
        other = self.env['res.partner'].create({'name': 'Cliente nuevo COA', 'is_company': True})
        picking = self._picking(other)
        self.assertFalse(picking.sgi_requires_coa)
        other.sgi_requires_coa = True
        self.assertTrue(picking.sgi_requires_coa, "La salida abierta toma el requisito.")
        self.assertEqual(picking.sgi_coa_status, 'pendiente',
                         "Y su estado se recalcula con él.")

    def test_05_bloqueo_y_excepcion_jefe_calidad(self):
        self.Param.set_param('quimibond_sgi.coa_block_validation', 'True')
        picking = self._picking(self.customer)
        with self.assertRaises(UserError):
            picking.button_validate()
        # El contexto con motivo no basta si no es el Jefe de Calidad.
        with self.assertRaises(UserError):
            picking.with_context(sgi_coa_exception_reason='x').button_validate()
        job = self.env['hr.job'].create({'name': 'JEFE DE CALIDAD PRUEBA'})
        self.Param.set_param('quimibond_sgi.coa_exception_job_id', str(job.id))
        head = self.env['res.users'].create({
            'name': 'Jefe Calidad', 'login': 'sgi_coa_jefe',
            'group_ids': [(6, 0, [self.env.ref('stock.group_stock_manager').id,
                                  self.env.ref('quality.group_quality_manager').id])]})
        self.env['hr.employee'].create({'name': 'Jefe Calidad', 'user_id': head.id,
                                        'job_id': job.id})
        action = picking.with_user(head).button_validate()
        self.assertEqual(action['res_model'], 'sgi.coa.exception.wizard')
        blank = self.env['sgi.coa.exception.wizard'].with_user(head).create({
            'picking_ids': [(6, 0, picking.ids)], 'reason': '   '})
        with self.assertRaises(UserError):
            blank.action_confirm()
        wizard = self.env['sgi.coa.exception.wizard'].with_user(head).create({
            'picking_ids': [(6, 0, picking.ids)], 'reason': 'Cliente autorizó por correo'})
        wizard.action_confirm()
        self.assertEqual(picking.state, 'done')
        self.assertTrue(any('Cliente autorizó por correo' in (m.body or '')
                            for m in picking.message_ids))

    def test_06_buzon_liga_por_nombre_de_archivo(self):
        order = self.env['sale.order'].create({'partner_id': self.customer.id})
        picking = self._picking(self.customer, sale=order)
        picking.button_validate()
        income = self.env['account.account'].search([('account_type', '=', 'income')], limit=1)
        invoice = self.env['account.move'].create({
            'move_type': 'out_invoice', 'partner_id': self.customer.id,
            'invoice_origin': order.name,
            'invoice_line_ids': [(0, 0, {
                'product_id': self.product.id, 'quantity': 5, 'price_unit': 10.0,
                'account_id': income.id, 'tax_ids': [(6, 0, [])]})]})
        invoice.action_post()
        filename = "COATEST160 %s.pdf" % invoice.name.replace('/', '-')
        self.assertEqual(sgi_parse_coa_filename(filename), ('COATEST160', invoice.name))
        inbox = self.env['sgi.coa.inbox'].create({'name': 'Coa Cliente COA'})
        att = self._pdf(filename)
        att.write({'res_model': 'sgi.coa.inbox', 'res_id': inbox.id})
        inbox._sgi_link_attachments(att)
        self.assertEqual(inbox.state, 'ligado', inbox.result)
        self.assertEqual(inbox.picking_ids, picking)
        self.assertEqual(picking.sgi_coa_status, 'enviado')
        # Lo que no se liga queda en la cola «COA sin ligar».
        lost = self.env['sgi.coa.inbox'].create({'name': 'Coa perdido'})
        bad = self._pdf('COATEST160 INV-1999-01-9999.pdf')
        bad.write({'res_model': 'sgi.coa.inbox', 'res_id': lost.id})
        lost._sgi_link_attachments(bad)
        self.assertEqual(lost.state, 'sin_ligar')
        other = self._picking(self.customer)
        lost.picking_id = other
        lost.action_link_manual()
        self.assertEqual(lost.state, 'ligado')
        self.assertEqual(other.sgi_coa_status, 'enviado')

    def test_07_pedido_con_dos_salidas_parciales(self):
        order = self.env['sale.order'].create({'partner_id': self.customer.id})
        first = self._picking(self.customer, sale=order)
        second = self._picking(self.customer, sale=order)
        first._sgi_coa_register(self._pdf(), sent=True)
        self.assertEqual(order.sgi_coa_status, 'pendiente', "Manda el peor estado.")
        second._sgi_coa_register(self._pdf('COATEST160 b.pdf'))
        self.assertEqual(order.sgi_coa_status, 'adjunto')
        second.sgi_coa_sent_date = second.sgi_coa_date
        self.assertEqual(order.sgi_coa_status, 'enviado')
        self.assertEqual(order.sgi_coa_attachment_count, 2)

    def test_08_otra_compania_no_se_afecta(self):
        other_company = self.env['res.company'].create({'name': 'Otra empresa COA'})
        self.assertTrue(self.customer.with_company(self.company).sgi_requires_coa)
        self.assertFalse(self.customer.with_company(other_company).sgi_requires_coa,
                         "El requisito es por compañía.")
