# -*- coding: utf-8 -*-
"""Bloque 1 de la migración de formatos Excel (56.0.0): solicitud de
desarrollo en Proyectos FT, ficha de proceso por máquina, eficiencias de
personal, EPP con renglones y Sign, etiquetas de calibración y lista
maestra global."""
from datetime import date

from odoo.exceptions import ValidationError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestExcelMigration(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.product = cls.env['product.product'].create({'name': 'Tela ZK', 'default_code': 'WR135Q46JNT165'})
        cls.workcenter = cls.env['mrp.workcenter'].create({'name': 'Circular ZK 7'})
        cls.employee = cls.env['hr.employee'].create({'name': 'Operador ZK'})

    def test_01_proyecto_ft_solicitud_de_desarrollo(self):
        project = self.env['project.project'].create({'name': 'FT-099-2046 WR135Q46JNT165', 'sgi_dev_type': 'carda'})
        self.assertTrue(project.sgi_is_ft, "El nombre FT-… marca el proyecto como desarrollo.")
        self.assertEqual(project.sgi_dev_format_code, 'F-P-D01-26')
        project.action_sgi_dev_load_lines()
        names = project.sgi_dev_line_ids.mapped('name')
        self.assertIn("Tipo de fibra", names)
        n = len(project.sgi_dev_line_ids)
        project.action_sgi_dev_load_lines()
        self.assertEqual(len(project.sgi_dev_line_ids), n, "Volver a proponer no duplica renglones.")
        other = self.env['project.project'].create({'name': 'Mantenimiento ZK'})
        self.assertFalse(other.sgi_is_ft)
        self.assertTrue(project.sgi_dev_format_info().startswith('F-P-D01-26'))
        html = self.env['ir.actions.report']._render_qweb_html('quimibond_sgi.report_dev_request_document', project.ids)[0]
        self.assertIn(b'Solicitud de desarrollo', html)

    def test_02_ficha_de_proceso_por_maquina(self):
        Sheet = self.env['sgi.machine.sheet']
        sheet = Sheet.create({'product_id': self.product.id, 'workcenter_id': self.workcenter.id})
        self.assertTrue(sheet.name.startswith('FPM-'))
        self.assertEqual(len(sheet.param_line_ids.filtered(lambda p: p.section == 'tela')), 7)
        sheet.action_set_current()
        self.assertEqual(sheet.state, 'vigente')
        second = Sheet.create({'product_id': self.product.id, 'workcenter_id': self.workcenter.id})
        with self.assertRaises(ValidationError):
            second.write({'state': 'vigente'})
        second.action_set_current()
        self.assertEqual((sheet.state, second.state, second.revision), ('obsoleta', 'vigente', 1))
        action = second.action_new_revision()
        new = Sheet.browse(action['res_id'])
        self.assertEqual((new.state, new.revision, len(new.param_line_ids)), ('borrador', 2, len(second.param_line_ids)))
        html = self.env['ir.actions.report']._render_qweb_html('quimibond_sgi.report_machine_sheet_document', second.ids)[0]
        self.assertIn(b'Ficha t', html)

    def test_03_eficiencias_de_personal(self):
        sheet = self.env['sgi.staff.efficiency'].create({'period_date': date(2046, 3, 1)})
        line = self.env['sgi.staff.efficiency.line'].create({
            'sheet_id': sheet.id, 'employee_id': self.employee.id, 'wage_daily': 300.0,
            'attendance_pct': 5.5, 'housekeeping_pct': 2.0, 'efficiency_pct': 1.0, 'quality_pct': 2.0})
        self.assertAlmostEqual(line.total_pct, 10.5)
        self.assertAlmostEqual(line.amount, 300.0 * 30 * 0.105)
        self.assertAlmostEqual(sheet.amount_total, line.amount)
        with self.assertRaises(ValidationError):
            line.write({'attendance_pct': 6.0})
        with self.assertRaises(Exception):
            self.env['sgi.staff.efficiency.line'].create({'sheet_id': sheet.id, 'employee_id': self.employee.id})
        sheet.action_compute_efficiency()  # sin órdenes de trabajo: no truena ni cambia el %
        self.assertAlmostEqual(line.efficiency_pct, 1.0)
        self.assertEqual(sheet.name, 'Eficiencias 03/2046')
        html = self.env['ir.actions.report']._render_qweb_html('quimibond_sgi.report_staff_efficiency_document', sheet.ids)[0]
        self.assertIn(b'Eficiencias de personal', html)

    def test_04_epp_renglones_y_sign(self):
        delivery = self.env['sgi.epp.delivery'].create({
            'employee_id': self.employee.id,
            'line_ids': [(0, 0, {'quantity': 2, 'uom': 'par', 'name': 'Guantes de nitrilo', 'size': 'M'}),
                         (0, 0, {'quantity': 1, 'uom': 'pz', 'name': 'Casco'})],
        })
        self.assertIn('2 par Guantes de nitrilo talla M', delivery.items)
        self.assertIn('1 pz Casco', delivery.items)
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.epp_sign_template_id', '')
        with self.assertRaises(Exception):
            delivery.action_send_sign_request()
        self.assertEqual(self.env['sgi.epp.delivery']._sgi_sync_from_sign(), 0)
        html = self.env['ir.actions.report']._render_qweb_html('quimibond_sgi.report_epp_delivery_document', delivery.ids)[0]
        self.assertIn(b'Responsiva de entrega', html)

    def test_05_etiquetas_de_calibracion(self):
        equipment = self.env['maintenance.equipment'].create({'name': 'Vernier ZK', 'sgi_is_measuring': True, 'serial_no': 'IMV-99'})
        cal = self.env['sgi.calibration'].create({
            'equipment_id': equipment.id, 'date': date(2046, 1, 10), 'calibration_type': 'interna', 'result': 'conforme'})
        Report = self.env['ir.actions.report']
        html = Report._render_qweb_html('quimibond_sgi.report_calibration_label_document', cal.ids)[0]
        self.assertIn(b'CALIBRADO', html)
        self.assertIn(b'IMV-99', html)
        html = Report._render_qweb_html('quimibond_sgi.report_out_of_service_label_document', cal.ids)[0]
        self.assertIn(b'FUERA DE SERVICIO', html)

    def test_06_lista_maestra_global(self):
        doc = self.env['documents.document'].create({
            'name': 'P-ZK99 Procedimiento ZK', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'procedimiento', 'sgi_code': 'P-ZK99', 'sgi_state': 'vigente'})
        action = self.env.ref('quimibond_sgi.sgi_document_master_list_action')
        self.assertIn('sgi_is_controlled', action.domain)
        self.assertEqual(action.res_model, 'documents.document')
        html = self.env['ir.actions.report']._render_qweb_html('quimibond_sgi.report_master_list_all_document', doc.ids)[0]
        self.assertIn(b'P-ZK99', html)
