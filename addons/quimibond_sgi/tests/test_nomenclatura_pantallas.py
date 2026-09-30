# -*- coding: utf-8 -*-
"""57.20.0 — nomenclatura en pantallas y reportes (D-002, D-003, D-004,
E-006; decisión 1 y D-02): ningún nombre de reporte ni encabezado impreso
lleva la clave del Dropbox escrita a mano. La clave del formato sale solo del
pie en vivo (C-006)."""
import re
from datetime import date

from odoo.tests import TransactionCase, tagged

# Claves del Dropbox: F-P-G05-01, F-IT-P-A10-01-01, IT-P-C11-05, P-C07…
OLD_KEY = re.compile(r'\b(?:F-IT-P|F-P|IT-P)-[A-Z]\d{2}|\bP-[A-Z]\d{2}\b|DOC-3|NC-6')


@tagged('post_install', '-at_install')
class TestNomenclaturaPantallas(TransactionCase):

    def test_01_report_names_without_old_keys(self):
        reports = self.env['ir.actions.report'].search([
            ('report_name', '=like', 'quimibond_sgi.%')])
        self.assertTrue(reports)
        for report in reports:
            self.assertFalse(OLD_KEY.search(report.name or ''),
                             "Reporte con clave del Dropbox: %s" % report.name)
            self.assertFalse(OLD_KEY.search(report.print_report_name or ''),
                             "Archivo con clave del Dropbox: %s" % report.print_report_name)

    # Lo que estaba escrito a mano en los encabezados. El pie en vivo
    # (sgi_format_footer) sí puede decir la clave del formato: esa es la
    # identificación correcta mientras el documento la conserve.
    HARDCODED = ('— F-P-G05-01', 'F-IT-P-A10-01-01', 'F-P-G03-03', 'F-P-G03-05',
                 'F-P-G03-06', 'F-P-G03-07', 'P-S02', 'P-C10', 'P-C07', 'F-P-G01-16')

    def _render(self, report_name, records):
        html = self.env['ir.actions.report']._render_qweb_html(
            report_name, records.ids)[0].decode()
        for text in self.HARDCODED:
            self.assertNotIn(text, html, "%s imprime «%s» escrito a mano." % (report_name, text))
        return html

    def test_02_render_nc_incident_review_audit(self):
        team = self.env.ref('quimibond_sgi.sgi_quality_team_internal')
        alert = self.env['quality.alert'].create({'title': 'NC nomenclatura', 'team_id': team.id})
        self.assertIn('Reporte de no conformidad',
                      self._render('quimibond_sgi.report_nc_document', alert))
        incident = self.env['sgi.incident'].create(
            {'name': 'Incidente nomenclatura', 'incident_type': 'casi_accidente'})
        self.assertIn('Investigación de incidente',
                      self._render('quimibond_sgi.report_incident_document', incident))
        review = self.env['sgi.management.review'].create(
            {'period_from': date(2045, 1, 1), 'period_to': date(2045, 6, 30)})
        self.assertIn('Acta de revisión por la dirección',
                      self._render('quimibond_sgi.report_mgmt_review_document', review))
        process = self.env['sgi.process'].create({'code': 'XNM', 'name': 'Nomenclatura'})
        audit = self.env['sgi.audit'].create({
            'audit_type': 'interna', 'process_ids': [(6, 0, process.ids)],
            'date_start': date(2045, 3, 10), 'date_end': date(2045, 3, 11)})
        self.assertIn('Plan de auditoría',
                      self._render('quimibond_sgi.report_audit_plan_document', audit))
        html = self._render('quimibond_sgi.report_audit_report_document', audit)
        self.assertIn('Reunión de apertura', html)
