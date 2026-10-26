# -*- coding: utf-8 -*-
"""57.173.0 — Interfaz, entrega 4 (auditoría docs/audit/ui-sgi-2026-10-20.md).

Las instrucciones dejan la ficha y van al `help` del campo (el globo del «?»);
en la ficha queda a lo más una línea o un aviso que depende del estado."""
from lxml import etree

from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestUiEntrega4(TransactionCase):

    def _text(self, xmlid):
        return ' '.join(' '.join(etree.fromstring(self.env.ref(xmlid).arch).itertext()).split())

    def test_01_instrucciones_fuera_de_la_ficha(self):
        for xmlid, phrase in (
                ('quimibond_sgi.sgi_process_flow_view_form', '¿Qué es un flujo?'),
                ('quimibond_sgi.sgi_indicator_view_form', 'Los escalones trimestrales se interpolan'),
                ('quimibond_sgi.sgi_indicator_view_form', "'{cierre-30d}'"),
                ('quimibond_sgi.sgi_management_review_view_form', 'Flujo: defina el periodo'),
                ('quimibond_sgi.sgi_risk_view_form', 'IPER usa escala'),
                ('quimibond_sgi.sgi_fmea_view_form', 'acción TERMINADA'),
                ('quimibond_sgi.sgi_audit_view_form', 'Una pregunta por actividad'),
                ('quimibond_sgi.sgi_document_view_form', 'Clave nueva: PR-'),
                ('quimibond_sgi.sgi_document_view_form', 'Ya migrado: cambia')):
            self.assertNotIn(phrase, self._text(xmlid), xmlid)

    def test_02_la_regla_queda_en_el_help(self):
        fields_help = (
            ('sgi.process.flow', 'name', 'entrega entre dos procesos'),
            ('sgi.indicator', 'step_ids', 'Escalones trimestrales'),
            ('sgi.indicator.term', 'domain', '{cierre-2dh}'),
            ('sgi.risk', 'attention_level', 'IPER'),
            ('sgi.fmea', 'line_ids', 'NPR = S × O × D'),
            ('sgi.audit', 'checklist_line_ids', 'Conforme'),
            ('documents.document', 'sgi_job_ids', 'Generar acuses'),
        )
        for model, field, phrase in fields_help:
            self.assertIn(phrase, self.env[model]._fields[field].help or '', "%s.%s" % (model, field))

    def test_03_aviso_de_documento_sin_puestos(self):
        arch = etree.fromstring(self.env.ref('quimibond_sgi.sgi_document_view_form').arch)
        warnings = arch.xpath("//page[@name='puestos']/div[contains(@class, 'alert-warning')]")
        self.assertEqual([w.get('invisible') for w in warnings], ["sgi_job_ids or sgi_state != 'vigente'"])
