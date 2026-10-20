# -*- coding: utf-8 -*-
"""57.170.0 — Interfaz, entrega 1 (auditoría docs/audit/ui-sgi-2026-10-20.md).

Lo que se puede probar sin navegador: los campos que se capturan no usan el
widget de solo lectura «badge», el hallazgo de una auditoría cerrada se ve en
solo lectura, y el aviso «Procedimiento vivo modificado» tiene su botón."""
from lxml import etree

from odoo.exceptions import UserError
from odoo.tests import TransactionCase, new_test_user, tagged


@tagged('post_install', '-at_install')
class TestUiEntrega1(TransactionCase):

    def _arch(self, xmlid):
        return etree.fromstring(self.env.ref(xmlid).arch)

    def test_01_campos_que_se_capturan_se_editan(self):
        """El widget «badge» solo muestra: Resultado, Severidad y Veredicto no se podían capturar."""
        for xmlid, field in (('quimibond_sgi.sgi_calibration_view_form', 'result'),
                             ('quimibond_sgi.sgi_incident_view_form', 'severity'),
                             ('quimibond_sgi.sgi_emergency_drill_view_form', 'result'),
                             ('quimibond_sgi.sgi_msa_study_view_form', 'verdict')):
            nodes = self._arch(xmlid).xpath("//field[@name='%s']" % field)
            self.assertTrue(nodes, xmlid)
            self.assertNotIn('badge', [n.get('widget') for n in nodes], xmlid)

    def test_02_un_primario_por_estado_en_el_incidente(self):
        form = self._arch('quimibond_sgi.sgi_incident_view_form')
        primaries = [b for b in form.xpath('//header/button') if 'btn-primary' in (b.get('class') or '')]
        conditions = [b.get('invisible') for b in primaries]
        self.assertIn("state != 'acciones'", conditions, "«Cerrar» es primario solo en acciones.")

    def test_03_hallazgo_de_auditoria_cerrada_en_solo_lectura(self):
        audit = self.env['sgi.audit'].create({})
        finding = self.env['sgi.audit.finding'].create({
            'audit_id': audit.id, 'finding_type': 'observacion', 'description': 'Hallazgo ZU1'})
        user = new_test_user(self.env, login='zu1_aud', groups='base.group_user,quimibond_sgi.group_sgi_auditor')
        self.assertFalse(finding.with_user(user).sgi_is_locked)
        # Solo el estado: cerrar de verdad pide su flujo completo.
        audit.sudo()._write({'state': 'cerrada'})
        self.env.invalidate_all()
        self.assertTrue(finding.with_user(user).sgi_is_locked)
        mast = new_test_user(self.env, login='zu1_mast', groups='base.group_user,quimibond_sgi.group_sgi_manager')
        self.assertFalse(finding.with_user(mast).sgi_is_locked, "El Jefe MAST sí lo edita.")

    def test_04_el_pdf_sigue_vigente(self):
        doc = self.env['documents.document'].create({
            'name': 'Procedimiento ZU1', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_procedure_dirty': True})
        plain = new_test_user(self.env, login='zu1_user', groups='base.group_user,quimibond_sgi.group_sgi_user')
        with self.assertRaises(UserError):
            doc.with_user(plain).action_sgi_procedure_still_valid()
        doc.action_sgi_procedure_still_valid()
        self.assertFalse(doc.sgi_procedure_dirty)
