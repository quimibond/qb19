# -*- coding: utf-8 -*-
"""OLA A — Blindaje de evidencia.

Paso 1 (seguridad): las líneas de evidencia (AMEF, PPAP, hallazgos) no se
borran cuando su padre está publicado/cerrado, salvo el Jefe MAST. En borrador
el equipo edita/elimina libremente (el candado protege lo publicado, no el
trabajo en curso).
"""
from odoo import fields
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestOlaASecurity(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        base = cls.env.ref('base.group_user').id
        sgiu = cls.env.ref('quimibond_sgi.group_sgi_user').id
        auditor = cls.env.ref('quimibond_sgi.group_sgi_auditor').id
        mgr = cls.env.ref('quimibond_sgi.group_sgi_manager').id
        cls.raso = cls.env['res.users'].create(
            {'name': 'A raso', 'login': 'olaa_raso',
             'group_ids': [(6, 0, [base, sgiu])]})
        cls.auditor = cls.env['res.users'].create(
            {'name': 'A auditor', 'login': 'olaa_auditor',
             'group_ids': [(6, 0, [base, sgiu, auditor])]})
        cls.mast = cls.env['res.users'].create(
            {'name': 'A MAST', 'login': 'olaa_mast',
             'group_ids': [(6, 0, [base, sgiu, mgr])]})

    # ---- AMEF ----------------------------------------------------------
    def _fmea_with_line(self):
        fmea = self.env['sgi.fmea'].create({'name': 'AMEF A'})
        line = self.env['sgi.fmea.line'].create({
            'fmea_id': fmea.id, 'step': 'paso',
            'severity': '5', 'occurrence': '5', 'detection': '5'})
        return fmea, line

    def test_01_fmea_line_deletable_in_draft(self):
        fmea, line = self._fmea_with_line()
        self.assertEqual(fmea.state, 'borrador')
        line.with_user(self.raso).unlink()
        self.assertFalse(line.exists(), "En borrador la línea se borra.")

    def test_02_fmea_line_locked_when_vigente(self):
        fmea, line = self._fmea_with_line()
        # Añade acción TERMINADA + re-evaluación a la baja para poder pasar
        # a vigente (candado H-AMEF).
        self.env['sgi.action.line'].create({
            'fmea_line_id': line.id, 'name': 'acc', 'responsible_id': self.mast.id,
            'date_commit': fields.Date.today(), 'date_done': fields.Date.today()})
        line.write({'severity_post': '5', 'occurrence_post': '2',
                    'detection_post': '5'})
        fmea.action_set_vigente()
        self.assertEqual(fmea.state, 'vigente')
        with self.assertRaises(UserError):
            line.with_user(self.raso).unlink()

    def test_03_fmea_line_mast_can_delete_vigente(self):
        fmea, line = self._fmea_with_line()
        self.env['sgi.action.line'].create({
            'fmea_line_id': line.id, 'name': 'acc', 'responsible_id': self.mast.id,
            'date_commit': fields.Date.today(), 'date_done': fields.Date.today()})
        line.write({'severity_post': '5', 'occurrence_post': '2',
                    'detection_post': '5'})
        fmea.action_set_vigente()
        line.with_user(self.mast).unlink()
        self.assertFalse(line.exists(), "MAST sí puede borrar evidencia.")

    # ---- PPAP ----------------------------------------------------------
    def test_04_ppap_element_locked_when_approved(self):
        partner = self.env['res.partner'].create({'name': 'Cliente A', 'is_company': True})
        product = self.env['product.template'].create({'name': 'Prod A'})
        ppap = self.env['sgi.ppap'].create({
            'partner_id': partner.id, 'product_tmpl_id': product.id})
        element = ppap.element_ids[:1]
        self.assertTrue(element, "El PPAP genera elementos del catálogo.")
        # En preparación el raso sí puede borrar.
        extra = self.env['sgi.ppap.element'].create({
            'ppap_id': ppap.id, 'name': 'extra'})
        extra.with_user(self.raso).unlink()
        self.assertFalse(extra.exists())
        # Aprobado: bloqueado. Deja listo el PSW para poder aprobar.
        ppap.element_ids.filtered(lambda e: e.template_id.is_psw).write({'state': 'listo'})
        ppap.element_ids.filtered(lambda e: e.state == 'pendiente').write({'state': 'na'})
        ppap.action_mark_enviado()
        ppap.action_approve()
        self.assertEqual(ppap.state, 'aprobado')
        with self.assertRaises(UserError):
            element.with_user(self.raso).unlink()

    # ---- Hallazgo de auditoría ----------------------------------------
    def test_05_audit_finding_locked_when_closed(self):
        audit = self.env['sgi.audit'].create({'audit_type': 'interna'})
        finding = self.env['sgi.audit.finding'].create({
            'audit_id': audit.id, 'finding_type': 'observacion',
            'description': 'obs', 'disposition': 'sin_accion',
            'reason_no_action': 'n/a'})
        # Auditoría abierta: el auditor puede borrar un hallazgo.
        extra = self.env['sgi.audit.finding'].create({
            'audit_id': audit.id, 'finding_type': 'observacion',
            'disposition': 'sin_accion', 'reason_no_action': 'n/a'})
        extra.with_user(self.auditor).unlink()
        self.assertFalse(extra.exists())
        audit.action_close()
        self.assertEqual(audit.state, 'cerrada')
        with self.assertRaises(UserError):
            finding.with_user(self.auditor).unlink()

    def test_06_force_close_acl_manager_only(self):
        model = self.env['ir.model.data']._xmlid_to_res_id(
            'quimibond_sgi.access_sgi_nc_force_close_user')
        access = self.env['ir.model.access'].browse(model)
        self.assertFalse(access.perm_create,
                         "El transitorio de cierre forzado no lo crea el usuario raso.")
        self.assertFalse(access.perm_write)
