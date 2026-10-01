# -*- coding: utf-8 -*-
"""57.90.0 (auditoría 2026-10, K-01, K-02, K-06, K-07, FUNC-C13): la evidencia
no se pierde ni se altera por error.

- Un documento controlado no va a la papelera ni se borra (Odoo lo borra a
  los 30 días y la cascada se llevaba sus acuses).
- Una NC con folio no se borra; sus acciones no se van en cascada.
- Solo el Jefe MAST o el dueño del proceso cierran una NC.
- Los procesos pesados no se disparan por RPC.
- La respuesta del proveedor se guarda escapada en el chatter."""
from datetime import date
from unittest.mock import patch

from psycopg2 import IntegrityError

from odoo.exceptions import AccessError, UserError, ValidationError
from odoo.tests import TransactionCase, new_test_user, tagged
from odoo.tools import mute_logger

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestCandadosEvidencia(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        sgi_hide_real_documents(env)
        # Quien puede editar en Documentos sin ser Jefe MAST: el camino real
        # por el que 3359, 5119 y 4995 terminaron en la papelera.
        cls.docs_editor = new_test_user(
            env, login='k01_docs_editor',
            groups='base.group_user,quimibond_sgi.group_sgi_user,documents.group_documents_manager')
        cls.mast = new_test_user(
            env, login='k01_mast', groups='base.group_user,quimibond_sgi.group_sgi_manager')
        cls.sgi_user = new_test_user(
            env, login='k01_sgi_user', groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.owner_user = new_test_user(
            env, login='k01_owner', groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.owner = env['hr.employee'].create({'name': 'K01 Dueño', 'user_id': cls.owner_user.id})
        cls.process = env['sgi.process'].create({
            'code': 'ZK1', 'name': 'Proceso candados', 'owner_id': cls.owner.id})
        cls.team_int = env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.stage_closed = env.ref('quimibond_sgi.sgi_nc_int_stage_closed')
        cls.employee = env['hr.employee'].create({'name': 'K01 Lector'})

    def _doc(self, state='vigente', code='F-ZK1-01'):
        doc = self.env['documents.document'].create({
            'name': 'K01 %s' % code, 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'formato', 'sgi_code': code, 'sgi_state': state,
            # «formato» exige proceso con clave nueva (_check_sgi_code).
            'sgi_process_id': self.process.id})
        # Un vigente nace de solo lectura (_sgi_share_controlled): se da
        # edición para que el usuario llegue al candado y no al permiso por
        # documento de Documents (mismo patrón que test_dropbox_section).
        if 'access_internal' in doc._fields:
            doc.sudo().write({'access_internal': 'edit'})
        return doc

    def _closable_nc(self):
        # La segunda NC del mismo proceso sale reincidente y pide una acción
        # CORRECTIVA terminada (_sgi_check_can_close): se registra correctiva.
        alert = self.env['quality.alert'].create({
            'title': 'K01 NC', 'team_id': self.team_int.id,
            'sgi_process_id': self.process.id})
        self.env['sgi.action.line'].create({
            'alert_id': alert.id, 'name': 'Corregir K01', 'responsible_id': self.env.user.id,
            'action_type': 'correctiva',
            'date_commit': date.today(), 'date_done': date.today(), 'progress': '100'})
        alert.write({'sgi_root_cause': 'Causa K01', 'sgi_effectiveness_note': 'Eficaz',
                     'sgi_effectiveness_date': date.today()})
        return alert

    def _assert_restricted(self, record):
        # Mismo patrón que test_integridad: «restrict» lo frena la base
        # (IntegrityError) o el ORM (UserError); cualquiera vale.
        raised = None
        try:
            with mute_logger('odoo.sql_db'), self.env.cr.savepoint():
                record.unlink()
                self.env.flush_all()
        except (IntegrityError, UserError) as exc:
            raised = exc
        self.assertIsNotNone(raised, "%s se borró estando en uso." % record.display_name)
        self.env.invalidate_all()
        self.assertTrue(record.exists())

    # ---- K-01 ---------------------------------------------------------------
    def test_01_documento_controlado_no_va_a_la_papelera(self):
        doc = self._doc()
        with self.assertRaisesRegex(UserError, 'papelera'):
            doc.with_user(self.docs_editor).write({'active': False})
        with self.assertRaisesRegex(UserError, 'papelera'):
            doc.with_user(self.docs_editor).action_archive()
        self.assertTrue(doc.active)

    def test_02_mast_si_archiva_y_el_borrador_no_tiene_candado(self):
        doc = self._doc(code='F-ZK1-02')
        doc.with_user(self.mast).write({'active': False})
        self.assertFalse(doc.active)
        draft = self._doc(state='borrador', code='F-ZK1-03')
        draft.with_user(self.docs_editor).write({'active': False})
        self.assertFalse(draft.active)

    def test_03_documento_controlado_no_se_borra(self):
        doc = self._doc(code='F-ZK1-04')
        with self.assertRaisesRegex(UserError, 'papelera'):
            doc.with_user(self.docs_editor).unlink()

    def test_04_el_acuse_detiene_el_borrado_fisico(self):
        doc = self._doc(code='F-ZK1-05')
        self.env['sgi.document.ack'].create({'document_id': doc.id, 'employee_id': self.employee.id})
        # La autolimpieza de la papelera corre como superusuario: lo que la
        # detiene es la llave foránea, no el candado de Python. Se archiva
        # primero para seguir el camino real (papelera → borrado).
        doc.sudo().write({'active': False})
        self._assert_restricted(doc.sudo())

    # ---- K-02 ---------------------------------------------------------------
    def test_05_nc_con_folio_no_se_borra(self):
        alert = self.env['quality.alert'].create({'title': 'K02 NC', 'team_id': self.team_int.id})
        self.assertTrue(alert.sgi_folio)
        with self.assertRaises(UserError):
            alert.with_user(self.sgi_user).unlink()

    def test_06_accion_detiene_el_borrado_de_su_nc(self):
        alert = self.env['quality.alert'].create({'title': 'K02 NC 2', 'team_id': self.team_int.id})
        self.env['sgi.action.line'].create({
            'alert_id': alert.id, 'name': 'Acción K02', 'responsible_id': self.env.user.id,
            'date_commit': date.today()})
        self._assert_restricted(alert.sudo())

    # ---- FUNC-C13 -----------------------------------------------------------
    def test_07_usuario_sgi_no_cierra_una_nc(self):
        alert = self._closable_nc()
        with self.assertRaises(UserError):
            alert.with_user(self.sgi_user).write({'stage_id': self.stage_closed.id})

    def test_08_dueno_del_proceso_y_mast_si_cierran(self):
        alert = self._closable_nc()
        alert.with_user(self.owner_user).write({'stage_id': self.stage_closed.id})
        self.assertEqual(alert.stage_id, self.stage_closed)
        other = self._closable_nc()
        other.with_user(self.mast).write({'stage_id': self.stage_closed.id})
        self.assertEqual(other.stage_id, self.stage_closed)

    # ---- K-06 ---------------------------------------------------------------
    def test_09_procesos_pesados_solo_del_sistema(self):
        calls = [
            lambda: self.env['sgi.activity.role'].with_user(self.mast).cron_sgi_sync_approvals(),
            lambda: self.env['sgi.checklist.template'].with_user(self.mast).cron_generate(),
            lambda: self.env['sgi.process.activity'].with_user(self.mast).cron_measure_activities(),
            lambda: self.env['sgi.indicator'].with_user(self.mast).cron_missing_trajectories(),
        ]
        for call in calls:
            with self.assertRaises(AccessError):
                call()

    # ---- K-07 ---------------------------------------------------------------
    def test_10_respuesta_del_proveedor_escapada(self):
        alert = self.env['quality.alert'].create({'title': 'K07 NC', 'team_id': self.team_int.id})
        alert.sudo().write({'sgi_supplier_state': 'enviada'})
        alert._sgi_supplier_answer('<script>x</script>Causa', '<b>Acción</b>')
        body = "".join(alert.message_ids.mapped('body'))
        self.assertIn('&lt;script&gt;', body)
        self.assertNotIn('<script>', body)
        self.assertIn('<b>Respuesta del proveedor</b>', body, "El formato propio sí se conserva.")

    # ---- K-01 (papelera) -----------------------------------------------------
    def _ack(self, doc):
        return self.env['sgi.document.ack'].create({'document_id': doc.id, 'employee_id': self.employee.id})

    def _rescue(self):
        self.env['documents.document']._gc_sgi_rescue_trashed_with_acks()
        self.env.invalidate_all()

    def _rescue_messages(self, doc):
        return doc.message_ids.filtered(lambda m: 'Rescatado de la papelera' in (m.body or ''))

    def test_11_la_papelera_rescata_lo_que_tiene_acuses(self):
        doc = self._doc(code='F-ZK1-06')
        self._ack(doc)
        doc.with_user(self.mast).write({'active': False})
        self._rescue()
        self.assertTrue(doc.active)
        self.assertEqual(doc.sgi_state, 'obsoleto')
        self.assertEqual(doc.sgi_obsolete_reason, "Rescatado de la papelera: tiene acuses de lectura.")
        self.assertEqual(len(self._rescue_messages(doc)), 1)

    def test_12_rescate_conserva_la_fecha_de_un_obsoleto(self):
        doc = self._doc(state='obsoleto', code='F-ZK1-07')
        doc.sudo().write({'sgi_obsolete_date': date(2025, 1, 15), 'sgi_obsolete_reason': 'Motivo original'})
        self._ack(doc)
        doc.with_user(self.mast).write({'active': False})
        self._rescue()
        self.assertTrue(doc.active)
        self.assertEqual(doc.sgi_state, 'obsoleto')
        self.assertEqual(doc.sgi_obsolete_date, date(2025, 1, 15))
        self.assertEqual(doc.sgi_obsolete_reason, 'Motivo original')

    def test_13_sin_acuses_no_se_toca(self):
        doc = self._doc(code='F-ZK1-08')
        doc.with_user(self.mast).write({'active': False})
        self._rescue()
        self.assertFalse(doc.active)
        self.assertEqual(doc.sgi_state, 'vigente')
        self.assertFalse(self._rescue_messages(doc))

    def test_14_rescate_idempotente(self):
        doc = self._doc(code='F-ZK1-09')
        self._ack(doc)
        doc.with_user(self.mast).write({'active': False})
        self._rescue()
        self._rescue()
        self.assertTrue(doc.active)
        self.assertEqual(len(self._rescue_messages(doc)), 1)

    def test_15_uno_que_falla_no_detiene_a_los_demas(self):
        bad = self._doc(code='F-ZK1-10')
        good = self._doc(code='F-ZK1-11')
        self._ack(bad)
        self._ack(good)
        (bad | good).with_user(self.mast).write({'active': False})
        Doc = type(self.env['documents.document'])
        original = Doc.action_unarchive

        def fake_unarchive(records):
            if bad.id in records.ids:
                raise ValidationError("Ya existe otra revisión activa (simulado).")
            return original(records)

        with patch.object(Doc, 'action_unarchive', fake_unarchive), \
                mute_logger('odoo.addons.quimibond_sgi.models.sgi_document'):
            self._rescue()
        self.assertFalse(bad.active)
        self.assertEqual(bad.sgi_state, 'vigente')
        self.assertTrue(good.active)
        self.assertEqual(good.sgi_state, 'obsoleto')

    def test_16_lote_mixto_no_se_archiva(self):
        draft = self._doc(state='borrador', code='F-ZK1-12')
        current = self._doc(code='F-ZK1-13')
        with self.assertRaisesRegex(UserError, 'papelera'):
            (draft | current).with_user(self.docs_editor).write({'active': False})
        self.assertTrue(draft.active)
        self.assertTrue(current.active)
