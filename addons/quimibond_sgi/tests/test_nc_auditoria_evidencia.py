# -*- coding: utf-8 -*-
"""57.93.0 (auditoría 2026-10: N-02, N-03, N-12, K-03 y lo que quedó de
FUNC-C13 en 57.91.0): la NC y la auditoría cierran con evidencia.

- La eficacia tiene resultado (Eficaz / No eficaz) y se registra en o después
  de la fecha programada; «No eficaz» regresa la NC a Seguimiento y pide una
  acción correctiva nueva. La verificación le llega a quien puede cerrar.
- Una acción correctiva no se termina sin evidencia.
- Una NC no nace cerrada ni cancelada.
- Con la NC, el incidente o la revisión cerrados, sus acciones terminadas y
  la NC misma solo las modifica el Jefe MAST; igual los hallazgos de una
  auditoría cerrada.
- Una NC menor o mayor de auditoría exige su NC ligada; el programa sugerido
  incluye los procesos en borrador y avisa la cobertura de 3 años.
- Toda recepción desde la ubicación de clientes levanta la NC de devolución."""
from datetime import date, timedelta

from odoo import fields
from odoo.exceptions import UserError
from odoo.tests import TransactionCase, new_test_user, tagged

from .common_users import assert_locked, sgi_set_mast


class _NcEvidenciaCase(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.mast = sgi_set_mast(env, login='n02_mast')
        cls.sgi_user = new_test_user(
            env, login='n02_user', groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.owner_user = new_test_user(
            env, login='n02_owner', groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.owner = env['hr.employee'].create({'name': 'N02 Dueño', 'user_id': cls.owner_user.id})
        cls.process = env['sgi.process'].create({
            'code': 'ZN2', 'name': 'Proceso eficacia', 'owner_id': cls.owner.id})
        cls.team = env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.stage_open = env.ref('quimibond_sgi.sgi_nc_int_stage_open')
        cls.stage_follow = env.ref('quimibond_sgi.sgi_nc_int_stage_followup')
        cls.stage_closed = env.ref('quimibond_sgi.sgi_nc_int_stage_closed')
        cls.stage_cancel = env.ref('quimibond_sgi.sgi_nc_int_stage_cancel')

    def _today(self):
        # La misma fecha que usa el código (zona horaria del usuario).
        return fields.Date.context_today(self.env.user)

    def _nc(self, **vals):
        return self.env['quality.alert'].create(dict({
            'title': 'N02 NC', 'team_id': self.team.id, 'stage_id': self.stage_follow.id,
            'sgi_process_id': self.process.id, 'sgi_root_cause': 'Causa N02'}, **vals))

    def _line(self, alert, **vals):
        return self.env['sgi.action.line'].create(dict({
            'alert_id': alert.id, 'name': 'Acción N02', 'action_type': 'correctiva',
            'responsible_id': self.sgi_user.id, 'date_commit': self._today()}, **vals))

    def _eficaz(self, alert):
        alert.write({'sgi_effective': 'eficaz', 'sgi_effectiveness_note': 'Sin reincidencia',
                     'sgi_effectiveness_date': self._today()})

    def _summaries(self, alert, prefix):
        return alert.activity_ids.filtered(lambda a: (a.summary or '').startswith(prefix))


@tagged('post_install', '-at_install')
class TestNcEficacia(_NcEvidenciaCase):

    def test_01_eficacia_antes_de_la_fecha_no_cierra(self):
        nc = self._nc()
        self._line(nc).action_mark_done()   # superusuario: sin candado de evidencia
        self.assertEqual(nc.sgi_effectiveness_due, self._today() + timedelta(days=90))
        self._eficaz(nc)
        with self.assertRaisesRegex(UserError, 'se programó'):
            nc.write({'stage_id': self.stage_closed.id})
        # La salida cuando no se puede esperar: cierre forzado con motivo.
        self.env['sgi.nc.force.close'].with_user(self.mast).create({
            'alert_id': nc.id, 'reason': 'El cliente dejó de comprar el producto'}).action_confirm()
        self.assertEqual(nc.stage_id, self.stage_closed)

    def test_02_eficaz_cierra_cuando_llega_la_fecha(self):
        nc = self._nc()
        self._line(nc).action_mark_done()
        nc.write({'sgi_effectiveness_due': self._today()})  # llegó la fecha programada
        self._eficaz(nc)
        nc.with_user(self.owner_user).write({'stage_id': self.stage_closed.id})
        self.assertEqual(nc.stage_id, self.stage_closed)

    def test_03_sin_resultado_no_cierra(self):
        nc = self._nc(sgi_effectiveness_note='Sin reincidencia',
                      sgi_effectiveness_date=self._today())
        self._line(nc, date_done=self._today())
        with self.assertRaisesRegex(UserError, 'Eficaz'):
            nc.write({'stage_id': self.stage_closed.id})
        nc.write({'sgi_effective': 'eficaz'})
        nc.write({'stage_id': self.stage_closed.id})
        self.assertEqual(nc.stage_id, self.stage_closed)

    def test_04_no_eficaz_regresa_y_pide_correctiva_nueva(self):
        nc = self._nc()
        self._line(nc).action_mark_done()
        nc.write({'sgi_effectiveness_due': self._today()})
        nc.write({'sgi_effective': 'no_eficaz', 'sgi_effectiveness_note': 'Volvió a pasar en el turno 3',
                  'sgi_effectiveness_date': self._today()})
        self.assertEqual(nc.stage_id, self.stage_follow)
        self.assertEqual(nc.sgi_ineffective_count, 1)
        # El resultado se limpia para la verificación siguiente; el «No
        # eficaz» queda en el chatter y en el contador.
        self.assertFalse(nc.sgi_effective)
        self.assertFalse(nc.sgi_effectiveness_note or nc.sgi_effectiveness_date or nc.sgi_effectiveness_due)
        self.assertIn('Volvió a pasar en el turno 3', "".join(nc.message_ids.mapped('body')))
        self.assertFalse(self._summaries(nc, 'Verificar eficacia'))
        todo = self._summaries(nc, 'Registrar acción correctiva nueva')
        self.assertEqual(todo.user_id, self.owner_user)
        self._eficaz(nc)
        with self.assertRaisesRegex(UserError, 'No eficaz'):
            nc.write({'stage_id': self.stage_closed.id})
        # La correctiva nueva cierra el aviso y vuelve a programar la eficacia.
        nc.write({'sgi_effectiveness_note': False, 'sgi_effectiveness_date': False})
        second = self._line(nc, name='Correctiva nueva N02')
        self.assertEqual(second.effectiveness_round, 1)
        self.assertFalse(self._summaries(nc, 'Registrar acción correctiva nueva'))
        second.action_mark_done()
        self.assertEqual(nc.sgi_effectiveness_due, self._today() + timedelta(days=90))
        nc.write({'sgi_effectiveness_due': self._today()})
        self._eficaz(nc)
        nc.write({'stage_id': self.stage_closed.id})
        self.assertEqual(nc.stage_id, self.stage_closed)

    def test_04b_no_eficaz_dos_veces(self):
        nc = self._nc()
        self._line(nc).action_mark_done()
        nc.write({'sgi_effectiveness_due': self._today()})
        nc.write({'sgi_effective': 'no_eficaz', 'sgi_effectiveness_note': 'Ronda 1 no eficaz',
                  'sgi_effectiveness_date': self._today()})
        second = self._line(nc, name='Correctiva ronda 2')
        self.assertEqual(second.effectiveness_round, 1)
        second.action_mark_done()
        nc.write({'sgi_effectiveness_due': self._today()})
        nc.write({'sgi_effective': 'no_eficaz', 'sgi_effectiveness_note': 'Ronda 2 no eficaz',
                  'sgi_effectiveness_date': self._today()})
        self.assertEqual(nc.sgi_ineffective_count, 2)
        self.assertFalse(nc.sgi_effective)
        self.assertEqual(nc.stage_id, self.stage_follow)
        bodies = "".join(nc.message_ids.mapped('body'))
        self.assertIn('Ronda 1 no eficaz', bodies)
        self.assertIn('Ronda 2 no eficaz', bodies)
        todo = self._summaries(nc, 'Registrar acción correctiva nueva')
        self.assertEqual(len(todo), 1, "Un aviso nuevo para la segunda ronda.")
        self.assertEqual(todo.user_id, self.owner_user)
        # La correctiva de la ronda 1 no basta para la ronda 2.
        self._eficaz(nc)
        with self.assertRaisesRegex(UserError, 'No eficaz'):
            nc.write({'stage_id': self.stage_closed.id})

    def test_04c_en_abierta_no_cambia_de_etapa(self):
        """Una NC en Abierta se queda ahí: moverla chocaría con la contención
        obligatoria de las reclamaciones (NC-2)."""
        nc = self._nc(stage_id=self.stage_open.id)
        self._line(nc).action_mark_done()
        nc.write({'sgi_effective': 'no_eficaz', 'sgi_effectiveness_note': 'Reincidió',
                  'sgi_effectiveness_date': self._today()})
        self.assertEqual(nc.stage_id, self.stage_open)
        self.assertEqual(nc.sgi_ineffective_count, 1)

    def test_04d_cierre_en_un_solo_write(self):
        """El formulario manda lo capturado y la etapa en un solo write: los
        candados se revisan con lo capturado."""
        nc = self._nc()
        self._line(nc, date_done=self._today())
        nc.with_user(self.owner_user).write({
            'sgi_effective': 'eficaz', 'sgi_effectiveness_note': 'Sin reincidencia',
            'sgi_effectiveness_date': self._today(), 'stage_id': self.stage_closed.id})
        self.assertEqual(nc.stage_id, self.stage_closed)

    def test_04e_cierre_sin_resultado_en_el_mismo_write(self):
        nc = self._nc(sgi_effective='eficaz', sgi_effectiveness_note='Sin reincidencia',
                      sgi_effectiveness_date=self._today())
        self._line(nc, date_done=self._today())
        with self.assertRaisesRegex(UserError, 'Eficaz'):
            nc.with_user(self.owner_user).write(
                {'sgi_effective': False, 'stage_id': self.stage_closed.id})
        with self.assertRaisesRegex(UserError, 'Eficaz'):
            nc.with_user(self.owner_user).write(
                {'sgi_effective': 'no_eficaz', 'stage_id': self.stage_closed.id})
        nc.invalidate_recordset()
        self.assertEqual(nc.stage_id, self.stage_follow)
        self.assertEqual(nc.sgi_effective, 'eficaz')
        self.assertEqual(nc.sgi_ineffective_count, 0)

    def test_04f_contador_y_fecha_programada_son_del_sistema(self):
        nc = self._nc()
        self._line(nc).action_mark_done()
        due = nc.sgi_effectiveness_due
        nc.write({'sgi_effectiveness_due': self._today()})
        nc.write({'sgi_effective': 'no_eficaz', 'sgi_effectiveness_note': 'Reincidió',
                  'sgi_effectiveness_date': self._today()})
        self.assertEqual(nc.sgi_ineffective_count, 1)
        nc.with_user(self.owner_user).write({'sgi_ineffective_count': 0})
        nc.with_user(self.owner_user).write({'sgi_effectiveness_due': due})
        nc.invalidate_recordset()
        self.assertEqual(nc.sgi_ineffective_count, 1, "El contador no se escribe desde el cliente.")
        self.assertFalse(nc.sgi_effectiveness_due, "La fecha programada no se escribe desde el cliente.")

    def test_05_mast_reabre_una_nc_cerrada_no_eficaz(self):
        nc = self._nc(sgi_effective='eficaz', sgi_effectiveness_note='Sin reincidencia',
                      sgi_effectiveness_date=self._today())
        self._line(nc, date_done=self._today())
        nc.write({'stage_id': self.stage_closed.id})
        nc.with_user(self.mast).write({'sgi_effective': 'no_eficaz',
                                       'sgi_effectiveness_note': 'Reincidió en planta'})
        self.assertEqual(nc.stage_id, self.stage_follow)
        self.assertEqual(nc.sgi_ineffective_count, 1)

    def test_06_la_verificacion_le_llega_a_quien_puede_cerrar(self):
        nc = self._nc()
        self._line(nc).action_mark_done()
        self.assertEqual(self._summaries(nc, 'Verificar eficacia').user_id, self.owner_user)
        orphan = self.env['sgi.process'].create({'code': 'ZN2B', 'name': 'Proceso sin dueño'})
        nc2 = self._nc(sgi_process_id=orphan.id)
        self._line(nc2).action_mark_done()
        self.assertEqual(self._summaries(nc2, 'Verificar eficacia').user_id, self.mast)

    def test_07_nc_no_nace_cerrada_ni_cancelada(self):
        Alert = self.env['quality.alert'].with_user(self.sgi_user)
        for stage in (self.stage_closed, self.stage_cancel):
            with self.assertRaisesRegex(UserError, 'nace abierta'):
                Alert.create({'title': 'N02 nace cerrada', 'team_id': self.team.id,
                              'stage_id': stage.id})
        with self.assertRaisesRegex(UserError, 'nace abierta'):
            Alert.with_context(default_stage_id=self.stage_closed.id).create(
                {'title': 'N02 nace cerrada', 'team_id': self.team.id})
        self.assertFalse(self.env['quality.alert'].search([('title', '=', 'N02 nace cerrada')]))
        legacy = self.env['quality.alert'].with_user(self.mast).create({
            'title': 'N02 carga histórica', 'team_id': self.team.id, 'stage_id': self.stage_closed.id})
        self.assertEqual(legacy.stage_id, self.stage_closed)

    def test_07b_nc_duplicada_nace_abierta(self):
        closed = self.env['quality.alert'].create({
            'title': 'N02 cerrada original', 'team_id': self.team.id,
            'stage_id': self.stage_closed.id})
        dup = closed.with_user(self.sgi_user).copy()
        self.assertEqual(dup.stage_id, self.stage_open)
        self.assertFalse(dup.stage_id.sgi_is_closing_stage or dup.stage_id.sgi_is_cancel_stage)
        self.assertNotEqual(dup.sgi_folio, closed.sgi_folio)


@tagged('post_install', '-at_install')
class TestAccionEvidencia(_NcEvidenciaCase):

    def test_08_correctiva_sin_evidencia_no_se_termina(self):
        line = self._line(self._nc())
        with self.assertRaisesRegex(UserError, 'evidencia'):
            line.with_user(self.sgi_user).action_mark_done()
        self.assertFalse(line.date_done)
        line.with_user(self.sgi_user).write(
            {'evidence_note': 'OT-1234 firmada por el supervisor del turno'})
        line.with_user(self.sgi_user).action_mark_done()
        self.assertTrue(line.date_done)

    def test_09_correccion_no_pide_evidencia(self):
        line = self._line(self._nc(), action_type='correccion')
        line.with_user(self.sgi_user).action_mark_done()
        self.assertTrue(line.date_done)

    def test_10_desde_el_chatter_con_archivo(self):
        nc = self._nc()
        line = self._line(nc)
        activity = line.activity_id
        self.assertTrue(activity)
        with self.assertRaisesRegex(UserError, 'evidencia'):
            activity.with_user(self.sgi_user).action_feedback(feedback="Hecho")
        attachment = self.env['ir.attachment'].create({
            'name': 'foto_evidencia.jpg', 'raw': b'evidencia',
            'res_model': 'quality.alert', 'res_id': nc.id})
        activity.with_user(self.sgi_user).action_feedback(
            feedback="Hecho", attachment_ids=attachment.ids)
        line.invalidate_recordset()
        self.assertTrue(line.date_done)
        self.assertIn(attachment, line.evidence_attachment_ids)


@tagged('post_install', '-at_install')
class TestCerradoEsEvidencia(_NcEvidenciaCase):

    def _closed_nc(self):
        nc = self._nc(sgi_effective='eficaz', sgi_effectiveness_note='Sin reincidencia',
                      sgi_effectiveness_date=self._today())
        line = self._line(nc, date_done=self._today(), evidence_note='OT-1')
        nc.write({'stage_id': self.stage_closed.id})
        return nc, line

    def test_11_nc_cerrada_solo_la_modifica_mast(self):
        nc, _line = self._closed_nc()
        assert_locked(self, nc.with_user(self.sgi_user).write, {'sgi_root_cause': 'Otra causa'})
        assert_locked(self, nc.with_user(self.sgi_user).write, {'stage_id': self.stage_follow.id})
        nc.with_user(self.sgi_user).message_post(body="Comentario después del cierre")
        nc.with_user(self.mast).write({'sgi_followup_comments': 'Nota del Jefe MAST'})
        # D-009: el dueño del proceso sí la reabre (solo la etapa).
        nc.with_user(self.owner_user).write({'stage_id': self.stage_follow.id})
        self.assertEqual(nc.stage_id, self.stage_follow)

    def test_12_acciones_de_una_nc_cerrada(self):
        nc, line = self._closed_nc()
        assert_locked(self, line.with_user(self.sgi_user).write, {'date_done': False})
        assert_locked(self, line.with_user(self.sgi_user).write, {'name': 'Otra cosa'})
        assert_locked(self, line.with_user(self.sgi_user).unlink)
        assert_locked(self, self.env['sgi.action.line'].with_user(self.sgi_user).create, {
            'alert_id': nc.id, 'name': 'Tarde', 'action_type': 'correccion',
            'responsible_id': self.sgi_user.id, 'date_commit': self._today()})
        line.with_user(self.mast).write({'name': 'Redacción corregida por el Jefe MAST'})

    def test_13_accion_pendiente_de_nc_forzada_se_termina(self):
        nc = self._nc()
        line = self._line(nc, action_type='correccion')
        self.env['sgi.nc.force.close'].with_user(self.mast).create({
            'alert_id': nc.id, 'reason': 'Proveedor dado de baja'}).action_confirm()
        line.with_user(self.sgi_user).action_mark_done()
        self.assertTrue(line.date_done)

    def test_14_incidente_y_revision_cerrados(self):
        incident = self.env['sgi.incident'].create({
            'name': 'N02 incidente', 'incident_type': 'casi_accidente',
            'immediate_causes': 'a', 'basic_causes': 'b', 'lack_of_control': 'c'})
        inc_line = self.env['sgi.action.line'].create({
            'incident_id': incident.id, 'name': 'Guarda en la cortadora', 'action_type': 'correccion',
            'responsible_id': self.sgi_user.id, 'date_commit': self._today(),
            'date_done': self._today()})
        incident.write({'state': 'cerrado'})
        assert_locked(self, inc_line.with_user(self.sgi_user).write, {'date_done': False})
        review = self.env['sgi.management.review'].create({
            'period_from': date(2049, 1, 1), 'period_to': date(2049, 6, 30), 'state': 'realizada'})
        done = self.env['sgi.action.line'].create({
            'review_id': review.id, 'name': 'Acuerdo cumplido', 'action_type': 'correccion',
            'responsible_id': self.sgi_user.id, 'date_commit': self._today(),
            'date_done': self._today()})
        pending = self.env['sgi.action.line'].create({
            'review_id': review.id, 'name': 'Acuerdo en curso', 'action_type': 'correccion',
            'responsible_id': self.sgi_user.id, 'date_commit': self._today()})
        review.write({'state': 'cerrada'})
        assert_locked(self, done.with_user(self.sgi_user).write, {'date_done': False})
        # Los acuerdos pendientes se siguen trabajando con la revisión cerrada.
        pending.with_user(self.sgi_user).action_mark_done()
        self.assertTrue(pending.date_done)

    def _force_closed(self, nc):
        self.env['sgi.nc.force.close'].with_user(self.mast).create({
            'alert_id': nc.id, 'reason': 'Proveedor dado de baja'}).action_confirm()
        self.assertTrue(nc.stage_id.sgi_is_closing_stage)

    def test_14b_lista_de_acciones_desde_la_nc_cerrada(self):
        nc = self._nc()
        done = self._line(nc, action_type='correccion', name='Corrección terminada',
                          date_done=self._today())
        pending = self._line(nc, name='Correctiva pendiente')
        self._force_closed(nc)
        # La lista editable de la ficha escribe a través de la NC: la pendiente se termina.
        nc.with_user(self.sgi_user).write({'sgi_action_line_ids': [(1, pending.id, {
            'evidence_note': 'OT-77 firmada', 'date_done': self._today()})]})
        self.assertEqual(pending.date_done, self._today())
        # La terminada no se cambia por la misma vía.
        with self.assertRaisesRegex(UserError, 'pertenece a un registro cerrado'):
            nc.with_user(self.sgi_user).write({'sgi_action_line_ids': [(1, done.id, {
                'name': 'Otra redacción'})]})
        # El dueño del proceso lee que puede reabrirla.
        with self.assertRaisesRegex(UserError, 'Usted es el dueño del proceso'):
            nc.with_user(self.owner_user).write({'sgi_root_cause': 'Otra causa'})

    def test_14c_correctiva_de_nc_cerrada_no_reprograma_eficacia(self):
        nc = self._nc()
        pending = self._line(nc, name='Correctiva tras el cierre forzado', evidence_note='OT-88')
        self._force_closed(nc)
        pending.with_user(self.sgi_user).action_mark_done()
        self.assertTrue(pending.date_done)
        nc.invalidate_recordset()
        self.assertFalse(nc.sgi_effectiveness_due, "Una NC cerrada no se reprograma.")
        self.assertFalse(self._summaries(nc, 'Verificar eficacia'))

    def test_14d_accion_no_se_muda_a_una_nc_cerrada(self):
        closed = self._nc()
        self._force_closed(closed)
        other = self._nc(title='N02 NC abierta')
        line = self._line(other, name='Acción que se muda')
        with self.assertRaisesRegex(UserError, 'pertenece a un registro cerrado'):
            with self.cr.savepoint():
                line.with_user(self.sgi_user).write({'alert_id': closed.id})
        with self.assertRaisesRegex(UserError, 'pertenece a un registro cerrado'):
            with self.cr.savepoint():
                closed.with_user(self.sgi_user).write({'sgi_action_line_ids': [(4, line.id)]})
        self.assertEqual(line.alert_id, other)


@tagged('post_install', '-at_install')
class TestAuditoriaCierre(_NcEvidenciaCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.auditor = new_test_user(
            cls.env, login='n03_auditor', groups='base.group_user,quimibond_sgi.group_sgi_auditor')
        cls.audited = cls.env['sgi.process'].create({'code': 'ZN3', 'name': 'Proceso auditado N03'})

    def _audit(self):
        return self.env['sgi.audit'].create({
            'audit_type': 'interna', 'process_ids': [(6, 0, self.audited.ids)], 'state': 'informe'})

    def test_15_nc_menor_exige_su_nc(self):
        audit = self._audit()
        finding = self.env['sgi.audit.finding'].create({
            'audit_id': audit.id, 'finding_type': 'nc_menor', 'process_id': self.audited.id,
            'description': 'Registro sin firma', 'disposition': 'sin_accion',
            'reason_no_action': 'Caso aislado'})
        assert_locked(self, audit.with_user(self.auditor).action_close)
        finding.write({'disposition': 'mejora'})
        assert_locked(self, audit.with_user(self.auditor).action_close)
        finding.with_user(self.auditor).action_generate_nc()   # el auditor solo lee NC; la levanta igual
        audit.with_user(self.auditor).action_close()
        self.assertEqual(audit.state, 'cerrada')

    def test_16_observacion_sin_accion_y_hallazgo_cerrado(self):
        audit = self._audit()
        finding = self.env['sgi.audit.finding'].create({
            'audit_id': audit.id, 'finding_type': 'observacion', 'process_id': self.audited.id,
            'description': 'Etiqueta borrosa', 'disposition': 'sin_accion',
            'reason_no_action': 'Se corrigió en sitio'})
        audit.with_user(self.auditor).action_close()
        self.assertEqual(audit.state, 'cerrada')
        assert_locked(self, finding.with_user(self.auditor).write, {'description': 'Otra redacción'})
        assert_locked(self, self.env['sgi.audit.finding'].with_user(self.auditor).create, {
            'audit_id': audit.id, 'finding_type': 'observacion', 'description': 'Tarde'})
        finding.with_user(self.mast).write({'description': 'Redacción corregida por el Jefe MAST'})

    def test_17_programa_sugerido_y_cobertura_de_tres_anos(self):
        Process = self.env['sgi.process']
        macro = Process.create({'code': 'ZN3M', 'name': 'Macro N03'})
        draft = Process.create({'code': 'ZN3A', 'name': 'Sub borrador N03', 'parent_id': macro.id})
        recent = Process.create({'code': 'ZN3B', 'name': 'Sub auditado en 2097', 'parent_id': macro.id})
        old = Process.create({'code': 'ZN3C', 'name': 'Sub auditado en 2096', 'parent_id': macro.id})
        Program = self.env['sgi.audit.program']
        Line = self.env['sgi.audit.program.line']
        Line.create({'program_id': Program.create({'year': 2097}).id, 'process_id': recent.id,
                     'planned_month': '3'})
        Line.create({'program_id': Program.create({'year': 2096}).id, 'process_id': old.id,
                     'planned_month': '3'})
        program = Program.create({'year': 2099})
        self.assertIn(draft, program.coverage_gap_ids)
        self.assertIn(old, program.coverage_gap_ids, "2096 queda fuera del ciclo 2097-2099.")
        self.assertNotIn(recent, program.coverage_gap_ids)
        self.assertNotIn(macro, program.coverage_gap_ids, "Solo subprocesos.")
        program.action_suggest_lines()
        self.assertIn(draft, program.line_ids.process_id, "Los procesos en borrador también se auditan.")
        self.assertNotIn(macro, program.line_ids.process_id)
        self.assertFalse(program.coverage_gap_ids & (draft | old))


@tagged('post_install', '-at_install')
class TestDevolucionCliente(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        # Almacén propio con entrega en tres pasos (Pick → Empaque → Entrega).
        cls.warehouse = env['stock.warehouse'].create({
            'name': 'Almacén N12', 'code': 'N12', 'delivery_steps': 'pick_pack_ship'})
        cls.customers = env.ref('stock.stock_location_customers')
        cls.suppliers = env.ref('stock.stock_location_suppliers')
        cls.partner = env['res.partner'].create({'name': 'Cliente N12', 'is_company': True})
        cls.product = env['product.product'].create({'name': 'Tela N12', 'is_storable': False})

    def _done(self, picking_type, src, dest, qty, origin_move=False):
        # Mismo patrón que test_fase8.TestCustomerReturnNc.
        picking = self.env['stock.picking'].create({
            'partner_id': self.partner.id, 'picking_type_id': picking_type.id,
            'location_id': src.id, 'location_dest_id': dest.id,
            'move_ids': [(0, 0, {
                'product_id': self.product.id, 'product_uom_qty': qty,
                'location_id': src.id, 'location_dest_id': dest.id,
                'origin_returned_move_id': origin_move.id if origin_move else False,
            })],
        })
        picking.action_confirm()
        picking.move_ids.write({'quantity': qty, 'picked': True})
        picking._action_done()
        self.assertEqual(picking.state, 'done')
        return picking

    def test_18_devolucion_de_una_entrega_en_tres_pasos(self):
        """Hipótesis de la auditoría (H-B12.1). Pasa ANTES del código: en
        producción las devoluciones sí apuntan al OUT (ver el plan, 1.5)."""
        wh = self.warehouse
        out_src = wh.out_type_id.default_location_src_id
        self.assertNotEqual(out_src, wh.lot_stock_id, "Con tres pasos la entrega sale de Salida.")
        out = self._done(wh.out_type_id, out_src, self.customers, 5)
        ret = self._done(wh.in_type_id, self.customers, wh.lot_stock_id, 2,
                         origin_move=out.move_ids[:1])
        self.assertTrue(ret.sgi_return_alert_id)

    def test_19_devolucion_capturada_a_mano(self):
        ret = self._done(self.warehouse.in_type_id, self.customers, self.warehouse.lot_stock_id, 3)
        alert = ret.sgi_return_alert_id
        self.assertTrue(alert, "Sin «Devolver» también es devolución: viene de Clientes.")
        self.assertEqual(alert.product_id, self.product)
        self.assertEqual(alert.sgi_origin_type, 'reclamacion')
        self.assertIn('<b>%s</b>' % alert.sgi_folio, "".join(ret.message_ids.mapped('body')))

    def test_20_recepcion_de_proveedor_no_es_devolucion(self):
        rec = self._done(self.warehouse.in_type_id, self.suppliers, self.warehouse.lot_stock_id, 3)
        self.assertFalse(rec.sgi_return_alert_id)
