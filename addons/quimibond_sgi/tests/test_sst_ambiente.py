# -*- coding: utf-8 -*-
"""57.96.0 (auditoría 2026-10: N-06, N-07 e incidente desde la incapacidad):
SST y ambiente.

- Jerarquía de controles (45001 8.1.2): un IPER de riesgo alto con solo EPP,
  o sin jerarquía, no se controla ni se cierra; no es retroactivo.
- El aspecto ambiental vive en la matriz: «Aspecto ambiental» no se elige a
  mano en un riesgo; etapa del ciclo de vida para evaluar; asistente manual
  que traspasa los riesgos ambientales a aspectos (cuenta, aplica, idempotente,
  conserva los riesgos salvo que se pida archivarlos).
- Lo legal con evidencia: los botones rápidos abren el asistente.
- Incidente: equipo de investigación con un trabajador o un integrante de la
  Comisión de Seguridad e Higiene, verificación de eficacia y, si es moderado
  o más, IPER reevaluado después del incidente.
- Permiso de trabajo: vencido guardado y avisado cada hora; no se cierra con
  un bloqueo (LOTO) aplicado; competencia exigida por tipo; evaluación SST del
  contratista (avisa o bloquea según el parámetro).
- Una incapacidad por riesgo de trabajo aprobada crea el incidente."""
from datetime import date, datetime, timedelta
from unittest.mock import patch

from odoo import fields
from odoo.exceptions import AccessError, UserError
from odoo.tests import TransactionCase, new_test_user, tagged

from .common_users import assert_locked, sgi_set_mast, sgi_test_user


class _SstCase(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.today = fields.Date.context_today(env.user)
        cls.mast = sgi_set_mast(env, login='zst_mast')
        cls.sgi_user = sgi_test_user(env, 'zst_usuario', 'quimibond_sgi.group_sgi_user')
        cls.process = env['sgi.process'].create({'code': 'ZST', 'name': 'Proceso 57.96'})
        cls.Risk = env['sgi.risk']
        cls.Activity = env['mail.activity'].with_context(active_test=False)

    def _done_action(self, record, level=False, **vals):
        field = 'risk_id' if record._name == 'sgi.risk' else 'incident_id'
        return self.env['sgi.action.line'].create(dict({
            field: record.id, 'name': 'Control ZST %s' % (level or 'sin nivel'),
            'responsible_id': self.mast.id, 'date_commit': self.today,
            'date_done': self.today, 'control_hierarchy': level}, **vals))

    def _notices(self, record, kind):
        return self.Activity.search([('res_model', '=', record._name), ('res_id', '=', record.id),
                                     ('sgi_cron_kind', '=', kind)])

    def _locked(self, text, method, *args, **kwargs):
        """Candado (UserError, no permiso) cuyo mensaje dice ``text``.

        No usar ``assertRaisesRegex``: el ``assertRaises`` de Odoo abre un
        savepoint y limpia la caché al fallar; ``assertRaisesRegex`` (unittest)
        no. Con los candados que se revisan DESPUÉS de escribir (cierre del
        incidente, H11 y jerarquía del riesgo) el estado quedaría escrito y el
        resto de la prueba correría sobre un registro ya «cerrado»."""
        with self.assertRaises(UserError) as caught:
            method(*args, **kwargs)
        self.assertNotIsInstance(caught.exception, AccessError, str(caught.exception))
        self.assertIn(text, str(caught.exception))


@tagged('post_install', '-at_install')
class TestJerarquiaControles(_SstCase):

    def _iper_alto(self, **vals):
        return self.Risk.create(dict({
            'name': 'Caída de altura ZST', 'instrument': 'iper', 'process_id': self.process.id,
            'eval_probability': '3', 'eval_impact': '3',
            'residual_probability': '1', 'residual_impact': '2'}, **vals))

    def test_01_iper_alto_con_solo_epp_no_se_controla(self):
        risk = self._iper_alto(control_hierarchy='epp')
        self.assertEqual(risk.attention_level, 'alto')
        self._done_action(risk, 'epp')
        risk.with_user(self.mast).action_set_en_tratamiento()
        self._locked("EPP", risk.with_user(self.mast).action_set_controlado)
        self.assertEqual(risk.state, 'en_tratamiento')
        self._done_action(risk, 'ingenieria')
        risk.with_user(self.mast).action_set_controlado()
        self.assertEqual(risk.state, 'controlado')
        self.assertIn('control_hierarchy', self.env['sgi.action.line']._fields)

    def test_02_sin_jerarquia_no_se_controla_y_otros_no_cambian(self):
        risk = self._iper_alto()
        self._done_action(risk)
        self._locked("jerarquía", risk.with_user(self.mast).write, {'state': 'controlado'})
        # R y O de atención inmediata: sigue solo con H11 (acción y residual).
        ryo = self.Risk.create({
            'name': 'Riesgo inmediato ZST', 'instrument': 'ryo', 'eval_probability': '5',
            'eval_impact': '5', 'residual_probability': '1', 'residual_impact': '1'})
        self._done_action(ryo)
        ryo.with_user(self.mast).write({'state': 'controlado'})
        # IPER medio: sin candado nuevo.
        medio = self.Risk.create({'name': 'IPER medio ZST', 'instrument': 'iper',
                                  'eval_probability': '2', 'eval_impact': '2'})
        medio.with_user(self.mast).write({'state': 'controlado'})
        self.assertEqual((ryo.state, medio.state), ('controlado', 'controlado'))

    def test_03_no_es_retroactivo(self):
        risk = self._iper_alto()
        line = self._done_action(risk)
        # Un IPER controlado antes de 57.96.0 (sin jerarquía).
        self.env.flush_all()
        self.env.cr.execute("UPDATE sgi_risk SET state = 'controlado' WHERE id = %s", (risk.id,))
        risk.invalidate_recordset(['state'])
        line.write({'name': 'Control renombrado ZST'})
        risk.with_user(self.mast).action_evaluate()
        self.assertEqual(risk.state, 'controlado', "Editar y evaluar no dispara el candado.")
        risk.with_user(self.mast).action_set_en_tratamiento()
        self._locked("jerarquía", risk.with_user(self.mast).action_set_controlado)


@tagged('post_install', '-at_install')
class TestAspectoUnicaFuente(_SstCase):

    def _aspect(self, **vals):
        return self.env['sgi.env.aspect'].create(dict({
            'process_id': self.process.id, 'activity': 'Lavado de tambos ZST',
            'name': 'Descarga con residuos ZST', 'impact': 'Contaminación del agua',
            'aspect_type': 'descarga', 'severity': '4', 'frequency': '3',
            'control_description': 'Trampa de grasas'}, **vals))

    def test_04_ambiental_solo_desde_la_matriz(self):
        self._locked("Aspectos ambientales", self.Risk.create,
                     {'name': 'Ambiental a mano ZST', 'instrument': 'ambiental'})
        ryo = self.Risk.create({'name': 'R y O ZST', 'instrument': 'ryo'})
        with self.assertRaises(UserError):
            ryo.write({'instrument': 'ambiental'})
        aspect = self._aspect(life_cycle_stage='proceso')
        aspect.action_create_risk()
        self.assertEqual(aspect.risk_id.instrument, 'ambiental')
        self.assertIn(aspect, aspect.risk_id.sgi_env_aspect_ids)
        aspect.risk_id.write({'name': 'Tratamiento renombrado ZST'})

    def test_05_ciclo_de_vida_para_evaluar(self):
        aspect = self._aspect()
        self._locked("ciclo de vida", aspect.action_evaluate)
        aspect.life_cycle_stage = 'fin_vida'
        aspect.action_evaluate()
        self.assertEqual(aspect.state, 'evaluado')


@tagged('post_install', '-at_install')
class TestTraspasoAmbiental(_SstCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Risk = cls.Risk.with_context(sgi_from_env_aspect=True)
        cls.risk_a = Risk.create({
            'name': 'Descarga de agua de tintorería ZST', 'instrument': 'ambiental',
            'kind': 'riesgo', 'process_id': cls.process.id, 'eval_probability': '3',
            'eval_impact': '4', 'consequence': 'Exceder límites de descarga',
            'existing_controls': 'Análisis periódicos'})
        cls.risk_b = Risk.create({
            'name': 'Merma de fibra ZST', 'instrument': 'ambiental', 'kind': 'oportunidad',
            'process_id': cls.process.id, 'eval_probability': '4', 'eval_impact': '3'})
        cls.requirement = cls.env['sgi.legal.requirement'].create({
            'name': 'NOM-001 ZST', 'system': 'ambiental', 'risk_ids': [(6, 0, cls.risk_a.ids)]})
        cls.mine = cls.risk_a | cls.risk_b
        cls.Wizard = cls.env['sgi.env.aspect.transfer']
        cls.Aspect = cls.env['sgi.env.aspect'].with_context(active_test=False)

    def _wizard(self, **vals):
        wizard = self.Wizard.with_user(self.mast).create(vals)
        # La copia de producción trae sus 5 riesgos ambientales: fuera de la prueba.
        (wizard.line_ids - wizard.line_ids.filtered(lambda l: l.risk_id in self.mine)).unlink()
        return wizard

    def test_06_al_abrir_solo_cuenta(self):
        wizard = self._wizard()
        self.assertEqual(wizard.line_ids.risk_id, self.mine)
        line_a = wizard.line_ids.filtered(lambda l: l.risk_id == self.risk_a)
        self.assertEqual((line_a.activity, line_a.aspect_type, line_a.condition),
                         (self.risk_a.name, 'otro', 'normal'))
        self.assertFalse(self.Aspect.search([('risk_id', 'in', self.mine.ids)]), "Abrir no escribe.")
        with self.assertRaises(AccessError):
            self.Wizard.with_user(self.sgi_user).create({})

    def test_07_traspasa_ligado_e_idempotente(self):
        wizard = self._wizard()
        wizard.line_ids.filtered(lambda l: l.risk_id == self.risk_a).write(
            {'aspect_type': 'descarga', 'life_cycle_stage': 'proceso'})
        wizard.action_apply()
        aspect = self.Aspect.search([('risk_id', '=', self.risk_a.id)])
        self.assertEqual(len(aspect), 1)
        self.assertEqual((aspect.name, aspect.impact, aspect.severity, aspect.frequency),
                         (self.risk_a.name, 'Exceder límites de descarga', '4', '3'))
        self.assertEqual((aspect.aspect_type, aspect.life_cycle_stage, aspect.state),
                         ('descarga', 'proceso', 'borrador'))
        self.assertEqual(aspect.process_id, self.process)
        self.assertEqual(aspect.control_description, 'Análisis periódicos')
        self.assertEqual(aspect.legal_requirement_ids, self.requirement)
        self.assertTrue(self.risk_a.active, "Por omisión el riesgo se conserva.")
        self.assertIn(aspect.folio, self.risk_a.message_ids[:1].body)
        self.assertEqual(wizard.done_count, 2)
        again = self._wizard()
        self.assertFalse(again.line_ids, "Con su aspecto ya no se proponen.")

    def test_08_archivar_solo_si_se_pide(self):
        wizard = self._wizard(archive_risks=True)
        wizard.line_ids.filtered(lambda l: l.risk_id == self.risk_a).unlink()
        wizard.action_apply()
        self.assertFalse(self.risk_b.active)
        self.assertEqual(self.Aspect.search([('risk_id', '=', self.risk_b.id)]).risk_id, self.risk_b)
        self.assertTrue(self.risk_a.active, "El renglón quitado no se toca.")


@tagged('post_install', '-at_install')
class TestLegalConEvidencia(_SstCase):

    def test_09_botones_rapidos_abren_el_asistente(self):
        req = self.env['sgi.legal.requirement'].create({'name': 'Permiso de descarga ZST',
                                                        'system': 'ambiental'})
        for method, result in (('action_mark_cumple', 'cumple'), ('action_mark_parcial', 'parcial'),
                               ('action_mark_no_cumple', 'no_cumple'),
                               ('action_mark_no_aplica', 'no_aplica')):
            action = getattr(req.with_user(self.mast), method)()
            self.assertEqual(action['res_model'], 'sgi.legal.evaluate')
            self.assertEqual(action['context']['default_result'], result)
        self.assertEqual(req.compliance_state, 'pendiente', "Ningún botón registra sin evidencia.")
        self.assertFalse(req.evaluation_ids)
        wizard = self.env['sgi.legal.evaluate'].with_user(self.mast).with_context(
            action['context']).create({'evidence': 'La planta no descarga a cuerpo federal'})
        wizard.action_confirm()
        self.assertEqual(req.compliance_state, 'no_aplica')
        self.assertEqual(req.evaluation_ids[:1].evidence, 'La planta no descarga a cuerpo federal')


@tagged('post_install', '-at_install')
class TestIncidenteInvestigacion(_SstCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Employee = cls.env['hr.employee']
        cls.boss = Employee.create({'name': 'Jefe de turno ZST'})
        cls.worker = Employee.create({'name': 'Operador ZST', 'parent_id': cls.boss.id})
        cls.csh_user = new_test_user(cls.env, login='zst_csh',
                                     groups='base.group_user,quimibond_sgi.group_sgi_csh')
        cls.csh_boss = Employee.create({'name': 'Supervisor CSH ZST', 'user_id': cls.csh_user.id})
        Employee.create({'name': 'Ayudante ZST', 'parent_id': cls.csh_boss.id})

    def _incident(self, **vals):
        incident = self.env['sgi.incident'].create(dict({
            'name': 'Golpe en cortadora ZST', 'severity': 'leve', 'incident_type': 'lesion',
            'date': fields.Datetime.now() - timedelta(days=2),
            'immediate_causes': 'a', 'basic_causes': 'b', 'lack_of_control': 'c'}, **vals))
        self._done_action(incident, 'ingenieria', date_done=self.today - timedelta(days=1))
        incident.with_user(self.mast).action_set_investigacion()
        return incident

    def _effective(self, incident, **vals):
        incident.with_user(self.mast).write(dict({
            'sgi_effective': 'eficaz', 'sgi_effectiveness_date': self.today,
            'sgi_effectiveness_note': 'Sin repetición en dos semanas'}, **vals))

    def test_10_equipo_con_trabajador_o_comision(self):
        incident = self._incident()
        self._effective(incident)
        assert_locked(self, incident.with_user(self.mast).action_set_cerrado)
        incident.with_user(self.mast).investigation_team_ids = [(6, 0, self.boss.ids)]
        self._locked("trabajador", incident.with_user(self.mast).action_set_cerrado)
        incident.with_user(self.mast).investigation_team_ids = [(4, self.worker.id)]
        incident.with_user(self.mast).action_set_cerrado()
        self.assertEqual(incident.state, 'cerrado')
        other = self._incident()
        self._effective(other, investigation_team_ids=[(6, 0, self.csh_boss.ids)])
        other.with_user(self.mast).action_set_cerrado()
        self.assertEqual(other.state, 'cerrado', "Un integrante de la Comisión basta.")

    def test_11_eficacia_y_no_eficaz(self):
        incident = self._incident(investigation_team_ids=[(6, 0, self.worker.ids)])
        self._locked("eficacia", incident.with_user(self.mast).action_set_cerrado)
        incident.with_user(self.mast).write({'sgi_effective': 'no_eficaz',
                                             'sgi_effectiveness_note': 'Se repitió el golpe'})
        self.assertEqual(incident.state, 'acciones')
        self.assertEqual(incident.sgi_ineffective_count, 1)
        self.assertFalse(incident.sgi_effective)
        self.assertTrue(incident.activity_ids.filtered(
            lambda a: a.summary.startswith("Registrar acción nueva del incidente")))
        self._effective(incident)
        self._locked("No eficaz", incident.with_user(self.mast).action_set_cerrado)
        self._done_action(incident, 'administrativo')
        incident.with_user(self.mast).action_set_cerrado()
        self.assertEqual(incident.state, 'cerrado')
        late = self._incident(investigation_team_ids=[(6, 0, self.worker.ids)])
        self._effective(late, sgi_effectiveness_date=self.today - timedelta(days=2))
        assert_locked(self, late.with_user(self.mast).action_set_cerrado)

    def test_12_iper_reevaluado_despues_del_incidente(self):
        risk = self.Risk.create({'name': 'Atrapamiento ZST', 'instrument': 'iper',
                                 'eval_probability': '2', 'eval_impact': '2',
                                 'last_eval_date': self.today - timedelta(days=30)})
        incident = self._incident(severity='moderado', risk_id=risk.id,
                                  investigation_team_ids=[(6, 0, self.worker.ids)])
        self._effective(incident)
        self._locked("IPER", incident.with_user(self.mast).action_set_cerrado)
        risk.action_evaluate()
        incident.with_user(self.mast).action_set_cerrado()
        self.assertEqual(incident.state, 'cerrado')

    def test_13_solo_quien_investiga_registra_la_eficacia(self):
        incident = self.env['sgi.incident'].create({
            'name': 'Resbalón ZST', 'incident_type': 'casi_accidente',
            'reporter_id': self.sgi_user.id, 'reporter_employee_id': False})
        assert_locked(self, incident.with_user(self.sgi_user).write, {'sgi_effective': 'eficaz'})


@tagged('post_install', '-at_install')
class TestPermisoDeTrabajo(_SstCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        # La copia de producción puede traer configuración: fuera de la prueba.
        env['sgi.work.permit.skill'].search([]).write({'active': False})
        env['ir.config_parameter'].sudo().set_param('quimibond_sgi.permit_contractor_eval_required', '0')
        cls.requester = sgi_test_user(env, 'zst_solicita', 'quimibond_sgi.group_sgi_user')
        cls.boss = sgi_test_user(env, 'zst_jefe_area', 'quimibond_sgi.group_sgi_user')
        cls.worker = env['hr.employee'].create({'name': 'Electricista ZST'})
        cls.equipment = env['maintenance.equipment'].create({'name': 'Carda ZST'})
        cls.Cron = env['sgi.cron']

    def _permit(self, **vals):
        start = datetime(2046, 5, 4, 8, 0)
        permit = self.env['sgi.work.permit'].with_user(self.requester).create(dict({
            'name': 'Cambio de interruptor ZST', 'work_type': 'electrico', 'location': 'Subestación',
            'date_start': start, 'date_end': start + timedelta(hours=8),
            'area_manager_id': self.boss.id, 'hazards': 'Arco eléctrico',
            'executor_ids': [(6, 0, self.worker.ids)]}, **vals))
        permit.check_ids.write({'answer': 'si'})
        return permit

    def _authorized(self, **vals):
        permit = self._permit(**vals)
        permit.action_submit()
        permit.with_user(self.boss).action_approve_area()
        permit.with_user(self.mast).action_approve_sst()
        self.assertEqual(permit.state, 'autorizado')
        return permit

    def test_14_vencido_guardado_y_aviso_cada_hora(self):
        self.assertTrue(self.env['sgi.work.permit']._fields['expired'].store)
        permit = self._authorized()
        self.env.flush_all()
        self.env.cr.execute("UPDATE sgi_work_permit SET date_start = %s, date_end = %s WHERE id = %s",
                            (datetime(2020, 1, 1, 8), datetime(2020, 1, 1, 18), permit.id))
        permit.invalidate_recordset()
        self.assertFalse(permit.expired, "Sin la corrida, la columna no sabe que pasó la hora.")
        self.Cron.cron_work_permits()
        self.assertTrue(permit.expired)
        boss_notice = self._notices(permit, 'permiso_vencido').filtered('active')
        mast_notice = self._notices(permit, 'permiso_vencido_mast').filtered('active')
        self.assertEqual((boss_notice.user_id, mast_notice.user_id), (self.boss, self.mast))
        self.Cron.cron_work_permits()
        self.assertEqual(len(self._notices(permit, 'permiso_vencido')), 1, "Cada hora, un solo aviso.")
        permit.close_note = 'Tableros cerrados'
        permit.action_close()
        self.assertFalse(permit.expired)
        self.Cron.cron_work_permits()
        self.assertFalse(boss_notice.active)
        self.assertTrue(boss_notice.sgi_episode_closed)
        with self.assertRaises(AccessError):
            self.Cron.with_user(self.mast).cron_work_permits()

    def test_15_no_se_cierra_con_loto_aplicado(self):
        permit = self._authorized()
        locker = self.env['hr.employee'].create({'name': 'Mecánico ZST'})
        loto = self.env['sgi.loto'].create({
            'equipment_id': self.equipment.id, 'name': 'Bloqueo ZST', 'work_permit_id': permit.id,
            'affected_notified': True, 'zero_energy_verified': True,
            'zero_energy_method': 'Intento de arranque',
            'energy_ids': [(0, 0, {'energy_type': 'electrica', 'isolation_point': 'Interruptor ZST'})],
            'lock_ids': [(0, 0, {'employee_id': locker.id, 'lock_number': 'C-ZST',
                                 'tag_number': 'T-ZST'})]})
        loto.action_apply()
        self.assertIn(loto, permit.loto_ids)
        permit.close_note = 'Área limpia'
        self._locked("bloqueo", permit.action_close)
        with self.assertRaises(UserError):
            permit.with_user(self.mast).write({'state': 'cerrado'})
        loto.lock_ids.action_remove_lock()
        loto.write({'area_notified_end': True, 'removal_note': 'Guardas colocadas'})
        loto.action_remove()
        permit.action_close()
        self.assertEqual(permit.state, 'cerrado')

    def test_16_competencia_por_tipo_de_permiso(self):
        # is_certification: la vigencia (valid_to) es de las certificaciones en
        # Odoo 19; así la prueba no depende de si se guarda en las demás.
        skill_type = self.env['hr.skill.type'].create({
            'name': 'Permisos ZST', 'is_certification': True,
            'skill_ids': [(0, 0, {'name': 'Trabajo eléctrico NOM-029 ZST'})],
            'skill_level_ids': [(0, 0, {'name': 'Aprobado ZST', 'level_progress': 100})]})
        skill = skill_type.skill_ids
        self._permit().action_submit()  # sin configuración, nada cambia
        self.env['sgi.work.permit.skill'].create({'work_type': 'electrico', 'skill_id': skill.id,
                                                  'note': 'NOM-029-STPS'})
        permit = self._permit()
        self._locked("Electricista ZST", permit.action_submit)
        employee_skill = self.env['hr.employee.skill'].create({
            'employee_id': self.worker.id, 'skill_id': skill.id, 'skill_type_id': skill_type.id,
            'skill_level_id': skill_type.skill_level_ids.id, 'valid_to': date(2046, 5, 1)})
        assert_locked(self, permit.action_submit)  # vence antes del fin del permiso
        employee_skill.valid_to = date(2047, 1, 1)
        permit.action_submit()
        self.assertEqual(permit.state, 'solicitado')

    def test_17_evaluacion_sst_del_contratista(self):
        contractor = self.env['res.partner'].create({'name': 'Instalaciones ZST', 'is_company': True})
        # El Jefe MAST de la prueba solo trae sus grupos: que pueda editar contactos.
        self.mast.group_ids = [(4, self.env.ref('base.group_partner_manager').id)]
        permit = self._permit(executor_ids=[(5, 0, 0)], contractor_id=contractor.id)
        self.assertFalse(permit.sgi_contractor_eval_ok)
        permit.action_submit()  # por omisión solo avisa
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.permit_contractor_eval_required', '1')
        blocked = self._permit(executor_ids=[(5, 0, 0)], contractor_id=contractor.id)
        self._locked("evaluación SST", blocked.action_submit)
        with self.assertRaises(UserError):
            contractor.with_user(self.sgi_user).write({'sgi_sst_eval_valid_until': date(2047, 1, 1)})
        contractor.with_user(self.mast).write({'sgi_sst_eval_valid_until': date(2047, 1, 1),
                                               'sgi_sst_eval_note': 'REPSE, SUA y DC-3 revisados'})
        blocked.invalidate_recordset(['sgi_contractor_eval_ok'])
        self.assertTrue(blocked.sgi_contractor_eval_ok)
        blocked.action_submit()
        self.assertEqual(blocked.state, 'solicitado')


@tagged('post_install', '-at_install')
class TestIncidenteDesdeIncapacidad(_SstCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        # «hr»: nace «Por aprobar» y se aprueba con action_approve (camino de
        # write de RH). El tipo sin validación (otro_tipo, y test_19) cubre el
        # camino de create.
        cls.work_risk = env['hr.leave.type'].create({
            'name': 'Riesgo de trabajo ZST', 'requires_allocation': False,
            'leave_validation_type': 'hr'})
        cls.other_type = env['hr.leave.type'].create({
            'name': 'Permiso ZST', 'requires_allocation': False,
            'leave_validation_type': 'no_validation'})
        env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.work_risk_leave_type_ids', str(cls.work_risk.id))
        cls.employee = env['hr.employee'].create({'name': 'Tejedor ZST'})

    def _leave(self, leave_type, day_from, day_to):
        leave = self.env['hr.leave'].create({
            'employee_id': self.employee.id, 'holiday_status_id': leave_type.id,
            'request_date_from': day_from, 'request_date_to': day_to,
            'private_name': 'Diagnóstico confidencial ZST'})
        if leave.state != 'validate':
            self.assertFalse(leave.sgi_incident_id, "Por aprobar no crea incidente.")
            leave.action_approve()  # Odoo 18/19: aprueba y, si no es doble, valida
        if leave.state != 'validate':
            leave.action_validate()
        self.assertEqual(leave.state, 'validate')
        return leave

    def test_18_incapacidad_aprobada_crea_el_incidente(self):
        first = self._leave(self.work_risk, date(2046, 3, 5), date(2046, 3, 7))
        incident = first.sgi_incident_id
        self.assertTrue(incident)
        self.assertEqual((incident.state, incident.incident_type, incident.severity),
                         ('reportado', 'lesion', 'moderado'))
        self.assertEqual(incident.employee_ids, self.employee)
        self.assertEqual(incident.days_lost, round(first.number_of_days))
        self.assertTrue(incident.sgi_from_leave)
        self.assertNotIn('Diagnóstico', incident.name + (incident.description or ''))
        notice = self._notices(incident, 'incidente_incapacidad').filtered('active')
        self.assertEqual(notice.user_id, self.mast)
        first.write({'private_name': 'Otra nota ZST'})
        self.assertEqual(self.env['sgi.incident'].search_count(
            [('id', '=', incident.id)]), 1)
        self.assertEqual(len(self._notices(incident, 'incidente_incapacidad')), 1)
        # La subsecuente se liga al mismo incidente y suma días.
        second = self._leave(self.work_risk, date(2046, 3, 8), date(2046, 3, 9))
        self.assertEqual(second.sgi_incident_id, incident)
        self.assertEqual(incident.days_lost, round(first.number_of_days + second.number_of_days))
        self.assertEqual(incident.sgi_leave_count, 2)

    def test_19_otro_tipo_no_y_rechazo_deja_nota(self):
        other = self._leave(self.other_type, date(2046, 4, 2), date(2046, 4, 3))
        self.assertFalse(other.sgi_incident_id)
        first = self._leave(self.work_risk, date(2046, 3, 5), date(2046, 3, 7))
        incident = first.sgi_incident_id
        second = self._leave(self.work_risk, date(2046, 3, 8), date(2046, 3, 9))
        second.action_refuse()  # VERIFICAR (1.10)
        self.assertTrue(incident.exists(), "Nada se borra.")
        self.assertEqual(incident.days_lost, round(first.number_of_days))
        self.assertTrue(incident.message_ids.filtered(
            lambda m: 'ya no está aprobada' in (m.body or '')))

    def test_20_la_aprobacion_no_falla_por_el_sgi(self):
        Leave = type(self.env['hr.leave'])
        with patch.object(Leave, '_sgi_link_incident',
                          side_effect=ValueError("Falla simulada del SGI")), \
                self.assertLogs('odoo.addons.quimibond_sgi.models.sgi_incident_leave',
                                level='WARNING'):
            leave = self._leave(self.work_risk, date(2046, 6, 1), date(2046, 6, 1))
        self.assertEqual(leave.state, 'validate')
        self.assertFalse(leave.sgi_incident_id)
