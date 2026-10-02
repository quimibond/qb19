# -*- coding: utf-8 -*-
"""57.97.0 (auditoría 2026-10: N-05 y N-09): cláusulas y revisión por la
dirección.

- Cláusulas de tercer nivel (6.1.2, 6.1.3, 6.1.4, 7.1.5, 8.1.2, 8.1.3, 8.1.4,
  9.1.2) como datos con xmlid; las de normas sin xmlid (CLI-xx) no se tocan;
  el pre-migrate solo liga el xmlid a una cláusula que ya exista con ese
  numeral en la misma norma.
- Una NC con folio no sale de Abierta hacia Seguimiento (ni directo a Cerrada)
  sin clasificación y cláusula; solo en ese cambio de etapa y después de
  escribir; no atrapa lo que ya está en Seguimiento o cerrado.
- Revisión por la dirección: entradas de incidentes, contexto, aspectos y
  mejoras (solo conteos donde hay personas); scrap de la empresa del SGI;
  acuerdos del tipo «acuerdo»; conclusiones 9.3.3 para marcarla realizada;
  los acuerdos abiertos pasan a la siguiente revisión."""
import importlib.util
import os
from datetime import date, timedelta

from odoo import fields
from odoo.exceptions import AccessError, UserError, ValidationError
from odoo.tests import TransactionCase, new_test_user, tagged

from .common_users import sgi_set_mast

_MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PRE_MIGRATE = os.path.join(_MODULE_DIR, 'migrations', '19.0.57.97.0', 'pre-migrate.py')

CONCLUSIONS = {
    'conclusion_suitability': 'Sigue siendo conveniente para el contexto actual.',
    'conclusion_adequacy': 'Cubre los procesos y requisitos vigentes.',
    'conclusion_effectiveness': 'Logra los resultados previstos, salvo los rojos con plan.',
    'output_needs': 'Sin cambios al SGI; un medidor de energía como recurso.',
}


def _pre_migrate():
    """El pre-migrate de 57.97.0 (su tabla NEW_CLAUSES es la única fuente de
    las cláusulas nuevas). Se carga dentro de la prueba: un archivo que falta
    no debe romper la carga de todo el paquete de pruebas."""
    spec = importlib.util.spec_from_file_location('sgi_mig_57_97_0', _PRE_MIGRATE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Case(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.today = fields.Date.context_today(env.user)
        cls.mast = sgi_set_mast(env, login='zcl_mast')
        cls.sgi_user = new_test_user(
            env, login='zcl_usuario', groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.director = new_test_user(
            env, login='zcl_direccion', groups='base.group_user,quimibond_sgi.group_sgi_director')

    def _locked(self, text, method, *args, **kwargs):
        """Candado (UserError, no permiso) cuyo mensaje dice ``text``. El
        ``assertRaises`` de Odoo abre un savepoint: el candado que se revisa
        después de escribir no deja la etapa escrita."""
        with self.assertRaises(UserError) as caught:
            method(*args, **kwargs)
        self.assertNotIsInstance(caught.exception, AccessError, str(caught.exception))
        self.assertIn(text, str(caught.exception))


@tagged('post_install', '-at_install')
class TestClausulasTercerNivel(_Case):

    def _new(self):
        return _pre_migrate().NEW_CLAUSES

    def test_01_clausulas_nuevas_con_xmlid(self):
        rows = self._new()
        self.assertEqual(len(rows), 13)
        Clause = self.env['sgi.norm.clause']
        for name, norm_xmlid, code, label in rows:
            clause = self.env.ref('quimibond_sgi.' + name)
            self.assertEqual(clause.norm_id, self.env.ref('quimibond_sgi.' + norm_xmlid), name)
            self.assertEqual(clause.code, code, name)
            self.assertEqual(Clause.search_count(
                [('norm_id', '=', clause.norm_id.id), ('code', '=', code)]), 1,
                "%s: una sola cláusula con ese numeral en su norma." % name)
            self.assertEqual(Clause._sgi_find(clause.short_label), clause, name)
        jerarquia = self.env.ref('quimibond_sgi.' + 'c_45001_8_1_2')
        self.assertEqual(jerarquia.short_label, '45001 8.1.2')

    def test_02_las_normas_sin_xmlid_no_se_tocan(self):
        norms = {self.env.ref('quimibond_sgi.' + n) for n in
                 ('sgi_norm_9001', 'sgi_norm_14001', 'sgi_norm_45001')}
        for _name, norm_xmlid, code, _label in self._new():
            self.assertIn(self.env.ref('quimibond_sgi.' + norm_xmlid), norms)
            self.assertFalse(code.upper().startswith('CLI'))
        # Ningún xmlid del módulo apunta a una cláusula de otra norma (CLI-xx, A-029).
        bound = self.env['ir.model.data'].search([
            ('module', '=', 'quimibond_sgi'), ('model', '=', 'sgi.norm.clause')])
        clauses = self.env['sgi.norm.clause'].browse(bound.mapped('res_id')).exists()
        self.assertFalse(clauses.filtered(lambda c: c.norm_id not in norms))

    def test_03_pre_migrate_solo_liga_el_xmlid(self):
        mig = _pre_migrate()
        IMD = self.env['ir.model.data']
        Clause = self.env['sgi.norm.clause']
        # Una cláusula capturada a mano con el mismo numeral (sin xmlid):
        # el pre-migrate le liga el xmlid y no crea otra.
        manual = self.env.ref('quimibond_sgi.' + 'c_45001_8_1_3')
        IMD.search([('module', '=', 'quimibond_sgi'), ('name', '=', 'c_45001_8_1_3')]).unlink()
        # Una cláusula nueva que nadie capturó: no hay qué ligar (el XML la crea).
        missing = self.env.ref('quimibond_sgi.' + 'c_9001_7_1_5')
        IMD.search([('module', '=', 'quimibond_sgi'), ('name', '=', 'c_9001_7_1_5')]).unlink()
        missing.code = 'Z7.1.5'
        self.env.flush_all()
        self.env.registry.clear_cache()  # VERIFICAR (1.9)
        before = Clause.search_count([])
        name_before = manual.name
        # Sin assertLogs: el logger del archivo cargado con importlib se llama
        # «sgi_mig_57_97_0»; el script solo registra INFO.
        mig.migrate(self.env.cr, '19.0.57.96.0')
        self.env.invalidate_all()
        self.env.registry.clear_cache()
        self.assertEqual(self.env.ref('quimibond_sgi.' + 'c_45001_8_1_3'), manual)
        self.assertEqual(manual.name, name_before, "Solo metadato: el registro no cambia.")
        self.assertFalse(self.env.ref('quimibond_sgi.' + 'c_9001_7_1_5', raise_if_not_found=False))
        self.assertEqual(Clause.search_count([]), before, "No crea cláusulas.")
        # Idempotente: una segunda corrida no agrega filas.
        mig.migrate(self.env.cr, '19.0.57.96.0')
        self.assertEqual(IMD.search_count(
            [('module', '=', 'quimibond_sgi'), ('name', '=', 'c_45001_8_1_3')]), 1)
        # Instalación nueva (sin versión): no hace nada.
        mig.migrate(self.env.cr, None)

    def test_04_la_matriz_muestra_el_tercer_nivel(self):
        norm = self.env.ref('quimibond_sgi.sgi_norm_45001')
        rows = [row['clause'] for row in norm._sgi_compliance_matrix()['rows']]
        codes = [c.code for c in rows]
        self.assertIn('8.1.2', codes)
        self.assertLess(codes.index('8.1'), codes.index('8.1.2'))
        self.assertLess(codes.index('8.1.4'), codes.index('8.2'))


@tagged('post_install', '-at_install')
class TestNcClasificacion(_Case):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env = cls.env
        cls.owner_user = new_test_user(
            env, login='zcl_dueno', groups='base.group_user,quimibond_sgi.group_sgi_user')
        owner = env['hr.employee'].create({'name': 'ZCL Dueño', 'user_id': cls.owner_user.id})
        cls.process = env['sgi.process'].create(
            {'code': 'ZCL', 'name': 'Proceso 57.97', 'owner_id': owner.id})
        cls.team = env.ref('quimibond_sgi.sgi_quality_team_internal')
        cls.stage_open = env.ref('quimibond_sgi.sgi_nc_int_stage_open')
        cls.stage_follow = env.ref('quimibond_sgi.sgi_nc_int_stage_followup')
        cls.stage_closed = env.ref('quimibond_sgi.sgi_nc_int_stage_closed')
        cls.clause = env.ref('quimibond_sgi.c_9001_102')

    def _nc(self, **vals):
        return self.env['quality.alert'].create(dict({
            'title': 'ZCL NC', 'team_id': self.team.id, 'stage_id': self.stage_open.id,
            'sgi_process_id': self.process.id}, **vals))

    def test_05_no_pasa_a_seguimiento_sin_clasificacion_ni_clausula(self):
        nc = self._nc()
        as_owner = nc.with_user(self.owner_user)
        self._locked("clasificación", as_owner.write, {'stage_id': self.stage_follow.id})
        self.assertEqual(nc.stage_id, self.stage_open)
        as_owner.write({'sgi_classification': 'menor'})
        self._locked("cláusula", as_owner.write, {'stage_id': self.stage_follow.id})
        # El formulario manda la cláusula y la etapa en un solo write: cuenta.
        as_owner.write({'sgi_norm_clause_id': self.clause.id, 'stage_id': self.stage_follow.id})
        self.assertEqual(nc.stage_id, self.stage_follow)
        # Nacer en Seguimiento (alta rápida en esa columna del kanban) también
        # es pasar a Seguimiento: se revisa antes de crear.
        Alert = self.env['quality.alert'].with_user(self.owner_user)
        self._locked("Seguimiento", Alert.with_context(default_stage_id=self.stage_follow.id).create,
                     {'title': 'ZCL NC rápida', 'team_id': self.team.id,
                      'sgi_process_id': self.process.id})
        self.assertFalse(self.env['quality.alert'].search([('title', '=', 'ZCL NC rápida')]))

    def test_06_tampoco_directo_a_cerrada_y_el_cierre_forzado_si(self):
        nc = self._nc()
        self._locked("clasificación", nc.with_user(self.mast).write,
                     {'stage_id': self.stage_closed.id})
        self.assertEqual(nc.stage_id, self.stage_open)
        self.env['sgi.nc.force.close'].with_user(self.mast).create({
            'alert_id': nc.id, 'reason': 'Registro duplicado de otra NC'}).action_confirm()
        self.assertEqual(nc.stage_id, self.stage_closed)

    def test_07_no_atrapa_lo_existente(self):
        # Editar una NC abierta sin clasificación sigue permitido.
        nc = self._nc()
        nc.with_user(self.owner_user).write({'sgi_deviation': 'Desviación ZCL'})
        # Reabrir una cerrada (D-009) no pide clasificación.
        self.env['sgi.nc.force.close'].with_user(self.mast).create({
            'alert_id': nc.id, 'reason': 'Cierre de prueba ZCL'}).action_confirm()
        nc.with_user(self.owner_user).write({'stage_id': self.stage_follow.id})
        self.assertEqual(nc.stage_id, self.stage_follow)
        # El sistema (superusuario) no pasa por el candado.
        other = self._nc()
        other.write({'stage_id': self.stage_follow.id})
        self.assertEqual(other.stage_id, self.stage_follow)


@tagged('post_install', '-at_install')
class TestRevisionPorLaDireccion(_Case):

    def _review(self, **vals):
        return self.env['sgi.management.review'].create(dict({
            'date': date(2051, 7, 1), 'period_from': date(2051, 1, 1),
            'period_to': date(2051, 6, 30)}, **vals))

    def _agreement(self, review, name, responsible=None, deadline=date(2051, 9, 30)):
        return self.env['sgi.management.review.agreement'].create({
            'review_id': review.id, 'name': name,
            'responsible_id': (responsible or self.sgi_user).id, 'deadline': deadline})

    def test_08_entradas_nuevas(self):
        review = self._review(date=self.today, period_from=self.today - timedelta(days=10),
                              period_to=self.today)
        self.env['sgi.incident'].create({
            'name': 'ZRD Golpe en la cortadora', 'incident_type': 'lesion', 'severity': 'moderado',
            'days_lost': 4, 'date': fields.Datetime.now() - timedelta(days=1)})
        self.env['sgi.interested.party'].create({
            'name': 'ZRD Vecinos del parque', 'party_type': 'externa', 'category': 'comunidad',
            'needs': 'Sin ruido de noche'})
        self.env['sgi.risk'].create({'name': 'ZRD Fortaleza nueva', 'instrument': 'foda',
                                     'foda_type': 'fortaleza'})
        self.env['sgi.risk'].create({'name': 'ZRD Oportunidad de reúso de agua',
                                     'instrument': 'ryo', 'kind': 'oportunidad'})
        process = self.env['sgi.process'].create({'code': 'ZRD', 'name': 'Proceso revisión'})
        aspect = self.env['sgi.env.aspect'].create({
            'company_id': self.env['sgi.config']._sgi_company().id,
            'process_id': process.id, 'activity': 'Lavado ZRD', 'name': 'Descarga ZRD',
            'impact': 'Contaminación del agua', 'aspect_type': 'descarga', 'severity': '5',
            'frequency': '5', 'life_cycle_stage': 'proceso'})
        self.assertTrue(aspect.significant)
        project = self.env.ref('quimibond_sgi.sgi_project_improvement')
        self.env['project.task'].create({'name': 'ZRD Mejora del secado', 'project_id': project.id})
        review.action_load_inputs()
        self.assertIn('Lesión / accidente', review.incidents_summary)
        self.assertIn('Días perdidos', review.incidents_summary)
        self.assertNotIn('ZRD Golpe', review.incidents_summary, "Solo conteos: sin títulos.")
        self.assertIn('ZRD Vecinos del parque', review.context_summary)
        self.assertIn('ZRD Fortaleza nueva', review.context_summary)
        self.assertIn(aspect.folio, review.env_aspects_summary)
        self.assertIn('ZRD Mejora del secado', review.improvement_summary)
        self.assertIn('ZRD Oportunidad de reúso de agua', review.improvement_summary)
        # Dirección los corre sin permisos de SST, Inventario ni Proyecto.
        as_director = review.with_user(self.director)
        for method in ('_sgi_load_incidents', '_sgi_load_context', '_sgi_load_env_aspects',
                       '_sgi_load_improvements', '_sgi_load_env'):
            self.assertTrue(getattr(as_director, method)(), method)

    def test_09_scrap_de_la_empresa_del_sgi(self):
        review = self._review()
        company = self.env['sgi.config']._sgi_company()
        self.assertIn(('company_id', '=', company.id), review._sgi_scrap_domain())
        self.assertIn(company.name, review._sgi_load_env())

    def test_10_acuerdo_es_su_propio_tipo(self):
        review = self._review()
        review.write(CONCLUSIONS)
        agreement = self._agreement(review, 'ZRD Comprar el medidor de energía')
        review.action_mark_done()
        line = agreement.action_line_id
        self.assertEqual(line.action_type, 'acuerdo')
        # Se termina sin la evidencia que pide una correctiva.
        line.with_user(self.sgi_user).action_mark_done()
        self.assertTrue(line.date_done)
        # Fuera de una revisión, «acuerdo» no se usa.
        risk = self.env['sgi.risk'].create({'name': 'ZRD Riesgo', 'instrument': 'ryo'})
        with self.assertRaises(ValidationError):
            self.env['sgi.action.line'].create({
                'risk_id': risk.id, 'action_type': 'acuerdo', 'name': 'ZRD mal tipo',
                'responsible_id': self.sgi_user.id, 'date_commit': self.today})

    def test_11_conclusiones_para_marcar_realizada(self):
        review = self._review()
        self._agreement(review, 'ZRD Acuerdo')
        self._locked("conclusiones", review.with_user(self.mast).action_mark_done)
        review.write(dict(CONCLUSIONS, conclusion_adequacy='   '))
        self._locked("Adecuación", review.with_user(self.mast).action_mark_done)
        self.assertEqual(review.state, 'borrador')
        review.write(CONCLUSIONS)
        review.with_user(self.mast).action_mark_done()
        self.assertEqual(review.state, 'realizada')

    def test_12_acuerdos_abiertos_pasan_a_la_siguiente(self):
        first = self._review()
        first.write(CONCLUSIONS)
        open_agr = self._agreement(first, 'ZRD Acuerdo abierto')
        done_agr = self._agreement(first, 'ZRD Acuerdo cumplido')
        first.action_mark_done()
        # Fecha de hoy: una acción no se termina en el futuro (_sgi_check_done).
        done_agr.action_line_id.write({'date_done': self.today})
        # Una revisión en borrador con acuerdo: no cuenta (no se realizó).
        draft = self._review(date=date(2051, 8, 1))
        draft_agr = self._agreement(draft, 'ZRD Acuerdo de borrador')
        first.with_user(self.mast).action_close()
        self.assertEqual(first.state, 'cerrada', "Cerrar no se bloquea.")
        self.assertTrue(first.message_ids.filtered(
            lambda m: 'ZRD Acuerdo abierto' in (m.body or '')
            and 'siguiente revisión' in (m.body or '')))
        second = self._review(date=date(2052, 1, 10), period_from=date(2051, 7, 1),
                              period_to=date(2051, 12, 31))
        second.action_load_inputs()
        self.assertIn(open_agr, second.carried_agreement_ids)
        self.assertNotIn(done_agr, second.carried_agreement_ids)
        self.assertNotIn(draft_agr, second.carried_agreement_ids)
        self.assertEqual(open_agr.review_id, first, "El acuerdo sigue siendo de su revisión.")
        html = self.env['ir.actions.report']._render_qweb_html(
            'quimibond_sgi.report_mgmt_review_document', second.ids)[0].decode()
        for text in ('Acuerdos abiertos de revisiones anteriores', 'ZRD Acuerdo abierto',
                     '15. Incidentes y desempeño de SST', 'Conclusiones (9.3.3)'):
            self.assertIn(text, html)
