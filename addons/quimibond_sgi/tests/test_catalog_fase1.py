# -*- coding: utf-8 -*-
"""Fase 1 del catálogo: roles, carga idempotente por API, revisión entera,
tipos de documento y puestos."""
from datetime import timedelta

from odoo import fields
from odoo.exceptions import UserError, ValidationError
from odoo.tests import TransactionCase, tagged
from odoo.tools import mute_logger


@tagged('post_install', '-at_install')
class TestCatalogFase1(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Job = cls.env['hr.job']
        cls.job_inv = Job.create({'name': 'JEFE DE INVENTARIOS Y \nALMACENES QA'})
        cls.job_alm = Job.create({'name': 'ENCARGADO DE ALMACEN QA'})
        cls.job_plan = Job.create({'name': 'PLANEADOR DE PRODUCCION QA'})
        cls.user = cls.env['res.users'].create({
            'name': 'Dueño QA', 'login': 'dueno.qa.sgi',
        })
        cls.owner = cls.env['hr.employee'].create({
            'name': 'Dueño QA', 'job_id': cls.job_inv.id, 'user_id': cls.user.id,
        })
        # Todos los puestos que usa la carga tienen al menos una persona.
        cls.env['hr.employee'].create({'name': 'Almacenista QA', 'job_id': cls.job_alm.id})
        cls.env['hr.employee'].create({'name': 'Planeador QA', 'job_id': cls.job_plan.id})
        cls.Activity = cls.env['sgi.process.activity']
        cls.Process = cls.env['sgi.process']
        cls.Family = cls.env['sgi.job.family']

    def _process(self, code='QA1'):
        return self.Process.create({'code': code, 'name': 'Proceso %s' % code})

    def _payload(self, **extra):
        payload = {
            'processes': [{
                'code': 'QC6', 'name': 'Almacén e inventarios QA',
                'process_type': 'cadena de valor',
                'owner_job': 'Jefe de inventarios y almacenes qa',
                'start_trigger': 'Llega material', 'end_trigger': 'Inventario cuadrado',
            }],
            'activities': [
                {
                    'process': 'QC6', 'number': 'QC6.22', 'section': 'D. Inventario',
                    'block': 'desarrollo',
                    'name': 'Revisar cada diferencia y registrar su causa',
                    'value_class': 'nva_n',
                    'roles': [
                        {'role': 'ejecuta', 'job': 'JEFE DE INVENTARIOS Y ALMACENES QA'},
                        {'role': 'participa', 'job': 'encargado de almacen qa'},
                    ],
                    'evidence': [{
                        'source_type': 'odoo_model', 'model': 'res.partner',
                        'domain': "[('active', '=', True)]",
                        'date_field': 'create_date', 'user_field': 'create_uid'}],
                    'automation': {'current': 'manual', 'target': 'asistido',
                                   'method': 'accion_automatizada'},
                    'cadence': 'semanal',
                    'links_to': ['QC6.23'],
                },
                {
                    'process': 'QC6', 'number': 'QC6.23', 'name': 'Ajustar inventario',
                    'roles': [{'role': 'ejecuta', 'job': self.job_alm.id}],
                },
            ],
            'indicators': [{'code': 'QA-IND-01', 'name': 'Exactitud QA',
                            'process': 'QC6', 'responsible': self.user.login}],
        }
        payload.update(extra)
        return payload

    # ------------------------------------------------------------------
    # Roles
    # ------------------------------------------------------------------
    def test_01_activity_needs_exactly_one_executor(self):
        process = self._process()
        with self.assertRaises(ValidationError):
            self.Activity.create({'process_id': process.id, 'number': 'QA.01', 'name': 'Sin ejecutor'})
        with self.assertRaises(ValidationError):
            self.Activity.create({
                'process_id': process.id, 'number': 'QA.02', 'name': 'Dos ejecutores',
                'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job_inv.id}),
                             (0, 0, {'role': 'ejecuta', 'job_id': self.job_alm.id})]})
        act = self.Activity.create({
            'process_id': process.id, 'number': 'QA.03', 'name': 'Un ejecutor',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job_inv.id})]})
        self.assertEqual(act.responsible_job_ids, self.job_inv,
                         "Puestos responsables se calcula desde los roles «ejecuta».")
        # Automática: cero ejecutores.
        auto = self.Activity.create({
            'process_id': process.id, 'number': 'QA.04', 'name': 'Automática',
            'automation_level_current': 'automatico'})
        self.assertFalse(auto.role_ids)
        with self.assertRaises(ValidationError):
            auto.write({'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job_inv.id})]})

    def test_02_one_unconditional_approver(self):
        process = self._process()
        with self.assertRaises(ValidationError):
            self.Activity.create({
                'process_id': process.id, 'number': 'QA.10', 'name': 'Dos aprueban',
                'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job_inv.id}),
                             (0, 0, {'role': 'aprueba', 'job_id': self.job_alm.id}),
                             (0, 0, {'role': 'aprueba', 'job_id': self.job_plan.id})]})
        act = self.Activity.create({
            'process_id': process.id, 'number': 'QA.11', 'name': 'Aprueba con condición',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job_inv.id}),
                         (0, 0, {'role': 'aprueba', 'job_id': self.job_alm.id}),
                         (0, 0, {'role': 'aprueba', 'job_id': self.job_plan.id,
                                 'condition': 'arriba del monto que se fije'})]})
        self.assertEqual(self.job_alm.sgi_approve_count, 1)
        self.assertEqual(len(act.role_ids), 3)

    def test_03_swap_executor_in_one_write(self):
        process = self._process()
        act = self.Activity.create({
            'process_id': process.id, 'number': 'QA.20', 'name': 'Cambio de ejecutor',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job_inv.id})]})
        role = act.role_ids
        act.write({'role_ids': [(2, role.id), (0, 0, {'role': 'ejecuta', 'job_id': self.job_alm.id})]})
        self.assertEqual(act.responsible_job_ids, self.job_alm)
        # Quitar el único ejecutor directamente (fuera de la actividad) no se permite.
        with self.assertRaises(ValidationError):
            act.role_ids.unlink()

    def test_04_legacy_responsible_jobs_write_creates_roles(self):
        process = self._process()
        act = self.Activity.with_context(sgi_skip_role_check=True).create({
            'process_id': process.id, 'number': 'QA.30', 'name': 'Heredada'})
        act.write({'responsible_job_ids': [(6, 0, self.job_plan.ids)]})
        self.assertEqual(act.role_ids.job_id, self.job_plan)
        self.assertEqual(act.role_ids.role, 'ejecuta')

    def test_05_number_unique_per_process(self):
        process = self._process()
        vals = {'process_id': process.id, 'number': 'QA.40', 'name': 'A',
                'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job_inv.id})]}
        self.Activity.create(vals)
        with self.assertRaises(ValidationError):
            self.Activity.create(dict(vals, name='B', role_ids=[
                (0, 0, {'role': 'ejecuta', 'job_id': self.job_alm.id})]))
        # En otro proceso sí puede repetirse.
        other = self._process('QA2')
        self.Activity.create(dict(vals, process_id=other.id, role_ids=[
            (0, 0, {'role': 'ejecuta', 'job_id': self.job_alm.id})]))

    # ------------------------------------------------------------------
    # Carga por API
    # ------------------------------------------------------------------
    def test_10_load_is_idempotent(self):
        result = self.Process.load_payload(self._payload())
        self.assertTrue(result['ok'], result['errors'])
        self.assertEqual(result['summary']['created']['activity'], 2)
        self.assertEqual(result['summary']['created']['link'], 1)
        process = self.Process.search([('code', '=', 'QC6')])
        self.assertEqual(process.owner_id, self.owner)
        self.assertEqual(process.process_type, 'cop')
        self.assertEqual(process.doc_approver_id, self.user,
                         "El aprobador por omisión es el usuario del dueño.")
        act = self.Activity.search([('process_id', '=', process.id), ('number', '=', 'QC6.22')])
        self.assertEqual(act.responsible_job_ids, self.job_inv,
                         "El nombre con saltos de línea y mayúsculas se resuelve normalizado.")
        self.assertEqual(act.measure_model_name, 'res.partner')
        self.assertEqual(act.measure_cadence, 'semanal')
        self.assertEqual(act.automation_level_target, 'asistido')
        self.assertEqual(act.next_activity_ids.number, 'QC6.23')
        indicator = self.env['sgi.indicator'].search([('code', '=', 'QA-IND-01')])
        self.assertTrue(indicator.nc_on_red, "Con responsable, NC en rojo por omisión.")

        roles_before = self.env['sgi.activity.role'].search_count([('process_id', '=', process.id)])
        again = self.Process.load_payload(self._payload())
        self.assertTrue(again['ok'], again['errors'])
        self.assertFalse(again['changes'], "La segunda carga no cambia nada.")
        dry = self.Process.load_payload(self._payload(), dry_run=True)
        self.assertFalse(dry['changes'], "El dry-run de la segunda vez reporta cero cambios.")
        self.assertEqual(
            self.env['sgi.activity.role'].search_count([('process_id', '=', process.id)]),
            roles_before)

    def test_11_dry_run_writes_nothing(self):
        result = self.Process.load_payload(self._payload(dry_run=True))
        self.assertTrue(result['dry_run'])
        self.assertEqual(result['summary']['created']['process'], 1)
        self.assertFalse(self.Process.with_context(active_test=False).search(
            [('code', '=', 'QC6')]), "El dry-run no deja nada escrito.")

    def test_12_unknown_job_is_an_error_and_never_created(self):
        payload = self._payload()
        payload['activities'][1]['roles'] = [{'role': 'ejecuta', 'job': 'PUESTO QUE NO EXISTE'}]
        jobs_before = self.env['hr.job'].search_count([])
        with mute_logger('odoo.sql_db'):
            result = self.Process.load_payload(payload)
        self.assertFalse(result['ok'])
        self.assertTrue(any('PUESTO QUE NO EXISTE' in e['message'] for e in result['errors']))
        self.assertEqual(self.env['hr.job'].search_count([]), jobs_before)
        # Una transacción por proceso: nada del proceso quedó.
        self.assertFalse(self.Process.search([('code', '=', 'QC6')]))

    def test_13_missing_activities_are_archived(self):
        self.Process.load_payload(self._payload())
        payload = self._payload()
        payload['activities'] = payload['activities'][:1]
        payload['activities'][0].pop('links_to')
        result = self.Process.load_payload(payload, dry_run=True)
        self.assertEqual(result['summary'].get('archived', {}).get('activity'), 1)
        result = self.Process.load_payload(payload)
        self.assertTrue(result['ok'], result['errors'])
        archived = self.Activity.with_context(active_test=False).search([('number', '=', 'QC6.23')])
        self.assertFalse(archived.active)

    def test_14_ambiguous_job_reported(self):
        self.env['hr.job'].create({'name': 'Encargado de almacen QA'})
        job, error = self.env['hr.job']._sgi_find_job('ENCARGADO DE ALMACEN QA', self.env.company)
        self.assertFalse(job)
        self.assertIn('ambiguo', error)
        report = self.env['hr.job'].sgi_merge_duplicate_jobs(dry_run=True)
        merged = [g for g in report['merged'] if g['name'].casefold() == 'encargado de almacen qa']
        self.assertEqual(len(merged), 1)
        self.assertTrue(self.env['hr.job'].search([('name', '=ilike', 'encargado de almacen qa')]),
                        "El dry-run de la fusión no archiva nada.")

    # ------------------------------------------------------------------
    # Documentos: tipo, revisión, clave anterior
    # ------------------------------------------------------------------
    def _doc(self, **vals):
        base = {'name': 'Doc QA', 'type': 'binary', 'sgi_is_controlled': True}
        base.update(vals)
        return self.env['documents.document'].create(base)

    def test_20_doc_type_patterns(self):
        process = self._process('C6Q')
        dtype = self.env.ref('quimibond_sgi.sgi_doc_type_instructivo')
        doc = self._doc(sgi_doc_type_id=dtype.id, sgi_code='IT-C6Q-06', sgi_process_id=process.id)
        self.assertEqual(doc.sgi_doc_type, 'instructivo', "El campo viejo se calcula del tipo.")
        with self.assertRaises(ValidationError):
            self._doc(sgi_doc_type_id=dtype.id, sgi_code='IT-X9-06', sgi_process_id=process.id)
        # La nomenclatura heredada se sigue aceptando.
        self._doc(sgi_doc_type='instructivo', sgi_code='IT-P-A28-09')
        self.assertEqual(dtype.sgi_next_code(process), 'IT-C6Q-07')
        # Escribir el campo viejo resuelve el tipo.
        legacy = self._doc(sgi_doc_type='formato', sgi_code='F-P-A28-77')
        self.assertEqual(legacy.sgi_doc_type_id, self.env.ref('quimibond_sgi.sgi_doc_type_formato'))

    def test_21_revision_unique_and_increasing(self):
        self._doc(sgi_doc_type='procedimiento', sgi_code='P-A77', sgi_revision=3,
                  sgi_state='obsoleto')
        with self.assertRaises(ValidationError):
            self._doc(sgi_doc_type='procedimiento', sgi_code='P-A77', sgi_revision=3,
                      sgi_state='vigente')
        with self.assertRaises(ValidationError):
            self._doc(sgi_doc_type='procedimiento', sgi_code='P-A77', sgi_revision=2,
                      sgi_state='vigente')
        doc = self._doc(sgi_doc_type='procedimiento', sgi_code='P-A77', sgi_revision=4,
                        sgi_state='vigente')
        with self.assertRaises(ValidationError):
            doc.write({'sgi_revision': 1})
        doc.write({'sgi_revision': 5})
        self.assertEqual(doc.sgi_revision_label, '05')

    def test_22_code_not_reused_in_another_family(self):
        dtype = self.env.ref('quimibond_sgi.sgi_doc_type_formato')
        self._doc(sgi_doc_type_id=dtype.id, sgi_code='F-P-A66-01', sgi_state='obsoleto',
                  sgi_process_id=self._process('QX1').id)
        with self.assertRaises(ValidationError):
            self._doc(sgi_doc_type_id=dtype.id, sgi_code='F-P-A66-01', sgi_revision=1,
                      sgi_process_id=self._process('QX2').id)

    def test_23_previous_code_still_found(self):
        doc = self._doc(sgi_doc_type='procedimiento', sgi_code='P-A55', sgi_state='vigente')
        doc.write({'sgi_code': 'PR-QPC', 'sgi_process_id': self._process('QPC').id})
        self.assertEqual(doc.sgi_previous_code, 'P-A55')
        found = self.env['documents.document']._sgi_find_by_code('P-A55')
        self.assertEqual(found, doc)

    def test_30_process_owner_validity(self):
        process = self.Process.create({'code': 'QOW', 'name': 'Dueño', 'owner_id': self.owner.id})
        self.assertTrue(process.owner_valid)
        orphan = self.env['hr.employee'].create({'name': 'Sin usuario QA'})
        process.owner_id = orphan
        self.assertFalse(process.owner_valid)
        self.assertEqual(process.health, 'rojo')

    # ------------------------------------------------------------------
    # Fase 1.1: familias, roles relativos, vacantes
    # ------------------------------------------------------------------
    def _jobs(self, *names):
        return self.env['hr.job'].create([{'name': n} for n in names])

    def test_40_family_counts_as_one_executor(self):
        process = self._process('QF1')
        jobs = self._jobs('OPERADOR TEJIDO QA A', 'OPERADOR TEJIDO QA B')
        family = self.Family.create({'code': 'QA-TEJ', 'name': 'Tejido QA',
                                     'job_ids': [(6, 0, jobs.ids)]})
        self.assertEqual(jobs.sgi_family_id, family)
        act = self.Activity.create({
            'process_id': process.id, 'number': 'QF.01', 'name': 'Tejer',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'target_type': 'family',
                                 'family_id': family.id})]})
        self.assertEqual(act.responsible_job_ids, jobs, "La familia se expande a sus puestos.")
        with self.assertRaises(ValidationError):
            self.Activity.create({
                'process_id': process.id, 'number': 'QF.02', 'name': 'Dos ejecutores',
                'role_ids': [(0, 0, {'role': 'ejecuta', 'target_type': 'family',
                                     'family_id': family.id}),
                             (0, 0, {'role': 'ejecuta', 'job_id': self.job_inv.id})]})

    def test_41_target_must_match_type(self):
        process = self._process('QF2')
        family = self.Family.create({'code': 'QA-F2', 'name': 'F2',
                                     'job_ids': [(6, 0, self._jobs('F2 QA').ids)]})
        with self.assertRaises(ValidationError):
            self.Activity.create({
                'process_id': process.id, 'number': 'QF.10', 'name': 'Mal',
                'role_ids': [(0, 0, {'role': 'ejecuta', 'target_type': 'family',
                                     'family_id': family.id, 'job_id': self.job_inv.id})]})

    def test_42_relative_executor(self):
        process = self._process('QF3')
        act = self.Activity.create({
            'process_id': process.id, 'number': 'QF.20', 'name': 'Solicitar',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'target_type': 'relative',
                                 'relative_role': 'solicitante'})]})
        self.assertEqual(act.role_ids._sgi_staffing_state(), 'na',
                         "Un relativo no dispara «puesto sin persona».")
        self.assertIsNone(act._sgi_executor_jobs())
        payload = {'processes': [{'code': 'QF3', 'name': 'Proceso QF3'}],
                   'activities': [{'process': 'QF3', 'number': 'QF.20', 'name': 'Solicitar',
                                   'roles': [{'role': 'ejecuta', 'relative': 'solicitante'},
                                             {'role': 'aprueba',
                                              'relative': 'jefe_del_solicitante'}]}]}
        result = self.Process.load_payload(payload)
        self.assertTrue(result['ok'], result['errors'])
        self.assertFalse(result['warnings'])

    def test_43_zero_employee_job_and_vacancy(self):
        empty = self._jobs('COORDINADOR VACANTE QA')

        def load():
            return self.Process.load_payload({
                'processes': [{'code': 'QF4', 'name': 'Proceso QF4'}],
                'activities': [{'process': 'QF4', 'number': 'QF.30', 'name': 'Coordinar',
                                'roles': [{'role': 'ejecuta', 'job': empty.id}]}]},
                dry_run=True)
        result = load()
        self.assertFalse(result['ok'], "Sin vacante: error.")
        empty.write({'sgi_vacancy_approved': True,
                     'sgi_vacancy_until': fields.Date.today() + timedelta(days=30)})
        result = load()
        self.assertTrue(result['ok'], result['errors'])
        self.assertTrue(any('vacante' in w['message'] for w in result['warnings']))
        empty.write({'sgi_vacancy_until': fields.Date.today() - timedelta(days=1)})
        self.assertFalse(load()['ok'], "Vacante vencida: error.")

    def test_44_job_in_one_family_per_company(self):
        job = self._jobs('UNA FAMILIA QA')
        self.Family.create({'code': 'QA-UNO', 'name': 'Uno', 'job_ids': [(6, 0, job.ids)]})
        with self.assertRaises(ValidationError):
            self.Family.create({'code': 'QA-DOS', 'name': 'Dos', 'job_ids': [(6, 0, job.ids)]})

    def test_45_families_block_idempotent(self):
        jobs = self._jobs('BLOQUE QA A', 'BLOQUE QA B')
        payload = {'families': [{'code': 'QA-BLK', 'name': 'Bloque QA', 'jobs': jobs.ids}]}
        first = self.Process.load_payload(payload)
        self.assertEqual(first['summary']['created']['family'], 1)
        again = self.Process.load_payload(payload)
        self.assertFalse(again['changes'])
        self.assertFalse(self.Process.load_payload(payload, dry_run=True)['changes'])

    def test_46_ambiguous_name_lists_candidates(self):
        dup = self._jobs('Ambiguo QA', 'AMBIGUO   QÁ')
        self.env['hr.employee'].create([{'name': 'e%d' % j.id, 'job_id': j.id} for j in dup])
        result = self.Process.load_payload({
            'processes': [{'code': 'QF5', 'name': 'Proceso QF5'}],
            'activities': [{'process': 'QF5', 'number': 'QF.40', 'name': 'x',
                            'roles': [{'role': 'ejecuta', 'job': 'ambiguo qa'}]}]},
            dry_run=True)
        message = ' '.join(e['message'] for e in result['errors'])
        self.assertIn('ambiguo', message)
        for job in dup:
            self.assertIn(str(job.id), message)

    # ------------------------------------------------------------------
    # Fase 1.1: método de medición y quién ejecutó
    # ------------------------------------------------------------------
    def _measured(self, process, number, ref, roles, **extra):
        vals = {
            'process_id': process.id, 'number': number, 'name': 'Medida %s' % number,
            'measure_method': 'odoo', 'measure_model_name': 'res.partner',
            'measure_domain': "[('ref', '=', '%s')]" % ref,
            'measure_date_field': 'create_date', 'measure_user_field': 'user_id',
            'role_ids': roles,
        }
        vals.update(extra)
        return self.Activity.create(vals)

    def _user_with_job(self, login, job):
        user = self.env['res.users'].create({'name': login, 'login': login})
        self.env['hr.employee'].create({'name': login, 'user_id': user.id, 'job_id': job.id})
        return user

    def test_50_adherence_with_family(self):
        process = self._process('QM1')
        fam_jobs = self._jobs('FAMILIA ADH QA A', 'FAMILIA ADH QA B')
        family = self.Family.create({'code': 'QA-ADH', 'name': 'Adh',
                                     'job_ids': [(6, 0, fam_jobs.ids)]})
        inside = self._user_with_job('adh.dentro.qa', fam_jobs[1])
        outside = self._user_with_job('adh.fuera.qa', self.job_plan)
        self.env['res.partner'].create([
            {'name': 'P1', 'ref': 'QA-ADH', 'user_id': inside.id},
            {'name': 'P2', 'ref': 'QA-ADH', 'user_id': outside.id}])
        act = self._measured(process, 'QM.01', 'QA-ADH', [
            (0, 0, {'role': 'ejecuta', 'target_type': 'family', 'family_id': family.id})])
        act._sgi_measure()
        self.assertEqual(act.measure_count_30d, 2)
        self.assertEqual(act.measure_adherence_pct, 50.0)
        self.assertEqual(act.measure_count_other_job, 1)
        classes = {row['user_id']: row['class'] for row in act.measure_executor_json}
        self.assertEqual(classes[inside.id], 'correcto')
        self.assertEqual(classes[outside.id], 'otro_puesto')

    def test_51_generic_and_system(self):
        process = self._process('QM2')
        worker = self._user_with_job('gen.worker.qa', self.job_inv)
        generic = self.env['res.users'].create({'name': 'Supervisor QA', 'login': 'sup.gen.qa'})
        self.env['ir.config_parameter'].sudo().set_param(
            'quimibond_sgi.generic_user_ids', str(generic.id))
        root = self.env.ref('base.user_root')
        self.env['res.partner'].create([
            {'name': 'G1', 'ref': 'QA-GEN', 'user_id': worker.id},
            {'name': 'G2', 'ref': 'QA-GEN', 'user_id': generic.id},
            {'name': 'G3', 'ref': 'QA-GEN', 'user_id': root.id}])
        act = self._measured(process, 'QM.10', 'QA-GEN', [
            (0, 0, {'role': 'ejecuta', 'job_id': self.job_inv.id})])
        act._sgi_measure()
        classes = {row['user_id']: row['class'] for row in act.measure_executor_json}
        self.assertEqual(classes[generic.id], 'generico')
        self.assertEqual(classes[root.id], 'sistema')
        self.assertEqual(act.measure_count_system, 1)
        self.assertEqual(act.measure_adherence_pct, 50.0,
                         "El sistema no cuenta: 1 correcto entre 2 atribuibles.")
        self.assertIn('genérica', act.measure_warning)

    def test_52_manual_looks_automatic(self):
        process = self._process('QM3')
        worker = self._user_with_job('auto.worker.qa', self.job_inv)
        root = self.env.ref('base.user_root')
        self.env['res.partner'].create(
            [{'name': 'S%d' % i, 'ref': 'QA-AUTO', 'user_id': root.id} for i in range(3)]
            + [{'name': 'W%d' % i, 'ref': 'QA-AUTO', 'user_id': worker.id} for i in range(2)])
        act = self._measured(process, 'QM.20', 'QA-AUTO', [
            (0, 0, {'role': 'ejecuta', 'job_id': self.job_inv.id})],
            automation_level_current='manual')
        act._sgi_measure()
        self.assertIn('Parece automática', act.measure_warning or '')

    def test_53_consequence_copies_and_no_cycles(self):
        process = self._process('QM4')
        roles = [(0, 0, {'role': 'ejecuta', 'job_id': self.job_inv.id})]
        self.env['res.partner'].create([{'name': 'C%d' % i, 'ref': 'QA-CON'} for i in range(2)])
        proof = self._measured(process, 'QM.30', 'QA-CON', roles)
        act = self.Activity.create({
            'process_id': process.id, 'number': 'QM.31', 'name': 'Descargar',
            'measure_method': 'consecuencia', 'measure_proxy_activity_id': proof.id,
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job_alm.id})]})
        (proof | act)._sgi_measure()
        self.assertEqual(act.measure_count_30d, 2)
        self.assertEqual(act.measure_state, proof.measure_state)
        with self.assertRaises(ValidationError):
            proof.write({'measure_method': 'consecuencia',
                         'measure_proxy_activity_id': act.id})

    def test_54_procedure_needs_measure_methods(self):
        process = self._process('QM5')
        act = self.Activity.create({
            'process_id': process.id, 'number': 'QM.40', 'name': 'Sin método',
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job_inv.id})]})
        doc = self._doc(sgi_doc_type='procedimiento', sgi_code='P-A91',
                        sgi_process_id=process.id, sgi_state='borrador')
        with self.assertRaises(UserError):
            doc.write({'sgi_state': 'vigente'})
        act.measure_method = 'no_aplica'
        with self.assertRaises(UserError):
            doc.write({'sgi_state': 'vigente'})
        act.measure_justification = "Su resultado es la orden de producción."
        doc.write({'sgi_state': 'vigente'})
        self.assertEqual(doc.sgi_state, 'vigente')
        # Ya vigente: una actividad nueva no entra sin método.
        with self.assertRaises(ValidationError):
            self.Activity.create({
                'process_id': process.id, 'number': 'QM.41', 'name': 'Nueva',
                'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': self.job_inv.id})]})

    def test_55_load_measure_block(self):
        payload = self._payload()
        payload['activities'][1]['measure'] = {'method': 'consecuencia', 'proxy': 'QC6.22'}
        result = self.Process.load_payload(payload)
        self.assertTrue(result['ok'], result['errors'])
        acts = self.Activity.search([('number', 'in', ('QC6.22', 'QC6.23'))])
        by_number = {a.number: a for a in acts}
        self.assertEqual(by_number['QC6.22'].measure_method, 'odoo',
                         "La evidencia implica «Registro en Odoo».")
        self.assertEqual(by_number['QC6.22'].measure_user_field, 'create_uid')
        self.assertEqual(by_number['QC6.23'].measure_proxy_activity_id, by_number['QC6.22'])
        self.assertFalse(self.Process.load_payload(payload)['changes'])
