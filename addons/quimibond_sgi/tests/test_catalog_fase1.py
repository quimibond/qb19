# -*- coding: utf-8 -*-
"""Fase 1 del catálogo: roles, carga idempotente por API, revisión entera,
tipos de documento y puestos."""
from odoo.exceptions import ValidationError
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
        cls.Activity = cls.env['sgi.process.activity']
        cls.Process = cls.env['sgi.process']

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
