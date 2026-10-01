# -*- coding: utf-8 -*-
"""Bloque 3 de formularios (57.70.0 a 57.72.0): duplicados, registro lleno,
clave equivocada, alta de formulario de Odoo, responsable = dueño del
proceso y clave nueva D-02 con la búsqueda por clave anterior.

Cada prueba trabaja con documentos, procesos y actividades propios (los
reales se ocultan con ``sgi_hide_real_documents``) y les pasa sus propias
tablas a los métodos, así que corren igual en una base nueva y en una copia
de producción."""
import importlib.util
import os

from odoo.tests import TransactionCase, tagged
from odoo.tests.common import new_test_user

from ..models.sgi_formatos_bloque3 import (
    SGI_B3_LINKS, SGI_B3_MERGES, SGI_B3_ODOO_FORMS, SGI_B3_RECODE, SGI_B3_UNCONTROL)
from .common_documents import sgi_hide_real_documents, sgi_neutralize_dropbox_contradictions

_MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _run_migration(env, version):
    path = os.path.join(_MODULE_DIR, 'migrations', version, 'post-migrate.py')
    spec = importlib.util.spec_from_file_location('sgi_mig_%s' % version.replace('.', '_'), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.migrate(env.cr, '19.0.57.68.0')


class _Bloque3Common(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        sgi_neutralize_dropbox_contradictions(cls.env)
        cls.Doc = cls.env['documents.document']
        cls.company = cls.env['sgi.config']._sgi_company()
        cls.job = cls.env['hr.job'].create({'name': 'ZB3 Puesto bloque 3'})
        cls.t = {code: cls.env['sgi.document.type'].search([('code', '=', code)], limit=1)
                 for code in ('formato', 'formato_it', 'instructivo', 'dat', 'anexo',
                              'formulario_odoo', 'procedimiento')}

    @classmethod
    def _process(cls, code, owner=None):
        return cls.env['sgi.process'].create({
            'code': code, 'name': 'Proceso %s' % code, 'company_id': cls.company.id,
            'owner_id': owner.id if owner else False})

    @classmethod
    def _activity(cls, process, step, docs=None):
        return cls.env['sgi.process.activity'].create({
            'process_id': process.id, 'step': step, 'name': 'Actividad %d' % step,
            'role_ids': [(0, 0, {'role': 'ejecuta', 'job_id': cls.job.id})],
            'format_document_ids': [(6, 0, docs.ids if docs else [])]})

    @classmethod
    def _doc(cls, code, dtype, process, **extra):
        vals = {'name': '%s prueba.xlsx' % code, 'type': 'binary', 'sgi_is_controlled': True,
                'sgi_doc_type_id': cls.t[dtype].id, 'sgi_code': code, 'sgi_state': 'vigente',
                'sgi_process_id': process.id}
        vals.update(extra)
        doc = cls.Doc.create(vals)
        # La clave del Dropbox la pone la migración 56.32.0.
        if doc.sgi_is_controlled and not doc.sgi_previous_code:
            cls.Doc._sgi_migrate_previous_codes(ids=doc.ids)
        return doc


@tagged('post_install', '-at_install')
class TestBloque3Duplicados(_Bloque3Common):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.proc = cls._process('XB3')
        cls.keep = cls._doc('F-P-A81-01', 'formato', cls.proc)
        cls.dup = cls._doc('F-P-A81-02', 'formato', cls.proc, sgi_migration_class='a')
        cls.other = cls._doc('F-P-A81-03', 'formato', cls.proc)
        cls.lonely = cls._doc('F-P-A81-04', 'formato', cls.proc)
        cls.printed = cls._doc('F-P-A81-05', 'formato', cls.proc)
        cls.act1 = cls._activity(cls.proc, 1, cls.dup | cls.other)
        cls.act2 = cls._activity(cls.proc, 2, cls.dup | cls.keep)
        cls.act3 = cls._activity(cls.proc, 3)
        cls.fmap = cls.env['sgi.format.map'].create({
            'document_id': cls.printed.id, 'sgi_code': 'F-P-A81-05', 'note': 'prueba bloque 3'})

    def test_01_fusion_mueve_ligas_y_da_de_baja_sin_archivar(self):
        table = (
            (self.dup.id, 'F-P-A81-02', self.keep.id, 'F-P-A81-01', "Duplicado de F-P-A81-01."),
            (self.lonely.id, 'F-P-A81-04', None, None, "Sin uso."),
            (self.printed.id, 'F-P-A81-05', self.keep.id, 'F-P-A81-01', "Lo imprime un mapeo."),
            (self.other.id, 'F-P-A81-99', self.keep.id, 'F-P-A81-01', "Clave que no coincide."),
        )
        done = self.Doc._sgi_b3_merge_duplicates(table, company=self.company)
        self.assertEqual(set(done), {self.dup.id, self.lonely.id})
        self.assertEqual(self.act1.format_document_ids, self.other | self.keep,
                         "La liga del duplicado pasa al que se conserva.")
        self.assertEqual(self.act2.format_document_ids, self.keep, "Ya lo tenía: solo sale el duplicado.")
        for doc in (self.dup, self.lonely):
            self.assertTrue(doc.active, "Activo: archivar manda a la papelera de Documentos.")
            self.assertEqual(doc.sgi_state, 'obsoleto')
            self.assertEqual(doc.sgi_migration_state, 'baja')
            self.assertTrue(doc.sgi_obsolete_reason)
            self.assertTrue(doc.sgi_obsolete_date)
        self.assertEqual(self.dup.sgi_obsolete_reason, "Duplicado de F-P-A81-01.")
        self.assertEqual(self.printed.sgi_state, 'vigente', "El que imprime un mapeo no se toca.")
        self.assertEqual(self.other.sgi_state, 'vigente', "Clave distinta a la esperada: se salta.")
        self.assertEqual(self.keep.sgi_state, 'vigente')
        # Idempotente.
        self.assertEqual(self.Doc._sgi_b3_merge_duplicates(table, company=self.company), {})
        self.assertEqual(self.act1.format_document_ids, self.other | self.keep)

    def test_02_ligas_agregan_sin_quitar(self):
        table = (('XB3.03', self.keep.id, 'F-P-A81-01'), ('XB3.01', self.keep.id, 'F-P-A81-01'),
                 ('XB3.99', self.keep.id, 'F-P-A81-01'))
        done = self.Doc._sgi_b3_link_formats(table, company=self.company)
        self.assertEqual(set(done), {'XB3.03', 'XB3.01'})
        self.assertEqual(self.act3.format_document_ids, self.keep)
        self.assertEqual(self.act1.format_document_ids, self.dup | self.other | self.keep)
        self.assertEqual(self.Doc._sgi_b3_link_formats(table, company=self.company), {})

    def test_03_otra_empresa_no_se_toca(self):
        other_company = self.env['res.company'].create({'name': 'ZB3 Otra empresa'})
        done = self.Doc._sgi_b3_merge_duplicates(
            ((self.dup.id, 'F-P-A81-02', self.keep.id, 'F-P-A81-01', "x"),), company=other_company)
        self.assertEqual(done, {})
        self.assertEqual(self.dup.sgi_state, 'vigente')

    def test_04_tablas_reales_bien_formadas(self):
        ids = [row[0] for row in SGI_B3_MERGES]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(sorted(ids), sorted([4026, 3752, 4023, 3725, 3943, 3989, 4001]))
        for _dup, code, keep, keep_code, reason in SGI_B3_MERGES:
            self.assertTrue(code and reason)
            self.assertEqual(bool(keep), bool(keep_code))
        self.assertEqual({row[0] for row in SGI_B3_LINKS}, {'C2.43', 'C6.12', 'E2.37'})
        self.assertEqual([row[0] for row in SGI_B3_UNCONTROL], [5152])
        self.assertEqual([row[0] for row in SGI_B3_RECODE], [4060])
        self.assertEqual([spec['code'] for spec in SGI_B3_ODOO_FORMS], ['F-P-A28-13', 'F-P-A28-11'])
        for spec in SGI_B3_ODOO_FORMS:
            self.assertRegex(spec['menu'], r'^\w+\.\w+$')


@tagged('post_install', '-at_install')
class TestBloque3DatosMalos(_Bloque3Common):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.proc = cls._process('XB4')
        cls.filled = cls._doc('F-P-V82-04', 'formato', cls.proc)
        cls.filled_linked = cls._doc('F-P-V82-05', 'formato', cls.proc)
        cls.lum = cls._doc('F-P-E82-01', 'formato', cls.proc)
        cls.act_linked = cls._activity(cls.proc, 1, cls.filled_linked)
        cls.act_nom = cls._activity(cls.proc, 2)
        cls.fmap = cls.env['sgi.format.map'].create({'sgi_code': 'F-P-E82-01', 'note': 'matriz'})

    def test_01_registro_lleno_deja_de_ser_controlado(self):
        table = ((self.filled.id, 'F-P-V82-04', "Registro lleno."),
                 (self.filled_linked.id, 'F-P-V82-05', "Ligado a una actividad."))
        done = self.Doc._sgi_b3_uncontrol(table, company=self.company)
        self.assertEqual(set(done), {self.filled.id})
        self.assertFalse(self.filled.sgi_is_controlled)
        self.assertFalse(self.filled.sgi_state)
        self.assertTrue(self.filled.active, "No se borra ni se archiva.")
        self.assertEqual(self.filled.sgi_previous_code, 'F-P-V82-04')
        self.assertTrue(self.filled_linked.sgi_is_controlled, "Ligado: MAST decide.")
        self.assertEqual(self.Doc._sgi_b3_uncontrol(table, company=self.company), {})

    def test_02_clave_equivocada_queda_libre(self):
        self.assertEqual(self.fmap.sgi_live_label(), 'F-P-E82-01 · Rev. 00',
                         "Antes, el mapeo sin documento encontraba la luminaria por su clave.")
        table = ((self.lum.id, 'F-P-E82-01', 'F-P-S82-02', "Es de SST.", 'XB4.02'),)
        done = self.Doc._sgi_b3_recode(table, company=self.company)
        self.assertEqual(done, {self.lum.id: ('F-P-E82-01', 'F-P-E82-01')})
        self.assertEqual(self.lum.sgi_code, 'F-P-S82-02')
        self.assertEqual(self.lum.sgi_previous_code, 'F-P-S82-02')
        self.assertEqual(self.lum.sgi_legacy_family, 'P-S82')
        self.assertEqual(self.act_nom.format_document_ids, self.lum, "Actividad vacía: se liga.")
        self.assertFalse(self.Doc._sgi_find_by_code('F-P-E82-01'), "La clave equivocada queda libre.")
        self.assertEqual(self.Doc._sgi_find_by_code('F-P-S82-02'), self.lum)
        self.env.invalidate_all()
        self.assertEqual(self.fmap.sgi_live_label(), 'F-P-E82-01',
                         "El mapeo de la matriz imprime la clave sin revisión hasta que exista.")
        # Idempotente, y la clave ocupada no se pisa.
        self.assertEqual(self.Doc._sgi_b3_recode(table, company=self.company), {})
        other = self._doc('F-P-E82-07', 'formato', self.proc)
        busy = self._doc('F-P-S82-08', 'formato', self.proc)
        self.assertEqual(self.Doc._sgi_b3_recode(
            ((other.id, 'F-P-E82-07', busy.sgi_code, "x", False),), company=self.company), {})
        self.assertEqual(other.sgi_code, 'F-P-E82-07')

    def test_03_alta_de_formulario_de_odoo_sin_archivo(self):
        model = self.env['ir.model']._get('res.partner')
        fmap = self.env['sgi.format.map'].create({
            'model_id': model.id, 'sgi_code': 'F-P-A83-18', 'sgi_code_alt': 'F-P-A83-13'})
        act = self._activity(self.proc, 3)
        spec = {'code': 'F-P-A83-13', 'name': 'F-P-A83-13 Pronóstico prueba', 'process': 'XB4',
                'menu': 'quimibond_sgi.menu_sgi_env_aspects', 'target': 'SGI → prueba',
                'activity': 'XB4.03', 'map_model': 'res.partner', 'reason': 'Alta de prueba.'}
        done = self.Doc._sgi_b3_register_odoo_forms((spec,), company=self.company)
        doc = self.Doc.browse(done['F-P-A83-13'])
        self.assertTrue(doc.sgi_is_controlled)
        self.assertEqual(doc.sgi_doc_type, 'formulario_odoo')
        self.assertEqual(doc.sgi_state, 'vigente')
        self.assertFalse(doc.attachment_id, "Sin archivo: lo que se llena es la pantalla.")
        self.assertEqual(doc.sgi_previous_code, 'F-P-A83-13')
        self.assertEqual(doc.sgi_odoo_menu_id, self.env.ref('quimibond_sgi.menu_sgi_env_aspects'))
        self.assertEqual(doc.sgi_migration_state, 'migrado')
        self.assertTrue(doc.sgi_owner_id, "Nace con responsable (N-001).")
        self.assertEqual(act.format_document_ids, doc)
        self.assertEqual(fmap.document_alt_id, doc)
        self.assertEqual(fmap.sgi_live_label(alt=True), 'F-P-A83-13 · Rev. 00')
        self.assertEqual(self.Doc._sgi_b3_register_odoo_forms((spec,), company=self.company), {},
                         "Idempotente: la clave ya existe.")

    def test_04_post_migrate_corre(self):
        _run_migration(self.env, '19.0.57.70.0')
        # Idempotente: la segunda corrida no cambia nada.
        again = self.Doc._sgi_formatos_bloque3()
        self.assertEqual(again['merges'], {})
        self.assertEqual(again['links'], {})
        self.assertEqual(again['uncontrol'], {})
        self.assertEqual(again['recode'], {})
        self.assertEqual(again['forms'], {})


@tagged('post_install', '-at_install')
class TestBloque3Responsable(_Bloque3Common):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        groups = 'base.group_user,quimibond_sgi.group_sgi_user'
        cls.mast = new_test_user(cls.env, login='zb3_mast',
                                 groups='base.group_user,quimibond_sgi.group_sgi_manager')
        cls.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.mast_user_id', str(cls.mast.id))
        cls.owner_user = new_test_user(cls.env, login='zb3_duenio', groups=groups)
        cls.gone_user = new_test_user(cls.env, login='zb3_baja', groups=groups)
        cls.gone_user.active = False
        cls.reassigned = new_test_user(cls.env, login='zb3_otro', groups=groups)
        Employee = cls.env['hr.employee']
        cls.p_ok = cls._process('XB5', Employee.create({'name': 'ZB3 Dueño', 'user_id': cls.owner_user.id}))
        cls.p_nouser = cls._process('XB6', Employee.create({'name': 'ZB3 Sin usuario'}))
        cls.p_gone = cls._process('XB7', Employee.create({'name': 'ZB3 Inactivo',
                                                          'user_id': cls.gone_user.id}))
        cls.p_mast = cls._process('XB8', Employee.create({'name': 'ZB3 MAST', 'user_id': cls.mast.id}))
        own = {'sgi_owner_id': cls.mast.id}
        cls.f_ok = cls._doc('F-P-A84-01', 'formato', cls.p_ok, **own)
        cls.fit_ok = cls._doc('F-IT-P-A84-01-01', 'formato_it', cls.p_ok, **own)
        cls.form_ok = cls._doc('F-P-A84-02', 'formulario_odoo', cls.p_ok, **own)
        cls.it_ok = cls._doc('IT-P-A84-01', 'instructivo', cls.p_ok, **own)
        cls.obs_ok = cls._doc('F-P-A84-03', 'formato', cls.p_ok, sgi_state='obsoleto', **own)
        cls.kept_ok = cls._doc('F-P-A84-04', 'formato', cls.p_ok, sgi_owner_id=cls.reassigned.id)
        cls.pi01 = cls._doc('F-P-I01-84', 'formato', cls.p_ok, **own)
        cls.f_nouser = cls._doc('F-P-A85-01', 'formato', cls.p_nouser, **own)
        cls.f_gone = cls._doc('F-P-A86-01', 'formato', cls.p_gone, **own)
        cls.f_mast = cls._doc('F-P-A87-01', 'formato', cls.p_mast, **own)
        cls.mine = (cls.f_ok | cls.fit_ok | cls.form_ok | cls.it_ok | cls.obs_ok | cls.kept_ok
                    | cls.pi01 | cls.f_nouser | cls.f_gone | cls.f_mast)

    def test_01_dueno_con_usuario_toma_sus_formatos(self):
        result = self.Doc._sgi_owner_from_process(company=self.company, ids=self.mine.ids)
        self.assertEqual(result['changed'], {'XB5': (self.owner_user.id, sorted(
            (self.f_ok | self.fit_ok | self.form_ok).ids))})
        for doc in (self.f_ok, self.fit_ok, self.form_ok):
            self.assertEqual(doc.sgi_owner_id, self.owner_user)
        self.assertEqual(self.it_ok.sgi_owner_id, self.mast, "Un instructivo no es formato.")
        self.assertEqual(self.obs_ok.sgi_owner_id, self.mast, "Obsoleto: no se toca.")
        self.assertEqual(self.kept_ok.sgi_owner_id, self.reassigned, "Lo ya reasignado se respeta.")
        self.assertEqual(self.pi01.sgi_owner_id, self.mast, "P-I01 queda fuera siempre.")

    def test_02_sin_usuario_se_queda_con_mast_y_se_lista(self):
        result = self.Doc._sgi_owner_from_process(company=self.company, ids=self.mine.ids)
        self.assertEqual(result['no_user'], {'XB6': self.f_nouser.ids, 'XB7': self.f_gone.ids})
        self.assertEqual(result['mast'], {'XB8': self.f_mast.ids})
        for doc in (self.f_nouser, self.f_gone, self.f_mast):
            self.assertEqual(doc.sgi_owner_id, self.mast)
        again = self.Doc._sgi_owner_from_process(company=self.company, ids=self.mine.ids)
        self.assertEqual(again['changed'], {}, "Idempotente.")

    def test_03_post_migrate_corre(self):
        _run_migration(self.env, '19.0.57.71.0')
        self.assertEqual(self.f_ok.sgi_owner_id, self.owner_user)
        self.assertEqual(self.f_nouser.sgi_owner_id, self.mast)
