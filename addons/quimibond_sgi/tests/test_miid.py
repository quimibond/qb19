# -*- coding: utf-8 -*-
"""57.105.0 — MIID desde Odoo.

Secciones de texto fijo (sgi.miid.section, sembradas con el borrador Rev. 03)
más bloques vivos, huella de lo que se imprime, «Por confirmar» y candados
(ninguna sección por confirmar y todos los procesos vigentes), solicitud de
cambio por el flujo documental de siempre (DOC-1), aviso diario al Jefe MAST
cuando el MIID vigente ya no coincide, pantalla y Diagnóstico.

La base del build es copia de producción: el MIID real (3495) se oculta con
sgi_hide_real_documents y todo se crea en una empresa propia (ZM Empresa
MIID, procesos ZM1/ZM2). Se renderiza HTML; el PDF se parcha."""
import json
from unittest.mock import patch

from odoo.exceptions import AccessError, UserError, ValidationError
from odoo.tests import TransactionCase, new_test_user, tagged

from .common_documents import sgi_hide_real_documents
from .common_users import sgi_set_mast

FAKE_PDF = b'%PDF-1.4 ZM MIID'


def _fake_render(self, mode='live', revision=None, issue_date=None, request=None):
    return FAKE_PDF + (mode or '').encode()


class _Case(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        env = cls.env
        cls.company = env['res.company'].create({'name': 'ZM Empresa MIID'})
        env['ir.config_parameter'].sudo().set_param('quimibond_sgi.sgi_company_id', str(cls.company.id))
        cls.mast = sgi_set_mast(env, login='zm_miid_mast')
        cls.mast.write({'company_ids': [(4, cls.company.id)]})
        cls.user = new_test_user(env, login='zm_miid_user',
                                 groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.user.write({'company_ids': [(4, cls.company.id)]})
        cls.emp_a = env['hr.employee'].create({'name': 'ZM Dueña A', 'company_id': cls.company.id})
        cls.emp_b = env['hr.employee'].create({'name': 'ZM Dueño B', 'company_id': cls.company.id})
        Process = env['sgi.process']
        cls.p1 = Process.create({'code': 'ZM1', 'name': 'Proceso MIID uno', 'process_type': 'cop',
                                 'owner_id': cls.emp_a.id, 'company_id': cls.company.id})
        # Nacen en borrador (como los 14 de producción): el candado de procesos aplica.
        Section = env['sgi.miid.section']
        cls.s_proc = Section.create({'company_id': cls.company.id, 'sequence': 10, 'clause': '4.4',
                                     'name': 'Procesos ZM', 'body': '<p>Texto ZM procesos</p>',
                                     'live_block': 'procesos'})
        cls.s_ctrl = Section.create({'company_id': cls.company.id, 'sequence': 20, 'clause': '8',
                                     'name': 'Operación ZM', 'body': '<p>Texto ZM operación</p>',
                                     'live_block': 'controles'})
        cls.miid_doc = env['documents.document'].create({
            'name': 'MIID ZM.pdf', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'miid', 'sgi_code': 'MIID', 'sgi_revision': 0,
            'sgi_state': 'vigente', 'company_id': cls.company.id, 'sgi_process_id': cls.p1.id})
        cls.Miid = env['sgi.miid']
        cls.miid = cls.Miid._sgi_get(cls.company)
        # Sin Sign en las pruebas: la categoría real se aprueba con el botón y
        # con el Jefe MAST de la prueba (se deshace al final).
        cls.category = env.ref('quimibond_sgi.sgi_approval_category_doc_change')
        cls.category.write({'sgi_sign_required': False, 'approval_minimum': 1,
                            'approver_ids': [(5, 0, 0), (0, 0, {'user_id': cls.mast.id, 'required': True})]})

    def _publish(self, processes=None):
        """«Vigente» sin la especificación completa que pide la vista: por SQL,
        solo para la prueba (se deshace al final)."""
        processes = processes or self.env['sgi.process'].search([('company_id', '=', self.company.id)])
        self.env.flush_all()
        self.env.cr.execute("UPDATE sgi_process SET state = 'vigente' WHERE id = ANY(%s)", [processes.ids])
        self.env.invalidate_all()

    def _control(self, code, **vals):
        return self.env['documents.document'].create(dict({
            'name': '%s control.pdf' % code, 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'control_operacional', 'sgi_code': code, 'sgi_state': 'vigente',
            'company_id': self.company.id, 'sgi_process_id': self.p1.id}, **vals))

    def _baseline(self):
        """Simula una revisión ya generada desde Odoo: huella y foto de hoy."""
        snap = self.miid._sgi_snapshot()
        req = self.env['approval.request'].create({
            'name': 'ZM base', 'category_id': self.category.id,
            'request_owner_id': self.mast.id, 'sgi_change_kind': 'modificacion',
            'sgi_document_id': self.miid_doc.id, 'sgi_new_revision': 1,
            'sgi_miid_hash': self.Miid._sgi_hash(snap), 'sgi_miid_snapshot': json.dumps(snap)})
        # Una revisión ya publicada: su solicitud no está «en curso».
        self.env.flush_all()
        self.env.cr.execute("UPDATE approval_request SET request_status = 'approved' WHERE id = %s", [req.id])
        self.env.invalidate_all()
        self.miid_doc.sudo().write({'sgi_content_hash': req.sgi_miid_hash,
                                    'sgi_doc_change_id': req.id})
        return req

    def _html(self):
        return self.env['ir.actions.report']._render_qweb_html(
            'quimibond_sgi.action_report_miid', self.miid.ids)[0].decode()


@tagged('post_install', '-at_install')
class TestMiidSecciones(_Case):

    def test_01_semilla_del_borrador_noupdate(self):
        Data = self.env['ir.model.data'].sudo()
        rows = Data.search([('module', '=', 'quimibond_sgi'), ('model', '=', 'sgi.miid.section')])
        self.assertEqual(len(rows), 46, "Una por título y subtítulo del borrador, sin 12.3.")
        self.assertTrue(all(rows.mapped('noupdate')), "Lo que escriba el Jefe MAST no se pisa.")
        notes = Data.search([('module', '=', 'quimibond_sgi'), ('model', '=', 'sgi.miid.row.note')])
        self.assertEqual(len(notes), 15, "La columna «Situación» de los 15 anexos.")
        self.assertTrue(all(notes.mapped('noupdate')))

    def test_02_acl_lee_todos_edita_mast_nadie_borra(self):
        section = self.s_proc.with_user(self.user)
        self.assertEqual(section.name, 'Procesos ZM')
        self.assertTrue(self.miid.with_user(self.user).name)
        with self.assertRaises(AccessError):
            section.write({'name': 'No'})
        self.s_proc.with_user(self.mast).write({'body': '<p>Texto ZM nuevo</p>'})
        with self.assertRaises(AccessError):
            self.s_proc.with_user(self.mast).unlink()

    def test_03_un_bloque_por_empresa(self):
        with self.assertRaises(ValidationError):
            self.env['sgi.miid.section'].create({
                'company_id': self.company.id, 'name': 'Duplicada', 'live_block': 'procesos'})
        archived = self.s_ctrl.copy({'live_block': False})
        self.s_ctrl.active = False
        archived.live_block = 'controles'  # la archivada ya no ocupa el bloque

    def test_03b_direccion_quita_por_confirmar_y_nada_mas(self):
        director = new_test_user(self.env, login='zm_miid_dir',
                                 groups='base.group_user,quimibond_sgi.group_sgi_director')
        director.write({'company_ids': [(4, self.company.id)]})
        self.s_proc.write({'to_confirm': True, 'to_confirm_note': 'Nota ZM'})
        self.s_proc.with_user(director).write({'to_confirm': False})
        self.assertFalse(self.s_proc.to_confirm)
        self.assertTrue(self.s_proc.message_ids.filtered(lambda m: 'quitado por' in (m.body or '')),
                        "Queda el rastro en el chatter.")
        with self.assertRaises(AccessError):
            self.s_proc.with_user(director).write({'body': '<p>No</p>'})


@tagged('post_install', '-at_install')
class TestMiidDatos(_Case):

    def test_04_foto_de_la_empresa(self):
        self._control('CO-ZM1-01')
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.nc_escalation_days', '6')
        snap = self.miid._sgi_snapshot()
        self.assertEqual(snap['processes']['ZM1'][:4], ['Proceso MIID uno', 'cop', 'ZM Dueña A', 'borrador'])
        self.assertIn('CO-ZM1-01', snap['controls'])
        self.assertEqual(snap['nc']['escalation'], 6)
        self.assertEqual(snap['nc']['containment'],
                         self.env['quality.alert']._sgi_deadline_days()['containment'],
                         "Los plazos de etapa salen de la misma fuente que usan las NC.")
        self.assertIn('miid', snap['doc_types'])
        self.assertEqual(set(snap['sections']), {str(self.s_proc.id), str(self.s_ctrl.id)})
        self.assertFalse([code for code in snap['processes'] if not code.startswith('ZM')],
                         "Solo los procesos de la empresa del MIID.")

    def test_05_procedimientos_anteriores_sin_excluidos(self):
        Doc = self.env['documents.document']
        base = {'type': 'binary', 'sgi_is_controlled': True, 'sgi_doc_type': 'procedimiento',
                'sgi_state': 'vigente', 'company_id': self.company.id}
        Doc.create(dict(base, name='P-V97 anterior', sgi_code='P-V97',
                        sgi_replaced_by_process_id=self.p1.id))
        Doc.create(dict(base, name='P-V98 excluido', sgi_code='P-V98'))
        self.env['ir.config_parameter'].sudo().set_param('quimibond_sgi.dropbox_excluded_codes', 'P-V98')
        previous = self.miid._sgi_snapshot()['previous']
        self.assertEqual(previous['P-V97'][3], 'ZM1')
        self.assertNotIn('P-V98', previous)
        Doc.create(dict(base, name='P-V96 por proceso', sgi_code='P-V96', sgi_process_id=self.p1.id))
        self.assertEqual(self.miid._sgi_snapshot()['previous']['P-V96'][3], 'ZM1',
                         "Sin «Lo sustituye», el proceso del documento (sgi_process_id).")

    def test_06_huella_cambia_con_lo_que_se_imprime(self):
        Miid = self.Miid
        base = Miid._sgi_hash(self.miid._sgi_snapshot())
        self.assertEqual(base, Miid._sgi_hash(self.miid._sgi_snapshot()), "Estable.")
        self.s_proc.body = '<p>Texto   ZM <b>procesos</b></p>'
        self.assertEqual(base, Miid._sgi_hash(self.miid._sgi_snapshot()),
                         "Negritas y espacios no son un cambio.")
        self.s_proc.write({'to_confirm': True, 'to_confirm_note': 'ZM por confirmar'})
        self.assertEqual(base, Miid._sgi_hash(self.miid._sgi_snapshot()),
                         "«Por confirmar» no entra a la huella (una revisión aprobada nunca lo tiene).")
        cases = [
            ('sección', lambda: self.s_proc.write({'body': '<p>Otro texto</p>'})),
            ('orden', lambda: self.s_ctrl.write({'sequence': 5})),
            ('dueño', lambda: self.p1.write({'owner_id': self.emp_b.id})),
            ('proceso nuevo', lambda: self.env['sgi.process'].create(
                {'code': 'ZM2', 'name': 'Proceso MIID dos', 'company_id': self.company.id})),
            ('control', lambda: self._control('CO-ZM1-02')),
            ('plazo NC', lambda: self.env['ir.config_parameter'].sudo().set_param(
                'quimibond_sgi.nc_effectiveness_days', '91')),
            ('tipo', lambda: self.env['sgi.document.type'].create(
                {'code': 'zm_tipo', 'name': 'Tipo ZM', 'prefix_pattern': 'ZM-{seq:02d}'})),
        ]
        last = base
        for label, change in cases:
            with self.subTest(label):
                change()
                now = Miid._sgi_hash(self.miid._sgi_snapshot())
                self.assertNotEqual(now, last, label)
                last = now

    def test_07_diferencias_legibles(self):
        old = self.miid._sgi_snapshot()
        self.p1.owner_id = self.emp_b
        self.env['sgi.process'].create({'code': 'ZM2', 'name': 'Proceso MIID dos', 'company_id': self.company.id})
        self._control('CO-ZM1-03')
        self.s_ctrl.body = '<p>Operación editada</p>'
        diffs = self.Miid._sgi_diff(old, self.miid._sgi_snapshot())
        text = "\n".join(diffs)
        self.assertIn("Proceso nuevo: ZM2", text)
        self.assertIn("Cambió el dueño de ZM1: ZM Dueña A → ZM Dueño B", text)
        self.assertIn("Control operacional nuevo: CO-ZM1-03", text)
        self.assertIn("Sección editada: 8 Operación ZM", text)
        old = self.miid._sgi_snapshot()
        self.s_proc.sequence = 30
        self.assertIn("Sección movida de lugar: 4.4 Procesos ZM",
                      self.Miid._sgi_diff(old, self.miid._sgi_snapshot()))

    def test_08_vista_html_con_pie_propio(self):
        self._control('CO-ZM1-04')
        html = self._html()
        for text in ('Procesos ZM', 'Texto ZM procesos', 'ZM1', 'CO-ZM1-04', 'Borrador — no vigente',
                     'Vista del sistema', 'Datos del sistema sin sección'):
            self.assertIn(text, html)
        self.assertIn('MIID', html)
        self.assertEqual(html.count('class="footer"'), 1, "Un pie por artículo.")
        self.assertNotIn('Formato controlado del SGI', html,
                         "El pie del MIID dice solo «Borrador — no vigente» (M-2).")


@tagged('post_install', '-at_install')
class TestMiidRevision(_Case):

    def setUp(self):
        super().setUp()
        self._publish()

    def _request(self):
        with patch.object(type(self.Miid), '_sgi_render_pdf', _fake_render):
            action = self.miid.with_user(self.mast).action_sgi_miid_request_change()
        return self.env['approval.request'].browse(action['res_id'])

    def test_09_solicitud_prellenada_y_sin_duplicar(self):
        with self.assertRaises(UserError):
            self.miid.with_user(self.user).action_sgi_miid_request_change()
        req = self._request()
        self.assertEqual(req.category_id, self.category)
        self.assertEqual(req.sgi_document_id, self.miid_doc)
        self.assertEqual(req.sgi_change_kind, 'modificacion')
        self.assertEqual(req.sgi_new_revision, 3, "Sin línea base: la primera desde Odoo es la Rev. 03 (Q5).")
        self.assertEqual(req.request_status, 'new', "Nunca se envía ni se aprueba solo.")
        self.assertEqual(req.sgi_miid_hash, self.Miid._sgi_hash(self.miid._sgi_snapshot()))
        self.assertTrue(req._sgi_change_attachment().name.endswith('(para aprobación).pdf'))
        self.assertEqual(self._request(), req, "La segunda vez abre la misma.")

    def test_10_al_enviar_con_datos_nuevos_regenera_y_renombra(self):
        req = self._request()
        first = req._sgi_change_attachment()
        self._control('CO-ZM1-05')
        with patch.object(type(self.Miid), '_sgi_render_pdf', _fake_render):
            req.with_user(self.mast).action_confirm()
        self.assertTrue(first.exists(), "Nada se borra.")
        self.assertIn('sustituido el', first.name)
        self.assertIn('CO-ZM1-05', req.sgi_miid_snapshot)
        self.assertNotEqual(req._sgi_change_attachment(), first)
        self.assertEqual(req.sgi_change_attachment_id, req._sgi_change_attachment(),
                         "Lo que se manda a firmar es el PDF nuevo (I-4).")

    def test_11_aprobada_publica_la_revision_con_huella(self):
        req = self._request()
        with patch.object(type(self.Miid), '_sgi_render_pdf', _fake_render):
            req.with_user(self.mast).action_confirm()
            req.with_user(self.mast).action_approve()
        self.assertEqual(req.request_status, 'approved')
        new = req.sgi_new_document_id
        self.assertTrue(new, "DOC-1: documento nuevo con el PDF de la solicitud.")
        self.assertEqual((new.sgi_code, new.sgi_revision, new.sgi_state), ('MIID', 3, 'vigente'))
        self.assertEqual(new.sgi_content_hash, req.sgi_miid_hash)
        self.assertEqual(new.name, 'MIID Rev. 03.pdf', "Se publica el PDF que se mandó a firmar (Q7).")
        self.assertEqual(self.miid_doc.sgi_state, 'obsoleto')
        self.miid.invalidate_recordset()
        self.assertEqual(self.miid.document_id, new)
        self.assertEqual(self.miid.state, 'al_dia')
        self.assertIn(self.miid_doc, self.miid.revision_ids)

    def test_12_aprobada_con_datos_cambiados_publica_lo_firmado(self):
        req = self._request()
        with patch.object(type(self.Miid), '_sgi_render_pdf', _fake_render):
            req.with_user(self.mast).action_confirm()
            self._control('CO-ZM1-06')
            req.with_user(self.mast).action_approve()
        new = req.sgi_new_document_id
        self.assertEqual(new.name, 'MIID Rev. 03.pdf')
        self.assertEqual(new.sgi_content_hash, req.sgi_miid_hash, "La huella es la de lo firmado.")
        self.assertTrue(req.message_ids.filtered(lambda m: 'cambiaron después del envío' in (m.body or '')))
        self.miid.invalidate_recordset()
        self.assertEqual(self.miid.state, 'desactualizado')


@tagged('post_install', '-at_install')
class TestMiidAviso(_Case):

    def _notice(self):
        return self.env['mail.activity'].sudo().with_context(active_test=False).search([
            ('res_model', '=', 'sgi.miid'), ('res_id', '=', self.miid.id),
            ('sgi_cron_key', '=', 'miid_desactualizado:%d' % self.company.id)])

    def test_13_sin_linea_base_no_avisa_y_con_cambio_un_solo_aviso(self):
        Cron = self.env['sgi.cron']
        Cron._sgi_miid_check()
        self.assertFalse(self._notice(), "Rev. cargada del Dropbox: no hay contra qué comparar (Q6).")
        self._baseline()
        Cron._sgi_miid_check()
        self.assertFalse(self._notice())
        self.miid.invalidate_recordset()
        self.assertEqual(self.miid.state, 'al_dia')
        self.p1.owner_id = self.emp_b
        Cron._sgi_miid_check()
        Cron._sgi_miid_check()
        notice = self._notice().filtered('active')
        self.assertEqual(len(notice), 1)
        self.assertEqual(notice.user_id, self.mast)
        self.assertEqual(notice.summary, "El MIID vigente ya no coincide con el sistema")
        self.assertIn("Cambió el dueño de ZM1", notice.note)
        self.assertTrue(self.miid.outdated_since)
        self.p1.owner_id = self.emp_a  # vuelve a coincidir
        Cron._sgi_miid_check()
        self.assertFalse(self._notice().filtered('active'))
        self.assertTrue(all(self._notice().mapped('sgi_episode_closed')))
        self.assertFalse(self.miid.outdated_since)

    def test_14_mis_pendientes_abre_la_pantalla(self):
        self._baseline()
        self.p1.owner_id = self.emp_b
        self.env['sgi.cron']._sgi_miid_check()
        notice = self._notice().filtered('active')
        rows = self.env['sgi.my.pending']._sgi_pending_values(self.mast)[self.mast.id]
        row = [r for r in rows if r['kind'] == 'aviso' and r['res_id'] == notice.id]
        self.assertTrue(row, "El aviso vence en 3 días hábiles: cae en Mis pendientes.")

    def test_15_diagnostico_documental(self):
        Miid = self.env['sgi.miid']
        texts = lambda: " ".join(l['text'] for l in Miid._sgi_diagnostic_lines())  # noqa: E731
        self.assertIn("no se generó desde Odoo", texts())
        self._baseline()
        self.assertIn("MIID al día", texts())
        self.p1.owner_id = self.emp_b
        self.assertIn("MIID desactualizado", texts())


@tagged('post_install', '-at_install')
class TestMiidPantalla(_Case):

    def test_16_pantalla_para_todos_y_menu(self):
        action = self.env['sgi.miid'].with_user(self.user).action_open()
        self.assertEqual(action['res_model'], 'sgi.miid')
        self.assertEqual(action['res_id'], self.miid.id, "Uno por empresa; no se duplica.")
        screen = self.miid.with_user(self.user)
        self.assertIn('Procesos ZM', screen.live_html)
        self.assertEqual(screen.state, 'sin_base')
        menu = self.env.ref('quimibond_sgi.menu_sgi_miid')
        # 57.113.0: en Sistema (antes Dirección), el manual del sistema.
        self.assertEqual(menu.parent_id, self.env.ref('quimibond_sgi.menu_sgi_processes'))
        self.assertFalse(menu.group_ids, "Todo Usuario SGI y Auditor (hereda la carpeta).")


@tagged('post_install', '-at_install')
class TestMiidCandados(_Case):
    """Ninguna revisión del MIID se envía ni se aprueba con secciones por
    confirmar o procesos sin publicar (Q16, Q17)."""

    def _request(self):
        with patch.object(type(self.Miid), '_sgi_render_pdf', _fake_render):
            action = self.miid.with_user(self.mast).action_sgi_miid_request_change()
        return self.env['approval.request'].browse(action['res_id'])

    def test_17_por_confirmar_impide_enviar(self):
        self._publish()
        self.s_proc.write({'to_confirm': True, 'to_confirm_note': 'Alcance ZM por confirmar'})
        req = self._request()  # crearla sí se puede (borrador de trabajo)
        with self.assertRaises(UserError) as caught:
            req.with_user(self.mast).action_confirm()
        self.assertIn('Procesos ZM', str(caught.exception))
        self.assertIn('por confirmar', str(caught.exception))
        self.assertEqual(req.request_status, 'new')

    def test_18_procesos_sin_publicar_impiden_enviar_y_aprobar(self):
        req = self._request()
        with self.assertRaises(UserError) as caught:
            req.with_user(self.mast).action_confirm()
        self.assertIn('ZM1', str(caught.exception))
        self.assertIn('vigentes', str(caught.exception))
        self._publish()
        with patch.object(type(self.Miid), '_sgi_render_pdf', _fake_render):
            req.with_user(self.mast).action_confirm()
        # Vuelve a borrador antes de aprobar (por ejemplo, un proceso regresa a piloto).
        self.env.flush_all()
        self.env.cr.execute("UPDATE sgi_process SET state = 'piloto' WHERE id = %s", [self.p1.id])
        self.env.invalidate_all()
        with self.assertRaises(UserError):
            req.with_user(self.mast).action_approve()
        self.assertEqual(self.miid_doc.sgi_state, 'vigente', "Nada se publicó.")

    def test_19_por_sign_no_truena_anota_una_vez_y_avisa(self):
        self._publish()
        req = self._request()
        with patch.object(type(self.Miid), '_sgi_render_pdf', _fake_render):
            req.with_user(self.mast).action_confirm()
        self.s_ctrl.write({'to_confirm': True, 'to_confirm_note': 'ZM pendiente'})
        approver = req.approver_ids[:1]
        req.with_user(self.mast).with_context(sgi_sign_sync=True).action_approve(approver=approver)
        req.with_user(self.mast).with_context(sgi_sign_sync=True).action_approve(approver=approver)
        self.assertEqual(req.request_status, 'pending', "Las firmas no aprueban el MIID con candados.")
        notes = req.message_ids.filtered(lambda m: 'no se aprueba hasta que' in (m.body or ''))
        self.assertEqual(len(notes), 1, "Una sola nota, no una por sincronización.")
        self.assertIn('Firma en curso', notes.body, "Sin la firma de Sign terminada no dice «Firmas completas».")
        self.assertNotIn('Firmas completas', notes.body)
        held = self.env['mail.activity'].search([
            ('res_model', '=', 'approval.request'), ('res_id', '=', req.id),
            ('sgi_cron_key', '=', 'miid_retenido:%d' % req.id)])
        self.assertEqual(held.user_id, self.mast, "El Jefe MAST recibe un aviso, no solo la nota.")
        rows = self.env['sgi.my.pending']._sgi_pending_values(self.mast)[self.mast.id]
        self.assertTrue([r for r in rows if r['kind'] == 'aviso' and r['res_id'] == held.id],
                        "El aviso de la solicitud retenida cae en Mis pendientes.")
        # Se levanta el candado: la siguiente sincronización aprueba y cierra el aviso.
        self.s_ctrl.write({'to_confirm': False})
        req.with_user(self.mast).with_context(sgi_sign_sync=True).action_approve(approver=approver)
        self.assertEqual(req.request_status, 'approved')
        self.assertFalse(req.sgi_miid_blocked_note)
        self.assertFalse(held.exists() and held.active)

    def test_20_borrador_marca_y_parte_el_texto(self):
        self.s_proc.write({'to_confirm': True, 'to_confirm_note': 'Nota ZM visible',
                           'body': '<p>Antes ZM</p><p>[[datos]]</p><p>Después ZM</p>'})
        self.s_ctrl.write({'body': '<p>Respaldo ZM</p>', 'body_fallback': True})
        html = self._html()
        self.assertIn('Borrador — no vigente', html)
        self.assertIn('POR CONFIRMAR', html)
        self.assertIn('Nota ZM visible', html)
        self.assertNotIn('[[datos]]', html)
        self.assertLess(html.index('Antes ZM'), html.index('ZM1'))
        self.assertLess(html.index('ZM1'), html.index('Después ZM'))
        self.assertIn('Respaldo ZM', html, "Sin controles vigentes, el texto de respaldo se imprime.")
        self._control('CO-ZM1-09')
        html = self._html()
        self.assertNotIn('Respaldo ZM', html, "Con datos, el respaldo no se imprime.")


@tagged('post_install', '-at_install')
class TestMiidSiembraRev02(TransactionCase):
    """57.110.0: el texto nuevo de la siembra llega solo a las secciones que
    nadie editó, y la nota de «Por confirmar» solo si sigue como se sembró."""

    def _forget_edits(self, section):
        self.env['mail.message'].sudo().search([
            ('model', '=', 'sgi.miid.section'), ('res_id', '=', section.id)]).unlink()

    def test_21_siembra_respeta_lo_editado(self):
        Section = self.env['sgi.miid.section']
        emerg = self.env.ref('quimibond_sgi.sgi_miid_section_32_emergencias')
        alcance = self.env.ref('quimibond_sgi.sgi_miid_section_05_alcance')
        old_note = "Nota ZM sembrada"
        # Como quedó en 57.105.0: texto corto, sin editar.
        self.env.cr.execute("UPDATE sgi_miid_section SET body = %s WHERE id = %s",
                            ('<p>Texto ZM viejo</p>', emerg.id))
        self.env.cr.execute("UPDATE sgi_miid_section SET to_confirm_note = %s WHERE id = %s",
                            ('Nota ZM corregida a mano', alcance.id))
        (emerg | alcance).invalidate_recordset()
        self._forget_edits(emerg)
        res = Section._sgi_seed_update(('sgi_miid_section_32_emergencias',),
                                       {'sgi_miid_section_05_alcance': old_note}, 'ZM')
        self.assertTrue(res['sgi_miid_section_32_emergencias'].startswith('actualizada'))
        self.assertIn('Escenarios', emerg.body)
        self.assertIn('Prueba periódica', emerg.body)
        self.assertEqual(alcance.to_confirm_note, 'Nota ZM corregida a mano',
                         "La nota corregida a mano no se pisa.")
        self.assertIn('editada a mano', res['sgi_miid_section_05_alcance'])
        self.assertFalse(self.env['mail.message'].search_count([
            ('model', '=', 'sgi.miid.section'), ('res_id', '=', emerg.id),
            ('body', 'ilike', 'Texto de la sección editado')]),
            "La siembra no deja la marca de edición a mano.")
        # Editada a mano: el texto ya no se toca.
        emerg.sudo().write({'body': '<p>Texto ZM del Jefe MAST</p>'})
        Section._sgi_seed_update(('sgi_miid_section_32_emergencias',), {}, 'ZM')
        self.assertIn('Texto ZM del Jefe MAST', emerg.body)

    def test_22_marca_de_siembra_solo_para_superusuario(self):
        section = self.env.ref('quimibond_sgi.sgi_miid_section_32_emergencias')
        mast = sgi_set_mast(self.env, login='zm_miid_seed_mast')
        mast.write({'company_ids': [(4, section.company_id.id)]})
        self._forget_edits(section)
        section.with_user(mast).with_company(section.company_id).with_context(
            sgi_miid_seed=True).write({'body': '<p>ZM</p>'})
        self.assertTrue(self.env['mail.message'].search_count([
            ('model', '=', 'sgi.miid.section'), ('res_id', '=', section.id),
            ('body', 'ilike', 'Texto de la sección editado')]),
            "Fuera de una migración la edición siempre deja su marca.")
