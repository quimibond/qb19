# -*- coding: utf-8 -*-
"""Entrega 1c de la auditoría 2026-09 (seguridad, segunda parte).

- F-005: la propuesta de cambio de una solicitud no se cuelga ni se cambia
  por RPC, y solo se aplica la de ESA solicitud.
- F-006: la respuesta del proveedor a una NC ya no es un método público.
- F-007: los pendientes de otra persona solo si está en tu equipo.
- F-008: los procesos de `sgi.cron` y `sgi.config` solo los corre el sistema.
- F-015: la foto del inventario ya no es un método público.
- N-001: todo documento controlado tiene responsable.
- D-001: las fichas estándar abren para quien no es del SGI."""
from lxml import etree

from odoo.exceptions import AccessError, ValidationError
from odoo.tests import TransactionCase, new_test_user, tagged

from .common_documents import sgi_hide_real_documents


@tagged('post_install', '-at_install')
class TestEntrega1c(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        sgi_hide_real_documents(cls.env)
        cls.user = new_test_user(cls.env, login='e1c_user', email='e1c.user@example.com',
                                 groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.other_user = new_test_user(cls.env, login='e1c_other', email='e1c.other@example.com',
                                       groups='base.group_user,quimibond_sgi.group_sgi_user')
        cls.plain = new_test_user(cls.env, login='e1c_plain', email='e1c.plain@example.com',
                                  groups='base.group_user')
        cls.emp = cls.env['hr.employee'].create({'name': 'E1c Empleado', 'user_id': cls.user.id})
        cls.stranger = cls.env['hr.employee'].create({'name': 'E1c Ajeno', 'user_id': cls.other_user.id})

    # ---- F-005 -------------------------------------------------------------
    def test_01_mp_proposal_fields_are_locked(self):
        request = self.env['approval.request'].search([], limit=1)
        if not request:
            self.skipTest("Base sin solicitudes de Aprobaciones.")
        with self.assertRaises(AccessError):
            request.with_user(self.user).write({'sgi_mp_change_type': 'quitar'})

    # ---- F-006 / F-015 -----------------------------------------------------
    def test_02_no_public_rpc_entry_points(self):
        self.assertFalse(hasattr(self.env['quality.alert'], 'sgi_supplier_answer'))
        self.assertTrue(hasattr(self.env['quality.alert'], '_sgi_supplier_answer'))
        self.assertFalse(hasattr(self.env['sgi.inventory.value'], 'sgi_snapshot'))

    # ---- F-007 -------------------------------------------------------------
    def test_03_pending_of_someone_outside_my_team(self):
        Public = self.env['hr.employee.public'].with_user(self.user)
        with self.assertRaises(AccessError):
            Public.browse(self.stranger.id).action_sgi_open_pending()
        with self.assertRaises(AccessError):
            Public.browse(self.stranger.id).action_sgi_print_my_procedure()
        # Lo propio sí.
        self.assertTrue(Public.browse(self.emp.id).action_sgi_open_pending())

    # ---- F-008 -------------------------------------------------------------
    def test_04_sgi_processes_only_for_the_system(self):
        with self.assertRaises(AccessError):
            self.env['sgi.cron'].with_user(self.user).cron_news()
        with self.assertRaises(AccessError):
            self.env['sgi.config'].with_user(self.user).seed_parameters()
        self.assertTrue(self.env['sgi.config'].seed_parameters(), "El sistema sí lo corre.")

    # ---- N-001 -------------------------------------------------------------
    def test_05_controlled_document_always_has_owner(self):
        doc = self.env['documents.document'].create({
            'name': 'E1c documento', 'type': 'binary', 'sgi_is_controlled': True,
            'sgi_doc_type': 'formato', 'sgi_code': 'F-P-A95-01', 'sgi_state': 'vigente'})
        self.assertTrue(doc.sgi_owner_id, "Nace con responsable (el Jefe MAST o quien lo crea).")
        with self.assertRaises(ValidationError):
            doc.write({'sgi_owner_id': False})
        free = self.env['documents.document'].create({'name': 'E1c libre', 'type': 'binary'})
        self.assertFalse(free.sgi_owner_id, "Los documentos no controlados no cambian.")

    # ---- D-001 -------------------------------------------------------------
    def test_06_standard_forms_open_without_sgi_groups(self):
        """El contador externo (sin SGI, Servicio de asistencia ni Compras) abría
        un contacto y fallaba por los contadores del SGI."""
        partner = self.env['res.partner'].create({'name': 'E1c Contacto'})
        views = self.env['res.partner'].with_user(self.plain).get_views([(False, 'form')])
        # Odoo 17+ calcula ``models[...]['fields']`` una vez para todos los
        # grupos (vista en caché) y quita del arch los nodos con ``groups``
        # después, por usuario; la ficha lee solo los campos que quedan en el
        # arch. Por eso se revisa el arch, no la lista de campos del modelo.
        arch = etree.fromstring(views['views']['form']['arch'])
        in_arch = set(arch.xpath('//field/@name'))
        for name in ('sgi_complaint_count', 'sgi_eval_count', 'sgi_nc_count', 'sgi_ppap_count'):
            self.assertNotIn(name, in_arch)
        partner_fields = views['models']['res.partner']['fields']
        self.assertTrue(partner.with_user(self.plain).read([n for n in in_arch if n in partner_fields]))
