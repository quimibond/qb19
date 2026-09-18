# -*- coding: utf-8 -*-
"""Cada prueba reproduce en la base un aviso real del build (qbtesting,
17-sep-2026) con campos y vistas creados como los crea Studio, corre la
limpieza y comprueba que el aviso ya no tiene de dónde salir."""
import json
import uuid

from odoo.tests.common import TransactionCase
from odoo.tools import mute_logger


# at_install a propósito: mientras se cargan los módulos el registro no está
# "ready" y crear campos de Studio no dispara _register_hook; así la prueba
# controla cuándo corre la limpieza.
class TestBuildLimpio(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Limpio = cls.env['qb.build.limpio']
        cls.Fields = cls.env['ir.model.fields']
        cls.partner_model_id = cls.env['ir.model']._get_id('res.partner')

    # ------------------------------------------------------------------
    def _campo(self, name, **vals):
        base = {'name': name, 'model_id': self.partner_model_id, 'state': 'manual', 'ttype': 'char'}
        base.update(vals)
        # Studio crea campos así; Odoo avisa de la etiqueta repetida al
        # reflejarlos. El aviso se silencia para que la prueba no ensucie el log.
        with mute_logger('odoo.addons.base.models.ir_model'):
            return self.Fields.create(base)

    def _etiqueta(self, name):
        self.env.cr.execute(
            "SELECT field_description->>'en_US' FROM ir_model_fields WHERE model = 'res.partner' AND name = %s", (name,))
        return self.env.cr.fetchone()[0]

    def test_etiquetas_duplicadas(self):
        self._campo('x_qbl_a', field_description='Prueba build limpio')
        self._campo('x_qbl_b', field_description='Prueba build limpio')
        self._campo('x_qbl_c', field_description='Prueba build limpio')
        # contra un campo de código: el de Studio es el que se renombra
        self._campo('x_qbl_d', field_description=self.env['res.partner']._fields['name'].string)

        hechos = self.Limpio._etiquetas_duplicadas()

        self.assertEqual(self._etiqueta('x_qbl_a'), 'Prueba build limpio', 'el más viejo conserva la etiqueta')
        self.assertEqual(self._etiqueta('x_qbl_b'), 'Prueba build limpio (2)')
        self.assertEqual(self._etiqueta('x_qbl_c'), 'Prueba build limpio (3)')
        self.assertEqual(self._etiqueta('x_qbl_d'), '%s (2)' % self.env['res.partner']._fields['name'].string)
        self.assertEqual(len([h for h in hechos if 'x_qbl_' in h]), 3)
        # idempotente: una segunda pasada no vuelve a tocar nada
        self.assertEqual([h for h in self.Limpio._etiquetas_duplicadas() if 'x_qbl_' in h], [])
        self.assertEqual(self._etiqueta('x_qbl_b'), 'Prueba build limpio (2)')

    def test_dependencias_no_buscables(self):
        # Un many2one calculado sin búsqueda (como product_variant_id o
        # account.account.group_id) y dos relacionados de Studio que pasan por él.
        self._campo('x_qbl_m2o', ttype='many2one', relation='res.partner', store=False,
                    field_description='Padre calculado', depends='parent_id',
                    compute="for r in self:\n    r['x_qbl_m2o'] = r.parent_id")
        self._campo('x_qbl_rel', field_description='Nombre del padre calculado', related='x_qbl_m2o.name')
        self._campo('x_qbl_rel2', field_description='Nombre del abuelo calculado', related='parent_id.x_qbl_m2o.name')

        hechos = self.Limpio._dependencias_no_buscables()

        self.assertEqual(len([h for h in hechos if 'x_qbl_rel' in h]), 2)
        self.env.cr.execute(
            "SELECT name, related, depends, compute FROM ir_model_fields WHERE model = 'res.partner' AND name IN ('x_qbl_rel', 'x_qbl_rel2')")
        filas = {r[0]: r for r in self.env.cr.fetchall()}
        self.assertIsNone(filas['x_qbl_rel'][1])
        self.assertEqual(filas['x_qbl_rel'][2], 'parent_id', 'sin prefijo buscable hereda las dependencias del campo calculado')
        self.assertEqual(filas['x_qbl_rel2'][2], 'parent_id', 'se queda con el prefijo buscable')
        self.assertIn("r['x_qbl_rel'] = v['name']", filas['x_qbl_rel'][3])

        # Al recargar el registro el campo calculado da lo mismo que daba el relacionado
        self.Fields.invalidate_model()
        self.registry._setup_models__(self.env.cr, ['res.partner'])
        campo = self.env['res.partner']._fields['x_qbl_rel']
        self.assertFalse(campo.related)
        self.assertEqual(tuple(self.registry.field_depends[campo]), ('parent_id',))
        abuelo = self.env['res.partner'].create({'name': 'Abuelo QBL'})
        padre = self.env['res.partner'].create({'name': 'Padre QBL', 'parent_id': abuelo.id})
        hijo = self.env['res.partner'].create({'name': 'Hijo QBL', 'parent_id': padre.id})
        self.assertEqual(hijo.x_qbl_rel, 'Padre QBL')
        self.assertEqual(hijo.x_qbl_rel2, 'Abuelo QBL')
        self.assertEqual([h for h in self.Limpio._dependencias_no_buscables() if 'x_qbl_' in h], [])

    def test_grupos_inexistentes(self):
        View = self.env['ir.ui.view']
        with mute_logger('odoo.addons.base.models.ir_ui_view'):
            vista = View.create({
                'name': 'qbl grupo inexistente',
                'model': 'res.partner',
                'inherit_id': self.env.ref('base.view_partner_form').id,
                'arch': """<data>
                    <xpath expr="//field[@name='function']" position="after">
                        <field name="ref" groups="qb_build_limpio.grupo_que_no_existe"/>
                        <field name="comment" groups="base.group_system,qb_build_limpio.grupo_que_no_existe"/>
                    </xpath>
                </data>""",
            })

        hechos = self.Limpio._grupos_inexistentes()

        self.assertTrue(any('vista %d' % vista.id in h and 'grupo_que_no_existe' in h for h in hechos), hechos)
        vista.invalidate_recordset()
        arch = vista.arch_db
        self.assertNotIn('grupo_que_no_existe', arch)
        self.assertNotIn('name="ref"', arch, 'el nodo cuyo único grupo no existía se quita: nadie lo veía')
        self.assertIn('groups="base.group_system"', arch, 'del nodo con varios grupos solo se quita el inexistente')
        self.assertEqual([h for h in self.Limpio._grupos_inexistentes() if 'vista %d' % vista.id in h], [])

    def _vista_studio(self, nombre, arch_valido, arch_roto):
        vista = self.env['ir.ui.view'].create({'name': nombre, 'model': 'res.partner', 'arch': arch_valido})
        self.env['ir.model.data'].create({
            'module': 'studio_customization', 'name': 'odoo_studio_default_%s' % uuid.uuid4().hex,
            'model': 'ir.ui.view', 'res_id': vista.id,
        })
        # Como en producción: la vista quedó rota en la base (cambió Odoo, no la vista).
        self.env.cr.execute("UPDATE ir_ui_view SET arch_db = %s::jsonb WHERE id = %s",
                            (json.dumps({'en_US': arch_roto}), vista.id))
        vista.invalidate_recordset()
        return vista

    def test_vistas_studio_invalidas(self):
        calendario = self._vista_studio(
            'Odoo Studio: Default calendar view for res.partner customization',
            '<calendar string="x" date_start="create_date"><field name="name"/></calendar>',
            '<calendar string="x" date_start="create_date" quick_add="0"><field name="name"/></calendar>')
        lista = self._vista_studio(
            'Odoo Studio: Default tree view for ir.model(1,) customization',
            '<list><field name="name"/></list>',
            '<list><field name="x_campo_que_no_existe"/></list>')
        sana = self._vista_studio(
            'Odoo Studio: Default list view for res.partner customization',
            '<list><field name="name"/></list>',
            '<list><field name="display_name"/></list>')

        hechos = self.Limpio._vistas_studio_invalidas()

        self.assertEqual(len(hechos), 2, hechos)
        (calendario + lista + sana).invalidate_recordset()
        self.assertTrue(calendario.active)
        self.assertIn('quick_create="0"', calendario.arch_db, 'el arreglo trivial se aplica en vez de archivar')
        self.assertFalse(lista.active, 'sin arreglo trivial se archiva')
        self.assertTrue(sana.active)
        self.assertEqual(self.Limpio._vistas_studio_invalidas(), [])

    def test_columnas_sin_not_null(self):
        # Como en producción: una columna required a la que Odoo no pudo poner
        # NOT NULL porque había filas en NULL (website.*, sale.order.template.*).
        # res.lang.direction: required, default 'ltr' y su write no tiene
        # efectos colaterales (ir.sequence, por ejemplo, recrea la secuencia SQL).
        idioma = self.env.ref('base.lang_en')
        self.env.cr.execute("ALTER TABLE res_lang ALTER COLUMN direction DROP NOT NULL")
        self.env.cr.execute("UPDATE res_lang SET direction = NULL WHERE id = %s", (idioma.id,))

        hechos = self.Limpio._columnas_sin_not_null()

        self.assertIn('res.lang.direction: NOT NULL puesto (1 filas rellenadas)', hechos)
        idioma.invalidate_recordset()
        self.assertEqual(idioma.direction, 'ltr', 'se rellena con el valor por defecto del campo')
        self.env.cr.execute("""
            SELECT a.attnotnull FROM pg_attribute a JOIN pg_class c ON a.attrelid = c.oid
             WHERE c.relname = 'res_lang' AND a.attname = 'direction'""")
        self.assertTrue(self.env.cr.fetchone()[0])

    def test_ejecutar_no_revienta(self):
        informe = self.Limpio.ejecutar()
        self.assertEqual(set(informe), {
            'etiquetas_duplicadas', 'dependencias_no_buscables', 'vistas_studio_invalidas',
            'grupos_inexistentes', 'columnas_sin_not_null'})
        for valores in informe.values():
            self.assertNotIn('ERROR: ver log', valores)
