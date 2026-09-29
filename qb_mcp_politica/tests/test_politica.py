# -*- coding: utf-8 -*-
"""La política manda sobre las casillas de mcp.enabled.model.

Las filas «viejas» (todas las casillas marcadas, como estaba producción antes
de F-001) se insertan por SQL: la regla de pantalla no las dejaría crear, y lo
que se prueba es justo que la política las vuelve inertes.
"""
from odoo.exceptions import ValidationError
from odoo.tests import TransactionCase, tagged

OPERACIONES = ('read', 'create', 'write', 'unlink')


@tagged('post_install', '-at_install')
class TestPoliticaMcp(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.Enabled = cls.env['mcp.enabled.model'].sudo()
        cls.filas = {
            nombre: cls._fila_vieja(nombre)
            for nombre in ('ir.cron', 'res.partner', 'ir.config_parameter')
        }

    @classmethod
    def _fila_vieja(cls, model_name):
        """Fila activa con todas las casillas marcadas, sin pasar por el ORM."""
        model = cls.env['ir.model']._get(model_name)
        cls.Enabled.with_context(active_test=False).search(
            [('model_id', '=', model.id)]).unlink()
        cls.env.cr.execute(
            """
            INSERT INTO mcp_enabled_model
                (model_id, model_name, active, allow_read, allow_create,
                 allow_write, allow_unlink, allow_method_calls)
            VALUES (%s, %s, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE)
            RETURNING id
            """,
            (model.id, model_name),
        )
        fila_id = cls.env.cr.fetchone()[0]
        cls.env.invalidate_all()
        cls.env.registry.clear_cache()
        return cls.Enabled.browse(fila_id)

    def _operaciones(self, model_name):
        return {
            op: self.Enabled.check_model_operation_enabled(model_name, op)
            for op in OPERACIONES
        }

    # -- puertas --------------------------------------------------------

    def test_protegido_solo_lectura(self):
        self.assertEqual(self._operaciones('ir.cron'), {
            'read': True, 'create': False, 'write': False, 'unlink': False,
        })
        self.assertFalse(self.Enabled.is_method_call_enabled('ir.cron'))
        self.assertTrue(self.Enabled.is_model_enabled('ir.cron'))

    def test_negocio_sin_cambios(self):
        self.assertEqual(
            self._operaciones('res.partner'), dict.fromkeys(OPERACIONES, True))
        self.assertTrue(self.Enabled.is_method_call_enabled('res.partner'))

    def test_desactivado_ni_leer(self):
        self.assertEqual(
            self._operaciones('ir.config_parameter'), dict.fromkeys(OPERACIONES, False))
        self.assertFalse(self.Enabled.is_method_call_enabled('ir.config_parameter'))
        self.assertFalse(self.Enabled.is_model_enabled('ir.config_parameter'))

    def test_operacion_invalida_sigue_fallando(self):
        # La validación del proveedor se conserva (super() va primero).
        with self.assertRaises(ValidationError):
            self.Enabled.check_model_operation_enabled('res.partner', 'drop')

    # -- regla de pantalla ----------------------------------------------

    def test_no_se_puede_marcar_escritura_en_protegido(self):
        with self.assertRaises(ValidationError):
            self.filas['ir.cron'].write({'allow_write': True})

    def test_protegido_limpio_y_luego_marcar(self):
        fila = self.filas['ir.cron']
        fila.write(dict(allow_create=False, allow_write=False,
                        allow_unlink=False, allow_method_calls=False))
        with self.assertRaises(ValidationError):
            fila.write({'allow_write': True})
        with self.assertRaises(ValidationError):
            fila.write({'allow_method_calls': True})

    def test_archivar_fila_vieja_no_estorba(self):
        self.filas['ir.cron'].write({'active': False})
        self.filas['ir.config_parameter'].write({'active': False})
        self.assertFalse(self.filas['ir.config_parameter'].active)

    def test_no_se_puede_activar_desactivado(self):
        fila = self.filas['ir.config_parameter']
        fila.write({'active': False})
        with self.assertRaises(ValidationError):
            fila.write({'active': True})

    def test_no_se_puede_crear_desactivado(self):
        model = self.env['ir.model']._get('res.users.apikeys')
        self.Enabled.with_context(active_test=False).search(
            [('model_id', '=', model.id)]).unlink()
        with self.assertRaises(ValidationError):
            self.Enabled.create({'model_id': model.id, 'allow_read': True})

    def test_negocio_se_puede_marcar(self):
        self.filas['res.partner'].write({'allow_write': False})
        self.filas['res.partner'].write({'allow_write': True})
        self.assertTrue(self.filas['res.partner'].allow_write)

    def test_adjuntos_exceptuados(self):
        """ir.attachment no es configuración: no cae en el prefijo «ir.»."""
        from ..models import politica
        self.assertFalse(politica.es_protegido('ir.attachment'))
        self.assertTrue(politica.es_protegido('ir.cron'))
