# -*- coding: utf-8 -*-
"""Aplica la política de ``politica.py`` sobre las puertas de ``mcp_server``.

Las tres puertas del proveedor (``is_model_enabled``,
``check_model_operation_enabled``, ``is_method_call_enabled``) están
decoradas con ``@tools.ormcache`` y leen la tabla ``mcp.enabled.model``. Las
consultan ``controllers/utils.py`` (``is_model_mcp_enabled``,
``check_model_operation_allowed``, ``check_model_method_allowed``,
``check_mcp_access``), que a su vez usan los CRUD, ``call_model_method``,
``get_fields`` y la vía XML-RPC.

Caché: la sobrescritura NO lleva ``@ormcache``. ``super()`` entra al método
decorado del padre, que sigue sirviendo de caché lo que dice la tabla (clave
``model_name``/``operation``, se limpia con ``registry.clear_cache()`` al
cambiar la configuración, igual que antes). Encima se aplica una función pura
del nombre del modelo y de constantes del código, así que el resultado es
siempre el mismo para la misma clave: no hay nada que invalidar y la caché no
puede quedar incoherente. Cambiar las listas exige reiniciar Odoo (código).
"""
from odoo import _, api, models
from odoo.exceptions import ValidationError

from . import politica


class McpEnabledModel(models.Model):
    _inherit = 'mcp.enabled.model'

    # ------------------------------------------------------------------
    # Puertas (tiempo de ejecución): mandan aunque la fila tenga casillas
    # marcadas. Así los registros viejos de producción quedan inertes.
    # ------------------------------------------------------------------
    @api.model
    def is_model_enabled(self, model_name):
        # Un modelo desactivado no existe para el MCP (tampoco get_fields).
        if politica.es_desactivado(model_name):
            return False
        return super().is_model_enabled(model_name)

    @api.model
    def check_model_operation_enabled(self, model_name, operation):
        # super() primero: conserva la validación del proveedor (operación
        # inválida → ValidationError) y su caché.
        permitido = super().check_model_operation_enabled(model_name, operation)
        return bool(permitido) and politica.operacion_permitida(model_name, operation)

    @api.model
    def is_method_call_enabled(self, model_name):
        permitido = super().is_method_call_enabled(model_name)
        return bool(permitido) and not politica.es_protegido(model_name)

    # ------------------------------------------------------------------
    # Pantalla: no dejar marcar lo que la política prohíbe.
    #
    # Solo se evalúa al crear o escribir los campos listados. Archivar
    # (``active = False``) una fila protegida no dispara la regla, para no
    # estorbar la limpieza de las filas viejas; en una desactivada sí se
    # evalúa, y archivarla siempre pasa.
    # ------------------------------------------------------------------
    @api.constrains('model_id', 'allow_create', 'allow_write', 'allow_unlink', 'allow_method_calls')
    def _check_politica_solo_lectura(self):
        for record in self:
            nombre = record.model_id.model
            if not record.active or not politica.es_protegido(nombre):
                continue
            marcadas = [
                record._fields[campo].string
                for campo in politica.CASILLAS_ESCRITURA
                if record[campo]
            ]
            if marcadas:
                raise ValidationError(_(
                    "El modelo «%(modelo)s» es técnico y el MCP solo puede leerlo "
                    "(política de Quimibond, auditoría SGI F-001). Desmarca: %(casillas)s. "
                    "Si la fila ya traía varias casillas marcadas, desmárcalas todas a la vez "
                    "o archiva la fila.",
                    modelo=nombre,
                    casillas=', '.join(marcadas),
                ))

    @api.constrains('model_id', 'active', 'allow_read')
    def _check_politica_desactivado(self):
        for record in self:
            nombre = record.model_id.model
            if record.active and politica.es_desactivado(nombre):
                raise ValidationError(_(
                    "El modelo «%(modelo)s» guarda secretos o credenciales y no se "
                    "expone por MCP, ni siquiera para leer (política de Quimibond, "
                    "auditoría SGI F-001). Deja la fila archivada.",
                    modelo=nombre,
                ))
