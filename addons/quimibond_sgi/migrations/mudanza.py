# -*- coding: utf-8 -*-
"""Mudanza de registros del SGI a un satélite, sin borrar ni recrear nada.

No es un script de migración: Odoo solo corre los ``pre-``/``post-``/``end-``
de las carpetas por versión y este archivo está en la raíz de
``migrations/``, así que lo ignora. Lo cargan por ruta los ``pre-migrate.py``
que sacan algo del núcleo (auditoría 2026-09, entrega 2: A-010, A-014, A-016,
A-018; decisión 5 de Jose: «lo que sale del SGI va a módulos propios, sin
borrar datos»; E-002: menús y acciones conservan su ``res_id``).

Cómo funciona una mudanza en el mismo update de producción:

1. ``pre-migrate`` del núcleo (antes de cargar su código nuevo): los XML IDs
   de lo que se va cambian de ``module`` a ``quimibond_sgi`` → el satélite en
   ``ir_model_data``. También ``ir_model_constraint.module`` e
   ``ir_model_relation.module`` de esos modelos. Los registros, las tablas y
   las columnas no se tocan.
2. El mismo ``pre-migrate`` marca el satélite para instalar
   (``button_install``). Odoo lo carga en la vuelta siguiente del paso 3 de
   ``load_modules`` («loop this step in case extra modules' states are
   changed to 'to install'»). Sin esto, ``auto_install`` no basta: Odoo solo
   instala en automático un módulo cuando otra de sus dependencias se está
   instalando, no cuando se actualiza.
3. Mientras carga el núcleo, las vistas mudadas ya no cuentan como suyas
   (``_get_inheriting_views`` filtra por módulos cargados), así que las
   herencias del satélite sobre vistas del núcleo no se validan antes de
   tiempo.
4. El satélite se instala: encuentra sus XML IDs y ACTUALIZA los registros
   existentes (mismo ``res_id``). En instalación Odoo sobrescribe también los
   ``noupdate``: por eso cada ``pre-migrate`` revisa antes en producción que
   crons y parámetros mudados coincidan con el XML.
5. Al final (``_process_end``) Odoo borra los registros de los módulos
   actualizados cuyo XML ID no se cargó. Como lo mudado ya es del satélite y
   el satélite lo carga, no se borra nada. Si un XML ID se muda y el satélite
   NO lo declara, se borraría: la lista de cada mudanza se sacó de los
   archivos del satélite y se revisa con ``faltantes``.

Todo es idempotente: una segunda corrida no encuentra nada que mudar.
"""
import logging

_logger = logging.getLogger(__name__)

ORIGEN = 'quimibond_sgi'

# Tablas de registros «de un modelo»: al mudar un modelo entero se mudan sus
# accesos, reglas, vistas, acciones, reportes, acciones de servidor, crons y
# plantillas de correo con XML ID del núcleo.
_POR_MODELO = [
    ('ir.model.access', "SELECT a.id FROM ir_model_access a JOIN ir_model m ON m.id = a.model_id "
                        "WHERE m.model = ANY(%(modelos)s)"),
    ('ir.rule', "SELECT r.id FROM ir_rule r JOIN ir_model m ON m.id = r.model_id "
                "WHERE m.model = ANY(%(modelos)s)"),
    ('ir.ui.view', "SELECT v.id FROM ir_ui_view v WHERE v.model = ANY(%(modelos)s)"),
    ('ir.actions.act_window', "SELECT a.id FROM ir_act_window a WHERE a.res_model = ANY(%(modelos)s)"),
    ('ir.actions.report', "SELECT a.id FROM ir_act_report_xml a WHERE a.model = ANY(%(modelos)s)"),
    ('ir.actions.server', "SELECT s.id FROM ir_act_server s JOIN ir_model m ON m.id = s.model_id "
                          "WHERE m.model = ANY(%(modelos)s)"),
    ('ir.cron', "SELECT c.id FROM ir_cron c JOIN ir_act_server s ON s.id = c.ir_actions_server_id "
                "JOIN ir_model m ON m.id = s.model_id WHERE m.model = ANY(%(modelos)s)"),
    ('mail.template', "SELECT t.id FROM mail_template t JOIN ir_model m ON m.id = t.model_id "
                      "WHERE m.model = ANY(%(modelos)s)"),
]


def _module_id(cr, name):
    cr.execute("SELECT id FROM ir_module_module WHERE name = %s", (name,))
    row = cr.fetchone()
    return row and row[0]


def _mover(cr, destino, model, ids, etiqueta):
    """Pasa a ``destino`` los XML IDs del núcleo de esos registros."""
    if not ids:
        return 0
    cr.execute("""
        UPDATE ir_model_data d SET module = %s
         WHERE d.module = %s AND d.model = %s AND d.res_id = ANY(%s)
           AND NOT EXISTS (SELECT 1 FROM ir_model_data x WHERE x.module = %s AND x.name = d.name)""",
               (destino, ORIGEN, model, list(ids), destino))
    n = cr.rowcount
    if n:
        _logger.info("%s: %d XML ID(s) de %s pasan a %s.", etiqueta, n, model, destino)
    return n


def _ids(cr, query, params):
    cr.execute(query, params)
    return [r[0] for r in cr.fetchall()]


def mover(cr, destino, etiqueta, modelos=(), campos=(), relaciones=(), nombres=()):
    """Muda al satélite ``destino``:

    - ``modelos``: modelos enteros (``ir.model``, sus campos, valores de
      selección, restricciones, relaciones y los registros de ``_POR_MODELO``).
    - ``campos``: campos sueltos que el núcleo agregaba a modelos que se
      quedan, como ``'budget.analytic.sgi_sales_budget_id'``.
    - ``relaciones``: tablas many2many de esos campos sueltos.
    - ``nombres``: XML IDs explícitos (menús, secuencias, plantillas QWeb,
      parámetros, vistas heredadas de otros modelos).

    Regresa el número de XML IDs mudados.
    """
    total = 0
    params = {'modelos': list(modelos)}
    if modelos:
        total += _mover(cr, destino, 'ir.model',
                        _ids(cr, "SELECT id FROM ir_model WHERE model = ANY(%(modelos)s)", params), etiqueta)
    field_ids = []
    if modelos:
        field_ids += _ids(cr, "SELECT id FROM ir_model_fields WHERE model = ANY(%(modelos)s)", params)
    for full in campos:
        model, name = full.rsplit('.', 1)
        field_ids += _ids(cr, "SELECT id FROM ir_model_fields WHERE model = %s AND name = %s", (model, name))
    total += _mover(cr, destino, 'ir.model.fields', field_ids, etiqueta)
    if field_ids:
        total += _mover(cr, destino, 'ir.model.fields.selection', _ids(
            cr, "SELECT id FROM ir_model_fields_selection WHERE field_id = ANY(%s)", (field_ids,)), etiqueta)

    origen_id, destino_id = _module_id(cr, ORIGEN), _module_id(cr, destino)
    if modelos:
        cons = _ids(cr, "SELECT c.id FROM ir_model_constraint c JOIN ir_model m ON m.id = c.model "
                        "WHERE m.model = ANY(%(modelos)s)", params)
        total += _mover(cr, destino, 'ir.model.constraint', cons, etiqueta)
        if cons and destino_id:
            cr.execute("""
                UPDATE ir_model_constraint c SET module = %s
                 WHERE c.id = ANY(%s) AND c.module = %s
                   AND NOT EXISTS (SELECT 1 FROM ir_model_constraint x
                                    WHERE x.module = %s AND x.name = c.name)""",
                       (destino_id, cons, origen_id, destino_id))
        for model, query in _POR_MODELO:
            total += _mover(cr, destino, model, _ids(cr, query, params), etiqueta)
    if destino_id and (modelos or relaciones):
        cr.execute("""
            UPDATE ir_model_relation r SET module = %s
              FROM ir_model m
             WHERE r.module = %s AND m.id = r.model
               AND (m.model = ANY(%s) OR r.name = ANY(%s))
               AND NOT EXISTS (SELECT 1 FROM ir_model_relation x
                                WHERE x.module = %s AND x.name = r.name)""",
                   (destino_id, origen_id, list(modelos), list(relaciones), destino_id))
    if nombres:
        cr.execute("""
            UPDATE ir_model_data d SET module = %s
             WHERE d.module = %s AND d.name = ANY(%s)
               AND NOT EXISTS (SELECT 1 FROM ir_model_data x WHERE x.module = %s AND x.name = d.name)""",
                   (destino, ORIGEN, list(nombres), destino))
        if cr.rowcount:
            _logger.info("%s: %d XML ID(s) por nombre pasan a %s.", etiqueta, cr.rowcount, destino)
        total += cr.rowcount
    _logger.info("%s: %d XML ID(s) mudados de %s a %s (los registros se quedan).",
                 etiqueta, total, ORIGEN, destino)
    return total


def faltantes(cr, destino, nombres, etiqueta):
    """XML IDs esperados que no están ni en el núcleo ni en el satélite: en una
    base que viene de una versión vieja pueden no existir (se crean al
    instalar); se avisan para revisarlos en el log."""
    if not nombres:
        return []
    cr.execute("SELECT name FROM ir_model_data WHERE module IN %s AND name = ANY(%s)",
               ((ORIGEN, destino), list(nombres)))
    found = {r[0] for r in cr.fetchall()}
    missing = sorted(set(nombres) - found)
    if missing:
        _logger.warning("%s: %d XML ID(s) esperados no existen (se crearán al instalar %s): %s",
                        etiqueta, len(missing), destino, missing)
    return missing


def instalar(cr, destino, requiere, etiqueta):
    """Marca el satélite para instalarse en este mismo update si están todas
    sus dependencias ajenas al SGI. Si falta alguna no se instala (no había
    datos que mudar) y se avisa. Si el satélite no está en la lista de
    módulos, se detiene el update: lo mudado se quedaría sin dueño."""
    from odoo import SUPERUSER_ID, api

    env = api.Environment(cr, SUPERUSER_ID, {})
    Module = env['ir.module.module']
    module = Module.search([('name', '=', destino)])
    if not module:
        raise RuntimeError("%s: el módulo %s no está en la lista de módulos; revisa que esté en el "
                           "repositorio antes de actualizar quimibond_sgi." % (etiqueta, destino))
    if module.state != 'uninstalled':
        _logger.info("%s: %s ya está en estado %s.", etiqueta, destino, module.state)
        return False
    ready = ('installed', 'to install', 'to upgrade')
    missing = Module.search([('name', 'in', list(requiere)), ('state', 'not in', ready)]).mapped('name')
    missing += sorted(set(requiere) - set(Module.search([('name', 'in', list(requiere))]).mapped('name')))
    if missing:
        _logger.warning("%s: %s no se instala porque faltan %s.", etiqueta, destino, missing)
        return False
    module.button_install()
    _logger.info("%s: %s marcado para instalar en este mismo update.", etiqueta, destino)
    return True
