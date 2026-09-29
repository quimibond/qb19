# -*- coding: utf-8 -*-
"""Política del MCP de Quimibond (auditoría SGI, hallazgo F-001).

Único lugar donde viven las listas. Origen:
``docs/audit/06-seguridad.md`` §5 y ``docs/audit/06-seguridad/mcp_propuesta.csv``
(30 modelos en solo lectura y 9 desactivados), más la decisión de Jose del
2026-09-29 (``docs/audit/decisiones.md``, F-001).

Por qué: la regla de ``mcp_server`` (``mcp.enabled.model``) es global y se
cambia con una casilla en Ajustes. En producción los modelos técnicos estaban
con CRUD completo, así que cualquier sesión del MCP podía reprogramar crons,
cambiar grupos y usuarios o leer y escribir parámetros del sistema (llaves de
Supabase, etc.). Las ACL de Odoo no frenan nada porque quien se conecta es
administrador. Este módulo deja la política en código para que nadie la vuelva
a abrir por error desde Ajustes.

Con escritura se quedan ``sgi.*`` y los modelos de negocio de la carga
(``documents.*``, ``approval.*``, ``fleet.*``, ``maintenance.*``, ``hr.*``,
``quality.*``, ``mail.activity``, ``mail.message``…): no están aquí.

Las funciones de este archivo son puras (dependen solo del nombre del modelo y
de estas constantes); por eso se pueden aplicar encima de los métodos
``@ormcache`` de ``mcp_server`` sin volver incoherente su caché.
"""

# ---------------------------------------------------------------------------
# Desactivados: el MCP no los ve ni para leer.
# Guardan secretos o credenciales (llaves API, semillas TOTP, secretos OAuth,
# contraseñas de buzones, llaves de Supabase en ir.config_parameter) o datos de
# sesión de los usuarios. Nombres exactos de mcp_propuesta.csv
# («desactivar (active = False)»).
# ---------------------------------------------------------------------------
MODELOS_DESACTIVADOS = frozenset({
    'ir.config_parameter',      # decisión de Jose 2026-09-29: contiene llaves
    'res.users.apikeys',
    'res.users.apikeys.show',
    'auth_totp.device',
    'auth.oauth.provider',
    'fetchmail.server',
    'iap.account',
    'res.users.deletion',
    'res.users.log',
})

# ---------------------------------------------------------------------------
# Protegidos (solo lectura): se pueden leer, pero no crear, modificar, borrar
# ni llamarles métodos. Son la configuración técnica de Odoo: quién puede qué
# (grupos, usuarios, reglas, accesos), qué corre solo (crons, acciones), qué se
# ve (menús, vistas) y qué está instalado (módulos).
# ---------------------------------------------------------------------------

# Prefijos: cualquier modelo cuyo nombre técnico empiece así.
PREFIJOS_PROTEGIDOS = (
    'ir.',                      # todos: ir.cron, ir.ui.*, ir.actions.*, ir.model*,
                                # ir.module.module, ir.rule, ir.default, ir.filters…
    'res.groups.',              # res.groups.privilege y demás
    'res.users.',               # res.users.settings*, etc.
    'auth.',                    # §5 opción B: auth* (los conocidos ya están desactivados)
    'auth_',
    'studio.',
    'spreadsheet.dashboard.',   # .group, .share
    'web_tour.',
    'iap.',
)

# Nombres exactos: los de mcp_propuesta.csv («solo lectura») que no cubre un
# prefijo, más los que pide F-001.
MODELOS_PROTEGIDOS = frozenset({
    'res.groups',
    'res.users',
    'res.company',
    'mail.template',
    'spreadsheet.dashboard',
    'spreadsheet.template',
    'spreadsheet.mixin',
})

# Casillas de mcp.enabled.model que dan escritura.
CASILLAS_ESCRITURA = ('allow_create', 'allow_write', 'allow_unlink', 'allow_method_calls')


def es_desactivado(model_name):
    """El MCP no expone este modelo ni para leer."""
    return bool(model_name) and model_name in MODELOS_DESACTIVADOS


# Excepciones al prefijo «ir.»: son datos de negocio, no configuración. Los
# adjuntos de documentos y mensajes del SGI deben poder subirse por MCP si
# algún día se habilita el modelo (hoy no está expuesto). Decisión F-001: la
# carga (documentos, aprobaciones…) conserva escritura.
MODELOS_EXCEPTUADOS = frozenset({
    'ir.attachment',
})


def es_protegido(model_name):
    """El MCP solo puede leer este modelo (los desactivados también cuentan)."""
    if not model_name:
        return False
    if model_name in MODELOS_EXCEPTUADOS:
        return False
    return (
        model_name in MODELOS_DESACTIVADOS
        or model_name in MODELOS_PROTEGIDOS
        or model_name.startswith(PREFIJOS_PROTEGIDOS)
    )


# ---------------------------------------------------------------------------
# D-22 (Jose, 2026-09-29): en el SGI no se borra por MCP; se archiva. El
# borrado (``unlink``) de cualquier modelo ``sgi.*`` se rechaza aunque la
# casilla «Allow Delete» esté marcada. Leer, crear y escribir (incluido
# ``active = False``) siguen como diga la casilla.
# ---------------------------------------------------------------------------
PREFIJOS_SIN_BORRADO = ('sgi.',)

MENSAJE_SIN_BORRADO = (
    "En el SGI no se borra por MCP: archívalo (active=False) con "
    "update_record. Si el modelo no tiene «active», ciérralo o cancélalo en "
    "pantalla (política de Quimibond, decisión D-22)."
)


def es_sin_borrado(model_name, operation):
    """El MCP no puede borrar registros de este modelo (D-22)."""
    return (operation == 'unlink' and bool(model_name)
            and model_name.startswith(PREFIJOS_SIN_BORRADO))


def operacion_permitida(model_name, operation):
    """Lo que la política deja pasar, antes de mirar la casilla de Ajustes.

    ``True`` no concede nada por sí solo: el resultado final es la casilla
    de ``mcp.enabled.model`` Y esta función.
    """
    if es_desactivado(model_name):
        return False
    if es_sin_borrado(model_name, operation):
        return False
    if es_protegido(model_name):
        return operation == 'read'
    return True
