# -*- coding: utf-8 -*-
"""57.99.0 — Salud del SGI (auditoría 2026-10, sección 8 y hallazgo D-01).

Constantes sin modelos: se pueden importar desde cualquier archivo de
``models/`` sin cargar ningún modelo (como ``sgi_control_hierarchy``)."""

HEALTH_MODES = (
    'salud_procesos', 'salud_personas', 'salud_planta', 'salud_acuses',
    'salud_validacion', 'salud_rojos', 'salud_nc', 'salud_avisos',
    'salud_auditoria', 'salud_formatos',
)
# Se reconstruyen por fechas; los demás miden el estado al calcular (foto).
HEALTH_DATED_MODES = ('salud_validacion', 'salud_nc')
HEALTH_SNAPSHOT_MODES = tuple(m for m in HEALTH_MODES if m not in HEALTH_DATED_MODES)
HEALTH_XMLIDS = tuple('quimibond_sgi.sgi_ind_salud_%s' % key for key in (
    'procesos', 'personas', 'planta', 'acuses', 'validacion', 'rojos', 'nc',
    'avisos', 'auditoria', 'formatos'))

# Modelos cuyas altas o escrituras cuentan como «usar el SGI» (SG-02 y los
# días sin movimiento del dueño de proceso). Cada uno solo si existe en el
# registro. ``quality.alert`` solo con folio y ``documents.document`` solo
# los controlados (filtros en ``_sgi_health_touches``). Los mensajes cuentan
# en cualquier modelo ``sgi.*`` salvo los de HEALTH_PRIVATE_MODELS.
HEALTH_TOUCH_MODELS = (
    'sgi.indicator.measure', 'sgi.action.line', 'sgi.document.ack', 'sgi.audit',
    'sgi.audit.finding', 'sgi.audit.checklist.line', 'sgi.incident', 'sgi.risk',
    'sgi.legal.requirement', 'sgi.management.review', 'sgi.process.activity',
    'sgi.interested.party', 'sgi.env.aspect', 'sgi.work.permit', 'sgi.loto',
    'sgi.calibration', 'sgi.checklist.line', 'sgi.epp.delivery',
    'quality.alert', 'quality.check', 'documents.document',
)
# Salud ocupacional: ni sus altas ni sus mensajes se leen para la salud del
# SGI (dato personal).
HEALTH_PRIVATE_MODELS = ('sgi.health.record',)

PEOPLE_DAYS = 30
VALIDATION_WINDOW_DAYS = 30
VALIDATION_PREFILTER_DAYS = 15
RED_WINDOW_MONTHS = 3
NC_WINDOW_DAYS = 90
NC_OPEN_DAYS = 60
FORMAT_USE_DAYS = 90
IDLE_DAYS = 90
# Validaciones atrasadas por dueño: solo mediciones de periodos recientes.
LATE_WINDOW_DAYS = 120
EXCLUDED_USERS_PARAM = 'quimibond_sgi.health_excluded_user_ids'
MAIL_USERS_PARAM = 'quimibond_sgi.health_mail_user_ids'


def param_ids(env, key):
    """Ids de usuario de un parámetro «1,2,3» (se ignora lo que no es número)."""
    raw = env['ir.config_parameter'].sudo().get_param(key) or ''
    return [int(p) for p in raw.replace(';', ',').split(',') if p.strip().isdigit()]
