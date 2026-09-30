<!-- Generado por tools/sgi_docs.py a partir del código. No editar a mano: correr `python3 tools/sgi_docs.py`. -->

# `sgi.config`

**Configuración SGI** (AbstractModel).

Utilidades de configuración del SGI (siembra idempotente).

Archivos: `addons/quimibond_sgi/models/sgi_format_map.py`, `addons/quimibond_sgi/models/sgi_archived_filters.py`, `addons/quimibond_sgi/models/sgi_business_calendar.py`, `addons/quimibond_sgi/models/sgi_catalog.py`.

## Métodos públicos (4)

| Método | Qué hace (docstring) |
|---|---|
| `migrate_document_families` | H21: llena sgi_parent_document_id (FK) donde esté vacío, infiriendo el procedimiento padre P-Xnn vigente por la nomenclatura. Idempotente: solo toca los documentos sin padre. Los que no matcheen qued… |
| `recompute_pending_measures` | Re-mide las mediciones PENDIENTES de indicadores automáticos. Es la contracara de la deuda B.16: el cron solo crea la medición faltante, así que activar un modo o corregir el motor dejaba los meses y… |
| `seed_parameters` | — |
| `sgi_drop_empty_studio_models` | D-11 (57.7.0, ampliada en 57.12.0): borra los modelos de Studio vacíos que el SGI sustituye y sus acompañantes, con sus menús, acciones y vistas. Se corre a mano en el shell de Odoo.sh, fuera de un u… |
