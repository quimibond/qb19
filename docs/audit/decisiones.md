# Decisiones de Jose sobre el SGI

Índice de las decisiones citadas en el código y el CHANGELOG como D-xx. La
fuente de cada una es la entrada del CHANGELOG indicada; aquí solo se listan
para encontrarlas. Las nuevas se agregan arriba con fecha.

El CHANGELOG mezcla dos series que son decisiones distintas: `D-xx` (dos dígitos, decisiones de Jose de la auditoría de septiembre) y `D-0xx` (tres dígitos, hallazgos de diseño de la auditoría 2026-09). La tabla las separa. `D-02` y `D-002` (o `D-14` y `D-014`, etc.) **no son la misma decisión**.

Las rutas son relativas a `addons/quimibond_sgi/` salvo donde dice `qb_mcp_politica/`. Cuando una clave solo aparece nombrada en una lista (sin texto propio), la fila dice «ver» y la línea.

| Serie | Clave | Decisión (resumen) | Primera aparición |
|---|---|---|---|
| D-xx | D-01 | Los 10 términos de fórmula de `sgi_indicator_formula_data.xml` salen del núcleo (pasan a `__export__` con prefijo `quimibond_sgi_legado_`; el mapa los trae). | `CHANGELOG.md:2579` (también `migrations/19.0.56.35.0/pre-migrate.py:5`) |
| D-xx | D-02 | Clave nueva por tipo y proceso (`PR-{proceso}`, `IT-{proceso}-{nn}`, `CO-{proceso}-{nn}`, `PROT-{proceso}-{nn}`); la clave del Dropbox queda como «clave anterior». Se aplica con script al final del bloque 3. | `CHANGELOG.md:132` (texto de la decisión en `CHANGELOG.md:214`) |
| D-xx | D-03 | El SGI es de una sola empresa (la del SGI, PNTQ id 1); nada de otra empresa entra a procesos, Mis pendientes ni mediciones. | `CHANGELOG.md:23` (también `README.md:4`, `models/sgi_multicompany.py:2`) |
| D-xx | D-04 | «Mis pendientes con todo adentro» (entrega 8a, auditoría 2026-09); incluye las firmas de Firma electrónica por hacer. | `models/sgi_my_pending.py:11` (no está en el CHANGELOG) |
| D-xx | D-05 | Auditor SGI: lectura para auditar; no ve salud ni salarios. | `README.md:72` |
| D-xx | D-06 | Incidentes: investigar, cerrar y reabrir es del Jefe MAST y de Salud ocupacional (junto con D-009). | `models/sgi_incident.py:98` |
| D-xx | D-08 | PIN obligatorio para firmar checklists (`quimibond_sgi.checklist_pin_required`), apagado por default; RH captura los PIN antes de encenderlo. | `CHANGELOG.md:317` (texto en `CHANGELOG.md:1623`) |
| D-xx | D-10 | La regla de aprobación de Studio del rol «Aprueba» sale al satélite `quimibond_sgi_studio` (A-010). | `CHANGELOG.md:2233` |
| D-xx | D-11 | Los modelos de Studio vacíos se borran solo si ninguno de los cuatro tiene registros; ampliada (57.12.0) a sus acompañantes `<modelo>_*` (Jose, 2026-09-29). | `CHANGELOG.md:2118` (también `models/sgi_catalog.py:787`) |
| D-xx | D-12 | Recalcular mediciones pendientes ya no corre en cada actualización: botón en la lista y corrida diaria en el cron de indicadores (A-006). | `CHANGELOG.md:2424` |
| D-xx | D-13 | El indicador E1-02 «Acuerdos de dirección cumplidos en fecha» se mide con `acuerdos_rxd` en lugar de captura manual. | `CHANGELOG.md:2379` |
| D-xx | D-14 | Correo semanal por persona con lo atrasado de su «Mis pendientes», en lugar del resumen semanal viejo a MAST y Dirección. | `CHANGELOG.md:2347` |
| D-xx | D-15 | «Solicitud de compra SGI» y «Cambio de proceso / infraestructura (MOC SGI)» se instalan archivadas; en producción se archivan (nunca se borran) con la familia OP-PTAR. | `CHANGELOG.md:2357` |
| D-xx | D-16 | La encuesta 151 «Checklist Auditoría Interna ISO 9001:2015» (legado) sale del manifest. | `CHANGELOG.md:2385` (también `migrations/19.0.57.6.0/pre-migrate.py:12`) |
| D-xx | D-17 | «Acciones correctivas» muestra todas las acciones (NC, riesgos, AMEF, incidentes, simulacros, objetivos, revisión por la dirección, indicadores en rojo) con filtro por origen; Auditor, MAST y Dirección. | `CHANGELOG.md:1280` (texto en `CHANGELOG.md:1538` y `views/sgi_menus.xml:124`) |
| D-xx | D-18 | El Auditor y Dirección leen el Diagnóstico (carpeta «6.4 Diagnóstico»). | `models/sgi_diagnostic.py:90` (también `views/sgi_menus.xml:251`) |
| D-xx | D-20 | Menús sin paréntesis («Tableros (SGI)» pasa a «Paretos de calidad»; Empleados: DNC y brechas, hojas de eficiencias). | `views/sgi_menus.xml:320` |
| D-xx | D-21 | «Documentos vigentes» en Inicio: título y revisión (sin la clave del Dropbox) y el acuse de lectura en el mismo renglón (entrega 4, I-015). | `models/sgi_current_documents.py:2` (también `views/sgi_menus.xml:44`) |
| D-xx | D-22 | En el SGI no se borra por MCP, se archiva: `delete_record` sobre cualquier modelo `sgi.*` se rechaza (Jose, 2026-09-29). | `qb_mcp_politica/README.md:25` (también `qb_mcp_politica/models/politica.py:110`) |
| D-xx | D-28 | Normas del SGI y su estado (organismo SIDE Certificaciones, acreditado ante la ema). | `README.md:10` (también `CHANGELOG.md:1337`) |
| D-xx | D-29 | `help` de campos y ayudas de pantalla vacía en «usted», para quien llena el campo. | `CHANGELOG.md:1272` (también `README.md:141`) |
| D-xx | D-30 | CHANGELOG del módulo reconstruido desde git y chequeo en `tools/check_addons.py` (K-018): subir la versión exige su sección `## <versión>`. | `CHANGELOG.md:2500` |
| D-xx | D-31 | Licencia OPL-1. | `CHANGELOG.md:1341` (también `README.md:152`) |
| D-0xx | D-001 | Las fichas estándar abren para quien no es del SGI. | `tests/test_entrega1c.py:11` |
| D-0xx | D-002 | El título limpio del documento manda; la clave vigente va debajo y la del Dropbox solo como «Clave anterior» (C-009). | `CHANGELOG.md:1556` (texto en `views/sgi_document_views.xml:42`) |
| D-0xx | D-003 | Nomenclatura en pantallas y reportes: la clave del Dropbox deja de estar escrita a mano (ver `CHANGELOG.md:1556`; sin texto propio). | `CHANGELOG.md:1556` |
| D-0xx | D-004 | Igual que D-003: parte de «nomenclatura en pantallas» (ver `CHANGELOG.md:1556`; sin texto propio). | `CHANGELOG.md:1556` |
| D-0xx | D-005 | El título de Dirección → Tablero es el nombre del menú. | `models/sgi_direction_board.py:159` |
| D-0xx | D-006 | Las reclamaciones son los tickets de los equipos marcados como «Equipo de reclamaciones (SGI)» (decisión 9), no un XML ID. | `CHANGELOG.md:177` (también `views/sgi_complaint_views.xml:34`) |
| D-0xx | D-007 | La acción correctiva (`sgi.action.line`) tiene historial (`mail.thread`) y chatter. Pendiente: evidencia obligatoria al terminar una acción. | `CHANGELOG.md:1506` (texto en `CHANGELOG.md:1536`) |
| D-0xx | D-008 | Confirmación («¿Desea continuar?») en los 29 botones que cierran, reabren, obsoletan, rechazan o regresan a borrador. | `CHANGELOG.md:1506` (texto en `CHANGELOG.md:1510`) |
| D-0xx | D-009 | Aprobar/rechazar, cerrar/reabrir y obsoletar son del Jefe MAST y del dueño del proceso (decisión de Jose, V-A05). | `CHANGELOG.md:1219` (también `models/sgi_base.py:36`) |
| D-0xx | D-010 | Reclamaciones por marca de equipo `helpdesk.team.sgi_is_complaint`, solo Jefe MAST (decisión 9 de la tanda 2). | `CHANGELOG.md:1591` |
| D-0xx | D-011 | Se retira la encuesta de auditoría legado (con B-009 y D-016). | `CHANGELOG.md:2385` |
| D-0xx | D-012 | Se retiran la acción `sgi_process_hierarchy_action` y las vistas `sgi_nc_view_pivot`/`sgi_nc_view_graph` (B-014); el Pareto usa sus propias vistas. | `CHANGELOG.md:2307` |
| D-0xx | D-013 | Se quitan los filtros por defecto muertos (`search_default_recent`, etc.; E-016). | `CHANGELOG.md:1506` (texto en `CHANGELOG.md:1522`) |
| D-0xx | D-014 | 14 modelos con búsqueda propia y «Archivados» en otros tres. | `CHANGELOG.md:1507` (texto en `CHANGELOG.md:1515`) |
| D-0xx | D-015 | Ayudas para «Hojas de checklist» y «Análisis por empleado»; la del Pareto ya no habla de «TEJIDO-*» (E-015). | `CHANGELOG.md:1507` (texto en `CHANGELOG.md:1526`) |
| D-0xx | D-016 | Campos técnicos (dominios, rutas, columna del XML ID) solo para MAST. | `CHANGELOG.md:1507` (texto en `CHANGELOG.md:1530`) |
| D-0xx | D-017 | La sustitución de un procedimiento se captura en el documento (el documento es la fuente de verdad; lo captura el Jefe MAST). | `views/sgi_process_views.xml:351` (también `views/sgi_document_views.xml:83`, `models/sgi_document.py:1069`) |
| D-0xx | D-020 | Parte de «nomenclatura en pantallas» (ver `CHANGELOG.md:1557`; sin texto propio). | `CHANGELOG.md:1557` |
| D-0xx | D-021 | En alertas de calidad sin folio del SGI se ocultan «Desviación y análisis», «Correcciones y acciones» y «Verificación y cierre». | `CHANGELOG.md:1507` (texto en `CHANGELOG.md:1533`) |
