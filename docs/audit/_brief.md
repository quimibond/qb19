# Brief común de los agentes de la auditoría SGI

Lee esto completo antes de empezar. Repo: `/home/user/qb19`, módulo `addons/quimibond_sgi` (Odoo 19). Producción: `quimibond-qb19.odoo.com`, empresa 1 «PRODUCTORA DE NO TEJIDOS QUIMIBOND». Versión instalada **19.0.56.24.1** (igual que el manifest en esta rama). Lee también `/home/user/qb19/CLAUDE.md` (reglas del build de Odoo.sh, versionado, herencia de vistas) y `docs/audit/00-inventario.md`.

## Negocio
Quimibond: textil técnico (tejido circular, tramado, tintorería y acabado, entretelas/no tejidos), ~160 personas, Toluca. Líneas: Confección e Industrial (automotriz). Exporta ~50 % a EUA/Canadá. SGI integrado ISO 9001:2015, 14001:2015, 45001:2018. **No** está certificada IATF 16949; los requisitos de clientes automotrices están como la norma «Requisitos específicos de clientes» (`sgi.norm` 5, cláusulas CLI-01…CLI-10).

## Sistema anterior (Dropbox) y nuevo (Odoo)
- Dropbox: ~50 procedimientos PDF (P-A01…P-S03), documentos F-P-xxx, IT-xxx, DAT, anexos; los procesos cargados primero en Odoo copiando esos procedimientos (P-VEN, P-SGI, MP-*, …) están **archivados**.
- Odoo: 14 procesos (C1–C6, S1–S6, E1, E2) → etapas → 310 actividades → roles (ejecuta/aprueba/participa/informa/escala por puesto, familia de puestos o rol relativo) → entregables de entrada y salida → vencimiento → medición automática. «Mis pendientes», «Mi procedimiento», propuestas de cambio.
- Grupos: Usuario SGI (316), Auditor SGI (317, sin miembros), Jefe MAST y SGI (318), Dirección de Operaciones (319), Administrador SGI (325), Captura de eficiencias (327).
- Satélites: `quimibond_sgi_pesaje`, `quimibond_sgi_plm`, `quimibond_sgi_revisado`. **En esta tanda solo se revisan sus dependencias y los puntos donde tocan el núcleo** (herencias, campos, menús, grupos). Su auditoría completa va después.

## Decisiones YA tomadas (no reabrir; verificar que se cumplan)
1. Nomenclatura nueva en menús, pantallas, títulos y nombres. La clave del Dropbox vive solo como dato del documento (`sgi_previous_code`) y en «Del Dropbox a Odoo». Nada de títulos mezclados («F-P-C09-02 Matriz…»).
2. Árbol del menú decidido: Inicio (Mis pendientes, Mi procedimiento, Mis indicadores, Mi equipo, Eficiencias de mi área) · Procesos (Mapa de procesos, Actividades, Matriz de responsabilidades, Puestos y procesos, Fichas de proceso por máquina, Del Dropbox a Odoo) · Mejora (No conformidades, Reclamaciones de clientes, Acciones correctivas, Mejora continua, Lecciones aprendidas, Quejas y sugerencias del personal; Auditorías: Programa, Auditorías realizadas) · Seguridad y ambiente (Incidentes y accidentes, Planes de emergencia, Simulacros, Recorridos CSH, Estudios de higiene y exámenes médicos, Responsivas de EPP, Hojas de checklist) · Dirección (Tablero, Revisión por la dirección, Política integral, Objetivos integrales, Riesgos y oportunidades, Requisitos legales, Partes interesadas, Satisfacción del cliente) · Administración SGI (Documentos: Documentos, Lista maestra, Documentos externos, Solicitudes de cambio, Tipos de documento · Indicadores: Indicadores, Mediciones, Mediciones por equipo o mercado · Aprobaciones del SGI · Diagnóstico · Firmas de lectura: Publicar Mi procedimiento, Acuses de lectura · Configuración). «Bitácora de bloqueo contable» y «Valor del inventario por mes» van en Contabilidad → Reportes (ya están ahí).
3. Nada archivado (actividades, procesos, categorías de aprobación, solicitudes cerradas) aparece en Mis pendientes ni en Mi procedimiento.
4. **Estructura contra datos:** el módulo trae estructura, catálogos base y configuración. Actividades y datos operativos se capturan en producción (sin XML ID) y ningún upgrade debe pisarlos ni duplicarlos.
5. Vencimientos: mensual (día hábil), semanal (día de la semana), trimestral/semestral/anual (mes + día), por evento (entrada con plazo en días hábiles).
6. «Cumple con» (`norm_clause_ids`) **ya está construido y cargado** (225 actividades). Las 85 actividades sin cláusula (finanzas, fiscal, nómina) se quedan así. Los 24 requisitos sin actividad son huecos que atiende Jose. **Nada de esto es hallazgo.**
7. Sección permanente «Del Dropbox a Odoo» (en Procesos; todos consultan, solo edita Jefe MAST y SGI): buscador por clave vieja, procedimientos anteriores, formatos anteriores con destino, rutina por rutina, tablero de avance. Integra el menú «Migración de formatos». Todo lo que sirva para esto (claves anteriores, `legacy_number`, destinos de migración) **no es obsoleto**: se reubica.

## Aclaraciones de Jose (no reportar como hallazgo)
- 427 documentos del Dropbox contra "~490": la segunda cifra incluía externos, procedimientos y Mi procedimiento. No es discrepancia.
- Roles de actividades archivadas: ya se borraron con respaldo en 56.24.0.

## Reglas
- **Solo lectura.** No edites nada en `addons/`, no hagas commits ni push, no toques git. Escribe **solo** tu archivo `docs/audit/NN-<tema>.md` (y, si lo necesitas, CSV de apoyo en `docs/audit/NN-<tema>/`).
- Producción por MCP (`mcp__Quimibond_-_Odoo__*`, cárgalas con ToolSearch): usa **solo** `search_records`, `aggregate_records`, `get_record`, `get_fields`, `list_models`. **Prohibido** `create_record`, `update_record`, `delete_record`, `call_model_method`, `post_message`. Filtra a `company_id = 1` cuando el modelo lo tenga.
- No borrar datos: lo que sobre se propone **archivar** (con respaldo) y se decide en la fase 2.
- Cero contraseñas, tokens o credenciales en lo que escribas.
- Cada hallazgo con evidencia verificable (archivo:línea, xml_id, modelo + id, o la consulta exacta que lo prueba). Si no pudiste verificar algo, dilo.
- No hay staging. Lo que necesite clics o capturas se marca **«pendiente de verificación visual»**.
- El inventario (`docs/audit/inventario/*.csv`) es tu universo. `campos.csv` trae `usos_py/usos_xml/usos_js/usos_tests` (conteo de menciones del nombre, no prueba de uso: confirma con grep dirigido y BD).
- Español claro. No reabras decisiones tomadas.

## Formato obligatorio del archivo
1. **Resumen** (máximo 10 líneas).
2. **Tabla de hallazgos:**
   `| ID | Elemento (xml_id / modelo.campo / archivo:línea) | Hallazgo | Evidencia | Severidad (Crítica/Alta/Media/Baja) | Acción (Eliminar/Corregir/Agregar/Mover/Documentar/Decidir) | Propuesta concreta | Esfuerzo (h) | Depende de |`
   IDs con tu prefijo: A-001 (arquitectura), B-001 (obsoleto), C-001 (modelo), F-001 (seguridad).
3. **Preguntas que requieren decisión de negocio** (para Jose), cada una con tu recomendación.
4. **Cobertura:** cuántos elementos del inventario revisaste de cuántos (por tipo). Debe ser 100 %; si no, di qué faltó y por qué.

Al terminar, responde con un resumen de 10–15 líneas: conteo de hallazgos por severidad, los 5 más importantes y las preguntas para Jose.
