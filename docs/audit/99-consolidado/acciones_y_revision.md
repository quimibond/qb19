# Acciones en producción por responsable y modelos a revisar

Versión del 2026-09-29, con el plan aprobado por Jose. Fuente: `99-consolidado.md` §6 y `decisiones.md`. Las fechas son propuestas.

Calendario de entregas usado para fijarlas: entrega 1 → 1b → 4 → 8a → 3 → 6 → 2 → 5 → resto de 8 → 9 → 10. Cada una va de su rama a `main` y de `main` a `quimibond`.

## Acciones en producción, agrupadas por responsable

### Jose

| # | Qué | Fecha propuesta | IDs |
|---|---|---|---|
| 1 | Aplicar el MCP en Ajustes (respaldo CSV; 30 modelos en solo lectura y 9 desactivados, entre ellos `ir.config_parameter`; límite de peticiones encendido). Avisarme para verificar | **Esta semana, antes de la entrega 1** (2-oct-2026) | F-001, F-002 |
| 7 | Archivar 3359, 4995, 4022, 3760, 3644 y 5119. Antes de archivar 5556, compararlo con 3518: si es una revisión más nueva de P-A14, subirlo como versión de 3518 | Antes de la entrega 3 (16-oct-2026) | C-008, L-016 |
| 8 | Nombre del dueño de C4, para que Sistemas le cree usuario | 2-oct-2026 | H-002 |
| 10 | Membresía del grupo de captura de eficiencias: salir tú (7) y Sistemas (152); entran 24 y 130 | Con la entrega 1b (6-oct-2026) | F-020, I-020 |
| 12 | Quitarle Calidad / Usuario (66) al contador externo (132) | 2-oct-2026 | I-018 |
| 17 | Aprobador correcto de E2.01, S4.03 y S6.07; yo te propongo quién | Antes de la entrega 8a (9-oct-2026) | H-018 |
| 18 | Resolver PV15254 y PV15323 (empresa 4) | Cuando quieras | I-004 |
| 19 | Dar el grupo Auditor SGI solo a Areli y a Oscar González (D-05) | Con la entrega 4 (9-oct-2026) | I-011 |
| 21 | Ligar en el documento la sustitución de los 13 procedimientos que hoy están vacíos, conforme se cierren sus rutinas | Continuo | C-001 |
| 25 | Archivar las carpetas 1805, 3078 y 1120 de Documentos; resolver los 8 de «POR CLASIFICAR» | Antes de la entrega 10 | K-024 |
| 26 | Archivar el tablero «Salud del SGI» (65) si está vacío | Con la entrega 4 | E-017 |

### Sistemas

| # | Qué | Fecha propuesta | IDs |
|---|---|---|---|
| 2 | **Cambiar las contraseñas de las cuentas que aparecen en P-I01** | **Ya** (30-sep-2026) | L-001 |
| 4 | Meter **solo** a Miguel Medina (88) en «Salud ocupacional (SGI)» y comprobar que el menú de exámenes ya no tenga el grupo 68 | El día que se despliegue la entrega 1 | F-004, I-013 |
| 5 | Depurar a los 20 miembros de «Empleados / Encargado» (`hr.group_hr_user`), según la lista que apruebes | Semana del despliegue de la entrega 1 | F-004 (b) |
| 8b | Crear el usuario del dueño de C4 y los de oficina que apruebes (Asistente de MAST, Almacenista K…) | Cuando los apruebes | H-001, H-002 |
| 11 | Cuentas 92 «Supervisor» y 80 «manufactura@» (D-08). **No apagarlas todavía: mueven producción y almacén** (ver `decisiones.md`, «D-08: qué hacen hoy»). Primero usuarios o tabletas por área y un periodo en paralelo; después se archivan | Tabletas: con la entrega 8 · Archivar: 2 semanas después, con tu visto bueno | I-017, D-08 |
| 11b | Configurar las 4 tabletas por área (Tejido, Tintorería, Acabado e Inspección) con PIN | Con la entrega 8 | I-005, D-08 |
| 24 | En el shell, `sgi_drop_empty_studio_models(dry_run=True)` y, si da 0 registros, la corrida real | Con la entrega 2, tras D-11 | B-007 |
| 27 | Después de cada PR de `main` a `quimibond`: `odoo-update`, reinicios y las consultas del RUNBOOK y de la columna «Evidencia en main/quimibond» | En cada despliegue | todos |

### Areli (MAST)

| # | Qué | Fecha propuesta | IDs |
|---|---|---|---|
| 3 | Emitir P-I01 sin credenciales, o darlo de baja si C4 y C5 lo cubren | Antes de la carga de rutinas (entrega 6) | L-001 |
| 6 | Fecha de revalidación de Protección Civil (E2.21) | 9-oct-2026 | H-003 |
| 9 | Reasignar C4.25 y las 50 actividades que hoy no tienen a nadie con usuario; en planta ejecuta el supervisor | Antes de la entrega 8a (9-oct-2026) | H-001, I-010 |
| 13 | Cargar el plan de emergencia vigente y un recorrido de la Comisión de Seguridad e Higiene | Antes de la entrega 10 | I-019 |
| 14 | Pasar a «Manual», con justificación, las mediciones contra módulos sin uso, y corregir los entregables (H-008, H-009, H-011, H-012, H-019). La carga la prepara el programador, con modo de prueba | Con la entrega 3 | H-004 y otros |
| 15 | Asignar equipos a las 3 plantillas de checklist | Antes de la entrega 8a (9-oct-2026) | H-013 |
| 16 | Poner propietario a los 487 documentos vigentes que no lo tienen, con los dueños de proceso | **Antes** de la regla de documentos sin propietario (entrega 1c, ver abajo) | H-015, N-001 |
| 20 | **Decidir las 38 rutinas pendientes con cada dueño de proceso, a más tardar el 16-oct-2026**; aclarar las familias P-A05, A13, A23, A30, C10 y P07; corregir las 13 contradicciones de clase y estado que marque el modo de prueba | **16-oct-2026** | L-013, L-014, L-015 |

### RH (Miguel Medina)

| # | Qué | Fecha propuesta | IDs |
|---|---|---|---|
| 6b | Periodo del plan de capacitación para el vencimiento del SIRCE (S4.30) | 9-oct-2026 | H-003 |
| 5b | Revisar con Sistemas la depuración de «Empleados / Encargado»: quién necesita de verdad gestionar empleados y ver salarios | Semana del despliegue de la entrega 1 | F-004 (b) |

### Programador

| # | Qué | Cuándo | IDs |
|---|---|---|---|
| 22 | Etiqueta de git sobre `main` antes de retirar migraciones | Entrega 2, después del PR del CHANGELOG | A-026, K-018 |
| 23 | Revisar el log del cron mensual (lunes 5-oct-2026) en busca de la foto del valor del inventario | 5-oct-2026 | A-017, G-018 |

**Nota sobre la «entrega 1c»:** la consolidación dejó en la entrega 1 cuatro piezas más:
- los candados contra llamadas RPC (F-005, F-006, F-007, F-008 y F-015);
- las fichas estándar sin grupo (D-001);
- la regla de documentos sin propietario (N-001);
- `qb_mcp_politica`.

Jose fijó la entrega 1 como «#452 más I-014». Esas cuatro van como **entrega 1c**, justo después de la 1b, salvo que Jose diga otra cosa. La regla de documentos sin propietario necesita antes la acción 16 de Areli.

## De los 78 modelos que quedaron «Se queda» solo por defecto: los que conviene revisar

Los otros 68 son extensiones de modelos estándar (sale.order, stock.lot, project.task…), asistentes transitorios o mixins técnicos sin datos propios. Se quedan sin más.

| Modelo (archivo:línea) | Por qué revisarlo | Tipo |
|---|---|---|
| `hr.version` (`models/sgi_kpi_fields.py:272`) | Extiende el contrato del empleado (datos de nómina) para un indicador. Hay que ver qué campo agrega y quién lo lee | Datos sensibles |
| `sgi.staff.efficiency.line` (`models/sgi_staff_efficiency.py:173`) | Guarda salario diario, mensual e importe. Lo cubre F-004 (b) mientras sigan abiertos a «Encargado de empleados» | Datos sensibles |
| `account.move.line` (`models/sgi_kpi_fields.py:122`) | Campos para indicadores sobre apuntes contables. Hay que confirmar que no exponen montos en vistas del SGI a quien no ve Contabilidad | Datos sensibles |
| `sgi.competence.gap` (`models/sgi_competence.py:30`) | Brechas de competencia por persona (evaluación). Verificar que el Usuario SGI no vea las de otros | Datos sensibles |
| `sgi.csh.finding` (`models/sgi_hse_records.py:175`) | Hallazgos de la Comisión de Seguridad e Higiene; pueden nombrar personas | Datos sensibles |
| `sgi.audit` / `sgi.audit.finding` / `sgi.audit.checklist.line` (`models/sgi_audit.py`, `models/sgi_norm_compliance.py`) | 0 auditorías en producción (D-011). Con D-05 (Areli y Oscar) empiezan a usarse: revisar que el checklist desde los requisitos funcione antes de la primera auditoría | Duda real |
| `sgi.legal.evaluation` (`models/sgi_legal.py:230`) | 25 requisitos legales nunca evaluados (H): ¿el modelo sirve como está? | Duda real |
| `sgi.machine.sheet.yarn` (`models/sgi_machine_sheet.py:111`) | Único modelo propio sin ninguna prueba (J) | Duda real |
| `ir.ui.view` / `ir.actions.act_window.view` (`models/sgi_diagram_view.py:21`, `:48`) | Registran el tipo de vista `sgi_diagram` en modelos técnicos de Odoo. Se quedan, pero cualquier cambio aquí puede romper el build | Duda real (técnica) |
