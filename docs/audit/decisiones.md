# Decisiones de Jose durante la auditoría

Registro fechado de lo que Jose decidió. Manda sobre cualquier propuesta de los informes `NN-*.md`.

## 2026-09-29 — respuestas a la tanda 1

### Prioridades
1. **MCP en solo lectura para modelos técnicos (F-001): aprobado.**
   - Antes de tocar nada, respaldo en CSV de la configuración.
   - Mantienen escritura: los modelos `sgi.*` y los de negocio que usa la carga (documentos, aprobaciones, flota, mantenimiento…).
   - Pasan a solo lectura: menús, crons, grupos, módulos, vistas y acciones.
   - Después, un módulo que imponga la política desde el código.
   - Los parámetros del sistema (`ir.config_parameter`) se **desactivan** en el MCP, no quedan en solo lectura.
   - Si la llave de Supabase salió al chat, se rota. Verificado el 2026-09-29: ninguna transcripción de los agentes de esta sesión contiene un valor con formato de llave de Supabase (JWT `eyJhbGci…` o `sb_secret_…`).
2. **Procesos viejos (A-002/A-003): aprobado como lo propone A.** Los procesos archivados no se borran nunca.
3. **Procedimiento ↔ proceso (C-001/C-002/C-014):**
   - El documento es la única fuente de verdad (`sgi_replaced_by_process_id`) y el borrado es `restrict`.
   - **No se rellena nada desde el lado del proceso.** Los 23 que hoy tiene el documento son la decisión.
   - Los 13 que solo están del lado del proceso (P-A01, P-A04, P-A24, P-A26, P-A28, P-C01, P-C02, P-C04, P-C13, P-C16, P-C17, P-P01, P-I01) se quedan **vacíos a propósito**: les faltan rutinas por cerrar y Jose los carga conforme se cierren.
   - P-A17, P-A18, P-A19, P-A20 y P-S03 **no se sustituyen**: siguen vigentes como control operacional.
   - La migración respalda el lado del proceso y lo recalcula como inverso del documento.
4. **Clave anterior (C-004/C-005): aprobado, pero C-006 va primero.** Primero se ligan al documento el pie de formato controlado y las 8 claves escritas en código; después corre la migración `sgi_code` → `sgi_previous_code`.

### Estado de migración de los procedimientos (C-003)
Ninguno queda como «migrado».
- Los 23 sustituidos pasan a «En curso», y a «Baja tramitada» cuando su proceso entre en vigor.
- Los 21 con pendientes pasan a «En curso».
- Los 5 de control operacional pasan a «No aplica (se queda)».
- **P-I01 se retira aparte porque contiene credenciales.**

### Preguntas de la tanda 1
1. Parámetros del sistema en el MCP: se desactivan (ver arriba).
2. Salud ocupacional: sí al grupo nuevo, y se depura a los 20 «Encargados de empleados».
3. Auditor SGI: ve todo el SGI menos salud y salarios. Dirección de Operaciones: consulta y aprueba, sin permisos de Jefe MAST.
4. Migraciones: no hay otra base además de producción y sus copias. Se sacan del módulo, **con una etiqueta de git antes**.
5. Lo que sale del SGI va a módulos propios, **sin borrar datos**:
   - Presupuesto de ventas: módulo aparte.
   - Bitácora de bloqueo contable y valor del inventario: a contabilidad. Si nada los alimenta, proponer quitarlos.
   - PPAP, AMEF y MSA: al satélite automotriz. Antes, revisar qué entregables y actividades apuntan a esos modelos; **PPAP se usa para CLI-01**.
   - `knowledge` no se quita si Mi procedimiento o los documentos usan artículos. Las demás dependencias solo se quitan si no tienen registros.
6. Instalación limpia: **el SGI se instala vacío.** El mapa de 14 procesos, etapas y actividades se exporta de producción a un módulo de datos aparte, para staging y recuperación.
7. Clave nueva:
   - Patrón `PR-{proceso}` (`P-C1` se confunde con el viejo `P-C01`).
   - Se vuelve a encender la validación de clave.
   - Los formatos que ya pasaron a Odoo conservan su clave vieja y quedan obsoletos.
8. Limpieza de documentos (5556, 3359, 4995, 4022, 3760, 3644): aprobada, **siempre archivando, nunca borrando**. Antes de archivar 5556 se compara con 3518: si es una revisión más nueva de P-A14, se sube como nueva versión de 3518 y después se archiva.

### Entrega 1
A-001 (instalación limpia), B-004 (prueba del árbol de menús), B-001 (ninguna siembra regresa a automático lo que MAST dejó en manual), F-003 (Sign) y F-004 (salud y salarios).

### Para la tanda 2
- «Cumple con» cargado (225 actividades); las 85 sin cláusula son a propósito; los 24 requisitos sin actividad los atiende Jose. **No son hallazgos.**
- El agente E incluye en su árbol final el menú de «Del Dropbox a Odoo» (el antes y el después).

## Aplicación de F-001 (MCP)

- **Quién:** Jose (o Sistemas) desde Ajustes → Técnico → MCP → *MCP Available Models*. El MCP no expone los modelos `mcp.*`, así que ni Claude ni ningún agente puede aplicar este cambio: es a propósito.
- **Paso a paso:** `06-seguridad.md` §5, opción A, con un ajuste por la decisión de Jose: `ir.config_parameter` pasa de «solo lectura» a **desactivado**. La lista final está en `06-seguridad/mcp_propuesta.csv`: 30 modelos en solo lectura y 9 desactivados. Los `ir.actions.*` ya no están expuestos, e `ir.ui.view`, `ir.model.access`, `ir.rule` e `ir.model.data` ya estaban en solo lectura.
- **Siguen con escritura:** `sgi.*` y los modelos de negocio de la carga (`documents.*`, `approval.*`, `fleet.*`, `maintenance.*`, `hr.*`, `quality.*`, …).
- **Respaldo:** export CSV de *MCP Available Models* antes de tocar nada (paso 1 de §5).
- **Después:** módulo `qb_mcp_politica` (opción B de §5) en un PR propio. El módulo `mcp_server` está en la raíz del repo y no se toca, porque es de un tercero.

## 2026-09-29 — respuestas a la tanda 2

### Vencimientos (H-003)
Jose cargó en producción 13 de los 15 vencimientos incompletos:

| Actividad | Vence |
|---|---|
| Presupuesto de ventas (E1.01) | 15 de noviembre |
| Presupuesto de gastos (E1.02) | 15 de diciembre |
| Plan estratégico (E1.03) | 31 de enero |
| Reporte a accionistas (E1.10) | 30 de abril, luego cada trimestre |
| Auditoría interna (E2.05) | 31 de marzo, luego cada trimestre |
| Aguinaldo (S4.22) | 16 de diciembre (el cuadro de antigüedades se firma antes; el límite legal es el 20) |
| PTU (S4.23) | 30 de mayo |
| Programa de capacitación (S4.24) | 31 de enero |
| Fijar la fecha del inventario general (S3.21) | 31 de mayo |
| Semestrales: inventario general, encuesta de clientes, matriz de habilidades, preventivo de cómputo | 30 de junio |

Faltan dos, que dependen de fechas externas: **E2.21** (revalidación de Protección Civil, según la fecha de su registro; la da Areli/MAST) y **S4.30** (SIRCE, según el periodo del plan de capacitación; lo confirma RH).

### Respuestas (Jose acepta las 14 recomendaciones, con estos matices)
1. **Usuarios:** oficina con usuario; en planta, el supervisor ejecuta. **Los usuarios los decide Jose y los crea Sistemas; el programador no crea ninguno, solo configura.** Jose pasa el nombre del dueño de C4.
2. **Calibración:** solo avisa, no bloquea, hasta que se carguen las fechas reales. Los avisos van al Coordinador de Laboratorio y al Jefe de Calidad.
3. **Mediciones automáticas:** las valida el dueño del indicador, en 3 días hábiles.
4. **Días inhábiles:**
   - Un vencimiento que cae en inhábil se **adelanta**.
   - Los escalamientos se cuentan en días hábiles.
   - Se cargan en el calendario los festivos de la LFT (art. 74) y los del contrato colectivo.
5. **Actividad hecha sin resolver la causa:** no se vuelve a crear mientras siga el mismo episodio.
6. **Mediciones contra módulos sin uso:** pasan a «Manual» con justificación.
7. **Aprobador igual al solicitante (E2.01, S4.03, S6.07):** se quita. El programador propone el aprobador correcto de cada una, Jose lo carga, y se agrega una validación para que no se repita.
8. **Numerales:** se congelan, con sus huecos.
9. **Helpdesk:** «Reclamaciones entretelas» y «ATENCION A CLIENTES» son del SGI. «Generar NC» solo en equipos del SGI.
10. **Carpeta Dirección:**
    - Abiertos para todos: Política, Objetivos, Riesgos y Requisitos legales.
    - El resto, solo Auditor, Jefe MAST y Dirección.
11. **Claves en Documentos:** el personal ve título y revisión. La clave vieja solo aparece en «Del Dropbox a Odoo».
12. **Aprobaciones huérfanas de Sandra:** se **cancelan** las 2 (no se aprueban).
13. **Carga del mapa:** manual, con modo de prueba primero. Aprobado el módulo `quimibond_sgi_mapa`.
14. **Checklists:** listos a las 05:30 hora de México. OdooBot con zona `America/Mexico_City`.

### Transición (para el agente L)
- **Son 49 procedimientos, no 47**, más **P-I01**, que va aparte por las credenciales.
- Análisis rutina por rutina: `Del_Dropbox_a_Odoo_rutina_por_rutina.xlsx`.
  - 815 rutinas: 709 cubiertas, 68 reemplazadas por Odoo, 38 pendientes.
  - Columnas: `clave, procedimiento, n, rutina, frecuencia, responsable_anterior, estado, actividades_odoo, motivo`.
  - P-I01 no está incluido.
- Clasificación de los 52 procedimientos: ver «Estado de migración (C-003)» arriba (23 sustituidos, 21 con pendientes, 5 de control operacional, P-I01 aparte).
