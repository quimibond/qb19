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
