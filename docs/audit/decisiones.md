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

## 2026-09-29 — aclaraciones durante la tanda 3

- **Aprobaciones de Sandra (E-003):** son `mail.activity` «Conceder aprobación» de la regla de Studio archivada 36, sobre las cotizaciones en borrador PV15254 y PV15323 (**empresa 4, BDC BOSQUES**, no la 1). **Las resuelve Jose por fuera; el programador no las toca.**
- **Entrega 1b:**
  - Mis pendientes no muestra avisos de reglas de aprobación archivadas.
  - Cuando se archiva una regla, se propone qué pasa con sus avisos abiertos.
  - Nota del programador: Mis pendientes tampoco filtra por empresa; esas dos son de la empresa 4. Va en el mismo PR.
- **OdooBot:** **no se le cambia la zona horaria**, porque con ese usuario corren todos los procesos automáticos de Odoo. El cron de checklists calcula las 05:30 en `America/Mexico_City` dentro del código. Cualquier otro proceso del SGI que dependa de la zona de OdooBot se lista en G (G-007) y se corrige igual, en código.
- **Excel de rutinas:** la hoja «Instrucciones» ya dice 49 procedimientos; los datos no cambian.

## 2026-09-29 — respuestas al agente K (documentación)

1. **Orden obligatorio:** primero se reconstruye el CHANGELOG y después se sacan las migraciones (A-026). Van en **PRs separados, en ese orden** (K-018).
2. **Si la documentación contradice `decisiones.md`, manda `decisiones.md`.** El README del módulo se reduce a qué es, cómo se instala y dónde está la documentación. El historial va al CHANGELOG.
3. **Manuales de usuario:**
   - Uno por rol: operador o supervisor, jefe de área, MAST, Dirección y auditor.
   - En español llano, con las rutas de menú del árbol final (`05-menus/arbol_final.md`).
   - Sin claves del Dropbox, salvo en la sección de transición.
   - **Se escriben después de aplicar los menús nuevos**, no antes, para no documentar rutas que van a cambiar.
4. **Aprobada la estructura `docs/sgi/`** (11-documentacion §6). La parte técnica se genera por script y se revisa en CI (`--check`).

## 2026-09-29 — P-I01 y respuestas al agente L

### P-I01 (L-001)
- **Acceso cerrado por Jose.** Verificado por MCP (lectura, `documents.document` 3560): «Permisos para usuarios internos» y acceso por enlace están en «Ninguno», con un solo acceso explícito. Además lo pueden ver los administradores de Documentos.
- **Pendiente de Sistemas:** cambiar las contraseñas de las cuentas que aparecen en P-I01. Cerrar el acceso no borra lo que alguien ya pudo haber visto.
- **Su familia** (7 DAT, 3 formatos, 6 instructivos): Jose buscó en el texto indexado de los 16 las palabras contraseña, password, login y «usuario:», y los dominios de Quimibond y de Odoo. No salió nada. **Siguen abiertos** para no parar a Inspección.
- **Pedido al programador:** una regla para que en adelante un documento controlado sin propietario no pueda quedar abierto a todos los internos sin revisión.

### Respuestas a L
1. **P-I01** queda «En curso» hasta que salga su versión limpia. MAST emite un P-I01 nuevo sin credenciales o, si C4 y C5 lo cubren, se da de baja. Sus rutinas las analiza Jose sin copiar el contenido sensible.
2. **Familias sin procedimiento** (P-A05, A13, A23, A30, C10 y P07): se le pregunta a Areli. Mientras, clase «Por definir».
3. **Instructivos, DAT, anexos, protocolos y reglamentos:** por default, clase D y «No aplica (se queda)». Los que ya pasaron a actividades de Odoo se marcan uno por uno.
4. **Las 38 rutinas pendientes:** las decide Areli con el dueño de cada proceso, **a más tardar el 16 de octubre de 2026**. El importador las acepta sin decisión, pero el tablero las muestra en rojo.
5. **Estado «eliminada»:** se conserva, para las rutinas que se dejan de hacer a propósito.
6. **Revisión y comentario:** se capturan en Odoo, en `sgi.legacy.routine`, para que la evidencia de revisión quede dentro del SGI.
7. **Documento 5119** (P-A28 obsoleto, sin archivo): se archiva.
8. **Las 13 contradicciones entre clase y estado:** el importador las detecta en modo de prueba y MAST las corrige antes de la carga real.

**Aprobados:** el diseño de L (`12-transicion.md`), su corrección a C-001 (L-003: no se rellena nada desde el lado del proceso) y la migración de la clave vieja sobre **490 documentos** (L-004).

## 2026-09-29 — Salud ocupacional y flujo de ramas

1. **Grupo «Salud ocupacional (SGI)»:** entra **solo Miguel Medina** (usuario 88, Coordinador de RH). Nadie más: ni Areli ni Dirección.
   - Se asigna en producción **después** de desplegar la entrega 1, que es la que crea el grupo. Va en «Acciones en producción».
   - Con esto se resuelve I-013 (b), no I-014 (corrección de la consolidación): el usuario 88 es `quimibond_sgi.rh_user_id`, así que los avisos de exámenes le llegan a alguien que sí puede abrirlos.
2. **Flujo de ramas (corregido por Jose el mismo día; no hay rama `staging`):** rama de desarrollo de cada entrega (p. ej. `claude/sgi-entrega-1`) → PR a **`main`** → PR de `main` a **`quimibond`** (producción).
   - La instalación limpia y las pruebas del SGI corren en el **build de desarrollo de la rama de la entrega**, antes del PR a `main`. Para la entrega 1: `test_cleanup_45`, `test_update_respects_mast` y `test_security_entrega1`.
   - `main` hace de paso previo a producción: ahí se prueba la actualización sobre la copia de producción. Ver `docs/RUNBOOK_DESPLIEGUE.md`.
   - Ninguna entrega se integra a `main` ni pasa a `quimibond` sin el visto bueno de Jose.
   - quimibond/qb19#452 se queda con destino `main`.

## 2026-09-29 — Aprobación del plan (fase 2 → fase 3)

**Plan aprobado.** Flujo confirmado: rama de desarrollo → `main` (paso previo a producción) → `quimibond` (producción).

### Orden de las entregas
1. **Entrega 1**: el PR quimibond/qb19#452 más I-014.
2. **Entrega 1b**.
3. **Entrega 4** (menús y grupos), en el mismo release que los permisos de Dirección (F-013, E-009).
4. **Entrega 8a** (nueva), con lo que ve el usuario:
   - I-001: Mis pendientes como bandeja única;
   - G-001: mediciones que nacen atrasadas;
   - G-004, G-005 y G-006: avisos duplicados y calibración;
   - I-002 e I-003: avisos de MAST que llegan a otras personas.
5. Después: **3 → 6 → 2 → 5 → resto de 8 → 9 → 10**.

Dentro de cada entrega se respetan los órdenes ya fijados: el CHANGELOG antes de retirar migraciones, y el pie de formato ligado al documento (C-006) antes de renombrar claves.

### Decisiones
- **D-03:** el SGI es **solo de la empresa 1**, y se declara en el código.
- **D-01:** como recomendó la consolidación. Los términos con IDs de producción, los objetivos y los planes de control van a `quimibond_sgi_mapa`. Los indicadores automáticos sin IDs fijos se quedan como catálogo del SGI.
- **D-02:** clave nueva `F-{proceso}-{nn}`, `IT-{proceso}-{nn}` y `DA-{proceso}-{nn}` (procedimientos `PR-{proceso}`), **sin renombrar archivos**.
- **D-04:** la bandeja oficial es **Mis pendientes, con todo adentro**: actividades atrasadas, firmas, acuses de lectura y aprobaciones.
- **D-05:** **Areli audita todo menos E2**. E2 lo audita **Oscar González** o un auditor externo. El grupo Auditor SGI queda para ellos dos.
- **D-08:** **una tableta por área** (Tejido, Tintorería, Acabado e Inspección) y **PIN obligatorio para firmar**. Se eliminan las cuentas «Supervisor» y «manufactura@», pero antes el programador informa qué hace hoy cada una.
- **D-32:** I-014 sigue abierto y va en la entrega 1. **Hecho** en la rama `claude/sgi-entrega-1` (commit 882af9c).

### D-08: qué hacen hoy las dos cuentas (MCP, solo lectura, desde el 1-jul-2026)
| Cuenta | Nombre en Odoo | Último acceso (UTC) | Actividad desde el 1-jul (registros creados por la cuenta) |
|---|---|---|---|
| 92 `supervisor@quimibond.com` | «Supervisor», sin empleado | 2026-09-28 16:51 | 653 órdenes de producción, 2,706 movimientos de inventario. Mensajes en 3,094 OP, 1,330 facturas o asientos, 1,023 albaranes, 684 lotes, más algunos en productos, firmas, aprobaciones y documentos |
| 80 `manufactura@quimibond.com` | «Guadalupe Ramos», sin empleado ligado | 2026-09-29 01:32 | 95 órdenes de producción, 958 movimientos de inventario. Mensajes en 512 OP, 377 albaranes, 172 facturas o asientos, 100 solicitudes de aprobación, 96 solicitudes de firma, 94 lotes y 9 documentos |

**Las dos son cuentas de operación diaria.** Apagarlas sin reemplazo detiene producción y almacén. Propuesta:
1. Averiguar quién usa hoy «Supervisor»: es una cuenta compartida, sin empleado.
2. Darle a cada persona su usuario, o la tableta de su área con PIN.
3. Pasar por un periodo en paralelo.
4. **Archivar** las cuentas (no borrarlas), para no perder el `create_uid` de miles de registros.

«manufactura@» ya tiene nombre de persona (Guadalupe Ramos): se liga a su empleado y se le cambia el login a uno personal. Así se conserva su historial. Lo decide Jose y lo ejecuta Sistemas.

## 2026-09-29 — Entrega 1c, reglas archivadas y cuentas compartidas

1. **Entrega 1c, justo después de la 1b:** los candados contra llamadas RPC (F-005, F-006, F-007, F-008, F-015), las fichas estándar sin grupo (D-001), `qb_mcp_politica` y la regla de documentos sin propietario (N-001).
   - Para que la regla no dependa de Areli, **una migración pone a Areli (usuario 128) como propietaria** de los documentos controlados que no tienen. **Solo después se enciende la regla.** Ella reasigna con el tiempo.
2. **Avisos de reglas de aprobación archivadas: opción A.** Al archivar una regla, sus avisos abiertos se marcan como hechos con la nota «Regla archivada el … por …», sin aprobar ni rechazar nada. **Va en un PR aparte** (entrega 1d).
3. **Cuentas compartidas: aprobada la propuesta.** Nada se borra.
   - «manufactura@» (80) se liga a Guadalupe Ramos y se le pone un login personal.
   - «Supervisor» (92) se archiva después de un periodo en paralelo, cuando cada quien tenga su usuario o una tableta con PIN.
   - Jose averigua quién usa hoy «Supervisor».
4. **Los 10 modelos a revisar** (`99-consolidado/acciones_y_revision.md`):
   - Los 5 sensibles quedan restringidos a su grupo (RH, Contabilidad, Salud ocupacional o Comisión, según el caso), dentro de la 1c o la 4: `hr.version` (extensión del SGI), `sgi.staff.efficiency.line`, `account.move.line` (campos del SGI), `sgi.competence.gap` y `sgi.csh.finding`.
   - Los otros 5 se quedan: auditorías, evaluaciones legales, fichas de hilo y vista de diagrama.
5. **Evidencia de Odoo.sh:** Jose pasa las capturas de los builds de las ramas de las entregas 1 y 1b. Mientras, se sigue con la 1c.

## 2026-09-29 — Lotes de entregas

- Para ahorrar builds, las entregas se juntan en **lotes**. El lote 1 es la rama de integración `claude/sgi-lote-1`, con las entregas 1, 1b, 1d y 1c en ese orden, un solo PR a `main` (quimibond/qb19#458) y **un solo build de Odoo.sh**.
- Los PRs de cada entrega (#452, #455, #456, #457) se quedan abiertos como referencia y se cierran cuando entre el lote.
- Si el build falla, el programador dice qué entrega lo rompió y lo corrige **en la rama del lote**.
- La entrega 4 va en el siguiente lote.

## 2026-09-29 — Decisiones de la entrega 4

- **D-17, D-18, D-19 y D-20:** confirmadas como las propuso la consolidación.
  - D-17: «Acciones correctivas» muestra todas las acciones, con filtro por origen.
  - D-18: Diagnóstico queda como carpeta con 5 submenús y el Auditor la lee.
  - D-19: el tablero «Salud del SGI» se archiva a mano si está vacío.
  - D-20: sin «(SGI)» en los nombres, y «Paretos de calidad».
- **D-06:** confirmada.
  - Todos reportan. Solo SST, MAST y Salud ocupacional investigan, cierran y reabren.
  - El reportante edita mientras el incidente siga «reportado».
  - **Agregado:** el reportante puede **consultar cómo se cerró su incidente**, la causa y las acciones, sin editarlas. La ISO 45001 pide que el trabajador participe y reciba respuesta.
- **D-21:** confirmada: «Documentos vigentes» para el Usuario SGI, con título y revisión. **Agregado:** desde ahí se da el acuse de lectura.
- **Corrección a los modelos sensibles:** los salarios de eficiencias quedan **solo para RH y Nóminas** (Coordinador de RH y Responsable de Nóminas), **no para el Jefe MAST**. Captura de eficiencias y los supervisores ven la eficiencia, sin el importe.
- La entrega 4 sigue sobre el lote 1 (rama `claude/sgi-entrega-4`).

## 2026-09-29 — Lote 1 integrado a `main`

- Por instrucción de Jose («push a main»), el lote 1 (quimibond/qb19#458: entregas 1, 1b, 1d y 1c) **entró a `main`** con un merge commit (`d11e35a`), **sin esperar el build de su rama**. Estado al integrar: CI de GitHub en verde y mergeable.
- **La evidencia pasa al build de `main` en Odoo.sh:**
  - instalación y actualización sin errores;
  - suite del SGI y de `qb_mcp_politica`;
  - las consultas de la sección «Upgrade» del PR.

  Si algo falla, se corrige en una rama nueva hacia `main`.
- Se cerraron, con nota, los PRs de referencia #452, #455, #456 y #457.
- **Pendiente para pasar a producción** (PR de `main` a `quimibond`, con visto bueno de Jose):
  - antes: aplicar la opción A del MCP en Ajustes;
  - después: instalar `qb_mcp_politica`, meter a Miguel Medina (88) en Salud ocupacional y depurar a «Empleados / Encargado».

## 2026-09-29 — Lotes 2 y 3 integrados a `main`

- **Lote 1:** build de `main` en verde (Jose).
- **Lote 2** (quimibond/qb19#463, `4ff9643`, `quimibond_sgi` 56.34.0): entregas 4, 3-integridad y 3-documentos. Build de `main` **naranja solo por dos avisos esperados**:
  - 56.31.0 listó 26 procedimientos solo del lado del proceso. La copia del build es anterior a la escritura de los 23 del lado del documento (hoy, 01:11 UTC), así que en producción deben ser 13. **Si en producción dice 26, detenerse.**
  - 56.30.0: F-P-A28-13 no tiene documento.
- **Lote 3** (quimibond/qb19#464, `65a5284`, `quimibond_sgi` 56.35.0 + `quimibond_sgi_mapa` 1.0.0): `export_payload` y el mapa. Integrado por instrucción de Jose («Push»). La evidencia es el build de `main`.
- **Ajustes de Jose a la entrega 4:**
  - **Jorge Ortiz (35, Dirección):** pierde solo lo de Jefe MAST. Conserva Administrador de Proyecto, Aprobaciones y Soporte, asignados directo por la migración 56.29.0 (`sgi.config._sgi_director_keep_admin_groups`), salvo que Jose diga otra cosa después de hablar con él.
  - **Salarios de eficiencias:** grupo propio `group_sgi_salary`, ya no el de Nómina. Arranca solo con Lorena (23), Miguel (88) y Jose (7).
    - El 152 (Mariano Domínguez, `sistemas@`) no entra hasta que Jose confirme.
    - El 12 es «Jose Mizrahi Daniel» (`jose023md@gmail.com`). Está en Nómina / Encargado; queda a decisión de Jose.
  - **Auditor:** lee los hallazgos de la Comisión, sin escribir, para poder auditar la ISO 45001.
- **Datos que hace Jose antes de pasar a producción:** ligar F-P-A28-13, el 5556 y la limpieza de C-008 (5556, 3359, 4995, 4022, 3760, 3644, 5119 y 3930).
- **Borrado de `restrict` en documentos de evidencia y en formatos ligados:** con `restrict`, un documento de esos en la papelera haría fallar todos los días la limpieza automática de Documentos. Quedan con su `ondelete` por default y `set null`.

## 2026-09-29 — Lote 4 integrado a `main`

- Lote 3 con build verde (Jose).
- Lote 4 (quimibond/qb19#465, `20dbec7`, `quimibond_sgi` 56.38.0): la entrega 8a, con Mis pendientes con todo adentro, los avisos de crons sin duplicar y la calibración sin bloqueo. Entró por instrucción de Jose («Push a main»). La evidencia es el build de `main`.
- Durante la revisión se corrigió que un jefe, desde Mi equipo, recibiera la liga de firma con token de su gente (`535ea9c`).
- **Siguen abiertos para Jose:**
  - si se liberan los 144 equipos en «No usar» (143 los puso el cron el 25-sep; el 743 lo bloqueó Jose al crearlo);
  - los plazos de Mis pendientes: acuse, 5 días hábiles; captura, 3;
  - si el aprobador ve como atrasado lo que el ejecutor no ha hecho;
  - corregir a mano el escalamiento de E2.31 y E2.32 (Dirección no puede leer salud);
  - avisar a Elena (16) de sus 174 firmas antes de desplegar a producción.

## 2026-09-29 — Decisiones de Jose antes de pasar los 4 lotes a `quimibond`

- **Equipos en «No usar»:** se liberan los 144. Va en la migración 56.38.1, con respaldo, en la rama `claude/sgi-8a-ajustes`.
- **Plazos de Mis pendientes:** 5 días hábiles para capturar una medición y 3 para validarla. El acuse de lectura se queda en 5.
- **Aprobador:** solo ve lo que ya le toca. El atraso del ejecutor le llega al rol que escala.
- **`quimibond_sgi_mapa`:** no se instala en producción, solo en staging y en una recuperación. Instalarlo no crea registros; su manifest solo trae ACL y vista.
- **Entrega 6:** se construye en su rama. No se hace merge hasta que producción esté estable con los 4 lotes.
- **C-008 y E2.31/E2.32:** Jose los corrige a mano, por MCP o en pantalla, con la lista que se le entregó.

## 2026-09-29 — C-008 hecho y listo para producción

- **C-008, hecho por Jose por MCP** (verificado):
  - 3359, 4995 y 5119 archivados.
  - 4022 con clave `F-P-P01-02`.
  - 3760 como formato.
  - 3644 sigue como formato, porque la validación no deja tipo instructivo con clave `F-P-*`. Después de la 56.32.0 se le asigna `IT-C4-nn` y en el mismo paso se le cambia el tipo.
- **Areli, en pantalla:**
  - subir el 5556 como versión nueva del 3518 y archivarlo;
  - ligar el F-P-A28-13.

  Ninguno de los dos bloquea el despliegue.
- **E2.31 y E2.32:** sin cambios. El escalamiento a Dirección abre la actividad, no los datos de salud.
- `main` quedó sincronizado con `quimibond` (quimibond/qb19#467). El PR de producción es quimibond/qb19#468.
- Entrega 6 en quimibond/qb19#469, sin merge hasta que producción esté estable.

## 2026-09-29 — Producción en 56.38.2 y entrega 6 aprobada

- **Producción** (verificado por MCP):
  - `quimibond_sgi` 19.0.56.38.2; `qb_mcp_politica` instalado; `quimibond_sgi_mapa` sin instalar.
  - 0 equipos en «No usar».
  - Avisos: Ana Silvia (67) en 0, Jose (7) en 2, Areli (128) en 37.
  - 487 documentos activos con clave anterior.
- **El primer update de producción falló**: la clave heredada no pasaba la validación de `sgi.format.map` porque en producción el tipo «formato» no exigía clave. Se corrigió con el hotfix 56.38.2 (quimibond/qb19#470) y se sincronizó a `main` (#471). En el intento fallido la 56.31.0 reportó los 13 procedimientos esperados.
- **Entrega 6 aprobada por Jose** (quimibond/qb19#469, integrada a `main`, `d5e0b61`). Cuando el build de `main` salga verde, pasa a `quimibond`.
  - Mariano (152) entra al SGI solo como dueño de S6, por el grupo «Dueño de proceso (SGI)». No entra a Nómina ni a Salud ocupacional.
  - Las 38 pendientes quedan en rojo en «Avance de la transición», con fecha límite del 16-oct-2026 (parámetro `quimibond_sgi.legacy_decision_deadline`).
  - **Después del despliegue:**
    - Areli corrige las 13 contradicciones de clase y estado.
    - Areli importa las rutinas: Probar → confirmar → Cargar. Deben ser 815; si no cuadra, no carga.
- **`validar_rutinas.py` sobre el libro de Jose (2026-09-29):**
  - 815 filas: 709 cubiertas, 68 reemplazadas, 38 pendientes.
  - 49 procedimientos.
  - 0 errores y 1 aviso: las 38 pendientes no tienen decisión.

## 2026-09-29 — Nuevo orden (red antes de la limpieza) y decisiones de la entrega 2

- **Orden:**
  - Primero, lo mínimo de la entrega 9: ningún PR se integra a `main` ni a `quimibond` sin el build de desarrollo de Odoo.sh en verde, como check obligatorio en GitHub.
  - D-23 (llave de `odoo/enterprise`) solo si eso no alcanza.
  - Después, la entrega 2, empezando por el CHANGELOG, la etiqueta `sgi-antes-de-limpieza`, el retiro de las migraciones viejas y los procesos viejos.
- **Hallazgo al preparar el check:** Odoo.sh no publica ningún status ni check en GitHub. En quimibond/qb19#472 (cabeza `main`), los únicos checks son `check` y `odoo-tests`, los de GitHub Actions, que no instalan el SGI. Con lo que hay hoy no se puede exigir su build desde las reglas de la rama. Ver las opciones que se le dieron a Jose.
- **Decisiones de la entrega 2:**
  - **D-10:** según la recomendación: la aprobación de Studio pasa a un satélite `auto_install` y sale `web_studio` del núcleo.
  - **D-11:** solo se borran los modelos `x_*` con 0 registros. Los que tienen datos se le enseñan a Jose antes, exportados.
  - **D-12:** el recálculo de mediciones va en el cron, más un botón solo para el administrador.
  - **D-13:** se activa `acuerdos_rxd` para E1-02; deja de ser manual.
  - **D-14:** un correo semanal por persona con sus atrasos de Mis pendientes, que cada quien pueda apagar.
  - **D-15:** archivar las categorías sin uso.
  - **D-16:** primero confirmar si CA-02 usa la encuesta 151. Si la usa, se queda.
  - **D-30:** sí al historial completo.
- **Otras decisiones:**
  - **D-22:** sí, el MCP archiva en vez de borrar en `sgi.*`.
  - **D-24 y D-25:** sí.
  - **D-26 y D-27:** manuales en Conocimiento, ligados desde el menú; carpeta SGI en Documentos.
  - **D-29:** el mismo tratamiento que usa el español de Odoo.
  - **D-31:** licencia OPL-1.
- **Pendientes:**
  - **D-28** (normas certificadas y fechas): la pasa Jose.
  - **D-09** (quién captura eficiencias): recomendación, el supervisor de cada turno; falta que Jose la confirme.

## 2026-09-29 — D-28, roles relativos y buscador de producción

- **D-28, normas certificadas.** Organismo: SIDE Certificaciones, acreditado ante la ema.
  - **ISO 9001:2015:** certificada desde el 24-ago-2021. Certificado vigente 24070237A, del 23-ago-2024 al 22-ago-2027.
  - **ISO 14001:2015:** certificada desde el 3-jul-2023. El certificado conocido vencía el 2-jul-2026; Jose confirma con MAST el vigente.
  - **ISO 45001:2018:** auditoría de certificación del 22 al 24-sep-2026, resultado pendiente. Se trata como «en certificación».
  - **IATF 16949:** no certificada. Los requisitos de clientes automotrices van en «Requisitos específicos de clientes».
- **Aprobadores:**
  - S4.03 (rol 1465) y S6.07 (rol 1577) quedaron en «Jefe del área que pide», cargados por Jose.
  - E2.01 no se toca.
- **«Dueño del proceso» se resuelve con el proceso de la actividad**, no con el del registro que se aprueba (`sgi_catalog.py:239`, `sgi_approval_native.py:174`). Los demás relativos no se resuelven a nadie.
- **Aprobado:** el arreglo de roles relativos es el primer bloque del resto de la entrega 8, después de la red de pruebas.
  - Con el arreglo en producción, Jose carga E2.01 y S6.07 como «Dueño del proceso».
  - La validación aprobador ≠ ejecutor revisa todo el catálogo y entrega la lista de casos.
- **Hotfix 57.0.1** (quimibond/qb19#473): el buscador «Del Dropbox a Odoo» tronaba en producción porque `documents.document.name` es jsonb. El build de `main` no lo atrapó porque es staging y no corre las pruebas.

## 2026-09-29 — Un solo paso a producción

- Jose: el arreglo del buscador (57.0.1, quimibond/qb19#473) **no** sale solo. Se junta con la entrega 2, el diagnóstico de indicadores y lo que se haga mientras tanto, y va a `quimibond` en **un solo paso**.
- Hasta entonces, en producción el Buscador de «Del Dropbox a Odoo» sigue fallando. El resto de la entrega 6 funciona: «Formatos y documentos anteriores», rutinas, avance e importación.
- quimibond/qb19#473 se redirigió a `main` para entrar en el paquete.

## 2026-09-29 — Lote 5 en `main` y acompañantes de Studio

- Jose ordenó integrar a `main` sin build de desarrollo previo: quimibond/qb19#474 (57.0.1–57.5.0) y quimibond/qb19#475 (57.6.0–57.8.0, `qb_mcp_politica` 1.1.0). Build de `main` verde en los dos (staging: no corre las pruebas del SGI).
- Etiqueta `sgi-antes-de-limpieza` creada por Jose sobre `d5e0b61`.
- **D-11 ampliada:** los acompañantes de Studio también se van: las 3 tablas `_stage` (3 registros cada una, con respaldo CSV antes), `x_no_conformidades_tag`, `x_no_conformidades_line_0ff2d` y el menú 1643. Se borran con el método manual de D-11, junto con su modelo padre.
- **Mapa:** se vuelve a exportar `quimibond_sgi_mapa` con E1-02 en `acuerdos_rxd` y sin reactivar lo archivado (OP-PTAR).
- `quimibond` sigue sin cambios hasta el paso único, al terminar la entrega 2.

## 2026-09-30 — Inventario de formularios

- **Bloque 1 (seguridad, salud y ambiente) primero.** Programación (~20 h): ligar las 13 actividades críticas (E2.23, E2.28, E2.30, E2.34, E2.35, E2.37, S5.14, S4.34 y demás del inventario), digitalizar la matriz de aspectos ambientales, el permiso de trabajo de alto riesgo y LOTO, ficha y menú de hallazgos de auditoría y de evaluación legal. La carga del histórico 2026 y la capacitación de MAST las agenda Jose con Areli cuando Carlos tenga usuario.
- **Clave D-02:** con script, al final del bloque 3. Orden: (1) fusionar duplicados; (2) corregir F-P-E01-01 (luminaria contra aspectos ambientales) y el formato con dos revisiones 0; (3) aplicar la clave, con la del Dropbox como clave anterior. El buscador por clave anterior debe seguir funcionando.
- **Responsable SGI de cada formato:** el dueño del proceso. Si el dueño no tiene usuario activo, se queda MAST y se entrega la lista a Jose. MAST conserva la aprobación y la publicación.
- **17 formatos citados que no existen:** para cada uno, propuesta «lo sustituye Odoo (dónde)» o «hay que crearlo». Varios ya existen en Odoo (encuesta de satisfacción, revisión por la dirección, conciliación bancaria).
- **Un formato por modelo:** pasa al bloque 2, junto con la clave por tipo de operación.
- **D-009 (vistas):** aprobar/rechazar PPAP, cerrar/reabrir riesgos y marcar obsoletos AMEF, planes de control y planes de emergencia: solo Jefe MAST y dueño del proceso (propuesta aplicada al no haber objeción).
