# SGI — Evaluación de estructura, menús y cierre de flujos

Evaluación contra: estructura de alto nivel de ISO 9001:2015 / 14001:2015 /
45001:2018 (cláusulas 4–10, ciclo PDCA), requisitos IATF citados por el propio
addon, y mejores prácticas de desarrollo de módulos Odoo. Verificada contra el
código de `quimibond_sgi` v19.0.14.1.0 (agosto 2026).

Escala de veredictos: ✅ correcto · ⚠️ mejorable · ❌ hueco real.

---

## 1. Menús: ¿están correctos y bien organizados?

**Veredicto general: ✅ la filosofía es correcta.** El menú está organizado por
*tarea del usuario* (persona → operación diaria → gestión → dirección →
configuración), no por cláusula ISO. Eso es la práctica correcta en Odoo: el
operador piensa "levantar una NC", no "cláusula 10.2". El mapeo a cláusulas
vive donde debe: en los procesos, las normas y los reportes.

Contra el ciclo PDCA, la cobertura es completa:

| PDCA | Dónde vive | Veredicto |
|---|---|---|
| Planear (4–6) | Panel, Procesos, Política, Objetivos, Riesgos, Presupuesto | ✅ |
| Hacer (7–8) | Documental, Automotriz, Metrología, apps nativas | ✅ |
| Verificar (9) | Medición, Auditorías, RxD | ✅ |
| Actuar (10) | Mejora continua (NC, acciones, mejoras) | ✅ |

**Hallazgos concretos (de mayor a menor impacto):**

1. ⚠️ **La cascada estratégica está partida en dos menús.** Política Integral
   vive en Panel, pero Objetivos Integrales vive en Medición. La cascada ISO
   (Política → Objetivos → Indicadores) es EL argumento del Panel; el usuario
   directivo debería recorrerla sin cambiar de menú.
   *Propuesta:* mover "Objetivos Integrales" a Panel, debajo de Política.
2. ⚠️ **"Auditorías y riesgos" mezcla dos mundos ISO.** Riesgos es
   planificación (6.1), auditorías es verificación (9.2), e Incidentes SST es
   un evento operativo (par de la NC, no de la auditoría).
   *Propuesta:* mover "SST → Incidentes" a Mejora continua (junto a NC y
   Reclamaciones — ahí vive el ciclo reactivo completo) y renombrar el grupo a
   "Riesgos y auditorías" con Riesgos primero (planear antes de verificar).
3. ⚠️ **Las acciones CAPA cuelgan del submenú equivocado.** "Todas las
   acciones" está bajo Mejora continua → No Conformidades, pero el modelo es
   transversal (NC + riesgos + AMEF + incidentes). Un auditor que busque "las
   acciones del sistema" no las encontrará bajo NC.
   *Propuesta:* subir "Acciones" a hijo directo de Mejora continua.
4. ⚠️ **El canal interno "Quejas y Sugerencias" no tiene menú SGI.** Existe
   (equipo Helpdesk con alias de correo, etapas y etiquetas SST) pero solo es
   accesible desde la app Helpdesk. La voz del trabajador (45001 5.4) merece
   entrada bajo Mejora continua.
5. ✅ Correcciones ya aplicadas en este PR: Procesos subió junto al Panel
   (general → particular).
6. ✅ Intencionales y correctos: "Hallazgos" sin menú propio (se llega por la
   auditoría), presupuesto espejado en Ventas, Configuración/RxD/Tableros solo
   para Jefe MAST.

**Menú objetivo propuesto** (cambios marcados con →):

```
SGI
├─ Panel                    (+ → Objetivos Integrales junto a Política)
├─ Procesos
├─ Mi trabajo
├─ Mejora continua          (NC · → Acciones · → Incidentes SST ·
│                            Reclamaciones · → Quejas y sugerencias · Mejoras)
├─ Documental
├─ Medición                 (Indicadores · Mediciones · Proveedores · DNC ·
│                            Presupuesto y pronóstico)
├─ Riesgos y auditorías     (→ Riesgos primero · Programa anual · Auditorías)
├─ Automotriz
├─ Revisión por la Dirección
└─ Configuración
```

---

## 2. ¿Los flujos cierran correctamente?

**Bucles que cierran completos (verificados en código):** ✅

1. **NC**: detección (8 vías) → causa raíz → CAPA → verificación de eficacia →
   lección aplicada → read-across a AMEF. Con escalamiento por inacción (5/3
   días) y de acciones vencidas (7/15 días). Es el bucle mejor cerrado del
   sistema.
2. **Incidente SST**: reporte → SCAT 3 capas → acciones → NC mayor automática
   → liga al riesgo IPER.
3. **Auditoría**: programa anual → aviso 15 días → auditoría → hallazgos →
   disposición obligatoria → NC → cierre bloqueado sin disposición.
4. **Documental**: solicitud (2 aprobadores) → versionado automático →
   obsolescencia de la revisión previa → acuses → NEWS mensual → revisión
   bienal vigilada → divergencia G14 del procedimiento vivo.
5. **KPI**: cron → captura/valor automático → validación restringida → NC en
   rojo → evidencia navegable.
6. **RxD**: 10 entradas automáticas → acuerdos → tareas → % de cierre visible
   en la siguiente RxD (el bucle se retroalimenta solo).
7. **Metrología**: calibración → bloqueo/desbloqueo → NC de impacto → candado
   duro en la inspección.
8. **Presupuesto**: captura → revisión → aprobación Dirección → cierre de mes
   → justificación de incumplimiento → revaluación S2 → nueva revisión.

**Bucles que NO cierran (el "Actuar" queda suelto):** ❌

1. **DNC / capacitación (7.2)**: la brecha se DETECTA (vista SQL) pero nada la
   TRATA: no hay plan de capacitación, la encuesta DNC F-P-A01-17 está
   huérfana (cero referencias en código) y el cron solo vigila certificaciones
   ya registradas. Falta: cron/botón que distribuya la encuesta + actividad a
   RH con brechas nuevas + (idealmente) vínculo a cursos.
2. **Satisfacción del cliente (9.1.2)**: la encuesta existe sembrada y NADA la
   envía ni la consume; no hay KPI de satisfacción. Es un requisito auditable
   directo de 9001.
3. **Política**: se publica sin difusión con firma propia (depende de ligar el
   MIID a mano y generar acuses desde el documento).
4. **Alta documental**: el cambio aprobado tipo "alta" termina en una
   actividad; el documento que se cree después no queda ligado a la solicitud
   que lo originó (trazabilidad rota en ese tramo).
5. **Oportunidades**: `sgi.risk` con kind='oportunidad' existe pero todo el
   tratamiento (niveles, candados) está diseñado para riesgo; la oportunidad
   no tiene flujo de aprovechamiento diferenciado. Menor.

---

## 3. Flujos faltantes contra las normas

| Requisito | Norma | Estado | Prioridad |
|---|---|---|---|
| Preparación y respuesta ante **emergencias** (planes, simulacros, evaluación) | 14001 §8.2 · 45001 §8.2 | ❌ Solo existe 'emergencia' como condición IPER | **Alta** (45001 en preparación) |
| **Satisfacción del cliente** medida | 9001 §9.1.2 | ❌ Encuesta huérfana, sin KPI | **Alta** |
| **DNC → plan de capacitación** | 9001 §7.2 · P-A01 | ❌ Solo detección | **Alta** |
| **Selección/alta de proveedores** (aprobación inicial, no solo desempeño) | 9001 §8.4.1 | ❌ Solo evaluación trimestral | Media |
| **MSA** (análisis de sistemas de medición) | IATF §7.1.5.1.1 | ❌ Solo como elemento PPAP sin registro | Media (si van a IATF) |
| **Gestión del cambio** general (procesos/infraestructura, no solo documentos y ECOs) | 9001 §6.3 · IATF | ⚠️ Parcial (doc change + PLM) | Media |
| **Diseño y desarrollo** (8.3) | 9001 §8.3 | ⚠️ Flujo P-D01 en el mapa sin modelo; PLM cubre solo cambios | Media |
| Plan/matriz de **comunicación** | Las tres §7.4 | ⚠️ NEWS + acuses cubren parte | Baja |
| Conocimientos de la organización | 9001 §7.1.6 | ⚠️ Parcial vía documental + lecciones NC | Baja |

**Flujos sobrantes o redundantes:** prácticamente ninguno. Los dos canales de
Helpdesk (reclamaciones externas vs quejas internas) son complementarios, no
duplicados; el presupuesto en dos menús es un espejo deliberado. Lo único
"muerto": la escritura no-op de `program_line.state='creada'` al cerrar la
auditoría (la línea no tiene estado "cerrada") — limpiar o añadir el estado.

---

## 4. Escalabilidad (mejores prácticas Odoo)

**Lo que ya está bien hecho** (y es raro verlo tan completo): extender apps
nativas en vez de reinventar, mixin común con folio+candado de evidencia,
secuencias con rango anual, 64 tests, crons idempotentes con dedup de
actividades, migraciones formales, cero Studio, validaciones en Python (no en
automation rules), índices únicos parciales en BD para las condiciones de
carrera.

**Riesgos de escalabilidad, en orden:**

1. ❌ **Multiempresa**: la instancia tiene 8 compañías y solo
   `sgi.sales.budget[.line]` tiene `company_id`. Todos los demás registros SGI
   (NCs vía quality son de compañía, pero riesgos, auditorías, documentos
   controlados, indicadores, procesos…) son globales. Hoy funciona porque el
   SGI es solo de PNTQ; el día que otra empresa lo use, se mezcla todo.
   *Decisión a tomar:* declarar formalmente "SGI = PNTQ" (documentado + record
   rule simple) o ir añadiendo `company_id` por fases.
2. ⚠️ **Sin record rules**: todo usuario SGI ve todos los registros quitando
   filtros. Con la plantilla actual es aceptable; con más usuarios/plantas no.
3. ⚠️ **"Jefe MAST" singular**: `_sgi_manager_user_id()` toma el PRIMER
   usuario del grupo. Con dos managers, todas las actividades caen en uno.
   Parametrizar el destinatario por área/proceso escala mejor que el grupo.
4. ⚠️ **Salud del proceso** hace ~4 búsquedas por registro al computar; con 22
   procesos es correcto, con cientos no. Aceptable, documentado.
5. ✅ Sin traducciones (`translate` casi nulo, textos en español directo):
   decisión válida para una empresa mexicana; dejarla documentada como tal.

---

## 5. Usabilidad

- ✅ "Mi trabajo" como segundo menú, semáforos consistentes, badges, banners
  de formato controlado, textos de ayuda en las acciones.
- ⚠️ **Falta la vista de actividades** (`activity`) en NC, acciones,
  documentos y mediciones: todo el motor del SGI corre sobre actividades y la
  vista nativa de Odoo para gestionarlas no está declarada en ningún modelo.
  Es un cambio barato con retorno alto para el Jefe MAST.
- ⚠️ **Riesgos sin botones de transición** (el usuario debe saber que el
  statusbar es clicable): añadir botones explícitos como en el resto de los
  modelos daría consistencia.
- ⚠️ Tableros de dirección: hoy son 2 paretos; el "dashboard" real es el Panel
  de procesos (bueno). Un tablero de dirección con los 9 objetivos y su salud
  cerraría la vista ejecutiva.

---

## 6. Plan propuesto (en orden)

| # | Cambio | Tipo | Riesgo |
|---|---|---|---|
| 1 | Reorganización de menús (§1: objetivos→Panel, incidentes→Mejora, acciones nivel 1, quejas internas, renombrar grupo) | UX | Nulo (solo XML) |
| 2 | Vistas de actividad en NC/acciones/documentos/mediciones + botones en riesgos | UX | Nulo |
| 3 | Cerrar bucle satisfacción: cron/botón de envío de encuesta + KPI `satisfaccion_cliente` | Funcional | Bajo |
| 4 | Cerrar bucle DNC: distribución de encuesta + actividad RH por brechas nuevas | Funcional | Bajo |
| 5 | Alta documental ligada: al crear el documento desde la actividad, enlazar la solicitud origen | Funcional | Bajo |
| 6 | Módulo de emergencias/simulacros (45001/14001 §8.2) | Funcional nuevo | Medio |
| 7 | Decisión multiempresa (documentar o `company_id` por fases) | Arquitectura | Decisión |
| 8 | Alta/aprobación de proveedores (8.4.1) y MSA (si IATF es meta) | Funcional nuevo | Medio |
