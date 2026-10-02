# Manual de administración del SGI (Jefe MAST y SGI)

Para el **Jefe MAST y SGI** y el **Administrador SGI**. Explica cómo se
configura y se mantiene el SGI en Odoo. El trabajo del día a día está en
[../usuarios/mast.md](../usuarios/mast.md); las tablas técnicas completas
(crons, parámetros, permisos) se generan del código en
[../tecnica/](../tecnica/README.md).

## 1. Los papeles

| Grupo | Qué hace |
|---|---|
| **Jefe MAST y SGI** | Administra el SGI: procesos, actividades, documentos, publicación de Mi procedimiento, indicadores, NC (cancelación y cierre forzado), fuentes de NC, programa de auditorías. Implica Usuario SGI y Auditor |
| **Administrador SGI** | Lo anterior más cargar el catálogo por archivo y los tipos de documento. Implica Jefe MAST y SGI |
| **Dirección de Operaciones (SGI)** | Consulta y aprueba. **No** tiene permisos de Jefe MAST |
| **Dueño de proceso (SGI)** | Se asigna solo (cron diario) a los dueños de procesos activos |
| **Auditor SGI** | Lee todo menos salud y salarios; escribe auditorías y hallazgos |
| **Salud ocupacional (SGI)** | Exámenes y estudios de higiene. Solo el Coordinador de RH |
| **Salarios de eficiencias (SGI)** | Importes de eficiencias. Solo RH y Nóminas |
| **Captura de eficiencias** | Jefe o supervisor que captura la hoja mensual |
| **Comisión de Seguridad e Higiene (SGI)** | Recorridos y hallazgos de la CSH |

El SGI es **de una sola empresa** (la del parámetro
`quimibond_sgi.sgi_company_id`; por omisión, la principal).

## 2. Arranque de una base vacía

El módulo se instala **vacío**: trae estructura, no el mapa de procesos.

1. Instalar `quimibond_sgi` (y sus satélites se instalan solos).
2. En staging o en una recuperación, instalar `quimibond_sgi_mapa` y, en
   **Administración SGI → Configuración → Cargar mapa de procesos**, primero
   **Probar** (no escribe nada), leer el reporte y luego **Cargar**.
   **En producción no se instala `quimibond_sgi_mapa`.**
3. Para cargas parciales, **Configuración → Cargar catálogo** (Administrador
   SGI) recibe el mismo formato JSON, también con prueba previa.

## 3. Personas y grupos

- Quién tiene usuario lo decide Dirección; los usuarios los crea Sistemas.
- Cada empleado necesita **puesto** en su ficha: de ahí salen su
  procedimiento, sus acuses y sus pendientes.
- En planta, **el supervisor ejecuta**; el capturista de checklists puede no
  tener usuario: firma en la tableta de su área (SGI en planta) con su PIN, o
  en una computadora con su nombre en «Quién lo llena» de la plantilla y,
  si se enciende, su PIN.
- Salud ocupacional: solo el Coordinador de RH. El Auditor no ve salud ni
  salarios.
- El Jefe MAST y SGI audita todo menos el proceso E2; E2 lo audita otro auditor interno
  designado o uno externo (decisión D-05).

### Tabletas de planta (SGI en planta)

**Apagado por ahora** (decisión de Dirección, 2-oct-2026): no se dan de alta
tabletas ni se capturan PIN, y «PIN obligatorio para firmar checklists» sigue
apagado. Sin tabletas dadas de alta, nadie entra a «SGI en planta». Si se
decide encenderlo, antes hay que contestar el límite de intentos del PIN
(pregunta Q12) y después seguir estos pasos:

1. Sistemas crea la cuenta compartida (por ejemplo supervisor@), **sin
   empleado ligado**, y le pone «SGI en planta» como acción de inicio.
2. Usted la da de alta en **SGI → Administración SGI → Configuración →
   Tabletas de planta**: nombre («Tableta Tejido»), cuenta, departamentos y
   los checklists que se llenan ahí. Al guardar, la cuenta recibe el grupo
   «Tableta de planta (SGI)».
3. Pida a Sistemas que le quite a la cuenta los grupos del SGI que no use
   (Usuario SGI): lo firmado en la tableta queda a nombre de quien teclea su
   PIN, y la cuenta sola no firma nada.
4. RH captura los PIN en la ficha de cada empleado. Cuando terminen,
   encienda «PIN obligatorio para firmar checklists» (Ajustes → SGI).
5. Los casi accidentes que llegan de la tableta le llegan como aviso
   «Revisar casi accidente…» (o a Salud ocupacional, si tiene miembros):
   clasifíquelos e investíguelos.

## 4. Procesos y actividades

- **Alta:** **Procesos → Mapa de procesos → Nuevo**: clave, nombre, tipo
  (cadena de valor, estratégico, soporte), dueño, etapas. Las actividades se
  capturan desde el botón **Actividades** del proceso.
- **Cada actividad** contesta siete preguntas: qué, quién, dónde, cómo,
  cuándo, cuándo está terminada y qué hacer si falla. Lo que falte aparece
  en **Diagnóstico → Faltantes de especificación**; los errores impiden
  publicar el proceso.
- **Quién:** exactamente un rol «Ejecuta» (ninguno si la actividad es
  automática), por puesto, familia de puestos o rol relativo (el
  solicitante, quien detecta, el dueño del proceso…). Si quien aprueba
  también ejecuta, la aprobación sube a su jefe.
- **Vencimientos:** mensual en un día hábil, semanal en un día de la semana,
  anual por mes y día, o por evento. Los días inhábiles son los del
  calendario del SGI (parámetro `quimibond_sgi.business_calendar_id`): la
  migración cargó los de la Ley Federal del Trabajo (art. 74); los del
  contrato colectivo se agregan a mano como ausencias globales.
- **Entradas:** lo que una actividad recibe de otra, con plazo en días
  hábiles. Si no llega a tiempo, el eslabón se ve atorado.
- **Numerales congelados:** el número de una actividad no se reutiliza ni se
  renumera; los huecos son normales.
- **Nunca borrar:** una actividad que ya no aplica se **archiva**.
- **Estado del proceso:** borrador → **Pasar a piloto** → **Publicar**
  (vigente). **Regresar a borrador** para corregir.

## 5. Propuestas de cambio

Cualquier persona propone desde **Mi procedimiento → Proponer cambio**. La
solicitud va a Aprobaciones («Proponer cambio a mi procedimiento (SGI)») y la
aprueban el jefe directo de quien propone y el dueño del proceso (más los
aprobadores que usted ponga en la categoría). Al aprobarse, el cambio se
aplica solo a la actividad y el procedimiento publicado queda marcado como
desactualizado hasta la siguiente publicación.

## 6. Documentos

- **Tipos de documento** (Configuración → Tipos de documento; los edita el
  Administrador SGI): cada tipo define el patrón de clave: `PR-{proceso}` para
  procedimientos, `F-{proceso}-{nn}`, `IT-{proceso}-{nn}`, `DA-{proceso}-{nn}`.
  La validación de clave está encendida. La clave anterior del Dropbox se
  guarda aparte y solo se busca en «Del Dropbox a Odoo».
- **Estados:** borrador, prueba piloto, vigente, obsoleto. Los cambios se
  piden con la categoría de Aprobaciones de cambio documental: al aprobarse,
  se crea la revisión nueva, la anterior queda obsoleta y se generan los
  acuses para los puestos del documento.
- **Revisión:** cada documento vigente tiene próxima revisión; llegan dos
  avisos al responsable (60 y 30 días antes, configurables).
- **Documentos externos:** normas, especificaciones de cliente, NOM,
  manuales de proveedor; hay 10 días hábiles desde la recepción para
  implantarlos y el dueño del proceso recibe el aviso.
- **Lista maestra:** Administración SGI → Documentos → Lista maestra.
- **Carpeta:** los documentos controlados viven en la carpeta «SGI» de
  Documentos.

### Empresa en documentos controlados (D-06 de datos)

**Administración SGI → Configuración → Empresa en documentos controlados**
(solo el Jefe MAST). Pone la empresa del SGI en los documentos controlados
que no tienen empresa (492 el 2026-10-02). Es un cambio de datos: **úselo
solo después del visto bueno por escrito de Jose** (pregunta Q1 de la
entrega 57.95.0).

1. Al abrirlo solo **cuenta**: cuántos documentos sin empresa hay (vigentes,
   obsoletos, en borrador o piloto) y cuántas rutinas del procedimiento
   anterior tomarán la empresa. No escribe nada.
2. **Ver los documentos** los lista para revisarlos.
3. **Asignar la empresa del SGI** escribe la empresa en lotes de 100, deja en
   el historial de cada documento la nota «Empresa del SGI asignada…» y
   registra en el log del servidor los documentos antes y cuántos quedaron
   después. Si alguno no se puede corregir, el asistente lo cuenta en
   «Documentos que no se pudieron corregir» y el motivo queda en el log.
4. Las rutinas del procedimiento anterior toman la empresa solas. No se borra
   nada.
5. Vuelva a abrirlo: debe contar 0.

Efecto a saber: quien trabaje con otra razón social seleccionada (sin PNTQ)
deja de ver esos documentos hasta seleccionarla, igual que hoy con los de Mi
procedimiento. Los documentos controlados nuevos ya nacen con la empresa del
SGI (o con la de su familia, si la clave ya existe). Dos casos raros:

- quien crea un controlado con clave nueva sin tener PNTQ activa en el
  selector de empresas recibe PNTQ en el documento y Odoo puede rechazarlo
  por la regla de empresa: active PNTQ y vuelva a intentarlo;
- marcar como controlado un documento existente cuya familia (misma clave)
  ya vive en PNTQ puede chocar con «una sola combinación clave + revisión por
  empresa»: revise la revisión antes de marcarlo.

## 7. Publicar Mi procedimiento

**Firmas de lectura → Publicar Mi procedimiento**. Revise antes las pestañas
de pendientes (puestos duplicados, empleados sin puesto, puestos sin roles,
puestos sin personas). Publicar crea la revisión del PDF del puesto y el
acuse de cada persona; si Ajustes → SGI → «Mi procedimiento se firma en
Sign» está encendido, la firma va por Sign. Cada semana le llega el aviso de
los puestos sin publicar o desactualizados. Cuando alguien entra a un puesto
ya publicado, revise en **Acuses de lectura** que tenga el suyo.

**Avisos de acuses pendientes (desde 57.95.0).** Cuando un acuse pasa
7 días hábiles sin firmar (parámetro `quimibond_sgi.doc_ack_pending_days`),
el cron diario de documentos ya no agenda un aviso por acuse; los agrupa:

- persona con usuario que puede abrir el documento: **un** aviso «Documentos
  por leer y firmar: N», a ella, sobre el documento de su acuse más viejo;
- gente sin usuario, o con usuario pero sin permiso para abrir el documento:
  **un** aviso por jefe, «Acuses pendientes de su gente: N (P personas)», al
  jefe, sobre su departamento; **Ir** en Mis pendientes abre la lista de esos
  acuses;
- a usted: los equipos de jefes sin usuario, los de jefes **con usuario pero
  sin departamento** (decisión de 57.95.0: el aviso de equipo vive en el
  departamento del jefe; sin él, va al Jefe MAST) y las personas sin jefe (un
  jefe archivado cuenta como sin jefe);
- la leyenda «Acuses pendientes de <persona> (no puede abrir el documento)»
  solo aparece cuando Odoo no dejó agendar el aviso de la persona (falla al
  agendarlo): entonces le llega a usted.

Los avisos viejos de uno por acuse se cierran solos en la primera corrida con
la nota «se reemplazó por un aviso por persona o por jefe». Un aviso se
cierra solo cuando ya no hay acuses pendientes en su grupo.

## 8. Indicadores y mediciones

- El cron crea las mediciones del periodo: las automáticas se calculan; las
  manuales llegan como pendiente a su dueño (5 días hábiles para capturar).
- El dueño **valida** en 3 días hábiles.
- Un indicador nace **En prueba**; el dueño lo pasa a **oficial** tras revisar
  una vez sus registros contra la realidad.
- Si un automático no da un valor confiable, cambie su modo de cálculo a
  captura manual y anote el motivo en el indicador.
- **NC en rojo** (`nc_on_red`): un rojo levanta NC; úselo en los críticos.
- **Recalcular mediciones pendientes** (en la lista de indicadores) vuelve a
  medir lo pendiente después de corregir una fórmula.

## 9. No conformidades y acciones

- Etapas: Abierta, Seguimiento, Cerrada, Cancelada. Plazos por etapa
  (contención, causa raíz, plan) en días hábiles desde que se abre (Ajustes →
  SGI → No Conformidades y AMEF).
- Candados de cierre: causa raíz, acciones terminadas (las correctivas, con
  evidencia: una nota o un archivo) y verificación de eficacia con resultado
  **Eficaz**, registrada en la fecha programada o después; en NC mayor,
  además los 5 porqués y la lección aplicada. Si la verificación sale **No
  eficaz**, la NC regresa a Seguimiento y pide una acción correctiva nueva.
  Una NC de reclamación no sale de Abierta sin contención.
- Solo cierran la NC el dueño del proceso o usted. Ya cerrada, solo usted la
  modifica (también sus acciones terminadas); el dueño del proceso puede
  reabrirla cambiando solo la etapa. Una NC no se crea directamente cerrada
  ni cancelada.
- **Cancelar** siempre con motivo; si la pide otra persona, usted la aprueba.
- **Cierre forzado (Jefe MAST):** con motivo, queda en el historial.
- **Fuentes de NC automáticas** (Configuración): cada automatismo que levanta
  NC tiene su interruptor. Apagarlo no desinstala nada, queda firmado en el
  historial y cuenta las omisiones.

## 10. Calendario

Días inhábiles: LFT art. 74 (cargados por la migración 57.15.0 para
2026-2028) más los del contrato colectivo, que se agregan a mano en el
calendario del parámetro `quimibond_sgi.business_calendar_id`. Un
vencimiento que cae en día inhábil se **adelanta** al día hábil anterior (si
eso lo sacara de su periodo, pasa al siguiente hábil). Las hojas de
checklist no se generan en festivos.

## 11. Crons

Hay 27 acciones planificadas del SGI (tabla completa en
[../tecnica/crons.md](../tecnica/crons.md)). Viven en `noupdate`: cambiarlas
en la base requiere migración. Desde 57.94.0 incluye «SGI: Empleados sin
puesto o sin correo (aviso a RH)», semanal (lunes). Desde 57.95.0 incluye
«SGI: Respaldo nocturno (Mi procedimiento y Mi equipo)», diario a las 02:15
de México: vuelve a calcular las listas de Mi procedimiento de cada persona y
el resumen de pendientes que usan los filtros de Mi equipo. En el log del
servidor deja «respaldo nocturno de Mi procedimiento: 0 cambios en N
personas»; si aparece «falta un disparo de recálculo», avise a quien
mantiene el módulo: alguna pantalla cambió roles o actividades sin
recalcular Mi procedimiento (el respaldo ya lo corrigió). Si una falla, Odoo la apaga tras 5 fallos en
más de 7 días: revise **Ajustes → Técnico → Acciones planificadas** (filtro
«SGI»).

## 12. Parámetros

Los editables están en **Ajustes → SGI**; la lista completa, con sus
valores de fábrica, en [../tecnica/parametros.md](../tecnica/parametros.md).
Otros que conviene conocer: `quimibond_sgi.mast_user_id` (a quién llegan los
avisos de MAST), `quimibond_sgi.rh_user_id`,
`quimibond_sgi.checklist_pin_required`, `quimibond_sgi.hr_user_id` (usuario de RH
que recibe el aviso semanal de empleados sin puesto o sin correo; vacío:
el de `quimibond_sgi.rh_user_id` y, sin él, el Jefe MAST; debe tener «Empleados /
Encargado» para que «Ir» abra la lista),
`quimibond_sgi.legacy_decision_deadline` (fecha límite de las rutinas
pendientes) y los plazos de Mis pendientes
(`measure_capture_business_days`, `measure_validate_business_days`,
`ack_business_days`).

## 13. «Del Dropbox a Odoo»

Solo MAST edita. **Importar rutinas** carga el libro «rutina por rutina»:
**Probar (modo de prueba)**, revisar, confirmar y **Cargar**; si el conteo no
cuadra, no carga. Cada formato anterior tiene su estado de migración
(pendiente, en curso, migrado a Odoo, baja tramitada, no aplica: se queda).
«Avance de la transición» muestra por proceso cuántos procedimientos están
sustituidos y cuántas rutinas faltan. Guía
completa: [../transicion/del-dropbox-a-odoo.md](../transicion/del-dropbox-a-odoo.md).

## 14. Diagnóstico

| Pantalla | Cómo se lee |
|---|---|
| Diagnóstico del SGI | Hallazgos de configuración y adopción (bien, aviso, mal) con cómo corregirlos |
| Cobertura de medición | Cada actividad por su método de medición; «Sin medir» solo se permite en borrador |
| Cumplimiento de procedimientos | Verde con evidencia en su periodo, rojo sin ella; agrupe por proceso o puesto |
| Faltantes de especificación | Lo que le falta a cada actividad; los errores impiden publicar |
| Cumplimiento semanal | Por actividad y semana: completas, a tiempo y vencidas abiertas |

## 15. Qué no hacer

- No usar **Studio** sobre modelos del SGI.
- No **borrar** registros del SGI: se archivan.
- No editar lo que trae el módulo (normas, áreas, tipos) sin pedirlo a
  desarrollo: un update puede sobrescribirlo o chocar.

## 16. A quién pedir qué

| Necesita… | Es… |
|---|---|
| Un proceso, una actividad, un documento, un indicador nuevo | Configuración: usted |
| Un modo de cálculo nuevo, un reporte, un cron, una pantalla | Desarrollo |
| Un usuario o un grupo | Dirección decide; Sistemas crea |
