# Manual del Jefe MAST y SGI (día a día)

Para quien tiene el grupo **Jefe MAST y SGI**. Aquí está el trabajo de todos
los días; la configuración (procesos, documentos, parámetros, crons) está en
[../administracion/manual-jefe-mast.md](../administracion/manual-jefe-mast.md).

## 1. Qué ve al entrar

Todo el menú **SGI**, ordenado por capítulos de la norma desde 57.113.0:
**Inicio**, **Reportar**, **Sistema** (mapa, actividades, documentos, el MIID
y «Del Dropbox a Odoo»), **Planeación** (política, objetivos, partes
interesadas, riesgos, aspectos ambientales y requisitos legales),
**Seguridad y ambiente**, **Desempeño** (Tablero, indicadores, satisfacción,
auditorías y revisión por la dirección), **Mejora** y **Administración**
(diagnóstico, aprobaciones, Publicar Mi procedimiento, Transición y
Configuración).
Usted también es Usuario SGI y Auditor: tiene su propio procedimiento y lee
todo, salvo exámenes médicos (Salud ocupacional) y salarios (RH).

## 2. Su día

### 2.1 Vaciar su bandeja

**SGI → Inicio → Mis pendientes.** A usted le llegan, además de lo suyo:

- solicitudes de **cancelación de NC** (apruébelas con **Cancelar** en la NC,
  o conteste en el chatter);
- plazos de NC vencidos que ya escalaron a MAST;
- **validaciones** de mediciones de indicadores sin dueño;
- avisos de los crons: documentos por revisar, pilotos por vencer, acuses
  pendientes, calibraciones, programa de auditorías, Mi procedimiento por
  publicar (aviso semanal).

### 2.2 No conformidades

1. **SGI → Mejora → No conformidades**: agrupe por etapa (Abierta,
   Seguimiento, Cerrada, Cancelada) y filtre las vencidas.
2. Una NC no se cierra sin causa raíz, acciones terminadas (las correctivas,
   con evidencia: una nota o un archivo) y verificación de eficacia con
   resultado **Eficaz**, registrada en la fecha programada o después (y en una
   NC mayor, los 5 porqués y la lección aplicada). Si la verificación sale
   **No eficaz**, la NC pide una acción correctiva nueva (y, si estaba
   cerrada, regresa a Seguimiento). Solo la cierran el dueño del proceso o el Jefe MAST; ya cerrada,
   solo el Jefe MAST la modifica (el dueño del proceso puede reabrirla
   cambiando solo la etapa). Si hay que cerrarla sin cumplirlo, use **Cierre
   forzado (Jefe MAST)** con motivo; queda en el historial.
3. Para pasar una NC de Abierta a Seguimiento (o cerrarla sin pasar por
   Seguimiento) capture la **clasificación** y el **requisito (cláusula)**.
   Desde 57.97.0 hay cláusulas de tercer nivel (por ejemplo, 45001 8.1.2
   jerarquía de controles, 8.1.4 contratistas, 14001 6.1.2 aspectos
   ambientales); use la más precisa. El cierre forzado no lo pide.
4. **SGI → Mejora → Acciones correctivas** muestra todas las acciones de
   todos los orígenes, con filtro «De …».

### 2.3 Publicar Mi procedimiento

1. **SGI → Administración → Publicar Mi procedimiento**.
2. Revise la pestaña de pendientes: puestos duplicados, empleados sin puesto,
   puestos sin roles y puestos con roles pero sin personas.
3. Publique un puesto o **Publicar todos los puestos**. A cada persona le
   llega el acuse de «leído y entendido».
4. **Acuses de lectura** muestra quién falta de firmar.
5. Los acuses pendientes ya no le llegan uno por uno. La gente con usuario
   recibe su propio aviso («Documentos por leer y firmar»); la que no tiene
   usuario, o que tiene usuario pero no puede abrir el documento, entra en el
   aviso de su jefe sobre el departamento del jefe («Acuses pendientes de su
   gente»). A usted solo le llegan los equipos de jefes sin usuario o **sin
   departamento** en su ficha y las personas sin jefe. La leyenda «no puede
   abrir el documento» solo aparece si Odoo no dejó agendar el aviso de la
   persona (falla al agendarlo); entonces le llega a usted. Para que deje de
   recibir los de un jefe, pida a RH que le asigne departamento o usuario.

### 2.4 Indicadores

1. **SGI → Desempeño → Indicadores → Mediciones**: filtre las pendientes
   de captura y de validación.
2. Un indicador nuevo nace «En prueba»; su dueño lo pasa a oficial (**Pasar
   a oficial**) después de revisar una vez los registros de una medición
   contra la realidad.
3. Si un automático no da valor, el campo «Último cálculo» dice por qué (sin
   fórmula, sin datos, error).
4. **Salud del SGI (desde 57.99.0):** los indicadores SG-01 a SG-10 nacen en
   prueba, del proceso E2 y con usted (o el dueño de E2) como responsable.
   Revise la primera medición de cada uno con **Registros** contra la
   realidad y páselo a oficial (regla I-2). Sus mediciones se quedan
   «capturadas»: no se validan ni piden causa y plan, y no salen en su
   bandeja. Para que alguien no cuente en SG-02 (por ejemplo, el CEO),
   ponga su id de usuario en `quimibond_sgi.health_excluded_user_ids`; para
   agregar destinatarios al correo de los lunes, en
   `quimibond_sgi.health_mail_user_ids` (ids separados por coma, en
   **Ajustes → Técnico → Parámetros del sistema**).

5. **Metas congeladas (desde 57.100.0):** al validar, la medición guarda las
   metas con las que se juzgó; si cambia la meta del indicador o corrige un
   escalón, las validadas no cambian de color (la ficha lo dice y el chatter
   del indicador cuenta cuántas). Para juzgar una validada con la meta nueva,
   use «Regresar a pendiente» y vuelva a validarla. Si corrige el valor de
   una validada sin reabrirla, el color sale de las metas guardadas.

6. **Sin dato y recálculo (desde 57.104.0):** el menú **Mediciones** y el
   botón «Mediciones» abren con el filtro «Con dato»; quítelo para ver las
   pendientes y las sin dato. El cron diario re-mide también las «sin dato»
   y las capturadas no validadas de los últimos 2 meses (el antes y el
   después quedan en el chatter de cada medición). Para re-medir todos los
   meses de un indicador: lista de indicadores → selecciónelo → **Recalcular
   mediciones** (solo Administrador SGI). Una medición con «Valor corregido
   a mano» no se recalcula sola; «Recalcular valor» quita la marca.
   **Registro vacío:** una fórmula «más bajo es mejor» cuya fuente nunca ha
   tenido registros (SST-01, C5-01) sale «Sin dato» hasta que alguien
   capture el primero. **TI-01:** junio, julio y agosto de 2026 quedaron
   validadas en 0 sin dato real; regréselas a pendiente, capture la
   disponibilidad del reporte de Odoo.sh y valídelas (las metas se vuelven a
   guardar).

### 2.4.1 Competencias, cliente automotriz e IA (desde 57.100.0)

- **Exámenes y cursos:** en **Empleados → Competencias SGI**, «Exámenes y
  competencias (Encuestas)» y «Cursos y competencias (eLearning)» ligan cada
  examen de certificación o curso con una competencia y un nivel; del curso,
  capture «Vigencia (meses)» si la competencia se renueva (por ejemplo,
  brigadas cada 12). Quien aprueba o termina la recibe sola y a los 90 días su
  jefe evalúa la eficacia (usted ve todas en «Eficacia de la capacitación» y
  es el único que cambia un resultado ya registrado).
- **Cliente automotriz:** en el contacto, «Exige PPAP ante cambios» y «Exige
  plan de contingencia» (las marca Calidad). Con ellas, el ECO de un producto
  que se les vende marca solo «Requiere PPAP» y genera un PPAP por cliente al
  aplicarse. Una salida validada sin CoA a un cliente que lo exige le avisa al
  Jefe de Calidad (o a usted si no hay) hasta que se adjunte.
- **IA en la NC:** apagada. Se enciende solo con la autorización escrita de
  Jose (manual de administración, sección 12). Encendida, en la pestaña
  «Desviación y análisis» de una NC abierta aparece «Pedir sugerencia a la
  IA»; la sugerencia es un borrador que usted copia con los botones. La causa
  raíz siempre la escribe el responsable.

### 2.5 Revisar la salud del SGI

**SGI → Administración → Diagnóstico**: Diagnóstico del SGI, Cumplimiento
de procedimientos (sin el filtro «Medibles» y agrupado por método de
medición: qué actividades se miden de verdad), Registro de cumplimiento (con
el botón **Por semana**) y Faltantes de especificación (lo que impide
publicar un proceso; filtro **Medición por revisar**). Detalle de cómo
leerlos en el manual de administración.

Desde 57.98.0 la sección **Documental** del Diagnóstico lista los reportes del
SGI que imprimen **sin formato controlado** (sin clave): dé de alta su mapeo en
**SGI → Administración → Configuración → Formatos en documentos de Odoo**.

### 2.5.1 Lo impreso y los colores (desde 57.98.0)

- **Pie de página:** los reportes propios del SGI (NC, incidente, revisión por
  la dirección, auditoría, CoA, matrices, AMEF, LOTO, lista maestra, NEWS,
  retención, permiso de trabajo y ficha por máquina) llevan en **cada hoja**
  la clave, la revisión, la **fecha de emisión** del documento vigente ligado y
  «Página x de y». Si el documento no tiene fecha de emisión, el pie sale sin
  ella: llénela en el documento (hoy faltan en 14). Los reportes de otras apps
  (venta, compra, entrega, orden de producción) y las etiquetas conservan el
  pie al final de la hoja, ahora con la emisión.
- **Colores:** un estado se pinta igual en todo el SGI: gris = borrador,
  cancelado u obsoleto; azul = abierto o en curso; amarillo = pendiente o por
  vencer; rojo = vencido o rechazado; verde = cerrado, vigente o validado
  (tabla completa en el README del módulo, sección «Colores»).
- **Filtros:** en todas las listas, **Míos** muestra lo suyo; en AMEF y
  riesgos, **De mis procesos**. Varias listas abren ya filtradas (permisos y
  revisiones sin cerrar, AMEF vigentes, PPAP en proceso, programa de este año,
  aspectos significativos, objetivos por política, proveedores por
  clasificación): quite el filtro para ver todo.

### 2.5.2 Fichas, diagramas y copias guardadas (desde 57.101.0)

- **8D, solicitud de desarrollo, responsiva de EPP y eficiencias** también
  llevan el pie en cada hoja. El 8D sale solo con la página: no comparte la
  clave del reporte de NC.
- **Diagnóstico → Documental:** los reportes cuyo formato aún no tiene clave
  del SGI (ficha del indicador, diagramas, programa contra realizado, mapa de
  calor de R&O y ambiental, 8D) salen en una línea aparte, informativa:
  imprimen solo con la página y la clave se da de alta en el código, no en
  «Formatos en documentos de Odoo».
- **Diagramas en PDF:** en la barra del diagrama, el botón PDF (junto a la
  impresora) imprime en formato controlado el mapa de procesos, la
  interacción (4.4), la tortuga, los roles (5.3) y el contexto (4.1/4.2); las
  flechas salen como tabla «Conexiones». En riesgos imprime el mapa de calor
  del instrumento elegido. Carriles y PDCA siguen con la impresora de
  pantalla.
- **Acta de revisión por la dirección:** al imprimir un acta **cerrada**, la
  copia queda guardada como adjunto y se vuelve a entregar igual. Si usted la
  regresa a borrador, la copia se renombra «(reabierta el …)» y no se borra;
  al cerrarla otra vez se guarda la nueva. El acta cerrada imprime la copia
  guardada; para corregirla, reábrala (la copia anterior se renombra).
- **Nombre del archivo** con folio en NC, 8D, auditoría, acta, incidente y
  AMEF.

### 2.5.3 El Manual del SGI (MIID) desde Odoo (desde 57.105.0)

**SGI → Sistema → Manual del SGI (MIID)** junta el texto fijo del manual
(una sección por título, «Textos del manual») y los datos vivos del SGI
(procesos con su mapa e interacción, política, objetivos, tipos de documento,
controles operacionales, plazos de NC, correspondencia por cláusula,
procedimientos anteriores, anexos y control de cambios).

- **Lo que ve en pantalla y en «Vista en PDF (borrador)»** es la vista del
  sistema, marcada «Borrador — no vigente». La revisión vigente es el
  documento MIID de Documentos («PDF de la revisión vigente»).
- **Textos del manual:** usted edita el texto de cada sección. Escriba
  `[[datos]]` en un párrafo propio para decidir dónde van los datos del
  sistema (si no, van al final). «Texto solo si no hay datos» deja el texto
  como respaldo (11.1: la tabla escrita sale solo si la matriz de
  cumplimiento no trae datos). La «Situación» de cada anexo es una nota por
  renglón (12.1). Las secciones se archivan; no se borran.
- **«Por confirmar»:** mientras una sección esté marcada (con la nota de qué
  falta), ninguna revisión del MIID se envía ni se aprueba. La carga trae
  cuatro: 1.2 (alcance de ISO 45001), 1.3 (justificación de las no
  aplicabilidades), 11.2 (P-A13, P-A22 y P-A30 sin registrar) y 12.1
  (situación de cada anexo). Lo quitan usted, Dirección o el Administrador
  SGI; queda en el historial de la sección.
- **Procesos:** el MIID solo se aprueba cuando **todos** los procesos activos
  están «Vigente» (piloto no cuenta). La pantalla y el Diagnóstico dicen qué
  falta.
- **Solicitar el cambio:** con todo confirmado y los procesos publicados,
  **Solicitar cambio del MIID** arma la solicitud de cambio documental de
  siempre con el PDF generado, la revisión propuesta (la primera desde Odoo es
  la 03) y las diferencias. Revísela y **Envíela** usted: si los datos
  cambiaron desde que se armó, al enviar se genera otra vez el PDF (el
  anterior queda renombrado «(sustituido el …)»). Firman: Elaboró usted,
  Revisó el dueño de E2 y Aprobó Dirección. Lo que se firma es lo que se
  publica: al aprobarse queda la revisión nueva (la anterior obsoleta, con su
  archivo) y empiezan los acuses.
- **Si los datos cambian a mitad de la firma:** se publica lo firmado y al día
  siguiente le llega el aviso de que el MIID ya no coincide; solicite otro
  cambio. Si las firmas se completan pero hay una sección por confirmar o un
  proceso sin publicar, la aprobación espera y le llega un aviso; al quitar
  el candado, la sincronización diaria con Sign la aprueba.
- **Aviso diario:** cuando el MIID vigente ya no coincide con el sistema le
  llega un solo aviso, «El MIID vigente ya no coincide con el sistema», con
  las diferencias; vence a los 3 días hábiles y se cierra solo cuando vuelve
  a coincidir. El MIID cargado del Dropbox no tiene contra qué comparar: no
  avisa hasta la primera revisión aprobada desde Odoo.
- **Antes de la primera aprobación:** capture los puestos del MIID vigente
  (los acuses salen de ahí) y, si lo decide, corrija su revisión de 00 a 02
  (el control de cambios imprime lo que hay en Odoo).

### 2.6 Seguridad y ambiente (desde 57.96.0)

- **IPER de riesgo alto:** al controlarlo o cerrarlo, declare la jerarquía del
  control (eliminación, sustitución, ingeniería, administrativo o EPP) en el
  riesgo o en sus acciones terminadas. Con solo EPP no se puede: registre y
  termine un control de mayor nivel.
- **Cerrar un incidente** pide, además del SCAT y las acciones terminadas:
  equipo de investigación con al menos un trabajador sin personal a su cargo
  o un integrante de la Comisión de Seguridad e Higiene; verificación de
  eficacia «Eficaz» con fecha y nota (pestaña **Investigación y eficacia**);
  y, si es moderado, grave o fatal con IPER ligado, el IPER reevaluado
  después del evento (**Registrar evaluación** en el riesgo). «No eficaz»
  regresa el incidente a Acciones y pide una acción nueva.
- **Permisos de trabajo:** cada hora le llega un aviso por cada permiso
  vencido que sigue autorizado. Un permiso no se cierra ni se cancela mientras
  un bloqueo (LOTO) ligado siga aplicado.
- **Requisitos legales:** los botones **Cumple**, **Cumple parcialmente**,
  **No cumple** y **No aplica** abren el registro de la evaluación con la
  evidencia (o el motivo por el que no aplica). Sin ella no se registra.
- **Aspectos ambientales:** se registran en **SGI → Planeación →
  Aspectos ambientales**, con su etapa del ciclo de vida. Un riesgo ya no se
  captura a mano como «Aspecto ambiental»; si el aspecto necesita acciones,
  use **Tratar como riesgo** desde el aspecto.

### 2.7 La transición

**SGI → Sistema → Del Dropbox a Odoo → Avance de la transición**: por proceso,
procedimientos sustituidos, rutinas resueltas y documentos migrados. Usted
importa las rutinas (**Importar rutinas**, primero **Probar (modo de
prueba)**) y corrige clase y estado de los formatos.

### 2.8 Instructivos en Conocimiento (desde quimibond_sgi_knowledge 1.2.0)

Los manuales del SGI, los instructivos (IT), controles operacionales (CO),
protocolos y reglamentos también se leen en **Conocimiento → SGI**. Los
manuales se actualizan solos con cada versión del sistema, salvo los que
alguien editó en Conocimiento (a esos les queda un mensaje para comparar).

1. **Importar:** **SGI → Administración → Transición → Importar documentos a
   Conocimiento**. Empiece con **CO y C4** (los cinco controles operacionales
   y los instructivos de C4); el asistente trabaja por lotes de 10 y dice qué
   importó, qué ya estaba, qué no pudo leer (PDF escaneado) y qué actividades
   ligó o sugiere. Los documentos de la familia P-I01 nunca se importan
   (L-001) y los restringidos tampoco. El documento controlado no cambia.
2. **Borradores:** cada documento queda como borrador bajo su proceso, con el
   PDF vigente adjunto. Solo lo ven el dueño del proceso y usted.
3. **Revisar:** **SGI → Sistema → Conocimiento del SGI** lista los borradores
   («Por publicar»). El dueño corrige el texto comparándolo con el PDF y pide
   la publicación con **Pedir publicación**: a usted le llega una actividad.
4. **Publicar:** en la lista o en la ficha del documento, **Publicar**
   congela el artículo como revisión nueva del documento (PDF, misma clave y
   título, clave anterior y documento padre), obsoleta la anterior, pide
   acuses a los puestos y re-apunta las actividades. El artículo queda
   bloqueado y lo leen todos. Los puestos que usan el documento verán su Mi
   procedimiento desactualizado: es la revisión nueva.
5. **Cambios después de publicar:** si alguien desbloquea y cambia un
   artículo publicado, sale como «Cambió desde la publicación» y le llega un
   aviso diario para publicarlo otra vez.

En Conocimiento no use **Mover a la papelera** con artículos del SGI: la
papelera los borra a los días. El espacio de 2025 quedó como «SGI (estructura
2025, sin uso)»; usted decide si lo archiva.

## 3. Lo que le llega solo

Además de lo anterior: la NC mayor (correo crítico), incidentes graves, los
escalamientos de acciones vencidas y el aviso semanal de puestos con Mi
procedimiento sin publicar o desactualizado.

## 4. Lo que no puede hacer y a quién pedirlo

| No puede… | Pídalo a… |
|---|---|
| Cargar el catálogo por archivo | Administrador SGI |
| Cambiar Ajustes generales | Administrador del sistema (Ajustes) |
| Crear usuarios o cambiar grupos | Los decide Dirección; los crea Sistemas |
| Ver exámenes médicos | Salud ocupacional (Coordinador de RH) |
| Ver salarios de eficiencias | RH y Nóminas |

## 5. Preguntas frecuentes

- **«Una persona dice que no le aparece su procedimiento.»** Revise que el
  empleado tenga puesto y usuario, que el puesto tenga actividades y que esté
  publicado (pestaña de pendientes de Publicar Mi procedimiento).
- **«Apagué una fuente de NC automática y quiero saber cuántas se
  omitieron.»** En Administración → Configuración → Fuentes de NC automáticas, cada fuente
  cuenta las omisiones y guarda la última.
- **«Un aviso sigue abierto aunque ya se resolvió.»** Los crons cierran solos
  los avisos cuya causa se resolvió en su siguiente corrida diaria.

## Completar las propuestas de actividad (57.108.0)

Quien propone una actividad contesta seis preguntas sencillas; lo técnico lo
completa usted antes de aprobar. En la solicitud de Aprobaciones
(«Proponer cambio a mi procedimiento»), pulse **Completar antes de aprobar**:
abre la propuesta con todos los campos y arriba **lo que falta para
publicarla** (criterio de terminado, escalamiento, vencimiento, pantalla de
Odoo, instructivo o pasos). Complete y pulse **Guardar y actualizar la
solicitud**: el antes → después de la solicitud ya incluye lo que agregó.

## Asistentes (57.109.0)

- **Configurar una aprobación.** En SGI → Administración → Aprobaciones del
  SGI, pulse **Configurar** en el renglón: diga qué se aprueba (la acción de
  un documento, una decisión que se pide en Aprobaciones o algo que se firma),
  si es siempre o solo a veces, y revise la vista previa antes de
  **Activar**. Las que solo necesitan una solicitud en Aprobaciones se activan
  en lote con **Activar las sugeridas como solicitud**. Las que faltan le
  llegan como aviso por proceso en Mis pendientes.
- **Nuevo indicador.** En SGI → Desempeño → Indicadores → Nuevo indicador:
  elija qué quiere saber, de qué registros y cuáles cuentan (filtro visual);
  la vista previa muestra cómo habrían salido los últimos tres periodos y
  qué registros cuenta. Se crea en «prueba».
- **Medición de una actividad.** En la pestaña de medición de la actividad,
  el filtro es visual, la fecha y quién la hizo se eligen por su nombre, y
  «Lo que cuenta hoy» muestra los registros de 30 días y a quién se le
  atribuyen.
- **Quién la hizo, por el historial (57.111.0).** Cuando el modelo no tiene
  un campo de «quién lo validó» (transferencias, órdenes de fabricación, NC,
  solicitudes), marque **Quién lo hizo: quien lo pasó a su estado
  (historial)**: cuenta a quien pasó el registro a su estado actual, no al
  último que lo editó. Las que se medían con «write_uid» ya vienen marcadas.
- **Medición manual a propósito (57.112.0).** Si la actividad se hace en Odoo
  pero su evidencia es una revisión, un reporte o una junta, escriba **Por qué
  se mide a mano** (qué se revisa y dónde queda la decisión): se mide con el
  registro de cumplimiento y deja de salir «Se hace en Odoo, se mide a mano».
- **Categorías con asunto (57.116.0).** Varias aprobaciones comparten una
  categoría de Aprobaciones por área. En la categoría, la lista **Asuntos**
  liga cada asunto con su aprobación del SGI. Quien pide elige el asunto, y
  ese asunto define quién aprueba.
- **Riesgos reportados.** Cualquiera puede reportar un riesgo u oportunidad
  en SGI → Reportar; a usted le llega «Evaluar riesgo reportado» para
  confirmar probabilidad, impacto, proceso y categoría.
