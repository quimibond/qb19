# Manual del Jefe MAST y SGI (día a día)

Para quien tiene el grupo **Jefe MAST y SGI**. Aquí está el trabajo de todos
los días; la configuración (procesos, documentos, parámetros, crons) está en
[../administracion/manual-jefe-mast.md](../administracion/manual-jefe-mast.md).

## 1. Qué ve al entrar

Todo el menú **SGI**, incluida **Administración SGI** (documentos,
indicadores, aprobaciones, diagnóstico, firmas de lectura y configuración).
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

1. **Administración SGI → Firmas de lectura → Publicar Mi procedimiento**.
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

1. **Administración SGI → Indicadores → Mediciones**: filtre las pendientes
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

**Administración SGI → Diagnóstico**: Diagnóstico del SGI, Cobertura de
medición (qué actividades se miden de verdad), Cumplimiento de
procedimientos, Faltantes de especificación (lo que impide publicar un
proceso) y Cumplimiento semanal. Detalle de cómo leerlos en el manual de
administración.

Desde 57.98.0 la sección **Documental** del Diagnóstico lista los reportes del
SGI que imprimen **sin formato controlado** (sin clave): dé de alta su mapeo en
**Administración SGI → Configuración → Formatos en documentos de Odoo**.

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
- **Aspectos ambientales:** se registran en **SGI → Seguridad y ambiente →
  Aspectos ambientales**, con su etapa del ciclo de vida. Un riesgo ya no se
  captura a mano como «Aspecto ambiental»; si el aspecto necesita acciones,
  use **Tratar como riesgo** desde el aspecto.

### 2.7 La transición

**Procesos → Del Dropbox a Odoo → Avance de la transición**: por proceso,
procedimientos sustituidos, rutinas resueltas y documentos migrados. Usted
importa las rutinas (**Importar rutinas**, primero **Probar (modo de
prueba)**) y corrige clase y estado de los formatos.

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
  omitieron.»** En Configuración → Fuentes de NC automáticas, cada fuente
  cuenta las omisiones y guarda la última.
- **«Un aviso sigue abierto aunque ya se resolvió.»** Los crons cierran solos
  los avisos cuya causa se resolvió en su siguiente corrida diaria.
