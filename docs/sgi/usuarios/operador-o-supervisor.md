# Manual del operador o supervisor

Para toda persona con usuario de Odoo en el grupo **Usuario SGI**, y para
quien captura checklists en planta. Si además es jefe de un área o dueño de
un proceso, lea también [jefe-de-area.md](jefe-de-area.md).

## 1. Qué ve al entrar

En el menú **SGI → Inicio** tiene todo lo suyo:

| Menú | Para qué |
|---|---|
| **Mis pendientes** | Todo lo que le toca y está por vencer o atrasado, en una sola lista |
| **Mi procedimiento** | Las actividades de su puesto, sus documentos, indicadores, EPP y su firma de «leído y entendido» |
| **Documentos vigentes** | Los procedimientos, instructivos y formatos vigentes, con su clave, tipo, proceso y revisión. Abre con **Míos** (los que le toca leer), agrupados por proceso |
| **Mis indicadores** | Los indicadores a su cargo, si tiene alguno |
| **Mi equipo** | Su equipo de trabajo (si tiene personas a su cargo) |
| **Checklists de hoy** | Las hojas de checklist del día (también en Mantenimiento) |

Para avisar de algo, **SGI → Reportar** abre la ficha nueva: **No
conformidad**, **Casi accidente o incidente** o **Queja o sugerencia** (vea
2.5). Además puede consultar **SGI → Planeación** (política, objetivos,
riesgos, aspectos ambientales, requisitos legales), **SGI → Mejora** (no
conformidades, reclamaciones, mejoras, quejas y sugerencias), **SGI →
Seguridad y ambiente** (incidentes, planes de emergencia), **SGI →
Desempeño** (auditorías) y **SGI → Sistema** (mapa de procesos y
actividades, el MIID y el buscador por clave anterior); los demás catálogos
(entregables, flujos, matriz, puestos y fichas por máquina) son para los
dueños de proceso, el Jefe MAST, Dirección y Auditoría.

En todas las listas del SGI, el filtro **Míos** muestra lo suyo, y los
colores dicen lo mismo en todas partes: gris = borrador o cancelado, azul =
abierto o en curso, amarillo = pendiente o por vencer, rojo = vencido o
rechazado, verde = cerrado, vigente o validado.

> Si no ve el menú SGI, no está en el grupo Usuario SGI: pídalo a su jefe;
> los usuarios los autoriza la Dirección y los crea Sistemas.

## 2. Su día

### 2.1 Revisar Mis pendientes

1. Abra **SGI → Inicio → Mis pendientes**.
2. Arriba aparecen primero los **atrasados**, luego lo que **vence esta
   semana**. Cada renglón dice qué es (acción, NC, medición, acuse, firma,
   aprobación, actividad atrasada…), de qué proceso y cuándo vence.
3. Toque el renglón para abrir su **ficha**: qué hay que hacer (criterio de
   terminado, pasos, instructivo, nota del aviso, plazos de la NC…), sobre
   qué registro, cuándo vence y los botones para actuar.
   Desde 57.174.0 cada renglón trae **dos botones**: el de su tipo (Hecho,
   Registrar paso, Evaluar, Revisar, Aprobar, Validar o Leído y entendido) y
   uno para abrir (Ir a hacerlo, Leer el documento, Firmar, Capturar o
   Abrir). La columna **Qué sigue** dice con palabras lo que toca. Lo demás
   (Rechazar, No corresponde, En proceso, No aplica este periodo, Posponer
   una semana) está en la ficha. El panel de la izquierda filtra por tipo.
4. **Hecho** aparece solo donde aplica, y sabe qué hacer según el tipo
   (desde 57.145.0):
   - **Actividad atrasada** de su procedimiento: abre el asistente de
     «Hecha» (si es de registro manual pide una nota o un archivo de
     evidencia). Si la actividad todavía no tiene su registro del periodo,
     Hecho lo crea en ese momento. En la ficha también tiene **En proceso**
     (nota de avance, no quita el atraso) y **No aplica este periodo**.
   - **Aviso** automático del SGI (en no conformidades, documentos,
     mantenimiento, mesa de ayuda o proyectos): Hecho lo cierra en Odoo. En
     la ficha, **Posponer una semana** mueve su fecha límite 5 días hábiles.
   - **Acción** de una NC, riesgo o incidente: Hecho la termina (fecha de hoy
     y 100 %).
   - **Acuse de lectura**: **Leer el documento** lo abre y **Leído y entendido**
     lo firma desde el renglón. Para firmar varios, selecciónelos en la lista
     y pulse **Leído y entendido (seleccionados)** (también en **Documentos
     vigentes**). **Validar medición**: **Validar**.
   - Desde 57.150.0 cada tipo trae su propio botón en lugar de Hecho:
     - **NC** de la que es responsable: **Registrar paso** abre el paso que
       toca (contención, causa raíz o plan de acción con responsable y
       compromiso). La verificación y el cierre siguen en la ficha de la NC.
     - **Requisito legal**: **Evaluar** abre la evaluación de cumplimiento
       (resultado, evidencia y próxima fecha).
     - **Documento por revisar**: **Revisar** pregunta si sigue vigente sin
       cambios (nueva revisión a dos años, con nota en su historial) o si
       necesita cambio (abre la solicitud de cambio ya ligada al documento).
     - **Solicitud** de Aprobaciones y **aprobación** de un documento:
       **Aprobar** desde el renglón; **Rechazar**, en la ficha.
     - **Revisión de medición**: **Abrir** los tres registros y, en la ficha,
       **Sí, es lo que hago** o **No corresponde**.
     - **Firma** y **Capturar medición**: sus botones de siempre.
     El renglón se quita en ese momento, para usted y para los demás.
   Al marcar algo, Odoo muestra «Hecha: …» y el renglón desaparece de la
   lista en ese momento. Un **Escalamiento** (periodo de otra persona que
   pasó sus días de atraso) dice de quién es: **Ir a hacerlo** abre ese periodo y, si lo
   resolvió con la persona, puede marcarlo **Hecho** (con evidencia) o **No
   aplica este periodo** (con motivo); desaparece en ese momento para todos.
   Si la actividad todavía no lleva registro de cumplimiento, solo tiene Ir a hacerlo.
5. **Vence** se lee como «Hace 3 días», «Hoy» o «En 5 días». Los filtros
   **Avisos**, **Actividades** y **Escalamientos** los muestran solos.

### 2.2 Hacer una actividad de su procedimiento

1. Abra **SGI → Inicio → Mi procedimiento**. Arriba, una línea dice cuántas
   actividades tiene y cuántas van atrasadas. En la pestaña **Actividades**
   están las que usted ejecuta o aprueba, como tarjetas con el **cómo** y el
   **dónde**, en dos bloques: **Periódicas** (cada día, semana, mes…) y
   **Cuando pasa algo** (al llegar un pedido, una orden, una queja). Lo
   atrasado va primero.
   - **Atrasada** (rojo): le toca a usted.
   - **Atrasada en la planta** (gris): hay trabajo sin terminar en toda la
     planta (por ejemplo, órdenes sin cerrar). No es solo suyo: le da
     seguimiento el dueño del proceso.
   - **Al día** o **Sin medición automática**: nada pendiente que el sistema vea.
2. Pulse **Ir a hacerlo**: lo lleva al menú de Odoo donde se hace
   (una orden de producción, una recepción, una solicitud…).
3. **Abrir** muestra la actividad completa; ahí están el **Instructivo** y
   **Proponer cambio**.
4. Haga el trabajo en Odoo como siempre. El sistema mide solo que se hizo:
   no hay que marcar nada aparte.

### 2.3 Leer y firmar su procedimiento

Cuando el Jefe MAST y SGI publica el procedimiento de su puesto, le llega un
pendiente de firma.

1. En **Mi procedimiento**, lea las actividades, los documentos y el EPP.
2. Pulse **Firmar leído y entendido**. Solo usted puede firmar el suyo.
   Si no tiene usuario de Odoo, firma en la **tableta de planta** de su
   área: elija su nombre, teclee su PIN (se lo da RH) y firme sus acuses.
3. Si necesita el papel, **Imprimir** genera el PDF.

Lo mismo con cada documento nuevo que aplique a su puesto: en
**Documentos vigentes**, el filtro **Mis acuses pendientes** los muestra y
el botón **Leído y entendido** deja su acuse. Para ver todos los vigentes,
quite el filtro **Míos**; puede buscar por clave (también la anterior, la del
Dropbox), por tipo o por proceso, y filtrar Procedimientos, Instructivos o
Formatos. Mi procedimiento no sale aquí: tiene su propia entrada en Inicio.

### 2.4 Capturar una medición (si tiene un indicador a su cargo)

1. El pendiente «Capturar medición» aparece en Mis pendientes. Tiene
   **5 días hábiles**.
2. Ábralo, capture el valor (o numerador y denominador) y pulse **Marcar
   capturado**.
3. Si el semáforo sale en rojo, capture la **causa** y el **plan de acción**.
4. Los indicadores automáticos se calculan solos; usted solo los revisa.

### 2.5 Levantar una no conformidad o una queja

- **No conformidad:** **SGI → Reportar → No conformidad** (o **SGI → Mejora →
  No conformidades → Nuevo**). Describa la desviación, el proceso donde la
  detectó y quién debe contestarla. El sistema le pone folio y plazos.
- **Queja o sugerencia del personal:** **SGI → Reportar → Queja o
  sugerencia** (la lista está en **SGI → Mejora → Quejas y sugerencias del
  personal**), o por correo al buzón `quejas-sugerencias@`.
- **Incidente o casi accidente de seguridad:** **SGI → Reportar → Casi
  accidente o incidente** (la lista está en **SGI → Seguridad y ambiente →
  Incidentes y accidentes**). Cualquiera puede reportar; reportar un
  casi accidente también cuenta. Mientras esté «Reportado» puede corregirlo, y
  después puede consultar cómo se cerró. Si a usted lo invitan a investigar un
  incidente, queda en el equipo de investigación (ISO 45001 pide que
  participen trabajadores).

### 2.6 Proponer un cambio a su actividad

Si una actividad ya no se hace así, o falta una:

1. En **Mi procedimiento**, en la actividad, pulse **Abrir** y luego
   **Proponer cambio** (o **Proponer actividad** arriba, para una nueva).
2. Conteste las seis preguntas con sus palabras: **qué se hace**, **cada
   cuándo**, **quién la hace**, **dónde se hace**, **cómo sabe que quedó
   bien** y **por qué** la propone. Abajo ve cómo quedará escrita en su
   procedimiento y, si algo falta o no se entiende, un aviso.
3. Pulse **Enviar propuesta**. Va a Aprobaciones: la revisan su jefe, el
   dueño del proceso y el Jefe MAST, que completa lo técnico (instructivo,
   formatos, a quién se escala). Cuando se aprueba, la actividad cambia
   sola.

### 2.7 Checklist de planta (capturista)

1. En **SGI → Inicio → Checklists de hoy** (también en **Mantenimiento →
   Checklists de hoy**) están las hojas del día de su equipo o unidad.
2. Toque **Bien**, **Falla** o **No aplica** en cada punto (un toque). Si casi
   todo está bien, marque las fallas y pulse **Marcar el resto como Bien**.
3. Escriba la observación en las fallas.
4. Pulse **Terminar checklist** y elija su nombre (el PIN solo se pide si el
   Jefe MAST lo activó).
5. Una hoja firmada ya no se cambia. De las fallas, Mantenimiento crea los
   correctivos.

Las hojas del día están listas desde las 05:30 (los días festivos del
calendario del SGI no hay hoja).

## 3. Lo que le llega solo

- **Actividades en Odoo** (el reloj de la barra superior) cuando algo vence:
  una acción de NC, una medición por capturar, un documento por firmar.
- **Escalamientos:** si una acción suya pasa de su fecha compromiso, se avisa
  a su jefe y, más tarde, a Dirección.
- **Correo semanal** con sus atrasos de Mis pendientes, si el Jefe MAST lo
  activa (cada quien puede apagarlo).

## 4. Lo que no puede hacer y a quién pedirlo

| No puede… | Pídalo a… |
|---|---|
| Cerrar una NC sin causa raíz, acciones terminadas (las correctivas, con evidencia) y verificación de eficacia **Eficaz** | Complete lo que falta; es el candado de la norma, no un error. La cierran el dueño del proceso o el Jefe MAST |
| Modificar una NC ya cerrada | El Jefe MAST; el dueño del proceso puede reabrirla |
| Cancelar una NC | Pulse **Cancelar** con el motivo: la cancelación la aprueba el Jefe MAST y SGI |
| Cambiar una actividad directamente | Use **Proponer cambio** |
| Ver exámenes médicos o salarios | Son solo de Salud ocupacional y de RH |
| Obtener usuario o grupo | Su jefe; lo autoriza Dirección y lo crea Sistemas |

## 5. Dónde quedó lo que usaba en el Dropbox

Busque la clave anterior (por ejemplo, la de un formato en Excel) en **SGI →
Sistema → Del Dropbox a Odoo → Buscador por clave anterior**. Le dice qué es
hoy y **Abrir en Odoo** lo lleva a la pantalla que lo sustituye. Más detalle
en [../transicion/del-dropbox-a-odoo.md](../transicion/del-dropbox-a-odoo.md).

## 6. Preguntas frecuentes

- **«No me deja cerrar la NC.»** Falta la causa raíz, una acción sin fecha de
  término, la acción correctiva no tiene evidencia (una nota o un archivo) o
  la verificación de eficacia no dice **Eficaz** o se registró antes de la
  fecha programada. En una NC mayor, además, los 5
  porqués y confirmar que la lección se aplicó al AMEF, al plan de control o
  al documento.
- **«La NC ya está cerrada y no me deja cambiarla.»** Ya cerrada, solo el Jefe
  MAST la modifica. Si usted es el dueño del proceso, reábrala cambiando solo
  la etapa y después capture el cambio (por ejemplo, una verificación **No
  eficaz**).
- **«Me llegó una actividad que no entiendo.»** Ábrala: siempre apunta al
  registro (NC, documento, medición) y trae la explicación.
- **«¿Sigo llenando el Excel?»** No, si el formato ya está migrado a Odoo.
  Un dato, un lugar: lo que está en Odoo es la verdad.
- **«¿Y los registros viejos?»** Quedaron en el respaldo del Dropbox, de solo
  lectura. La historia nueva empieza en Odoo.
