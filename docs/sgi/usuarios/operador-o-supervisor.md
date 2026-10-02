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
| **Documentos vigentes** | Los procedimientos, instructivos y formatos vigentes, con su revisión |
| **Mis indicadores** | Los indicadores a su cargo, si tiene alguno |
| **Mi equipo** | Su equipo de trabajo (si tiene personas a su cargo) |

Además puede usar **SGI → Mejora** (no conformidades, reclamaciones, mejoras,
quejas y sugerencias) y **SGI → Seguridad y ambiente** (reportar un
incidente, consultar planes de emergencia).

> Si no ve el menú SGI, no está en el grupo Usuario SGI: pídalo a su jefe;
> los usuarios los autoriza la Dirección y los crea Sistemas.

## 2. Su día

### 2.1 Revisar Mis pendientes

1. Abra **SGI → Inicio → Mis pendientes**.
2. Arriba aparecen primero los **atrasados**, luego lo que **vence esta
   semana**. Cada renglón dice qué es (acción, NC, medición, acuse, firma,
   aprobación, actividad atrasada…), de qué proceso y cuándo vence.
3. Pulse **Abrir** para ir al registro y resolverlo. Al resolverlo, el
   renglón desaparece solo.
4. Los avisos que Odoo le manda desde el SGI (actividades vencidas o que
   vencen esta semana) también salen en la misma lista, como **Aviso**.
   Cuando lo atienda, pulse **Hecho** en el renglón: el aviso se cierra en
   Odoo y sale de la lista. El filtro **Avisos** los muestra solos.

### 2.2 Hacer una actividad de su procedimiento

1. Abra **SGI → Inicio → Mi procedimiento**. En la pestaña **Actividades**
   están las que usted ejecuta o aprueba, con su estado (al día, atrasada o
   sin medición automática).
2. Pulse **Ir a hacerlo**: lo lleva al menú de Odoo donde se hace
   (una orden de producción, una recepción, una solicitud…).
3. Si hay instructivo, **Instructivo** lo abre.
4. Haga el trabajo en Odoo como siempre. El sistema mide solo que se hizo:
   no hay que marcar nada aparte.

### 2.3 Leer y firmar su procedimiento

Cuando el Jefe MAST y SGI publica el procedimiento de su puesto, le llega un
pendiente de firma.

1. En **Mi procedimiento**, lea las actividades, los documentos y el EPP.
2. Pulse **Firmar leído y entendido**. Solo usted puede firmar el suyo.
3. Si necesita el papel, **Imprimir** genera el PDF.

Lo mismo con cada documento nuevo que aplique a su puesto: en
**Documentos vigentes**, el filtro **Mis acuses pendientes** los muestra y
el botón **Leído y entendido** deja su acuse.

### 2.4 Capturar una medición (si tiene un indicador a su cargo)

1. El pendiente «Capturar medición» aparece en Mis pendientes. Tiene
   **5 días hábiles**.
2. Ábralo, capture el valor (o numerador y denominador) y pulse **Marcar
   capturado**.
3. Si el semáforo sale en rojo, capture la **causa** y el **plan de acción**.
4. Los indicadores automáticos se calculan solos; usted solo los revisa.

### 2.5 Levantar una no conformidad o una queja

- **No conformidad:** **SGI → Mejora → No conformidades → Nuevo**. Describa
  la desviación, el proceso donde la detectó y quién debe contestarla. El
  sistema le pone folio y plazos.
- **Queja o sugerencia del personal:** **SGI → Mejora → Quejas y sugerencias
  del personal**, o por correo al buzón `quejas-sugerencias@`.
- **Incidente o casi accidente de seguridad:** **SGI → Seguridad y ambiente →
  Incidentes y accidentes → Nuevo**. Cualquiera puede reportar; reportar un
  casi accidente también cuenta. Mientras esté «Reportado» puede corregirlo, y
  después puede consultar cómo se cerró.

### 2.6 Proponer un cambio a su actividad

Si una actividad ya no se hace así, o falta una:

1. En **Mi procedimiento**, en la actividad, pulse **Proponer cambio** (o
   **Proponer actividad** arriba, para una nueva).
2. Cambie solo lo que debe cambiar (quién, cuándo, dónde, cómo) y pulse
   **Enviar propuesta**.
3. La propuesta va a Aprobaciones: la revisan su jefe y el dueño del
   proceso. Cuando se aprueba, la actividad cambia sola.

### 2.7 Checklist de planta (capturista)

1. En **Mantenimiento → Checklists de hoy** están las hojas del día de su
   equipo o unidad.
2. Abra la hoja, marque cada punto y, si algo falla, anótelo.
3. Al final pulse **Terminar**, elija su nombre y, si está activado, capture
   su **PIN** de empleado.
4. Si marcó fallas, **Crear correctivos de las fallas** abre una solicitud
   correctiva por cada una para mantenimiento. Avise a su supervisor si la
   falla detiene la máquina.

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
| Cerrar una NC sin causa raíz, acciones terminadas y verificación de eficacia | Complete lo que falta; es el candado de la norma, no un error |
| Cancelar una NC | Pulse **Cancelar** con el motivo: la cancelación la aprueba el Jefe MAST y SGI |
| Cambiar una actividad directamente | Use **Proponer cambio** |
| Ver exámenes médicos o salarios | Son solo de Salud ocupacional y de RH |
| Obtener usuario o grupo | Su jefe; lo autoriza Dirección y lo crea Sistemas |

## 5. Dónde quedó lo que usaba en el Dropbox

Busque la clave anterior (por ejemplo, la de un formato en Excel) en **SGI →
Procesos → Del Dropbox a Odoo → Buscador por clave anterior**. Le dice qué es
hoy y **Abrir en Odoo** lo lleva a la pantalla que lo sustituye. Más detalle
en [../transicion/del-dropbox-a-odoo.md](../transicion/del-dropbox-a-odoo.md).

## 6. Preguntas frecuentes

- **«No me deja cerrar la NC.»** Falta la causa raíz, una acción sin fecha de
  término o la verificación de eficacia. En una NC mayor, además, los 5
  porqués y confirmar que la lección se aplicó al AMEF, al plan de control o
  al documento.
- **«Me llegó una actividad que no entiendo.»** Ábrala: siempre apunta al
  registro (NC, documento, medición) y trae la explicación.
- **«¿Sigo llenando el Excel?»** No, si el formato ya está migrado a Odoo.
  Un dato, un lugar: lo que está en Odoo es la verdad.
- **«¿Y los registros viejos?»** Quedaron en el respaldo del Dropbox, de solo
  lectura. La historia nueva empieza en Odoo.
