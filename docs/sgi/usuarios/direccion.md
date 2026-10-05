# Manual de Dirección

Para quien tiene el grupo **Dirección de Operaciones (SGI)**. Dirección
**consulta y aprueba**: ve todo el SGI (salvo exámenes médicos y salarios),
tiene su propio procedimiento como cualquier Usuario SGI y aprueba lo que le
toca, pero **no** tiene los permisos de configuración del Jefe MAST y SGI.

## 1. Qué ve al entrar

Al tocar la app **SGI** se abre directamente el **Tablero** (desde 57.98.0).
Sus propios pendientes siguen en **SGI → Inicio → Mis pendientes**.

| Menú | Para qué |
|---|---|
| **SGI → Dirección → Tablero** | Indicadores de dirección, salud del SGI (desde 57.99.0), rojos sin causa ni plan, procesos con más atrasos y acuerdos de la revisión vencidos |
| **SGI → Dirección → Revisión por la dirección** | La reunión de revisión: entradas, acuerdos y seguimiento |
| **SGI → Dirección → Política integral / Objetivos integrales** | La política vigente y los objetivos del año con su semáforo |
| **SGI → Dirección → Riesgos y oportunidades / Requisitos legales / Partes interesadas / Satisfacción del cliente** | Contexto, riesgos y cumplimiento |
| **SGI → Administración SGI** | Consulta de documentos, indicadores, aprobaciones y diagnóstico |
| **SGI → Inicio** | Sus propios pendientes y su procedimiento |
| **SGI → Reportar** | Una no conformidad, un casi accidente o incidente, o una queja o sugerencia (abre la ficha nueva) |

## 2. Su día

### 2.1 Revisar el tablero

1. Toque la app **SGI** (o **SGI → Dirección → Tablero**). Se calcula al
   abrirlo.
2. Revise primero **Rojos sin causa ni plan** y **Acuerdos de la RxD
   vencidos**: son lo que está detenido.
3. **Hoja de cálculo** abre el tablero en hoja de cálculo para analizarlo.
4. La página **Salud del SGI** (desde 57.99.0) dice si el SGI se está usando:
   diez indicadores SG-01 a SG-10 con el valor de la semana pasada, su
   semáforo y la semana anterior, y la tabla **Por dueño de proceso** (ver
   2.5).

Desde 57.104.0, un indicador sin ninguna medición con dato dice **Sin dato**
en el Tablero, en los «Últimos 6 periodos» y en el texto de la revisión por
la dirección, en lugar de 0. Un 0 que sí se midió se muestra como 0.

### 2.2 La revisión por la dirección

1. **Revisión por la dirección → Nuevo**, con el periodo (desde y hasta).
2. **Cargar entradas** llena las 18 entradas de la norma (auditorías, NC,
   indicadores en rojo, quejas, riesgos altos, cambios, satisfacción,
   proveedores y, desde 57.97.0, incidentes y desempeño de SST, cambios en
   el contexto y las partes interesadas, aspectos ambientales significativos
   y oportunidades de mejora) con los datos reales del periodo, y trae los
   **acuerdos abiertos de revisiones anteriores**. Se pueden ajustar. Desde
   57.104.0, «Validar mediciones» no valida las mediciones manuales en 0 sin
   nota: las lista en el chatter de la revisión para que su responsable las
   capture.
3. En la reunión, capture los **Acuerdos** con responsable y fecha.
4. Escriba las **Conclusiones (9.3.3)**: conveniencia, adecuación, eficacia
   y mejora, cambios y recursos. Sin ellas no se marca realizada; si no hay
   cambios, escríbalo.
5. **Marcar realizada**: cada acuerdo se vuelve una acción del tipo
   «Acuerdo» con seguimiento. Cerrar la revisión o regresarla a borrador lo
   hace el Jefe MAST y SGI. Cerrar con acuerdos abiertos se permite: quedan
   anotados en el historial y la siguiente revisión los carga.

### 2.3 Aprobar lo que le toca

Las aprobaciones llegan a **Mis pendientes** y a la app **Aprobaciones**:
cambios documentales, propuestas de cambio en las que usted es jefe directo
o dueño del proceso, y las actividades en las que su puesto «Aprueba».

### 2.4 Política, objetivos y riesgos

- **Política integral**: solo una puede estar vigente. **Generar acuses** la
  difunde con firma a los puestos del documento controlado donde se publica.
- **Objetivos integrales**: el semáforo de cada objetivo toma el peor color
  de sus indicadores.
- **Riesgos y oportunidades**: un riesgo alto sin acción abierta queda
  marcado y le llega el aviso al dueño del proceso; en la revisión por la
  dirección aparecen los de atención inmediata o alta.

### 2.4.1 Fichas de indicador y mapa de calor (desde 57.101.0)

- **Ficha del indicador:** en el indicador, **Ficha en PDF** (o Imprimir)
  da una hoja con cómo se mide, las metas, la gráfica de los últimos 12
  periodos con medición (franjas verde, amarilla y roja con las metas de cada
  periodo) y la causa y acciones de los rojos. Desde el proceso, Imprimir →
  **Fichas de indicadores del proceso** da todas.
- **Mapa de calor de riesgos:** en la lista de riesgos, Imprimir → **Mapa de
  calor de riesgos**: una hoja por instrumento (R&O, IPER, ambiental) con la
  cuadrícula probabilidad × impacto inicial y residual y los folios en cada
  celda. El color de cada celda es el nivel real de la escala del
  instrumento.
- La **tendencia** de un indicador promedia las mediciones del periodo (antes
  las sumaba) y abre con el filtro «Con dato».

### 2.5 El correo de los lunes (salud del SGI)

Cada lunes a las 08:00 le llega «SGI: salud del sistema, semana del…» con
lo mismo que la página **Salud del SGI** del Tablero:

- **Indicadores:** clave, valor de la semana pasada, meta, semáforo, semana
  anterior y una nota (por ejemplo, cuántas NC llevan más de 60 días
  abiertas o qué parte de los avisos vencidos tiene una sola persona, sin
  nombre).
- **Por dueño de proceso:** avisos del SGI vencidos, validaciones de
  mediciones atrasadas y días desde su último movimiento en el SGI. «Más de
  90» es que no ha tocado el SGI en tres meses; «—», que el dueño no tiene
  usuario. Un dueño con dos procesos sale en los dos renglones con las
  mismas cifras.
- Solo conteos; sin datos de salud ni de nómina.

Para agregar o quitar destinatarios, o para que alguien no cuente en
«Personas que usan el SGI», pídalo al Jefe MAST (parámetros
`quimibond_sgi.health_mail_user_ids` y `quimibond_sgi.health_excluded_user_ids`).
Los diez nacen **en prueba**: el Jefe MAST revisa su primera medición contra
la realidad y los pasa a oficial.

## 3. Lo que le llega solo

- Correo crítico de NC mayor e incidentes graves o fatales.
- Desde 57.99.0, el correo de los lunes con la salud del SGI (ver 2.5).
- Acciones vencidas que escalan a Dirección (después del jefe directo del
  responsable).

## 4. Lo que no hace

| No hace… | Lo hace… |
|---|---|
| Configurar procesos, documentos, tipos o parámetros | Jefe MAST y SGI |
| Cerrar o reabrir la revisión por la dirección | Jefe MAST y SGI |
| Cancelar o forzar el cierre de una NC | Jefe MAST y SGI |
| Ver exámenes médicos o salarios de eficiencias | Salud ocupacional; RH y Nóminas |
| Crear usuarios | Dirección los decide; Sistemas los crea |

## 5. Preguntas frecuentes

- **«¿Por qué un proceso sale en rojo?»** Tiene un riesgo de atención máxima
  abierto, o una NC abierta junto con un indicador en rojo.
- **«¿Cómo sé si una cifra del tablero es confiable?»** Cada indicador dice si
  está en prueba u oficial; las mediciones traen **Registros** con la evidencia
  de dónde salió el número.
