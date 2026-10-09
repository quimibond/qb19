# Manual del jefe de área o dueño de proceso

Para jefes con personas a su cargo, para los **dueños de proceso** (grupo
«Dueño de proceso (SGI)», que se asigna solo cuando la persona es dueña de
un proceso activo) y para quien captura eficiencias (grupo «Captura de
eficiencias»). Primero conviene leer
[operador-o-supervisor.md](operador-o-supervisor.md): todo lo de ahí también
aplica a usted.

## 1. Qué ve al entrar

Lo mismo que cualquier Usuario SGI, más:

| Menú | Para qué |
|---|---|
| **SGI → Inicio → Mi equipo** | Su equipo en organigrama, con el estado SGI de cada persona (pendientes, atrasos, firma de su procedimiento) |
| **SGI → Inicio → Eficiencias de mi área** | La hoja mensual de eficiencias (solo con el grupo Captura de eficiencias) |
| **SGI → Sistema → Entregables, Flujos entre procesos, Matriz de responsabilidades, Puestos y procesos, Fichas de proceso por máquina** | Los catálogos de los procesos (desde 57.98.0 solo para dueños de proceso, Jefe MAST, Dirección y Auditoría) |
| **SGI → Sistema → Del Dropbox a Odoo** | Como dueño de proceso, además del buscador: Procedimientos anteriores, Rutina por rutina y Avance de la transición |
| **SGI → Planeación → Riesgos y oportunidades** | Los riesgos de sus procesos |

En las listas, el filtro **Míos** muestra lo suyo (acciones, NC, mediciones,
actividades, procesos, permisos…) y, en AMEF y riesgos, **De mis procesos**
muestra los de los procesos de los que usted es dueño.

## 2. Su día

### 2.1 Revisar a su equipo

1. **SGI → Inicio → Mi equipo**. Cada tarjeta dice cuántos pendientes y
   atrasos tiene la persona y si ya firmó su procedimiento.
2. **Pendientes del equipo** abre los pendientes de todos juntos.
3. **Ver su procedimiento** abre el Mi procedimiento de una persona como ella
   lo ve.
4. **Acuses pendientes de su gente.** Si alguien de su equipo sin usuario de
   Odoo (o con usuario pero sin permiso para abrir el documento) lleva más de
   7 días hábiles sin firmar un documento, le llega **un** aviso «Acuses
   pendientes de su gente: N (P personas)», no uno por acuse ni por
   documento. El aviso aparece sobre su departamento; en Mis pendientes,
   **Ir** abre la lista de esos acuses (solo los que ya pasaron el plazo).
   Quien de su gente tiene usuario y puede abrir el documento recibe su propio
   aviso «Documentos por leer y firmar». Si usted no tiene departamento en su
   ficha de empleado,
   este aviso no le llega a usted sino al Jefe MAST: pida a RH que le asigne
   su departamento.
5. Los filtros de Mi equipo («Con pendientes atrasados», «Con pendientes por
   vencer», «Al día») usan el resumen de la noche o de la última vez que se
   abrió la lista de pendientes de la persona; las cifras de cada tarjeta son
   las de este momento. Si un filtro no coincide con la tarjeta, abra
   **Pendientes del equipo** y vuelva a filtrar.

### 2.2 Atender escalamientos

Cuando una actividad de su gente se atrasa, le escala a usted.

1. En **Mi procedimiento**, la pestaña **Escalamientos** lista las
   actividades que le escalan; **Ver atrasados** abre las que ya vencieron.
2. Hable con quien la ejecuta o reasígnela con una propuesta de cambio.

Como dueño de proceso también recibe los plazos vencidos de las NC de su
proceso (contención, causa raíz, plan) y los riesgos con revisión vencida.
Antes de mover una NC de su proceso a Seguimiento, capture su clasificación
y la cláusula que se incumplió.

### 2.3 Validar las mediciones de sus indicadores

1. En **Mis pendientes**, los renglones **Validar medición** son mediciones
   capturadas o calculadas de sus indicadores. Tiene **3 días hábiles**.
2. Revise el valor y la evidencia (**Registros** en la medición) y pulse
   **Validar**, desde el renglón o desde la medición.
3. Si el valor está mal, corríjalo antes de validar. Un rojo necesita causa y
   plan de acción; si el indicador tiene «NC en rojo», se levanta una NC.
4. Para validar varias a la vez, selecciónelas y pulse **Validar
   seleccionadas** (arriba de la lista). Solo se validan las de sus
   indicadores; las demás se dejan y un aviso le dice cuántas.

Mis pendientes abre con el filtro **Atrasadas o por vencer**. Las
validaciones que vencen más adelante no se ven con ese filtro: quítelo en
la barra de búsqueda para validarlas por adelantado.

Desde 57.101.0, **Tendencia** promedia las mediciones del periodo (los
indicadores semanales se ven por semana) y abre con el filtro «Con dato»:
quítelo si quiere ver también los pendientes y los «sin dato». **Ficha en
PDF** imprime la hoja del indicador con su gráfica, sus metas y la causa y
acciones de los rojos.

**«Sin dato» y el 0 (desde 57.104.0).** Si ninguna medición de un
indicador tiene dato, su «Último valor» dice **Sin dato** (no 0). Para
capturar un 0 real en un indicador de captura manual, escriba en la nota por
qué es cero («0: sin caídas en el mes»): una medición manual en 0 sin nota,
sin numerador ni denominador no se marca capturada ni se valida. Si corrige
a mano el valor de un indicador automático, la medición queda «Valor
corregido a mano» y el recálculo diario ya no la toca.

### 2.4 Aprobar propuestas de cambio de su proceso

Las propuestas que hace su gente con **Proponer cambio** llegan a la app
**Aprobaciones** como «Proponer cambio a mi procedimiento (SGI)». Las aprueba
el jefe directo de quien propone y el dueño del proceso.

1. Abra la solicitud: el motivo muestra lo que cambia contra lo de hoy.
2. **Aprobar** aplica el cambio a la actividad; **Rechazar** la deja como
   estaba. Si quien aprueba también ejecuta o pidió, la aprobación sube a
   su jefe.

### 2.5 Capturar las eficiencias del mes (grupo Captura de eficiencias)

1. **SGI → Inicio → Eficiencias de mi área → Nuevo**, con el mes y el área.
2. **Cargar empleados del área** y, si aplica, **Calcular eficiencia desde
   órdenes de trabajo**.
3. Capture asistencia, orden y limpieza y calidad de cada persona.
4. **Cerrar** la hoja. RH la recibe con **Recibir (RH)**. Los importes solo
   los ven RH y Nóminas.

### 2.6 Revisar los riesgos de su proceso

1. **SGI → Planeación → Riesgos y oportunidades**, filtre por su proceso.
2. Cuando llega el aviso de revisión (enero y julio), actualice probabilidad e
   impacto y pulse **Registrar evaluación**.
3. Un riesgo alto sin acción abierta le llega como aviso: registre al menos
   una acción.
4. Desde 57.96.0, al registrar una acción de un riesgo IPER o de un
   incidente, indique la **Jerarquía del control**: un IPER de riesgo alto no
   se controla con equipo de protección personal (EPP) como único control.

Si usted autoriza permisos de trabajo: cuando uno vence y sigue autorizado,
le llega un aviso cada hora hasta que se cierre o se renueve. El permiso no
se cierra ni se cancela mientras un bloqueo (LOTO) ligado siga aplicado.

### 2.7 Decidir las rutinas pendientes del Dropbox

En **Del Dropbox a Odoo → Rutina por rutina**, filtre **Pendientes** de su
proceso. Cada rutina que nadie cubre necesita su decisión: crear una
actividad, volverla regla o automatización, o eliminarla con motivo. La fecha límite la fija el
parámetro `quimibond_sgi.legacy_decision_deadline` (hoy 16-oct-2026).

### 2.8 Instructivos en Conocimiento

Los instructivos, controles operacionales y protocolos de su proceso se pasan
a **Conocimiento → SGI → <su proceso>** como borradores que solo ven usted y
el Jefe MAST.

1. Abra **SGI → Sistema → Conocimiento del SGI** (filtro **Por publicar**).
2. Abra el borrador, compare el texto con el PDF adjunto y corríjalo: el
   texto se sacó del PDF y puede venir sin formato.
3. Cuando esté listo, pulse **Pedir publicación**. El Jefe MAST lo revisa y lo
   publica como revisión nueva del documento, con acuses para los puestos.

## 3. Lo que le llega solo

- Escalamientos de actividades y acciones atrasadas de su gente.
- Plazos vencidos de NC de su proceso.
- Mediciones por validar de sus indicadores.
- Avisos de revisión de riesgos y de documentos de su proceso.
- Un aviso por los acuses pendientes de su gente sin usuario (ver 2.1).
- Solicitudes de aprobación de propuestas de cambio.
- Permisos de trabajo vencidos que usted autorizó por el área (cada hora,
  hasta que se cierren o se renueven).
- **Eficacia de la capacitación (desde 57.100.0):** a los 90 días de que
  alguien de su equipo apruebe un examen o termine un curso que le da una
  competencia, le llega «Evaluar la eficacia de la capacitación». Abra la
  evaluación y diga si fue eficaz: «Eficaz», o escriba en «Comentario» qué
  observó y pulse «No eficaz» (RH reprograma la capacitación). Solo ve las
  evaluaciones que le tocan.

## 4. Lo que no puede hacer y a quién pedirlo

| No puede… | Pídalo a… |
|---|---|
| Cambiar directamente las actividades de su proceso | Proponga el cambio; al aprobarse queda aplicado |
| Publicar el procedimiento de un puesto | Jefe MAST y SGI |
| Crear procesos, cargar el catálogo o cambiar tipos de documento | Jefe MAST y SGI / Administrador SGI |
| Ver salarios de eficiencias o exámenes médicos | RH y Salud ocupacional |
| Cancelar una NC | La cancelación la aprueba el Jefe MAST y SGI |

## 5. Dónde quedó lo que usaba en el Dropbox

**Del Dropbox a Odoo → Procedimientos anteriores** muestra, por cada
procedimiento del Dropbox de su proceso, qué proceso de Odoo lo sustituye,
en qué estado va y cuántas de sus rutinas ya están resueltas. Ver
[../transicion/del-dropbox-a-odoo.md](../transicion/del-dropbox-a-odoo.md).

## 5.1 El Manual del SGI (MIID)

**SGI → Sistema → Manual del SGI (MIID)**: «PDF de la revisión vigente» es
el manual aprobado (el que se firma de leído); la pestaña «Vista del sistema»
es el manual con los datos de hoy y no está vigente. Si cambia el dueño, el
estado o los flujos de su proceso, el Jefe MAST recibe el aviso para
actualizar el MIID.

## 6. Preguntas frecuentes

- **«Una persona de mi equipo no ve su procedimiento.»** Revise que tenga
  puesto en su ficha de empleado y que el puesto tenga actividades. Si el
  puesto no está publicado, pídalo al Jefe MAST y SGI.
- **«Me toca aprobar algo que yo mismo hice o pedí.»** No debería: nadie
  aprueba lo que él mismo pidió. Cuando quien aprueba también ejecuta o pidió,
  aprueba el **suplente** nombrado en el rol «Aprueba» (un puesto que define
  Dirección); la aprobación no sube sola al jefe. Si no hay suplente, nadie
  aprueba y el SGI avisa: pídale al Jefe MAST y SGI que Dirección lo nombre.
- **«Mi jefe hizo un paso que me toca a mí.»** Cuenta como cumplido por
  suplencia: la medición lo distingue del puesto asignado y de «otro puesto».
  Los botones del desarrollo restringidos a un puesto los puede usar también
  el jefe directo de ese puesto.
- **«¿Quién me ve como atrasado?»** Su jefe y, si el atraso escala, quien
  tenga el rol «Escala» en esa actividad.
