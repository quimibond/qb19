# Del Dropbox a Odoo

Guía para el personal y para el auditor externo: qué cambió, cómo encontrar
en Odoo lo que antes estaba en el Dropbox y cómo se demuestra que nada se
perdió en el camino.

## 1. Qué cambió y por qué

Antes, el SGI eran procedimientos en PDF y formatos en Excel en el Dropbox.
Cada procedimiento describía rutinas («el jefe de almacén revisa…»), pero no
había forma de saber si se hacían, quién las hacía ni cuándo.

Hoy cada rutina es una **actividad** de un **proceso** en Odoo, con:

- quién la hace (un puesto, una familia de puestos o un rol relativo);
- cuándo vence (día hábil del mes, día de la semana, fecha anual o por
  evento);
- dónde se hace en Odoo (botón **Ir a hacerlo**);
- cómo se sabe que se hizo (la medición automática sobre los registros de
  Odoo), y a quién le escala si se atrasa.

Cada persona ve sus actividades en **SGI → Inicio → Mi procedimiento** y lo
que le falta en **Mis pendientes**. Los formatos que se llenaban en Excel
ahora se llenan en la pantalla de Odoo que los sustituye (una orden de
producción, una recepción, una NC, una hoja de checklist…), con su clave y
revisión impresas en el PDF.

## 2. La regla de convivencia

- Cuando un formato se declara **Migrado a Odoo**, el Excel se deja de
  llenar. Como máximo, un mes de doble captura mientras se valida.
- **Un dato, un lugar:** si está en Odoo, Odoo es la verdad.
- La clave anterior del Dropbox **no** aparece en las pantallas del día a
  día: solo en «Del Dropbox a Odoo». En pantalla se usa la clave nueva
  (`PR-{proceso}`, `F-{proceso}-{nn}`, `IT-…`, `DA-…`); los archivos no se
  renombraron. Desde 57.84.0 (octubre de 2026) los formatos, formatos de
  instructivo, instructivos, DAT y protocolos (`PROT-{proceso}-{nn}`) ya
  tienen su clave nueva, y los procedimientos «No aplica (se queda)» son
  controles operacionales (`CO-{proceso}-{nn}`). Conservan la del Dropbox los
  procedimientos «En curso», los formularios de Odoo, los anexos y los
  reglamentos ([formatos-bloque-3.md](formatos-bloque-3.md) §3).

## 3. Cómo usar «Del Dropbox a Odoo»

Menú **SGI → Procesos → Del Dropbox a Odoo**:

| Submenú | Quién lo ve | Para qué |
|---|---|---|
| **Buscador por clave anterior** | Todos | Escriba la clave del Dropbox (procedimiento, formato, instructivo, DAT o rutina): dice qué es hoy y **Abrir en Odoo** lo lleva a la pantalla que lo sustituye |
| **Formatos y documentos anteriores** | Todos | Cada formato, instructivo, DAT, anexo, protocolo y reglamento del Dropbox, con su estado de migración y su destino en Odoo |
| **Procedimientos anteriores** | Dueños de proceso, Jefe MAST, Dirección y Auditor | Cada procedimiento del Dropbox: qué proceso de Odoo lo sustituye, en qué estado va y cuántas de sus rutinas ya están resueltas; **Ver el PDF** abre el histórico |
| **Rutina por rutina** | Los mismos | Cada rutina de cada procedimiento y qué pasó con ella |
| **Avance de la transición** | Los mismos | Un renglón por proceso: procedimientos sustituidos y con pendientes, rutinas resueltas y documentos que ya viven en Odoo o siguen en papel |

### Estados de una rutina

| Estado | Significa |
|---|---|
| Cubierta por una actividad | Una actividad de Odoo la hace (se ven su numeral y la actividad) |
| La hace Odoo | Odoo la hace solo, o era redundante |
| Eliminada a propósito | Se dejó a propósito, con motivo |
| Pendiente | Nadie la cubre todavía: necesita decisión (crear actividad, volverla regla o automatización, o eliminarla con motivo) |

### Estados de migración de un formato o documento

| Estado | Significa |
|---|---|
| Pendiente | Todavía no se decide su destino |
| En curso | Se está pasando a Odoo |
| Migrado a Odoo | Ya se llena en Odoo; el Excel se dejó de usar |
| Baja tramitada | Desapareció, con su motivo |
| No aplica (se queda) | Sigue como documento (por ejemplo, un control operacional) |

## 4. Dónde va la transición (29-sep-2026)

- **Procedimientos del Dropbox:** 52. De ellos, 23 ya están sustituidos por
  un proceso de Odoo, 21 tienen rutinas pendientes, 5 se quedan como
  documentos de control operacional y uno (P-I01) se trata aparte por
  decisión de Dirección.
- **Rutinas:** 815 de 49 procedimientos: 709 cubiertas por actividades, 68
  que ya hace Odoo y 38 pendientes, con fecha límite de decisión el 16 de
  octubre de 2026 (las ve en rojo «Avance de la transición»).
- **Documentos con clave anterior:** 487 en producción.

Las cifras vivas están siempre en **Avance de la transición**.

## 5. Qué pasa con los registros viejos

Los registros ya llenados (actas, concentrados, Excel de años pasados)
quedaron en el respaldo del Dropbox, de solo lectura. La historia nueva
empieza en Odoo. El PDF de cada procedimiento anterior se conserva y se abre
desde **Procedimientos anteriores → Ver el PDF**.

## 6. Para el auditor externo: la trazabilidad antes → ahora

1. **Del procedimiento a las actividades:** en **Procedimientos anteriores**,
   cada procedimiento dice qué proceso lo sustituye; **Rutina por rutina**
   muestra, rutina por rutina, la actividad (con su numeral) que la cubre o
   por qué ya no aplica.
2. **Del formato al registro:** en **Formatos y documentos anteriores**, cada
   formato dice dónde vive en Odoo; los registros de Odoo imprimen la clave
   y la revisión vigente del formato.
3. **Difusión:** cada procedimiento de puesto («Mi procedimiento») y cada
   documento vigente tiene sus **acuses de lectura** firmados
   (Administración SGI → Firmas de lectura → Acuses de lectura).
4. **Control de cambios:** los cambios a actividades pasan por Aprobaciones
   (propuestas de cambio) y los cambios a documentos por la categoría de
   cambio documental; todo queda en el historial del registro.

## 7. Preguntas frecuentes

- **«Busqué la clave y no aparece.»** Revise la escritura (con guiones). Si
  sigue sin aparecer, avise al Jefe MAST y SGI: puede que ese documento no
  tenga registrada su clave anterior.
- **«El Excel sigue en el Dropbox, ¿lo lleno?»** Solo si su estado no es
  «Migrado a Odoo». Si ya lo es, use la pantalla de Odoo.
- **«Mi rutina de siempre no aparece en mi procedimiento.»** Búsquela en
  **Rutina por rutina**: puede estar cubierta por otra actividad, hacerla
  Odoo solo o estar pendiente de decisión. Si nadie la cubre y usted cree que
  se necesita, **Proponer actividad** desde Mi procedimiento.
