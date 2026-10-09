# Formatos del SGI, bloque 3: lo que hizo el sistema y lo que queda para MAST

Propuesta de formatos aprobada completa por Jose el 2026-10-01 («arranca con
los formatos»; `docs/audit/decisiones.md`, 2026-09-30 y 2026-10-01). El bloque
3 se aplica con migraciones de `quimibond_sgi` (producción solo cambia al
actualizar el módulo). Cada sección dice qué cambió el sistema y qué tiene que
hacer MAST a mano.

Cifras: lectura de producción por MCP, solo lectura, compañía 1, 2026-10-01.

## 1. Duplicados y datos malos (57.82.0)

### Qué hizo el sistema

| Grupo | Se conserva | Se da de baja | Ligas que se movieron |
|---|---|---|---|
| Reporte de visita | F-P-A28-04 (3875) | 5152 F-P-V01-04 deja de ser **controlado** (es un reporte lleno); 3359 sigue archivado | C2.43 ahora tiene F-P-A28-04 |
| Calibración Wesco | F-IT-P-C05-07-01 (3902) | F-IT-P-P04-07-01 (4026) | C5.16 pierde el duplicado |
| Nota de venta | F-P-A16-07 (3774) | F-P-A23-08 (3752) | — |
| Solicitud de pruebas | F-P-C05-02 (3920) | F-P-P04-02 (4023) | C1.11: F-P-P04-02 → F-P-C05-02 |
| Lista de incidencias | F-P-A01-49 (3867) | F-P-A13-01 (3725) | — |
| Vales de salida | F-IT-P-A05-01-01 (3698) | F-P-C17-05 (3943) | C6.12 ahora también tiene F-IT-P-A05-01-01 |
| Órdenes de producción | F-P-P02-01, F-IT-P-P01-02-02, F-IT-P-P01-12-01 | F-IT-P-P01-01-03 (3989) y F-P-P01-01 (4001) | C4.14 pierde F-IT-P-P01-01-03 |
| F-P-E01-01 | — | — | La luminaria (4060) pasa a **F-P-S01-02** (SST, familia P-S01) y se liga a E2.31 (estudios de higiene y evaluaciones NOM). F-P-E01-01 queda libre para la matriz de aspectos ambientales |

**«Dar de baja» no es archivar.** En Documentos, archivar manda el documento a
la papelera y Odoo lo **borra solo** a los 30 días (`documents.deletion_delay =
30`). Por eso los duplicados quedan **activos, obsoletos**, con «Motivo de
obsolescencia» (de qué son duplicado) y estado de migración «Baja
tramitada». Se ven con el filtro de obsoletos.

Además se dieron de alta como **Formulario de Odoo** (sin archivo: lo que se
llena es la pantalla) dos claves que citan las rutinas y no existían:

| Clave | Qué es | Dónde vive | Actividad |
|---|---|---|---|
| F-P-A28-13 | Pronóstico de ventas (confección e industrial, una sola clave) | Ventas → Presupuesto y pronóstico → Pronósticos; el PDF del pronóstico ya imprimía esta clave (mapeo 9) | C2.40 |
| F-P-A28-11 | Encuesta de satisfacción del cliente | SGI → Desempeño → Satisfacción del cliente | E2.12 |

### Qué queda para MAST

1. **Papelera de Documentos (urgente).** Hoy hay 3 documentos controlados
   archivados: 3359 F-P-V01-04 (reporte de visita lleno), 5119 P-A28
   (procedimiento obsoleto) y 4995 DF-X-XXX-XX (plantilla de diagrama). Los
   tres se archivaron el 29-sep y **Odoo los borrará solos hacia el 29-oct**.
   Si se deben conservar como evidencia, restáurelos desde la papelera de
   Documentos (quedan obsoletos, no vuelven a estar vigentes).
2. **Matriz de aspectos ambientales en blanco (F-P-E01-01).** No hay una
   matriz en blanco en Documentos: 4850 es una carpeta de la cuarentena con
   subcarpetas por área, y 4868 («04.F-P-E01-01 … MAST Y SGI») es la matriz
   **llena** de MAST. Suba la matriz en blanco como formato controlado de E2
   (con la clave nueva D-02 que le toque y F-P-E01-01 como clave anterior),
   lígela a **E2.23** y al mapeo de «Aspectos ambientales» (SGI →
   Configuración → Formatos en documentos de Odoo). Mientras tanto, el PDF de
   la matriz imprime «F-P-E01-01» sin revisión.
3. **Nota de venta F-P-A16-07 (3774):** no hay una actividad de entrega de C2
   que sea claramente la suya. Candidatas: C2.33 «Entregar en ruta local y
   recabar la remisión firmada» o C2.42 «Crear cada mes el pedido de venta
   del desperdicio textil». Lígela usted.
4. **F-P-P01-01 «Orden de trabajo» (4001):** quedó dado de baja como dice la
   propuesta; abra el archivo y confirme si era la de mantenimiento
   (duplicado de F-P-M01-01) o de producción.
5. **Vale de laboratorio:** IT-P-A05-01 no aparece vigente (familia P-A05 sin
   procedimiento). Calidad confirma en la próxima revisión de P-C05 qué clave
   queda.
6. **Familia P-A13 (7 formatos en S2):** la propuesta la daba por mal
   asignada (a S4), pero sus formatos son reportes administrativos
   (anticipos, reporte de inventario, de facturación, de compras, de
   facturas de proveedores, resumen de importaciones) y una lista de
   asistencia del centro. No se movió. Si deben ir a otro proceso, cámbielos
   uno por uno.
7. **Encuesta de satisfacción:** configúrela en Ajustes del SGI (hoy
   `quimibond_sgi.satisfaction_survey_id = 0`); sin eso el menú, el cron y el
   indicador CA-02 no la ven.

## 2. Responsable SGI = dueño del proceso (57.83.0)

### Qué hizo el sistema

El responsable SGI de cada formato vigente (formato, F-IT, DAT, anexo y
formulario de Odoo) pasa de MAST al usuario del dueño de su proceso
(SGI → Sistema → Mapa de procesos → dueño del proceso). Solo se tocaron los que tenía MAST;
lo que alguien ya había reasignado se respeta. MAST conserva la aprobación y
la publicación (decisión del 2026-09-30).

| Proceso | Formatos | Nuevo responsable SGI |
|---|---|---|
| C1 Desarrollo y alta | 32 | Jessica Francisco |
| C2 Pedido a entrega | 31 (con F-P-A28-13) | Jessica Francisco |
| C3 Planeación | 7 | Paris César Villordo |
| C5 Calidad de producto | 60 | Oscar González |
| C6 Almacén | 10 | Cynthia Santana |
| E1 Dirección | 5 | Jorge Manuel Ortiz |
| S1 Compra a pago | 14 | Jorge Manuel Ortiz |
| S2 Facturación y cobranza | 7 | Irma Luna |
| S3 Contabilidad y costos | 9 | Irma Luna |
| S4 RH y nómina | 45 | Miguel Medina |
| S5 Mantenimiento | 18 | Manuel Juárez |
| **Cambian** | **238** | |
| E2 Gestión del SGI | 70 (con F-P-A28-11) | Blanca Ballesteros (la dueña es MAST) |
| C4 Producción | 64 | **Se quedan con MAST**: el dueño no tiene usuario |

### Para Jose: formatos que se quedan con MAST porque el dueño no tiene usuario

**C4 Producción**, dueño Francisco González Hernández (empleado 564, Jefe de
Manufactura), **sin usuario de Odoo**. Son 64: los 54 de abajo y 10 de la
familia P-I01 (7 DAT y F-P-I01-01 a -03), que ninguna migración toca. En
cuanto Francisco González tenga usuario (o se le ligue uno; ver la decisión
D-08 sobre «manufactura@»), basta con que MAST los reasigne o con volver a
correr `documents.document._sgi_owner_from_process()` desde un shell.

| # | Clave (Dropbox) | Título |
|---|---|---|
| 1 | DAT P-P01-01 | TABLA DE VISCOSIDADES |
| 2 | DAT P-P01-02 | FORMULA 26 |
| 3 | DAT P-P01-03 | Tabla de metraje para Jumbos (REV 01) |
| 4 | F-IT-P-P01-01-01 | Resultados de Laboratorio |
| 5 | F-IT-P-P01-01-02 | Gráfica de control |
| 6 | F-IT-P-P01-01-04 | CHECK LIST CARDA (REV 02) |
| 7 | F-IT-P-P01-01-05 | Analisis de resistencias |
| 8 | F-IT-P-P01-02-01 | Check List Cocina |
| 9 | F-IT-P-P01-02-02 | Orden de cocina |
| 10 | F-IT-P-P01-03-01 | BITACORA DE OPERACION (1) |
| 11 | F-IT-P-P01-04-01 | BITACORA DE OPERACION V18 |
| 12 | F-IT-P-P01-06-01 | CHECK LIST MÁQ. TEJIDO |
| 13 | F-IT-P-P01-06-02 | Calificacion de calidad en tejido |
| 14 | F-IT-P-P01-07-01 | CHEK LIST CORTE Y PERFORADO |
| 15 | F-IT-P-P01-08-01 | TARJETA VIAJERA (Formulario de Odoo) |
| 16 | F-IT-P-P01-08-02 | BITÁCORA DE TEJIDO CIRCULAR (Formulario de Odoo) |
| 17 | F-IT-P-P01-08-03 | CONTROL DE REVISADO DE TEJIDO (Formulario de Odoo) |
| 18 | F-IT-P-P01-08-04 | CONTROL DE OPERACIÓN MAQUINA DE TEJIDO |
| 19 | F-IT-P-P01-08-05 | FICHA TÉCNICA DE CONTROL DE PROCESO MÁQUINAS CIRCULARES |
| 20 | F-IT-P-P01-08-06 | REV CRUDO |
| 21 | F-IT-P-P01-08-07 | Check List Status Maquina Circular |
| 22 | F-IT-P-P01-08-08 | MANEJO DE MATERIAL EN MAQUINA DE TEJIDO CIRCULAR WJ035Q22HNT200 |
| 23 | F-IT-P-P01-08-09 | MANEJO DE MATERIAL EN REVISADO DE TEJIDO CIRCULAR WJ035Q22HNT200 |
| 24 | F-IT-P-P01-08-13 | REGISTRO DE TELA CON ACEITE |
| 25 | F-IT-P-P01-10-01 | IDENTIFICACIÓN DE ROLLOS DE TELA BASE QUE PASA DE RAMA A PUNTOS |
| 26 | F-IT-P-P01-10-02 | Check list Rama |
| 27 | F-IT-P-P01-10-03 | Identificación de Rollos en Rama |
| 28 | F-IT-P-P01-10-04 | IDENTIFICACION DE ROLLOS DE TELA BASE QUE PASAN DE RAMA A PUNTOS (2) |
| 29 | F-IT-P-P01-10-05 | BITACORA DE OPERACIÓN (1) |
| 30 | F-IT-P-P01-10-06 | ANCHOS DE TELA EN RAMA |
| 31 | F-IT-P-P01-12-01 | Orden de Trabajo (Teñido) |
| 32 | F-IT-P-P01-12-02 | SEGUIMIENTO DE TONO DE TINTORERIA |
| 33 | F-IT-P-P01-12-03 | SEGUIMIENTO DE TONO DE RAMA |
| 34 | F-IT-P-P01-12-04 | Bitacora de Condiciones de Operación del Jet Scholl 1 y 2 |
| 35 | F-IT-P-P01-13-01 | RESULTADOS DE TELA ACABADA |
| 36 | F-IT-P-P01-13-02 | Escala de Color (01) |
| 37 | F-IT-P-P01-13-03 | Formulación para Laboratorio de Tintorería (01) |
| 38 | F-IT-P-P01-13-04 | MASTER DE COLOR |
| 39 | F-IT-P-P01-13-05 | AUTORIZACIÓN DE TONOS |
| 40 | F-IT-P-P01-13-06 | IGUALACION DE TONO |
| 41 | F-IT-P-P01-15-01 | CHECK LIST PARA COCINA DE COLORES |
| 42 | F-IT-P-P01-20-01 | CONTROL DE PRUEBAS DE JARRAS |
| 43 | F-IT-P-P01-20-05 | PARAMETROS DE AGUA PARA PROCESO (1) |
| 44 | F-IT-P-P04-08-01 | BITACORA DE VERIFICACIÓN DE INSTRUMENTAL DE LABORATORIO |
| 45 | F-IT-P-P07-01-02 | BITACORA DE ANTIESPUMANTE |
| 46 | F-IT-P-P07-01-03 | BITACORA DE FLOCULANTE |
| 47 | F-IT-P-P07-01-04 | BITACORA DE COAGULANTE |
| 48 | F-IT-P-P07-01-05 | BITACORA DE SOSA CAUSTICA |
| 49 | F-IT-P-P07-02-06 | PARAMETROS DE AGUA PARA PROCESO |
| 50 | F-P-P01-02 | BITACORA DE REGISTRO DE ACTIVIDADES DE TAC |
| 51 | F-P-P02-01 | Orden de trabajo producción entretelas |
| 52 | F-P-P04-03 | REPORTE DE PRUEBA DE SOLVENTE |
| 53 | F-P-P04-08 | CONTROL DE PROCESO MAQUINA DE TEJIDO |
| 54 | F-P-P04-10 | COMPARATIVO DE PRODUCTOS QUÍMICOS |

## 3. Clave nueva D-02 (57.84.0)

### Qué hizo el sistema

Cada documento controlado, activo y no obsoleto recibe su clave nueva según
su proceso:

| Tipo | Clave nueva | Ejemplo |
|---|---|---|
| Formato y formato de instructivo (comparten consecutivo) | `F-{proceso}-{nn}` | F-IT-P-C05-07-01 → F-C5-nn |
| Instructivo | `IT-{proceso}-{nn}` | un IT-P-… de C4 → IT-C4-nn |
| DAT | `DA-{proceso}-{nn}` | DAT P-P01-01 (C4) → DA-C4-nn |
| Protocolo | `PROT-{proceso}-{nn}` | PROT-01 … PROT-05 → PROT-E2-01 … PROT-E2-05 |
| Procedimiento del Dropbox «No aplica (se queda)» | pasa a **Control operacional**, `CO-{proceso}-{nn}` | P-A17, P-A18, P-A19, P-A20, P-S03 → CO-E2-01 … CO-E2-05 |

- **Numeración determinista:** por proceso, por prefijo y por la clave del
  Dropbox, en orden alfabético (por eso los F-IT-… van antes que los F-P-…
  del mismo proceso). El consecutivo sigue al más alto que ya exista (C4 ya
  tenía IT-C4-01).
- **La clave del Dropbox queda como clave anterior** y se sigue buscando sin
  límite de tiempo: «Del Dropbox a Odoo → Buscador por clave anterior», la
  búsqueda «Clave SGI» de Documentos y las rutinas. Los archivos no se
  renombran.
- Todas las revisiones de una clave reciben la misma clave nueva; cada
  documento deja en su chatter «Clave nueva … (antes …)».
- Los PDF de Odoo con pie de formato controlado imprimen la clave nueva solos
  (el mapeo apunta al documento); su «Clave al ligar» se actualizó. El pie
  del bloqueo y etiquetado (P-A20) imprime CO-E2-04.
- Las ligas con actividades no cambian (son al documento, no a la clave).
- Los controles operacionales ya no salen en «Procedimientos anteriores»: se
  ven en «Formatos y documentos anteriores» y en el buscador por clave
  anterior.

Esperado en producción: **328** claves nuevas (C1 33, C2 21, C3 3, C4 67, C5
64, C6 6, E1 1, E2 49, S1 11, S2 7, S3 9, S4 41, S5 16) y **5** controles
operacionales.

### Decisiones de Jose (2026-10-01)

| Qué | Decisión |
|---|---|
| Procedimientos del Dropbox «En curso» (21) | **Conservan su clave** hasta que su proceso entre en vigor y queden obsoletos |
| Procedimientos «No aplica (se queda)» (5: P-A17, P-A18, P-A19, P-A20, P-S03) | Pasan a **Control operacional**, `CO-{proceso}-{nn}` |
| Formularios de Odoo (66 con las altas de 57.82.0) | **Conservan su clave** (L-004): son pantallas; lo que imprimen ya lleva la clave del formato ligado por el mapeo |
| Protocolos (5) | `PROT-{proceso}-{nn}` |
| Anexos (15) | **Conservan su clave**: siguen a su documento padre |
| Reglamentos (4, entre ellos el Reglamento Interior) | **Conservan su nombre y clave**: están registrados así ante la autoridad |

También conservan su clave el MIID, los diagramas, los obsoletos, P-I01 con
su familia y 5556 (procedimiento en borrador con la clave inválida, C-008).

## Anexo. Los 17 formatos citados que no existen (propuesta §1)

En código solo se hizo lo seguro: las altas de F-P-A28-13 y F-P-A28-11 (sección 1)
y la liga de F-IT-P-G03-01-01 a E2.37.
Lo demás es de MAST: corregir la cita en la rutina (`sgi.legacy.routine`) y en
la próxima revisión del procedimiento, o dar de alta el formato.

| # | Clave citada | Qué hacer | Con qué |
|---|---|---|---|
| 1 | F-P-A28-11 | **Hecho** (alta, sección 1). Configurar la encuesta 152 en Ajustes del SGI | — |
| 2 | F-IT-P-A10-01-01 | Corregir la cita | F-P-A10-01 (minuta, doc 3762, mapeo 8). La cita de P-A14 n.9 (E1.04) es F-P-A10-03 |
| 3 | F-P-A25-01 | Corregir la cita: la conciliación vive en Contabilidad | Conciliación bancaria; firma del contador con el cierre de periodo |
| 4 | F-P-A25-02 | **Decisión:** usar Declaración fiscal (`account.return`, 8 en «Nuevo») con el acuse del SAT, o dar de alta el Excel | — |
| 5 | F-P-A28-13 | **Hecho** (alta) | — |
| 6 | F-P-A31-01 | Quitar la cita (una sola clave de pronóstico) | F-P-A28-13 |
| 7 | F-P-A31-02 | Corregir la cita | F-P-A28-14 (doc 3054) |
| 8 | F-IT-P-C06-03-01 | Corregir la cita. **Hecho:** el 4063 quedó ligado también a E2.37 | F-IT-P-G03-01-01 (doc 4063, encuesta 155) |
| 9 | F-P-C05-04 | Corregir la cita | F-P-C06-07 (doc 3929) |
| 10 | F-P-V01-01 | Corregir la cita | F-P-A28-01 y F-P-A28-19 (Helpdesk y Reclamaciones) |
| 11 | F-P-C14-01 | Corregir la cita; los dos «F-P-C014-01» (3908, 3946) quedan como histórico | PPAP (`sgi.ppap`, sin registros) |
| 12 | F-P-A14-NOM-017-01 | Corregir la cita | F-P-S03-01 (doc 3805) |
| 13 | F-P-A14-04 | Corregir la cita | F-P-S03-02 (doc 3806) y Responsivas de EPP |
| 14 | F-P-A14-03 | **Dar de alta** el formato del permiso de trabajo (partir del 3818). La pantalla ya existe desde 57.51.0 (Permisos de trabajo de alto riesgo) y su PDF imprime F-P-A14-03 sin revisión | doc 3818 (no controlado, MAST) |
| 15 | F-P-A06-04 | **Dar de alta** la evaluación mensual 5S y crear la actividad | doc 3811 (no controlado, MAST) |
| 16 | F-P-A01-31 | Corregir la cita (o dar de alta el reporte mensual) | F-P-A01-30 (doc 3850) |
| 17 | F-P-C05-10 | Que Calidad confirme si es F-P-P04-10 (doc 4032); si sí, corregir la cita y ligarlo a C5.23; si no, dar de alta | — |
