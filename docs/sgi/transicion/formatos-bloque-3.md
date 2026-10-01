# Formatos del SGI, bloque 3: lo que hizo el sistema y lo que queda para MAST

Propuesta de formatos aprobada completa por Jose el 2026-10-01 («arranca con
los formatos»; `docs/audit/decisiones.md`, 2026-09-30 y 2026-10-01). El bloque
3 se aplica con migraciones de `quimibond_sgi` (producción solo cambia al
actualizar el módulo). Cada sección dice qué cambió el sistema y qué tiene que
hacer MAST a mano.

Cifras: lectura de producción por MCP, solo lectura, compañía 1, 2026-10-01.

## 1. Duplicados y datos malos (57.70.0)

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
| F-P-A28-11 | Encuesta de satisfacción del cliente | SGI → Dirección → Satisfacción del cliente | E2.12 |

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

## 2. Los 17 formatos citados que no existen (propuesta §1)

En código solo se hizo lo seguro: las altas de F-P-A28-13 y F-P-A28-11 (arriba)
y la liga de F-IT-P-G03-01-01 a E2.37.
Lo demás es de MAST: corregir la cita en la rutina (`sgi.legacy.routine`) y en
la próxima revisión del procedimiento, o dar de alta el formato.

| # | Clave citada | Qué hacer | Con qué |
|---|---|---|---|
| 1 | F-P-A28-11 | **Hecho** (alta). Configurar la encuesta 152 en Ajustes del SGI | — |
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
