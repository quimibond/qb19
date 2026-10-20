<!-- Texto fijo del Manual del SGI (MIID) y del P-G01 generados desde Odoo.
GENERADO por tools/sgi_miid_seed_md.py desde addons/quimibond_sgi/data/sgi_miid_sections.xml;
es la referencia a la que el aviso de la siembra manda comparar cuando una
sección editada a mano recibe texto nuevo. No se edita a mano.
El borrador Rev. 03 del 5-oct-2026 (46 secciones en prosa) ya no es la fuente
(Dirección 2026-10-13, «el MIID está como en el Dropbox»); desde 57.148.0 el
P-G01 es un documento generado propio con el mismo motor.
Desde 57.161.0 las listas y tablas son renglones (data/sgi_miid_items.xml) y las
rutas, menús ([En Odoo: …]).
Marcas: [VIVO: bloque] = datos que pone el sistema; `[[datos]]` = dónde van. -->

# Texto fijo de los documentos generados (siembra)

# Manual Integral de Información Documentada (MIID)

24 secciones; bloques vivos: identificacion, normas, procesos, politica, objetivos, roles, tipos_documento, controles, plazos_nc, correspondencia, procedimientos_anteriores, anexos, historial.

## Identificación del documento

[VIVO: identificacion — Identificación del documento]

Manual del Sistema de Gestión Integral de PNTQ (ISO 9001:2015, ISO 14001:2015 e ISO 45001:2018). Responsable: Jefe de MAST y SGI. Aprueba: Director de Operaciones.

`[[datos]]`

## 1 Objeto, alcance y control del Manual

### 1.1 Objeto

Documento de referencia del SGI: su alcance, lo que no aplica, dónde vive cada requisito en Odoo y las reglas que el sistema hace cumplir. Lo que Odoo ya muestra no se repite aquí.

El SGI sirve para:

- Suministrar productos y servicios conformes
- Gestionar el ambiente con perspectiva de ciclo de vida
- Cumplir los requisitos legales aplicables en México
- Identificar y controlar peligros y riesgos
- Dar condiciones de trabajo seguras y saludables
- Fomentar la consulta y participación de los trabajadores

`[[datos]]`

### 1.2 Alcance del SGI

[POR CONFIRMAR: Alcance: confirmar contra el certificado o el informe de la auditoría de septiembre de 2026. La revisión 02 trae dos redacciones del alcance general: en 1.2, «maquila, fabricación, distribución y venta de entretelas tejidas y no tejidas, telas de todo tipo de fibras, así como la aplicación de resinas, laminados y acabados industriales»; en 3.3, «la maquila, fabricación, distribución, comercialización, importación y exportación de entretelas tejidas y no tejidas, telas de cualquier tipo de fibra y demás productos textiles, así como la aplicación de resinas, laminados y acabados industriales relacionados con dichos productos, incluyendo las distintas áreas, procesos y actividades que integran PNTQ». Para ISO 45001 dice «aplica a las actividades incluidas dentro del alcance definido del SGI».]

| Norma | Alcance |
| --- | --- |
| **ISO 9001:2015** | Maquila, fabricación, distribución y venta de entretelas tejidas y no tejidas de todo tipo de fibras; aplicación de resinas, laminados y acabados industriales. |
| **ISO 14001:2015** | Sector 04: fabricación de entretelas tejidas de fibras industriales. |
| **ISO 45001:2018** | Actividades, instalaciones y personas bajo control de PNTQ en Toluca. |

`[[datos]]`

Sitio: Carretera Toluca-Naucalpan km 52.8, Parque Industrial Toluca 2000, Calle 3 Sur, Mz. 7, Lotes 7, 9 y 10, C.P. 50233, Toluca, Estado de México.

### 1.3 Requisitos que no aplican y su justificación

[POR CONFIRMAR: La revisión 02 sí justifica las dos (3.3, 8.4.1 y 8.5.1): 8.4.1 b), «los productos y servicios no son proporcionados directamente a los clientes por proveedores externos en nombre de la organización»; 8.5.1 f), «los procesos de producción permiten la verificación de sus resultados mediante actividades de seguimiento y medición posteriores, por lo que no se requiere validación de procesos especiales». La justificación de esta sección dice lo mismo; falta que Dirección la confirme.]

| Requisito de ISO 9001:2015 | No aplica | Por qué |
| --- | --- | --- |
| **8.4.1 b)** | Proveedores externos que entregan al cliente en nombre de PNTQ | PNTQ entrega directamente |
| **8.5.1 f)** | Validación de procesos cuyas salidas no se pueden verificar | Todo se verifica con inspección y laboratorio antes de liberar |

`[[datos]]`

No afectan la conformidad de los productos ni la satisfacción del cliente.

### 1.4 Revisión y control de este Manual

[En Odoo: SGI → Sistema → Manual del SGI → Manual (MIID)]

| Concepto | Cómo | En Odoo |
| --- | --- | --- |
| **Quién lo edita** | El Jefe de MAST y SGI edita los textos; los datos los pone Odoo. |  |
| **Revisión nueva** | Solicitud de cambio documental; firma Dirección; lo firmado es lo que se publica. | SGI → Sistema → Manual del SGI → Solicitudes de cambio |
| **Al día** | Odoo lo compara cada día con el sistema y avisa si ya no coincide. |  |
| **Cuándo se revisa** | Cuando el sistema cambia y al menos cada dos años. |  |
| **Copias** | Una copia impresa no es controlada; fuera de PNTQ requiere autorización de Dirección. |  |

`[[datos]]`

## 2 Referencias normativas

[VIVO: normas — Normas y cláusulas que rigen el SGI]
[En Odoo: SGI → Administración → Configuración → Normas]

Cada actividad declara en «Cumple con» las cláusulas que cumple (5.1).

- **Requisitos legales**: Las disposiciones legales y reglamentarias aplicables. — En Odoo: SGI → Planeación → Requisitos legales
- **Normas de apoyo**: ISO 9000:2015, NMX-CC-9001-IMNC-2015 y NMX-CC-9000-IMNC-2015.

`[[datos]]`

## 3 Dónde vive el sistema

[En Odoo: SGI → Sistema → Del Dropbox a Odoo → Buscador por clave anterior]

Lo que está en Odoo es la versión oficial.

| Término | Qué es | En Odoo |
| --- | --- | --- |
| **Proceso** | Uno del mapa (E, C o S), con dueño, actividades, indicadores, riesgos y documentos. | SGI → Sistema → Mapa de procesos |
| **Actividad** | Una tarea de un proceso: quién, cómo, cuándo y cuándo está terminada. | SGI → Sistema → Actividades |
| **Dueño de proceso** | Responde por el resultado, los indicadores, las NC y los documentos del proceso. |  |
| **Mi procedimiento** | Lo que le toca a cada puesto; cada persona lo firma de leído. | SGI → Inicio → Mi procedimiento |
| **Mis pendientes** | La bandeja de cada persona. | SGI → Inicio → Mis pendientes |
| **Acuse de lectura** | La firma «leído y entendido» de un documento. | SGI → Inicio → Documentos vigentes |
| **NC** | No conformidad. | SGI → Mejora → No conformidades |
| **MAST** | Medio Ambiente y Seguridad en el Trabajo. |  |
| **Dropbox** | Archivo histórico de solo lectura; su equivalencia, en «Del Dropbox a Odoo». | SGI → Sistema → Del Dropbox a Odoo → Buscador por clave anterior |
| **Manuales de uso** | Conocimiento → SGI → Cómo usar el sistema (de consulta, no controlados). |  |

`[[datos]]`

## 4 El sistema

### 4.1 Procesos, mapa e interacción

[VIVO: procesos — Procesos del SGI, mapa e interacción]
[En Odoo: SGI → Sistema → Mapa de procesos]

Cada proceso tiene dueño, entradas, salidas, actividades, indicadores y riesgos. El mapa, la interacción y la tortuga salen de Odoo.

`[[datos]]`

### 4.2 Política integral

[VIVO: politica — Política integral vigente]
[En Odoo: SGI → Planeación → Política integral]

Una sola política vigente; se comunica con acuse de lectura. Compromisos:

- Cumplir los requisitos aplicables
- Mejorar continuamente
- Proteger el medio ambiente
- Dar condiciones de trabajo seguras y saludables
- Eliminar peligros y reducir riesgos
- Consultar y hacer participar a los trabajadores

`[[datos]]`

### 4.3 Objetivos integrales

[VIVO: objetivos — Objetivos integrales e indicadores]
[En Odoo: SGI → Planeación → Objetivos integrales]

Medibles y ligados a sus indicadores; toman el peor color de ellos y se revisan en la revisión por la dirección.

`[[datos]]`

### 4.4 Dueños de proceso, roles y autoridades

[VIVO: roles — Dueños de proceso, roles y autoridades]
[En Odoo: SGI → Sistema → Matriz de responsabilidades]

| Nivel | Quién responde | En Odoo |
| --- | --- | --- |
| **Proceso** | El dueño del proceso. | SGI → Sistema → Mapa de procesos |
| **Actividad** | Quien ejecuta y, si aplica, quien aprueba y a quien se escala. | SGI → Sistema → Actividades |
| **Puesto** | «Mi procedimiento», firmado de leído. | SGI → Sistema → Puestos y procesos |
| **Jefe de MAST y SGI** | La conformidad del SGI, el informe a la dirección y su integridad en los cambios. |  |

`[[datos]]`

Quien aprueba no es quien ejecuta, y nadie audita su propio proceso.

## 5 Cómo cumple el SGI cada requisito

### 5.1 Cláusulas, procesos y actividades

[VIVO: correspondencia — Correspondencia por cláusula]
[En Odoo: SGI → Administración → Configuración → Normas]

De aquí salen la Matriz de cumplimiento y el checklist de auditoría. Una cláusula sin actividad sale en rojo en el Diagnóstico.

`[[datos]]`

### 5.2 Reglas que el sistema hace cumplir

Lo que no es una actividad lo cumple el sistema con candados y avisos:

| Tema | Cláusula | Regla | En Odoo |
| --- | --- | --- | --- |
| **Contexto y partes interesadas** | 4.1, 4.2 | Se registran en Odoo y entran a la revisión por la dirección. | SGI → Planeación → Partes interesadas |
| **Riesgos y oportunidades** | 6.1 | Evaluación inicial y residual; se reevalúan en enero, julio y tras un incidente o cambio. Un riesgo alto sin acción abierta avisa al dueño. | SGI → Planeación → Riesgos y oportunidades |
| **Controles de seguridad** | 6.1, 8.1 | Siguen la jerarquía de controles; un riesgo alto no se controla solo con EPP. | SGI → Planeación → Riesgos y oportunidades |
| **Requisitos legales** | 6.1.3, 9.1.2 | Cada uno se evalúa con evidencia. | SGI → Planeación → Evaluaciones de cumplimiento legal |
| **Cambios** | 6.3, 8.5.6 | Actividad: propuesta aprobada por el jefe y el dueño. Documento: solicitud de cambio. Proceso o infraestructura (MOC): motivo, afectados y riesgos. | SGI → Sistema → Documentos → Solicitudes de cambio |
| **Competencia** | 7.2 | Cada puesto define sus competencias; la brecha es el programa de capacitación; eficacia a los 90 días. | Competencias SGI → Brechas de competencia (DNC) |
| **Toma de conciencia** | 7.3 | Inducción en los primeros tres meses; firma de Mi procedimiento y de la política. | SGI → Inicio → Mi procedimiento |
| **Comunicación y participación** | 7.4, 5.4 | Acuses, avisos de Odoo, Tablero y actas; consulta previa en SST por la Comisión de Seguridad e Higiene. | SGI → Seguridad y ambiente → Recorridos CSH |
| **Equipos de medición** | 7.1.5 | Una calibración vencida no libera producto. | Calidad preventiva → Metrología → Equipos de medición |
| **Control operacional** | 8.1 | Criterio de terminado por actividad; permisos de alto riesgo y bloqueo antes de iniciar. | SGI → Seguridad y ambiente → Permisos de trabajo de alto riesgo |
| **Emergencias** | 8.2 | Planes y simulacros; el plan se revisa tras cada uno; una emergencia real levanta NC. | SGI → Seguridad y ambiente → Planes de emergencia |
| **Indicadores** | 9.1 | Medición por periodo validada por el dueño; un rojo exige causa y plan. | SGI → Desempeño → Indicadores → Todos los indicadores |
| **Auditoría interna** | 9.2 | Programa anual, todos los procesos cada tres años, nadie audita lo suyo; cada hallazgo, una NC. | SGI → Desempeño → Auditorías → Programa |
| **Revisión por la dirección** | 9.3 | Entradas cargadas por el sistema; cada acuerdo es una acción con responsable y fecha. | SGI → Desempeño → Revisión por la dirección |
| **Mejora** | 10 | Una NC cierra solo con eficacia «Eficaz»; se cancela solo con motivo aprobado. | SGI → Mejora → Mejora continua |

`[[datos]]`

### 5.3 No conformidad y acción correctiva

[VIVO: plazos_nc — Plazos de las NC]
[En Odoo: SGI → Mejora → No conformidades]

Toda NC se registra en Odoo, sea cual sea su origen.

| Etapa o caso | Qué exige | En Odoo |
| --- | --- | --- |
| **Abierta** | Contención, causa raíz y plan en los plazos de abajo. |  |
| **Seguimiento** | Clasificación y cláusula incumplida. |  |
| **Cerrada** | Acciones con evidencia y eficacia verificada. |  |
| **NC mayor** | Análisis completo; la lección pasa al AMEF, al plan de control o al documento; se informa a Dirección. |  |
| **Reclamación** | Respuesta en el plazo acordado; 8D si el cliente lo pide. | SGI → Mejora → Reclamaciones de clientes |
| **Incidente** | Se investiga con participación de los trabajadores. | SGI → Seguridad y ambiente → Incidentes y accidentes |

`[[datos]]`

### 5.4 Controles operacionales ambientales y de seguridad y salud

[VIVO: controles — Controles operacionales vigentes]

| Control | Cómo | En Odoo |
| --- | --- | --- |
| **Controles operacionales** | Documentos con clave CO: alcance, controles, responsables y qué hacer en desviación. | SGI → Sistema → Documentos → Documentos controlados |
| **Contratistas** | Reglamento de seguridad y los mismos permisos. | SGI → Seguridad y ambiente → Permisos de trabajo de alto riesgo |
| **Equipo de protección** | Responsiva por persona. | SGI → Seguridad y ambiente → Responsivas de EPP |
| **Residuos, descargas y consumos** | Según los aspectos ambientales significativos y los requisitos legales. | SGI → Planeación → Aspectos ambientales |

`[[datos]]`

## 6 Información documentada

[En Odoo: SGI → Sistema → Manual del SGI → Control de información documentada (P-G01)]

Lo regula el P-G01, generado desde Odoo con el mismo motor que este Manual. Aquí solo van los tipos de documento y sus claves.

### 6.1 Tipos de documento y claves

[VIVO: tipos_documento — Tipos de documento y su clave]
[En Odoo: SGI → Sistema → Documentos → Lista maestra]

| Forma | Qué es | Dónde vive | En Odoo |
| --- | --- | --- | --- |
| **Documentos controlados** | Con clave, revisión, estado, proceso y responsable. | Documentos → carpeta SGI | SGI → Sistema → Documentos → Documentos controlados |
| **Registros** | La evidencia; imprimen la clave y revisión del formato que sustituyen. | Odoo |  |
| **Registros en papel** | Bitácoras y formatos no migrados. | Con el jefe del área |  |

`[[datos]]`

Solo lo vigente se usa. La clave la asigna y la valida Odoo; la anterior del Dropbox se busca en «Del Dropbox a Odoo».

## 7 Transición desde el Dropbox

[En Odoo: SGI → Sistema → Del Dropbox a Odoo → Rutina por rutina]

Un procedimiento anterior queda obsoleto cuando entra en vigor el proceso que lo sustituye; sus rutinas se resolvieron una por una.

### 7.1 Procedimientos anteriores y proceso que los sustituye

[VIVO: procedimientos_anteriores — Procedimientos anteriores por proceso]
[POR CONFIRMAR: P-A13, P-A22 y P-A30 no están registrados como procedimientos en Odoo (S3, S2 y S6 en el borrador).]
[En Odoo: SGI → Sistema → Del Dropbox a Odoo → Procedimientos anteriores]

### 7.2 Anexos

[VIVO: anexos — Anexos vigentes]
[POR CONFIRMAR: Situación de cada anexo: el Jefe MAST y SGI y la Dirección deben confirmar qué anexos se sustituyen y cuáles se conservan. En la revisión 02 el Anexo 13 se llama «Plan integral» (aquí, «Plan de calidad») y es donde se presenta el resultado de la planificación operacional (8.1): confirmar su nombre y si se conserva o lo sustituyen las actividades de los procesos.]
[En Odoo: SGI → Sistema → Documentos → Documentos controlados]

Notas por renglón (`sgi.miid.row.note`):

- **ANEXO 1**: Se conserva como documento
- **ANEXO 2**: Lo sustituye el mapa de procesos de Odoo
- **ANEXO 3**: Lo sustituyen los diagramas de interacción y tortuga de Odoo
- **ANEXO 4**: La sustituye la matriz de responsabilidades de Odoo
- **ANEXO 5**: La sustituyen los roles «Aprueba» de las actividades y las reglas de Aprobaciones
- **ANEXO 6**: Se llevan en Odoo; el anexo se conserva como documento firmado
- **ANEXO 7**: Se conserva como documento firmado
- **ANEXO 8**: Se conserva como documento firmado
- **ANEXO 9**: Se lleva en Odoo con acuses; el anexo se conserva como documento firmado
- **ANEXO 10**: Se conserva como documento
- **ANEXO 11**: La sustituye la lista maestra de Odoo
- **ANEXO 12**: La sustituye el organigrama de Empleados en Odoo
- **ANEXO 13**: Se conserva como documento
- **ANEXO 14**: La sustituye Lecciones aprendidas
- **ANEXO 15**: Se conserva como documento; su revisión es entrada de la revisión por la dirección

## 8 Control de cambios

[VIVO: historial — Control de cambios]
[En Odoo: SGI → Sistema → Manual del SGI → Revisiones]

# Elaboración y control de información documentada (P-G01)

14 secciones; bloques vivos: identificacion, tipos_documento, plazos_documentales, actividades, normas, historial.

## Identificación del documento

[VIVO: identificacion — Identificación del documento]

Procedimiento del SGI de PNTQ. Proceso E2 Gestión del SGI. Responsable: Jefe de MAST y SGI. Aprueba: Director de Operaciones.

`[[datos]]`

## 1 Propósito

Que el personal trabaje siempre con la versión vigente de cada documento y que la evidencia de cada registro se conserve y pueda auditarse.

## 2 Alcance

Toda la información documentada del SGI, generada o recibida en PNTQ:

| Forma | Qué incluye | Dónde vive | En Odoo |
| --- | --- | --- | --- |
| **Documentos controlados** | Manual, procedimientos, instructivos, controles operacionales, protocolos, DAT, métodos, anexos, descripciones de puesto, Mi procedimiento, reglamentos y externos. | Documentos → carpeta SGI | SGI → Sistema → Documentos → Documentos controlados |
| **Registros** | La evidencia: órdenes, inspecciones, NC, checklists, mediciones, actas, permisos. | Odoo |  |
| **Registros en papel** | Bitácoras de piso y formatos no migrados. | Con el jefe del área |  |

`[[datos]]`

## 3 Responsabilidades

| Quién | Actividad | Responsabilidad | En Odoo |
| --- | --- | --- | --- |
| **Jefe de MAST y SGI** | E2.01 | Responder cada solicitud de cambio en 5 días hábiles. | SGI → Sistema → Documentos → Solicitudes de cambio |
| **Jefe de MAST y SGI** | E2.02 | Publicar la revisión, dejar obsoleta la anterior y avisar en 2 días hábiles. |  |
| **Jefe de MAST y SGI** | E2.03 | Revisar cada mes la vigencia de los documentos. |  |
| **Jefe de MAST y SGI** | E2.36 | Registrar los documentos externos. | SGI → Sistema → Documentos → Documentos externos |
| **Jefe de MAST y SGI** | E2.42 | Verificar que cada nuevo titular firme y revise su Mi procedimiento. |  |
| **Jefe de MAST y SGI** | E2.43 | Autorizar con Dirección las entregas a externos. |  |
| **Jefe de MAST y SGI** |  | Mantener el MIID, este procedimiento, los tipos de documento, el boletín NEWS y Mi procedimiento de cada puesto. |  |
| **Director de Operaciones** |  | Aprobar el MIID, este procedimiento y los procedimientos que Dirección decida; autorizar entregas a externos. |  |
| **Dueño de proceso** |  | Responder por los documentos de su proceso, firmar «Revisó», pedir la publicación de sus instructivos e implantar los externos en 10 días hábiles. |  |
| **Quien elabora un documento** |  | Solicitar el cambio con el archivo propuesto («Elaboró») e implantarlo en 15 días hábiles. | SGI → Sistema → Documentos → Solicitudes de cambio |
| **Quien entra a un puesto** |  | Firmar su Mi procedimiento y, en 30 días naturales, proponer lo que falte o sobre; su jefe lo verifica. | SGI → Inicio → Mi procedimiento |
| **Jefe de área y supervisor** |  | Que su gente use la versión vigente, firme sus acuses y conserve los registros en papel. |  |
| **Todo el personal** |  | Consultar en Odoo y firmar «Leído y entendido» en 7 días hábiles. | SGI → Inicio → Documentos vigentes |
| **Administrador SGI** |  | Dar de alta los tipos de documento y sus claves. | SGI → Administración → Configuración → Tipos de documento |

`[[datos]]`

Las firmas se registran en Odoo (Firma electrónica).

## 4 Desarrollo

### 4.1 Tipos de documento y claves

[VIVO: tipos_documento — Tipos de documento y su clave]
[En Odoo: SGI → Administración → Configuración → Tipos de documento]

La clave la asigna Odoo según el tipo y el proceso. La NOM o el requisito de cliente va en el título, no en la clave.

`[[datos]]`

### 4.2 Quién elabora qué y dónde

| Documento | Lo elabora | Dónde | Cómo queda controlado | En Odoo |
| --- | --- | --- | --- | --- |
| **Procedimiento de proceso (PR)** | Dueño del proceso | Datos del proceso en Odoo | Odoo genera el PDF; el Jefe MAST publica | SGI → Sistema → Mapa de procesos |
| **Mi procedimiento (MP)** | Odoo, con los roles del puesto | Publicar Mi procedimiento | Revisión con acuse de cada persona | SGI → Administración → Publicar Mi procedimiento |
| **Instructivo, CO, protocolo, reglamento** | Dueño del proceso | Conocimiento → SGI | «Pedir publicación»; el Jefe MAST lo congela como revisión |  |
| **Formato (F)** | Sistemas, con lo que define el dueño | Pantalla de Odoo | El registro imprime su clave y revisión | SGI → Administración → Configuración → Formatos en documentos de Odoo |
| **DAT, método, anexo, archivo** | Responsable del documento | PDF en la solicitud de cambio | Se publica al aprobarse | SGI → Sistema → Documentos → Solicitudes de cambio |
| **Manual (MIID)** | Jefe de MAST y SGI | Manual del SGI | Solicitud de cambio y firma de Dirección | SGI → Sistema → Manual del SGI → Manual (MIID) |
| **Documento externo** | El cliente, la autoridad o el proveedor | Documentos externos | Emisor, revisión, recepción y proceso | SGI → Sistema → Documentos → Documentos externos |

`[[datos]]`

Los archivos conservan la plantilla de la revisión 11 (logo, razón social, nombre, clave, emisión, área y revisión; PDF). Prohibido trabajar con lo que no esté vigente en Odoo; un formato «Migrado a Odoo» ya no se llena en papel ni en Excel.

### 4.3 Aprobación, publicación y cambios

[En Odoo: SGI → Sistema → Documentos → Solicitudes de cambio]

| Documento | Elaboró | Revisó | Aprobó |
| --- | --- | --- | --- |
| **Manual (MIID)** | Jefe de MAST y SGI | Dueño de E2 | Director de Operaciones |
| **Procedimiento de proceso** | Dueño del proceso | — | Quien declare el proceso; Vo.Bo. del Jefe MAST |
| **Instructivo, CO, protocolo, reglamento, DAT, formato, anexo** | Quien solicita | Dueño del proceso | Jefe de MAST y SGI |
| **Actividades y roles** | Quien propone | Su jefe directo | Dueño del proceso |

`[[datos]]`

**Cómo se pide:** solicitud de cambio con el archivo propuesto. Odoo pide las firmas en orden y, con la última, publica la revisión, deja obsoleta la anterior y pide los acuses.

**Cuándo se modifica:** cambio de norma, requisito legal o de cliente, decisión de Dirección, hallazgo o NC, vencimiento, documento externo o aviso de que ya no coincide con el sistema. El cambio de titular no es motivo.

### 4.4 Plazos

[VIVO: plazos_documentales — Plazos documentales]
[En Odoo: SGI → Administración → Configuración → Ajustes]

Los aplica el sistema solo, con sus avisos:

`[[datos]]`

### 4.5 Difusión, distribución y copias

| Medio | Cómo | En Odoo |
| --- | --- | --- |
| **Acuse de lectura** | Cada persona firma «Leído y entendido» en Mis pendientes, Documentos vigentes o la tableta de su área. | SGI → Inicio → Mis pendientes |
| **NEWS Quimibond** | Boletín mensual de los cambios del mes anterior; lo envía el Jefe MAST. |  |
| **Reunión** | Si el cambio lo amerita: minuta, lista de asistencia y acuse. |  |
| **Distribución** | Solo en Odoo; lo impreso es copia no controlada. | SGI → Inicio → Documentos vigentes |
| **Entrega a externos** | Con autorización de Dirección, marcada «COPIA NO CONTROLADA» y anotada en el historial. |  |

`[[datos]]`

### 4.6 Conservación, resguardo y seguridad de la información

| Tema | Regla | En Odoo |
| --- | --- | --- |
| **Respaldo** | Todo vive en Odoo; Odoo.sh respalda diario (responde Sistemas). |  |
| **Nada se borra** | Lo que deja de usarse queda obsoleto con fecha, motivo e historial. |  |
| **Retención** | Cada documento declara sus años de retención y su disposición final. | SGI → Sistema → Documentos → Documentos controlados |
| **Registros físicos** | Con jefes y supervisores, en orden; después, al archivo muerto de MAST. |  |
| **Historia** | Lo anterior a Odoo está en el Dropbox, de solo lectura. | SGI → Sistema → Del Dropbox a Odoo → Buscador por clave anterior |
| **Datos personales** | Confidenciales, con acceso por grupo; se eliminan al terminar la relación y los plazos legales. |  |

`[[datos]]`

## 5 Rutinas: actividades del proceso E2

[VIVO: actividades — Rutinas: actividades del proceso ligadas al documento]
[En Odoo: SGI → Sistema → Del Dropbox a Odoo → Rutina por rutina]

Las actividades de E2 ligadas a este documento («Procedimiento relacionado»):

`[[datos]]`

## 6 Marco normativo

[VIVO: normas — Normas y cláusulas que rigen el SGI]

Cláusula 7.5 de las tres normas y los requisitos de clientes cargados en Odoo.

`[[datos]]`

## 7 Control de cambios

[VIVO: historial — Control de cambios]
[En Odoo: SGI → Sistema → Manual del SGI → Revisiones]

`[[datos]]`

