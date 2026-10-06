<!-- MIID Rev. 03 (borrador del 5-oct-2026). Texto base para las secciones de `sgi.miid.section` (entrega 57.103.0).
Marcas para la carga:
  [FIJO]  texto que edita el Jefe MAST en la sección.
  [VIVO]  bloque que debe salir de datos de Odoo; lo escrito aquí es solo la foto del 5-oct-2026.
Pendientes de confirmar antes de aprobar: sección 12.3.
Rutas del menú actualizadas en 57.113.0 (menús por capítulos), igual que data/sgi_miid_sections.xml. -->

# MIID — Manual de Información Documentada del SGI (Rev. 03, borrador)

Oct 5, 2026 · @Jose Jaime Mizrahi

## Identificación del documento

<!-- [VIVO] revisión, emisión y estado: del documento MIID (3495). [FIJO] lo demás. -->

| Dato | Valor |
| --- | --- |
| Organización | Productora de No Tejidos Quimibond S.A. de C.V. (PNTQ) |
| Documento | Manual Integral de Información Documentada. Sistema de Gestión Integral ISO 9001:2015, ISO 14001:2015 e ISO 45001:2018 |
| Clave | MIID |
| Área | SGI |
| Emisión original | Febrero de 2023 |
| Revisión | 03, en borrador. Sustituye a la revisión 02 de junio de 2026 |
| Responsable del documento | Jefe de MAST y SGI |
| Aprueba | Director de Operaciones |

**Estado de este borrador.** Está redactado para el SGI que opera en Odoo. Entra en vigor cuando se publiquen los procesos en Odoo y Dirección lo apruebe. Al 5 de octubre de 2026, 13 de los 14 procesos están en borrador y uno (C2) en piloto, por lo que la revisión 02 sigue vigente hasta entonces.

### Qué cambia frente a la revisión 02

| Tema | Revisión 02 | Revisión 03 |
| --- | --- | --- |
| Dónde vive el sistema | Carpetas del Dropbox, con PDF y Excel | Odoo, menú SGI |
| Cómo se describe la operación | 52 procedimientos por área o puesto (P-A, P-C, P-D, P-E, P-G, P-M, P-P, P-S) | 14 procesos con actividades, dueño y medición |
| Registros | Formatos en Excel y papel | Registros de Odoo, que imprimen la clave y la revisión del formato |
| Riesgos, aspectos, IPER, requisitos legales | Matrices en Excel por área | Registros en Odoo con evaluación, acciones e historial |
| Indicadores | Captura mensual en F-P-A10-03 | Medición automática o captura, validada por el dueño |
| No conformidades | Control en Excel (F-P-G05-02) | NC con folio, plazos y candados de cierre |
| Difusión | Correo NEWS y listas de asistencia | Acuse de lectura firmado por persona |
| Control de cambios | Minuta y solicitud de modificación | Solicitudes en Aprobaciones, con historial |

No cambian el alcance, las no aplicabilidades, la política integral, los objetivos ni la estructura por cláusulas de las normas.

## 1. Objeto y campo de aplicación

### 1.1 Objeto

Este Manual es el documento de referencia del Sistema de Gestión Integral (SGI) de PNTQ. Sus objetivos son:

1. Establecer y mantener la información documentada que requiere el SGI, con base en ISO 9001:2015, ISO 14001:2015 e ISO 45001:2018 y sus equivalentes nacionales.
2. Dar el marco para demostrar la capacidad de PNTQ de suministrar productos y servicios conformes, dentro de los procesos que controla o sobre los que influye.
3. Gestionar las responsabilidades ambientales de manera sistemática, con perspectiva de ciclo de vida.
4. Demostrar el cumplimiento de los requisitos legales, reglamentarios y normativos aplicables en México.
5. Identificar oportunamente los riesgos y peligros presentes en las áreas de PNTQ.
6. Promover condiciones de trabajo seguras y saludables, eliminando peligros y reduciendo riesgos.
7. Fomentar la consulta y la participación de los trabajadores y sus representantes en lo relativo a seguridad y salud en el trabajo.

### 1.2 Alcance del SGI

- **ISO 9001:2015:** maquila, fabricación, distribución y venta de entretelas tejidas y no tejidas de todo tipo de fibras, así como la aplicación de resinas, laminados y acabados industriales.
- **ISO 14001:2015:** sector 04, proceso de fabricación de entretelas tejidas de fibras industriales.
- **ISO 45001:2018:** las actividades, instalaciones y personas bajo el control de PNTQ en su sitio de Toluca. *Por confirmar contra el certificado o el informe de la auditoría de septiembre de 2026. La revisión 02 trae dos redacciones del alcance general: en 1.2, «maquila, fabricación, distribución y venta de entretelas tejidas y no tejidas, telas de todo tipo de fibras, así como la aplicación de resinas, laminados y acabados industriales»; en 3.3, «la maquila, fabricación, distribución, comercialización, importación y exportación de entretelas tejidas y no tejidas, telas de cualquier tipo de fibra y demás productos textiles, así como la aplicación de resinas, laminados y acabados industriales relacionados con dichos productos, incluyendo las distintas áreas, procesos y actividades que integran PNTQ». Para ISO 45001 dice «aplica a las actividades incluidas dentro del alcance definido del SGI».*

Sitio: Carretera Toluca-Naucalpan km 52.8, Parque Industrial Toluca 2000, Calle 3 Sur, Manzana 7, Lotes 7, 9 y 10, C.P. 50233, Toluca, Estado de México.

### 1.3 Requisitos que no aplican y su justificación

| Requisito de ISO 9001:2015 | No aplica | Justificación |
| --- | --- | --- |
| 8.4.1 inciso b) | Productos y servicios proporcionados directamente a los clientes por proveedores externos en nombre de la organización | PNTQ entrega directamente; ningún proveedor externo entrega al cliente en su nombre |
| 8.5.1 inciso f) | Validación y revalidación periódica de procesos cuyas salidas no pueden verificarse después | Las salidas de todos los procesos se verifican con inspección y pruebas de laboratorio antes de liberar |

La revisión 02 sí justifica las dos con el mismo sentido (8.4.1 b: los productos y servicios no los proporcionan proveedores externos al cliente en nombre de PNTQ; 8.5.1 f: los resultados se verifican con seguimiento y medición posteriores, sin procesos especiales que validar); falta que Dirección lo confirme. Estas no aplicabilidades no afectan la responsabilidad de asegurar la conformidad de los productos y servicios ni el aumento de la satisfacción del cliente.

### 1.4 Revisión y control de este Manual

<!-- [FIJO] -->

El Manual se revisa cada dos años, o antes si lo piden un cambio en las normas, en los procesos, una auditoría o una no conformidad. Sus cambios se solicitan y aprueban en Odoo como cualquier documento controlado (ver 7.5). El Manual y los documentos del SGI son oficiales y de cumplimiento obligatorio; su reproducción requiere autorización escrita de la Dirección.

## 2. Referencias normativas

- ISO 9001:2015, Sistemas de gestión de la calidad. Requisitos (NMX-CC-9001-IMNC-2015).
- ISO 14001:2015, Sistemas de gestión ambiental. Requisitos con orientación para su uso.
- ISO 45001:2018, Sistemas de gestión de la seguridad y salud en el trabajo. Requisitos con orientación para su uso.
- ISO 9000:2015, Fundamentos y vocabulario (NMX-CC-9000-IMNC-2015).

Las disposiciones legales y reglamentarias aplicables se llevan en Odoo, en **SGI → Planeación → Requisitos legales** (ver 6.1.3).

## 3. Términos y dónde vive el sistema

Aplican los términos y definiciones de ISO 9000:2015, ISO 14001:2015 e ISO 45001:2018. Además, en este Manual:

| Término | Significado |
| --- | --- |
| Odoo | El sistema de gestión empresarial de PNTQ. El SGI opera en su menú **SGI** |
| Proceso | Uno de los 14 procesos del SGI, con clave (E, C o S), dueño y actividades |
| Actividad | Una tarea de un proceso: qué se hace, quién, dónde, cómo, cuándo, cuándo está terminada y qué hacer si falla |
| Dueño de proceso | Quien responde por el resultado de un proceso |
| Mi procedimiento | El conjunto de actividades, documentos, indicadores y equipo de protección del puesto de cada persona |
| Acuse de lectura | La firma «leído y entendido» de un documento, con persona, fecha y hora |
| NC | No conformidad |
| MAST | Medio Ambiente y Seguridad en el Trabajo |

**Regla general.** La información documentada del SGI se mantiene y se conserva en Odoo. Lo que está en Odoo es la versión oficial. El Dropbox queda como archivo histórico de solo lectura; la correspondencia entre cada documento anterior y su sustituto está en **SGI → Sistema → Del Dropbox a Odoo**.

El detalle de operación de cada pantalla está en el Manual de usuario del SGI en Odoo, que es documento de apoyo de este Manual.

## 4. Contexto de la organización

### 4.1 Comprensión de la organización y de su contexto

PNTQ fue fundada en 1992 por los señores José Mizrahi Daniel, Jaime Víctor Mizrahi Penhos y Jacobo Mizrahi Penhos. Inició con entretelas no tejidas, fusionables y no fusionables; en 2009 incorporó la entretela tejida y después se extendió a mercados industriales: pieles sintéticas, calzado, colchonería e industrial.

**Productos.** Entretela no tejida termofusionada de poliéster o poliamida; entretela tejida de tejido circular; entretela fusionable (base tejida o no tejida con resina termoactivable); y entretela no tejida con hilos encadenados.

**Servicios.** Corte y perforado; tintorería y acabado de telas tejidas; aplicación de resinas, laminados y acabados industriales; desarrollo de productos según especificación del cliente; y distribución de otros tipos de entretelas.

**Cuestiones internas y externas.** La Dirección las determina y revisa en el Anexo 15 y en la Planeación Estratégica 2026-2030 (Anexo 1). Incluyen las condiciones ambientales que afectan o son afectadas por PNTQ, los peligros y riesgos, y las condiciones de trabajo. En seguridad y salud consideran, entre otros, la seguridad de la maquinaria, los equipos y las instalaciones; la exposición a agentes físicos, químicos, ergonómicos y psicosociales; el cumplimiento de los requisitos legales; la preparación ante emergencias; y los cambios tecnológicos, operacionales y organizacionales. Su revisión es entrada de la revisión por la dirección (9.3).

### 4.2 Necesidades y expectativas de las partes interesadas

Las partes interesadas pertinentes (clientes, proveedores externos, el personal y los propietarios) y sus requisitos se registran en Odoo, en **SGI → Planeación → Partes interesadas**. Las necesidades que se convierten en requisitos legales u otros requisitos se llevan en **Requisitos legales** (6.1.3).

En seguridad y salud, las necesidades del personal incluyen condiciones de trabajo seguras y saludables, capacitación, consulta y participación, cumplimiento de los requisitos legales, prevención de lesiones, enfermedades e incidentes, y equipo de protección personal adecuado.

### 4.3 Alcance

El alcance del SGI es el declarado en 1.2 y las no aplicabilidades, las de 1.3. Se mantiene como información documentada en este Manual.

### 4.4 El SGI y sus procesos

<!-- [VIVO] tabla de procesos: sgi.process activos (clave, nombre, tipo). [FIJO] los párrafos. -->

PNTQ opera su SGI mediante 14 procesos. Cada uno tiene dueño, entradas, salidas, actividades con responsable y vencimiento, indicadores y riesgos. Se consultan en **SGI → Sistema → Mapa de procesos**.

| Clave | Proceso | Tipo |
| --- | --- | --- |
| E1 | Dirección y planeación | Estratégico |
| E2 | Gestión del SGI | Estratégico |
| C1 | Desarrollo y alta de producto | Cadena de valor |
| C2 | Pedido a entrega | Cadena de valor |
| C3 | Planeación y programación | Cadena de valor |
| C4 | Producción | Cadena de valor |
| C5 | Calidad de producto | Cadena de valor |
| C6 | Almacén e inventarios | Cadena de valor |
| S1 | Compra a pago | Soporte |
| S2 | Facturación, crédito y cobranza | Soporte |
| S3 | Contabilidad, costos y cierre | Soporte |
| S4 | Recursos Humanos y nómina | Soporte |
| S5 | Mantenimiento e instalaciones | Soporte |
| S6 | Tecnología | Soporte |

Para cada proceso, Odoo mantiene:

- **La secuencia e interacción:** los diagramas de mapa de procesos, interacción (4.4) y tortuga, que sustituyen a los Anexos 2 y 3.
- **Los criterios y métodos de control:** las actividades del proceso y la forma en que cada una se mide.
- **Las responsabilidades y autoridades:** la matriz de responsabilidades y los roles de cada actividad (5.3).
- **Los riesgos y oportunidades:** ligados al proceso (6.1).
- **La evaluación:** sus indicadores y el cumplimiento de sus actividades (9.1).

Los recursos se aseguran mediante el Plan de Asignación de Recursos, que es información confidencial de PNTQ y no se audita sin autorización de la Dirección.

## 5. Liderazgo

### 5.1 Liderazgo y compromiso

La alta dirección demuestra su liderazgo y compromiso con el SGI:

| Compromiso | Cómo se evidencia |
| --- | --- |
| Rinde cuentas de la eficacia del SGI | Manifiesto de compromiso (Anexo 8), informes de auditoría y actas de revisión por la dirección en Odoo |
| Establece política y objetivos compatibles con el contexto y la estrategia | **SGI → Planeación → Política integral** y **Objetivos integrales**; Planeación Estratégica (Anexo 1) |
| Integra los requisitos del SGI en los procesos de negocio | Las actividades del SGI se ejecutan en las mismas pantallas de Odoo donde se opera el negocio |
| Promueve el enfoque a procesos y el pensamiento basado en riesgos | Mapa de procesos y **Riesgos y oportunidades** |
| Asegura los recursos | Plan de Asignación de Recursos (confidencial) |
| Comunica la importancia de una gestión eficaz | Difusión de la política con acuse de lectura |
| Asegura que el SGI logra sus resultados | Tablero de Dirección, salud del SGI y salidas de la revisión |
| Promueve la mejora y una cultura de prevención | Seguimiento de acuerdos, de NC mayores y de incidentes graves, que le llegan por correo |
| Garantiza la consulta y participación de los trabajadores | Comisión de Seguridad e Higiene, reporte abierto de incidentes y participación en las investigaciones |

**Enfoque al cliente (5.1.2).** Los requisitos del cliente y los legales se determinan y se cumplen en los procesos C1 y C2. Los riesgos que afectan la conformidad se gestionan según 6.1. La satisfacción se sigue con reclamaciones, devoluciones y **Satisfacción del cliente** (9.1.2).

### 5.2 Política integral

<!-- [VIVO] texto de la política vigente. [FIJO] los párrafos. -->

La política integral (Anexo 9) se mantiene en **SGI → Planeación → Política integral**; solo una puede estar vigente. La política:

- es apropiada al propósito y contexto de PNTQ, incluidos sus impactos ambientales;
- da el marco para los objetivos integrales;
- incluye los compromisos de cumplir los requisitos aplicables, mejorar continuamente, proteger el medio ambiente, proporcionar condiciones de trabajo seguras y saludables, eliminar peligros y reducir riesgos, y consultar y hacer participar a los trabajadores.

Se comunica a cada persona mediante acuse de lectura y está disponible para las partes interesadas.

### 5.3 Roles, responsabilidades y autoridades

Las responsabilidades y autoridades se asignan en tres niveles, todos consultables en Odoo:

1. **Por proceso:** cada proceso tiene un dueño, que responde por su resultado, valida sus indicadores y cierra sus no conformidades.
2. **Por actividad:** cada actividad tiene un rol que ejecuta y, cuando aplica, uno que aprueba y uno al que se escala. Quien aprueba no es quien ejecuta.
3. **Por puesto:** «Mi procedimiento» reúne lo que le toca a cada puesto, y cada persona lo firma de leído.

La matriz de responsabilidades y el diagrama de roles de Odoo sustituyen a los Anexos 4 y 5. El Jefe de MAST y SGI asegura la conformidad del SGI con las normas, informa a la alta dirección sobre su desempeño y mantiene su integridad cuando hay cambios. Nadie audita su propio proceso.

En seguridad y salud, toda persona es responsable de cumplir los controles operacionales, usar su equipo de protección, reportar actos y condiciones inseguras, participar en capacitaciones y simulacros y colaborar en la investigación de incidentes.

## 6. Planificación

### 6.1 Acciones para abordar riesgos y oportunidades

<!-- [FIJO] (los instrumentos son fijos; no listar riesgos). -->

PNTQ identifica, evalúa y trata sus riesgos y oportunidades en Odoo, con cuatro instrumentos. Cada registro lleva proceso, evaluación inicial y residual, acciones con responsable e historial de evaluaciones.

Se abordan para asegurar que el SGI logre sus resultados previstos, aumentar los efectos deseables, prevenir o reducir los efectos no deseados y lograr la mejora. Para determinarlos se consideran las cuestiones de 4.1 y los requisitos de las partes interesadas de 4.2.

| Instrumento | Qué cubre | Dónde | Sustituye a |
| --- | --- | --- | --- |
| Riesgos y oportunidades | Lo que puede afectar los resultados del SGI y de cada proceso (6.1.1) | SGI → Planeación → Riesgos y oportunidades | Matriz F-P-C09-02 y FODA por área |
| Aspectos ambientales | Aspectos e impactos con perspectiva de ciclo de vida, y cuáles son significativos (6.1.2 de ISO 14001) | SGI → Planeación → Aspectos ambientales | Matriz F-P-E01-01 |
| IPER | Peligros y riesgos de seguridad y salud (6.1.2 de ISO 45001) | Riesgos y oportunidades, instrumento IPER | Matriz F-P-S01-01 |
| Requisitos legales y otros requisitos | Obligaciones aplicables y su evaluación de cumplimiento (6.1.3) | SGI → Planeación → Requisitos legales | Matriz F-P-E02-01 |

Reglas de operación:

- Los riesgos se reevalúan al menos dos veces al año, en enero y julio, y después de un incidente o un cambio.
- Un riesgo alto debe tener al menos una acción abierta; si no, el sistema lo marca y avisa al dueño del proceso.
- La identificación de peligros considera actividades rutinarias y no rutinarias, condiciones normales y de emergencia, contratistas y visitantes, factores humanos y cambios.
- Los controles de seguridad siguen la jerarquía: eliminación, sustitución, ingeniería, administrativos y equipo de protección personal. Un riesgo alto no se da por controlado solo con equipo de protección.
- Cada requisito legal se evalúa como cumple, cumple parcialmente, no cumple o no aplica, siempre con evidencia.

### 6.2 Objetivos integrales y planificación para lograrlos

<!-- [VIVO] lista de objetivos integrales con sus indicadores. [FIJO] el párrafo. -->

Los objetivos integrales (Anexo 6) se mantienen en **SGI → Planeación → Objetivos integrales**. Son coherentes con la política, medibles y comunicados. Cada objetivo se liga a sus indicadores y toma el peor color de ellos, de modo que su seguimiento es continuo. Para cada uno se define qué se hará, con qué recursos, quién responde, cuándo termina y cómo se evalúa. Se revisan en la revisión por la dirección.

### 6.3 Planificación de los cambios

Los cambios al SGI se hacen de forma planificada y quedan registrados en Odoo:

- **Cambios a una actividad o a un proceso:** propuesta de cambio, aprobada por el jefe directo de quien propone y por el dueño del proceso.
- **Cambios a un documento:** solicitud de cambio documental; al aprobarse se crea la revisión nueva, la anterior queda obsoleta y se generan los acuses.
- **Cambios de producto o de ingeniería:** órdenes de cambio en el proceso C1.

Cada cambio considera su propósito y consecuencias, la integridad del SGI, los recursos, las responsabilidades, sus efectos en el ambiente y en la seguridad y salud, cuándo se termina y cómo se evalúa su resultado.

## 7. Apoyo

### 7.1 Recursos

- **Personas:** la alta dirección determina y proporciona el personal necesario; la plantilla autorizada se lleva en el proceso S4.
- **Infraestructura:** edificios y servicios, maquinaria y equipo, transporte y tecnologías de la información. Se mantiene en los procesos S5 (mantenimiento preventivo, correctivo y checklists diarios) y S6 (equipos de cómputo, sistemas y comunicaciones).
- **Ambiente para la operación:** factores físicos (temperatura, iluminación, ventilación, ruido e higiene), sociales (trato no discriminatorio, ambiente libre de conflictos) y psicosociales (prevención del estrés y del agotamiento). Las condiciones de trabajo, los estudios de higiene y los controles de seguridad están en el menú **Seguridad y ambiente**.
- **Recursos de seguimiento y medición (7.1.5):** los equipos de medición y de laboratorio, sus calibraciones y verificaciones se controlan en **Calidad → Calidad preventiva → Metrología**. Un equipo con calibración vencida no debe usarse para liberar producto. Si un equipo resulta fuera de calibración, se evalúa si afectó resultados ya liberados y se toman las acciones necesarias, con una no conformidad cuando aplique.
- **Conocimientos de la organización (7.1.6):** se conservan en las actividades e instructivos de cada proceso y en **Mejora → Lecciones aprendidas**, que sustituye a la bitácora de experiencias (Anexo 14).

### 7.2 Competencia

Cada puesto define las competencias que requiere. La competencia de cada persona se registra en su ficha de empleado y la brecha es la base del programa de capacitación (**Empleados → Competencias SGI**). Una competencia se obtiene con educación, formación o experiencia comprobable; la eficacia de cada capacitación la evalúa el jefe inmediato a los 90 días.

### 7.3 Toma de conciencia

Toda persona conoce la política, los objetivos que le aplican, su contribución al SGI, los peligros y aspectos ambientales de su trabajo y las consecuencias de no cumplir.

- **Inducción:** al ingresar, Recursos Humanos imparte la inducción a la empresa y, con el área de SGI, la inducción al SGI: misión, visión y valores; política y objetivos integrales; seguridad y salud en el trabajo (tipos de riesgo, actos y condiciones inseguras); planes de emergencia; y manejo de residuos. Se completa dentro de los tres meses siguientes al ingreso.
- **Puesto:** el jefe inmediato refuerza lo propio de cada puesto.
- **Evidencia:** la firma de su procedimiento y de la política; se verifica en las auditorías internas.

### 7.4 Comunicación, consulta y participación

PNTQ determina qué comunica, a quién, cómo, cuándo y quién lo hace:

| Qué | A quién | Cómo y cuándo | Quién |
| --- | --- | --- | --- |
| Política, objetivos, documentos y sus cambios | Personal | Acuse de lectura en Odoo, al publicarse | Jefe de MAST y SGI |
| Actividades, vencimientos y no conformidades | Responsables | Avisos y actividades de Odoo, al generarse | Dueño del proceso |
| Resultados del SGI: indicadores, auditorías y revisión por la dirección | Dirección y dueños de proceso | Tablero y actas en Odoo, en cada revisión | Jefe de MAST y SGI |
| Requisitos, pedidos, compras y reclamaciones | Clientes y proveedores | Correo y documentos del pedido o de la compra, cuando ocurren | Dueño del proceso (C2, S1, C5) |
| Trámites y reportes oficiales | Autoridades | Plataformas y trámites oficiales, en los plazos legales | Responsable del requisito legal |
| Peligros, controles y emergencias | Personal, contratistas y visitantes | Inducción, señalización, reglamento para contratistas y simulacros | Jefe de MAST y SGI |

La información ambiental y de seguridad que se comunica es coherente con la generada en el SGI y fiable. PNTQ responde a las comunicaciones pertinentes de las partes interesadas y conserva evidencia de sus comunicaciones en el historial de los registros de Odoo y en el correo.

**Consulta y participación de los trabajadores:** Comisión de Seguridad e Higiene y sus recorridos; reporte de incidentes, casi accidentes, actos y condiciones inseguras abierto a todo el personal; formato de quejas y sugerencias, en papel y por código QR; encuesta de consulta y participación; Semana de seguridad y salud anual; y participación en la identificación de peligros, la determinación de controles y las investigaciones. Los trabajadores se consultan antes de los cambios que afecten su seguridad y salud.

### 7.5 Información documentada

<!-- [VIVO] tabla de tipos de documento y patrón de clave: sgi.document.type. [FIJO] las viñetas. -->

La información documentada del SGI se crea, actualiza y controla en Odoo. Este apartado sustituye al procedimiento P-G01.

| Tipo | Clave |
| --- | --- |
| Manual | MIID |
| Procedimiento de proceso | PR-{proceso} |
| Control operacional | CO-{proceso}-{nn} |
| Instructivo de trabajo | IT-{proceso}-{nn} |
| Formato | F-{proceso}-{nn} |
| DAT | DA-{proceso}-{nn} |
| Protocolo | PROT-{proceso}-{nn} |
| Anexos y reglamentos | Conservan su clave |

- **Estados:** borrador, prueba piloto, vigente y obsoleto. Solo lo vigente se usa.
- **Identificación:** todo documento lleva clave, revisión y fecha de emisión. Los registros que imprime Odoo los llevan en cada hoja, con el número de página.
- **Revisión y aprobación:** todo cambio pasa por una solicitud aprobada. Cada documento vigente tiene fecha de próxima revisión y su responsable recibe aviso.
- **Distribución y acceso:** el personal consulta en **SGI → Inicio → Documentos vigentes**. La difusión se evidencia con el acuse de lectura.
- **Lista maestra:** **SGI → Sistema → Documentos → Lista maestra**.
- **Documentos de origen externo:** normas, especificaciones de cliente y disposiciones oficiales se registran como documentos externos y se implantan en 10 días hábiles.
- **Conservación y protección:** los registros no se borran: se archivan o se cancelan con motivo. Lo cerrado solo lo modifica el Jefe de MAST y SGI. El acceso depende del grupo de cada usuario. Los datos de salud y de salarios tienen acceso restringido.
- **Historia:** los documentos y registros anteriores a Odoo se conservan en el Dropbox, de solo lectura.

## 8. Operación

### 8.1 Planificación y control operacional

La operación se planifica y controla mediante las actividades de cada proceso. Cada actividad define su criterio de terminación y qué hacer si falla, y el sistema mide su cumplimiento con los registros de Odoo.

| Requisito | Cómo se cumple | Proceso |
| --- | --- | --- |
| 8.2 Requisitos para los productos y servicios | Cotización, revisión del pedido, confirmación y comunicación con el cliente | C2 |
| 8.3 Diseño y desarrollo | Solicitud de desarrollo, etapas, revisiones, validación con el cliente, cambios de ingeniería, AMEF, plan de control y PPAP cuando el cliente lo exige | C1 |
| 8.4 Procesos, productos y servicios suministrados externamente | Selección y evaluación de proveedores, órdenes de compra con requisitos, inspección de recibo y certificados de análisis | S1, C5 |
| 8.5.1 Control de la producción | Programa de producción, órdenes de fabricación, instructivos, fichas de proceso por máquina, mantenimiento y acciones para prevenir errores humanos | C3, C4, S5 |
| 8.5.2 Identificación y trazabilidad | Lote por orden de fabricación, de la materia prima al producto entregado | C4, C6 |
| 8.5.3 Propiedad del cliente o del proveedor | Materiales de maquila y bienes del cliente o del proveedor identificados y controlados en inventario; si se pierden, se deterioran o no sirven, se informa a su dueño, se toman acciones de contingencia y se conserva el registro | C6 |
| 8.5.4 Preservación | Almacenamiento, manejo, empaque y embarque | C6, C2 |
| 8.5.5 Actividades posteriores a la entrega | Atención de reclamaciones y devoluciones, considerando los requisitos legales, las consecuencias no deseadas, la naturaleza y vida útil del producto, y los requisitos y la retroalimentación del cliente | C5, C2 |
| 8.5.6 Control de los cambios | Ver 6.3 | C1, E2 |
| 8.6 Liberación de los productos | Inspección, pruebas de laboratorio y reporte de conformidad antes de embarcar | C5 |
| 8.7 Salidas no conformes | Retención e identificación del producto, disposición y no conformidad (ver 10.2) | C5 |

No aplican 8.4.1 b) y 8.5.1 f), según 1.3. La situación del Plan integral (Anexo 13), donde la revisión 02 presentaba el resultado de esta planificación, se define en 12.1.

### 8.2 Control operacional ambiental y de seguridad y salud

<!-- [VIVO] lista de controles operacionales vigentes (tipo control_operacional). [FIJO] lo demás. -->

- **Controles operacionales documentados:** almacenamiento y transporte de sustancias peligrosas (CO-E2-01), manipulación de cargas (CO-E2-02), trabajos en altura (CO-E2-03), bloqueo de energía (CO-E2-04) y equipo de protección personal (CO-E2-05).
- **Permisos de trabajo de alto riesgo** y **bloqueo y etiquetado**, registrados en Odoo antes de iniciar el trabajo.
- **Contratistas:** se sujetan al reglamento de seguridad para contratistas y a los mismos permisos.
- **Equipo de protección personal:** se entrega con responsiva por persona.
- **Residuos, descargas y consumos:** se controlan según los aspectos ambientales significativos y los requisitos legales.
- **Checklists diarios** de equipos y unidades; sus fallas generan correctivos de mantenimiento.

### 8.3 Preparación y respuesta ante emergencias

PNTQ se prepara y responde ante emergencias con los planes de emergencia del centro de trabajo y los simulacros, que se mantienen en **SGI → Seguridad y ambiente**.

- **Escenarios:** emergencias médicas, incendio, fugas o derrames de sustancias químicas, fenómenos naturales y fallas operacionales que puedan afectar a las personas o al ambiente.
- **Recursos:** sistemas de alarma, equipo contra incendio, estaciones de primeros auxilios, equipo de contención de derrames, medios de comunicación interna y brigadas.
- **Prueba periódica:** los simulacros se programan y se evalúan por tiempo de respuesta, eficacia de la comunicación y desempeño de las brigadas. Cada uno queda registrado con su evaluación y sus oportunidades de mejora.
- **Revisión:** el plan se revisa después de cada simulacro y de cada emergencia real; de una emergencia real se levanta una no conformidad.
- **Información y formación:** el personal se capacita desde su inducción; contratistas y visitantes reciben la información de emergencia a su ingreso.

## 9. Evaluación del desempeño

### 9.1 Seguimiento, medición, análisis y evaluación

**Indicadores (9.1.1).** Cada indicador tiene proceso, dueño, fórmula, frecuencia, objetivo y rango aceptable. Sustituyen a la captura mensual en el formato F-P-A10-03.

1. El sistema crea la medición de cada periodo; la calcula con los registros de Odoo o la pide a su responsable.
2. El dueño del indicador la valida contra la evidencia.
3. Un resultado en rojo exige causa y plan de acción; en los indicadores críticos, además, levanta una no conformidad.
4. La medición validada conserva las metas con las que se juzgó.

Un indicador nuevo opera «en prueba» hasta que su dueño confirma una medición contra la realidad y lo pasa a oficial. La ficha de cada indicador, con su tendencia, se imprime desde Odoo.

**Cumplimiento de las actividades.** El sistema mide por proceso y por puesto si cada actividad se hizo en su periodo (**SGI → Administración → Diagnóstico → Cumplimiento de procedimientos**).

**Desempeño de seguridad y salud.** Se evalúa con sus indicadores, los incidentes, los actos y condiciones inseguras, las inspecciones y recorridos, y el cumplimiento de los requisitos legales.

**Satisfacción del cliente (9.1.2).** Se evalúa con las reclamaciones y devoluciones, las calificaciones de los clientes y el registro de **Satisfacción del cliente**.

**Evaluación del cumplimiento (9.1.2 de ISO 14001 e ISO 45001).** Cada requisito legal se evalúa periódicamente con evidencia; el historial queda en **Evaluaciones de cumplimiento legal**.

**Análisis y evaluación (9.1.3).** Los dueños de proceso y la Dirección analizan los resultados del seguimiento y la medición para evaluar la conformidad de los productos y servicios, la satisfacción del cliente, el desempeño y la eficacia del SGI, si lo planificado se implementó de forma eficaz, la eficacia de las acciones sobre riesgos y oportunidades, el desempeño de los proveedores externos y la necesidad de mejoras. Sus conclusiones son entrada de la revisión por la dirección (9.3).

### 9.2 Auditoría interna

Las auditorías se planifican, ejecutan y cierran en **SGI → Desempeño → Auditorías**. Este apartado sustituye al procedimiento P-G03.

- **Programa anual:** lo aprueba el Jefe de MAST y SGI y cubre todos los procesos en un ciclo máximo de tres años. El reporte «Programado contra realizado» muestra su avance.
- **Independencia:** ningún auditor audita su propio trabajo. El proceso E2 lo audita un auditor distinto del Jefe de MAST y SGI.
- **Ejecución:** el checklist se genera de las actividades de los procesos auditados; se registran las reuniones de apertura y cierre.
- **Hallazgos:** cada uno lleva tipo, cláusula, proceso y evidencia. Toda no conformidad, mayor o menor, genera su NC.
- **Informe:** queda archivado en Odoo al cerrar la auditoría.

### 9.3 Revisión por la dirección

La Dirección de Operaciones revisa el SGI a intervalos planificados y después de cada auditoría, en **SGI → Desempeño → Revisión por la dirección**. Sustituye al instructivo IT-P-A10-01.

**Entradas.** El sistema carga, con los datos reales del periodo: el estado de los acuerdos anteriores; los cambios en el contexto y en las partes interesadas; el desempeño de los procesos y de los indicadores; las no conformidades y acciones correctivas; los resultados de auditorías; la satisfacción del cliente, las quejas y las reclamaciones; el desempeño de los proveedores; los riesgos y oportunidades; los aspectos ambientales significativos; el cumplimiento legal; los incidentes y el desempeño de seguridad y salud; la consulta y participación de los trabajadores; la adecuación de los recursos; y las oportunidades de mejora.

**Salidas.** Las conclusiones sobre la conveniencia, adecuación y eficacia del SGI, las oportunidades de mejora, las necesidades de cambio y de recursos, y los acuerdos. Cada acuerdo se convierte en una acción con responsable y fecha; los que no se cierran se cargan en la siguiente revisión.

El acta de cada revisión cerrada se conserva en Odoo como evidencia.

## 10. Mejora

### 10.1 Generalidades

PNTQ determina y selecciona las oportunidades de mejora a partir de la evaluación del desempeño y las registra en **SGI → Mejora → Mejora continua**, que sustituye al plan de mejora F-P-A10-02.

### 10.2 No conformidad y acción correctiva

<!-- [VIVO] plazos 1/10/15: parámetros quimibond_sgi.nc_days_*. [FIJO] lo demás. -->

Toda no conformidad se registra y se trata en **SGI → Mejora → No conformidades**, sea cual sea su origen: proceso, auditoría interna o externa, reclamación de cliente, indicador incumplido, incidente o recorrido de la Comisión. Este apartado sustituye a los procedimientos P-G04 y P-G05 y al control F-P-G05-02.

Ante una no conformidad se reacciona para controlarla y corregirla y se hace frente a sus consecuencias, incluida la mitigación de los impactos ambientales adversos. Después se evalúa si hace falta eliminar su causa para que no vuelva a ocurrir ni ocurra en otra parte, revisando si existen no conformidades similares o que puedan ocurrir.

| Etapa | Qué se exige |
| --- | --- |
| Abierta | Contención en 1 día hábil; causa raíz en 10; plan de acción en 15 |
| Seguimiento | Clasificación (mayor, menor u observación) y cláusula incumplida; acciones con responsable y fecha |
| Cerrada | Causa raíz, acciones terminadas con evidencia y eficacia verificada con resultado «Eficaz» |
| Cancelada | Solo con motivo aprobado por el Jefe de MAST y SGI |

- La verificación de eficacia y el cierre corresponden al dueño del proceso o al Jefe de MAST y SGI.
- Una eficacia «No eficaz» obliga a una acción correctiva nueva.
- Una no conformidad mayor exige el análisis completo de causa, llevar la lección al AMEF, al plan de control o al documento, y se informa a la Dirección.
- El sistema identifica las reincidencias por proceso.
- Las reclamaciones de cliente se responden en los plazos acordados; cuando el cliente lo pide, con formato 8D.
- Los incidentes y accidentes se investigan con análisis de causas, participación de trabajadores y verificación de eficacia (**SGI → Seguridad y ambiente → Incidentes y accidentes**).

Si es necesario, el tratamiento de una no conformidad actualiza los riesgos y oportunidades y genera cambios al SGI.

### 10.3 Mejora continua

PNTQ mejora continuamente la conveniencia, adecuación y eficacia del SGI con los resultados de los indicadores, las auditorías, las no conformidades y las salidas de la revisión por la dirección.

## 11. Correspondencia

### 11.1 Cláusulas de las normas y dónde se evidencian en Odoo

<!-- [VIVO] idealmente de la matriz de cumplimiento por cláusula; la tabla escrita sirve de respaldo. -->

| Cláusula | Tema | Evidencia en Odoo |
| --- | --- | --- |
| 4.1 y 4.2 | Contexto y partes interesadas | SGI → Planeación → Partes interesadas; diagrama de contexto |
| 4.4 | Procesos | SGI → Sistema → Mapa de procesos y Actividades |
| 5.2 | Política | SGI → Planeación → Política integral, con sus acuses |
| 5.3 | Roles y responsabilidades | Matriz de responsabilidades; Mi procedimiento |
| 6.1 | Riesgos, aspectos, IPER y requisitos legales | SGI → Planeación → Riesgos y oportunidades, Aspectos ambientales y Requisitos legales |
| 6.2 | Objetivos | SGI → Planeación → Objetivos integrales |
| 6.3 y 8.5.6 | Cambios | App Aprobaciones; Solicitudes de cambio |
| 7.1.5 | Equipos de medición | Calidad → Calidad preventiva → Metrología |
| 7.2 y 7.3 | Competencia y toma de conciencia | Empleados → Competencias SGI; acuses de lectura |
| 7.4 | Comunicación, consulta y participación | Recorridos CSH; Reportar; Quejas y sugerencias del personal |
| 7.5 | Información documentada | SGI → Sistema → Documentos y Lista maestra |
| 8.1 | Control operacional | Actividades de cada proceso; permisos de trabajo; bloqueo y etiquetado; checklists |
| 8.2 de ISO 14001 e ISO 45001 | Emergencias | Seguridad y ambiente → Planes de emergencia y Simulacros |
| 8.3 | Diseño y desarrollo | Proceso C1; AMEF, planes de control y PPAP |
| 8.4 | Proveedores | Compras → Evaluación de proveedores; CoA recibidos |
| 8.7 y 10.2 | Salidas no conformes, NC y acciones | SGI → Mejora → No conformidades y Acciones correctivas |
| 9.1 | Indicadores y cumplimiento | SGI → Desempeño → Indicadores; Administración → Diagnóstico |
| 9.1.2 | Satisfacción del cliente; cumplimiento legal | Satisfacción del cliente; Evaluaciones de cumplimiento legal |
| 9.2 | Auditoría interna | SGI → Desempeño → Auditorías |
| 9.3 | Revisión por la dirección | SGI → Desempeño → Revisión por la dirección |
| 10.2 de ISO 45001 | Incidentes | Seguridad y ambiente → Incidentes y accidentes |
| 10.3 | Mejora continua | SGI → Mejora → Mejora continua y Lecciones aprendidas |

El detalle, cláusula por cláusula y actividad por actividad, está en la matriz de cumplimiento de Odoo.

### 11.2 Procedimientos anteriores y proceso que los sustituye

<!-- [VIVO] documentos tipo procedimiento por sgi_replaced_by_process_id / sgi_process_id. -->

Según el registro de Odoo al 5 de octubre de 2026. Un procedimiento anterior queda obsoleto cuando el proceso que lo sustituye se publica.

| Proceso | Procedimientos anteriores |
| --- | --- |
| E1 Dirección y planeación | P-A10, P-A32 |
| E2 Gestión del SGI | P-A04, P-A14, P-A24, P-A33, P-E01, P-E02, P-E04, P-E06, P-G01, P-G03, P-G05, P-S01, P-S02 |
| C1 Desarrollo y alta de producto | P-D01, P-D02 |
| C2 Pedido a entrega | P-A16, P-A28, P-C07 |
| C3 Planeación y programación | P-A03, P-A12 |
| C4 Producción | P-I01, P-P01, P-P02 |
| C5 Calidad de producto | P-C01 a P-C06, P-C09, P-C11 a P-C14, P-C16, P-C17, P-G04 |
| C6 Almacén e inventarios | P-A07 |
| S1 Compra a pago | P-A02 |
| S2 Facturación, crédito y cobranza | P-A22 (pendiente de registrar en Odoo) |
| S3 Contabilidad, costos y cierre | P-A25; P-A13 (pendiente de registrar en Odoo) |
| S4 Recursos Humanos y nómina | P-A01, P-A26 |
| S5 Mantenimiento e instalaciones | P-M01 |
| S6 Tecnología | P-A30 (pendiente de registrar en Odoo) |

Se conservan como controles operacionales, con clave nueva: P-A17 (CO-E2-01), P-A18 (CO-E2-02), P-A19 (CO-E2-03), P-A20 (CO-E2-04) y P-S03 (CO-E2-05).

## 12. Anexos y control de cambios

### 12.1 Anexos

<!-- [VIVO] anexos vigentes (tipo anexo). La columna «Situación» es [FIJO] y está por confirmar. -->

| Anexo | Nombre | Situación en la revisión 03 |
| --- | --- | --- |
| 1 | Planeación Estratégica 2026-2030 | Se conserva como documento |
| 2 | Mapeo de procesos | Lo sustituye el mapa de procesos de Odoo |
| 3 | Modelo de interacción de procesos | Lo sustituyen los diagramas de interacción y tortuga de Odoo |
| 4 | Matriz de responsabilidades | La sustituye la matriz de responsabilidades de Odoo |
| 5 | Matriz de autoridad | La sustituyen los roles «Aprueba» de las actividades y las reglas de Aprobaciones |
| 6 | Objetivos integrales | Se llevan en Odoo; el anexo se conserva como documento firmado |
| 7 | Nombramiento de la alta dirección | Se conserva como documento firmado |
| 8 | Manifiesto de compromiso | Se conserva como documento firmado |
| 9 | Política integral | Se lleva en Odoo con acuses; el anexo se conserva como documento firmado |
| 10 | Misión y visión | Se conserva como documento |
| 11 | Carta de documentos | La sustituye la lista maestra de Odoo |
| 12 | Estructura organizacional | La sustituye el organigrama de Empleados en Odoo |
| 13 | Plan de calidad | Se conserva como documento |
| 14 | Bitácora de experiencias adquiridas | La sustituye Lecciones aprendidas |
| 15 | Cuestiones internas y externas | Se conserva como documento; su revisión es entrada de la revisión por la dirección |

La columna de situación es una propuesta: el Jefe de MAST y SGI y la Dirección deben confirmar qué anexos se sustituyen y cuáles se conservan.

### 12.2 Control de cambios

<!-- [VIVO] historial de revisiones del documento MIID. -->

| Revisión | Fecha | Cambio |
| --- | --- | --- |
| 03 | Borrador del 5-oct-2026 | El SGI pasa a operar en Odoo: 14 procesos con actividades en lugar de procedimientos por área; riesgos, indicadores, no conformidades, auditorías, documentos y revisión por la dirección en Odoo; claves nuevas de documentos; correspondencia con los procedimientos anteriores |
| 02 | Junio de 2026 | Revisión vigente hasta la aprobación de la 03 |

### 12.3 Pendiente antes de aprobar

<!-- NO CARGAR: lista de trabajo del borrador, no es parte del manual. -->

- [ ] Publicar los 14 procesos en Odoo (hoy 13 en borrador y C2 en piloto).
- [ ] Confirmar el alcance de ISO 45001 contra el certificado o el informe de auditoría.
- [ ] Confirmar la justificación de las no aplicabilidades de 1.3.
- [ ] Confirmar la situación de cada anexo (12.1), incluido el Anexo 13: la revisión 02 lo llama «Plan integral» y presenta en él el resultado de la planificación operacional (8.1).
- [ ] Registrar en Odoo los procedimientos P-A13, P-A22 y P-A30.
- [ ] Revisión del Jefe de MAST y SGI y aprobación del Director de Operaciones.
- [ ] Cargarlo en Odoo como revisión nueva del MIID, mediante solicitud de cambio documental, y generar los acuses.
