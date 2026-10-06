# C1 Desarrollo y alta de producto: migración de formatos a Odoo

> Copia del documento de Drive `C1_desarrollo_producto_brief_claude_code.md`
> (6 de octubre de 2026), guardada aquí para que las decisiones queden en el
> repo. El plan de implementación y lo que cambia respecto a este documento
> al leer el código está en
> `docs/superpowers/plans/2026-10-06-c1-desarrollo-producto-plan.md`.

Encargo para una sesión de Claude Code sobre el repositorio quimibond/qb19. Fecha del levantamiento: 6 de octubre de 2026. Quien lo pide: Jose Mizrahi (Director de Finanzas y Administración, usuario Odoo id 7), que programa él mismo el módulo.

## 1. Qué se te pide

Quimibond está sacando de Excel y Word todos los formatos del procedimiento **C1 Desarrollo y alta de producto** para operarlo completo en Odoo 19 con el módulo quimibond_sgi. Hoy se revisó el procedimiento actividad por actividad con su dueña, Jessica Francisco, y con Jose. Este documento trae lo que se confirmó.

Tu trabajo tiene dos partes, en este orden:

1. **Código** en quimibond/qb19: los cambios de modelo, vistas, reportes y automatizaciones de la sección 6.
2. **Datos por MCP** en la base de Odoo, al final y solo cuando el código esté desplegado: las correcciones de la sección 7 (fichas del SGI, plantillas, catálogos, limpieza).

El resultado bueno es este: una solicitud de cliente entra por correo, se convierte en un proyecto, y de ahí hasta la liberación del artículo nadie llena un Excel, nadie recaptura un dato que ya existe y nadie escribe en texto libre algo que es un dato.

## 2. Reglas que no se negocian

- **Nada de texto plano para datos.** Es la objeción central de Jose: "son datos que dejamos de aprovechar". Si algo se puede capturar como número, selección, relación o renglón, no va en un campo de texto ni en la descripción. El texto libre queda solo para observaciones.
- **Captura única.** La misma tabla de características aparece hoy en al menos siete formatos distintos. En Odoo es una sola tabla con varias columnas (sección 5).
- **Nombres nuevos y limpios.** No mezcles las claves viejas del Dropbox (F-P-D01-02, P-A28, etc.) en nombres de menús, vistas o tareas. Usa las claves nuevas del SGI (F-C1-01, etc.) solo donde vaya la clave del formato impreso.
- **Solo Quimibond.** La base es multicompañía. Todo se filtra a company_id = 1 (PRODUCTORA DE NO TEJIDOS QUIMIBOND).
- **JSON-RPC, no XML-RPC,** en cualquier integración.
- **Ramas.** Ramas de desarrollo → main (staging; ahí se sube todo antes de producción) → quimibond (producción). qbtesting es copia de producción para pruebas. No subas nada a quimibond sin que Jose lo indique.
- **Aprobaciones por puesto, no por persona.** El SGI asigna por hr.job. Cuando un puesto está vacante hay suplencia definida (sección 4).
- **Lee el código antes de proponer.** Este levantamiento se hizo leyendo datos por MCP, no el código del módulo. Todo lo que aquí dice "el módulo ya tiene X" está verificado contra los modelos y campos visibles por MCP, pero no contra la lógica. Confirma en el código cómo se calcula cada cosa antes de cambiarla.
- **No inventes lo pendiente.** La sección 8 lista lo que nadie ha definido. Déjalo como parámetro vacío o como tarea abierta; no lo rellenes con un supuesto.

## 3. Estado actual, verificado en la base

### Proceso y personas

| Dato | Valor |
|---|---|
| Proceso | sgi.process id 105, "C1 - Desarrollo y alta de producto", estado borrador |
| Dueña | Jessica Francisco (usuario 22, empleado 244), puesto Administrador de Ventas y Marketing |
| Actividades | sgi.process.activity ids 564 a 581 (C1.01 a C1.18) y 755 (C1.19) |
| Otro proceso de Jessica | C2 Pedido a entrega, id 93, estado piloto. Fuera de este encargo |

| Persona | Usuario | Empleado | Puesto (hr.job) |
|---|---|---|---|
| Jessica Francisco | 22 | 244 | 207 Administrador de Ventas y Marketing |
| Selena Manríquez | 107 | — | 224 Diseño y Desarrollo de Producto |
| Yetlanezi Tablas ("Yet") | 149 | 470 | 225 Diseño y Desarrollo de Procesos |
| Jose Mizrahi | 7 | — | 183 Director de Finanzas y Administración |
| Jorge Ortiz | 35 | 6 | 230 Director de Operaciones |
| Francisco González | **sin usuario** | 564 | 211 Jefe de Manufactura |
| Ariadna Lara | 131 | 90 | 188 Coordinador de Laboratorio y MP |
| Cynthia Santana | 15 | 11 | 194 Jefe de Inventarios y Almacenes |
| Eduardo Santiago | 164 | 352 | 199 Ingeniero de Calidad |
| José Luis Almazán | 43 | 21 | 209 Supervisor de Entretelas (en la práctica es Inspección; ver sección 8) |

Otros puestos que se usan: 221 Planeador de Producción, 204 Jefe de Calidad, 214 Auxiliar de Compras, 205 Supervisor Tintorería, 187 Supervisor TAC, 63 Mecánico de Tejido, 182 Director Estratégico.

### Lo que ya existe en el módulo y no se usa

| Pieza | Estado |
|---|---|
| Campos sgi_dev_* en project.project (tipo de desarrollo, fecha, solicitante, uso, muestra en m y kg, norma, empaque, propiedad del cliente, precio objetivo, volumen, elaboró, aprobó) | Existen. La pestaña solo se ve si sgi_is_ft es verdadero |
| sgi_is_ft | Verdadero en **2 de 79** proyectos cuyo nombre empieza con FT-. Los existentes no se marcaron |
| sgi.dev.characteristic (característica, dirección largo/ancho, unidad, valor pedido, tolerancia, método) | **0 registros** en toda la base. Todos los valores son char |
| qb.producto.ficha y qb.producto.ficha.spec (ficha técnica por producto, con sección, valor, tolerancia, método y marca "aparece en el certificado") | 1,839 fichas, **0 renglones de especificación** |
| sgi.machine.sheet con sgi.machine.sheet.param y sgi.machine.sheet.yarn (ficha de proceso de máquina circular: poleas, hilos, parámetros, cuatro firmas) | **0 registros** |
| sgi.ppap con catálogo de 18 elementos AIAG en sgi.ppap.element.template | **0 PPAP** |
| sgi.control.plan | 10 planes, todos genéricos por área; ninguno por artículo |
| qb.cotizacion (módulo de costeo y cotización) | En uso: 71 registros. Estados draft, done (Presentada), won, lost, superseded. Sin paso de aprobación y sin liga a proyecto |
| Plantillas de proyecto | 480 "PLANTILLA - Diseño y Desarrollo TAC" y 481 "PLANTILLA - Diseño y Desarrollo Entretelas". Sus tareas se llaman como los formatos viejos |

### Cómo se trabaja hoy en realidad

- **Proyectos FT.** 79 con nombre FT-…. De los 48 más recientes, 33 no tienen ninguna tarea. Desde que existen las plantillas (25-ago-2026) se abrieron 2 proyectos y ninguno salió de plantilla. El folio se escribe a mano y con dos formatos (FT-005/2026 y FT-005-2026). 27 no tienen cliente.
- **La etapa del proyecto se usa como nombre de cliente** (project.project.stage: SHAWMUT, BOWEN, CONTITECH…), no como avance.
- **Proyecto de análisis.** El 6-oct-2026 Selena creó el proyecto 490 "ANALISIS DE PROYECTO SHAWMUT" con 8 tareas (analizar muestra, ruta de proceso, costeo, altas de código, cotización, aprobación de inicio). Es una forma nueva que quieren adoptar: analizar y cotizar antes de abrir el FT. Las características que pide el cliente se anotan en texto en la descripción.
- **Muestras con artículo genérico.** De julio a hoy hay 65 órdenes de fabricación de muestra; las 30 más recientes usan [MUESTRA PILOTO TEJIDO] (producto 16292) o [MUESTRA PILOTO TINTORERÍA] (producto 16293), en los tipos de operación normales (85 Tejido Circular, 88 Tintorería), sin origin. El tipo 86 "Tejido Desarrollo" no se usa desde el 16-jul-2026. El motivo original del genérico era no crear códigos que no se iban a usar.
- **Almacén de desarrollos.** Ubicación 57 "Toluca/Stock/31 DESARROLLOS TOLUCA": 152 lotes con existencia, el más antiguo de abril de 2022. Ahí sí están con su código real. Nadie decide qué hacer con el sobrante.
- **Cotizaciones.** De 32 vigentes: 6 presentadas con vigencia vencida, 1 ganada, 0 perdidas, 25 en borrador. Nadie les da seguimiento ni las cierra. El cliente sigue recibiendo un Word porque la plantilla PDF del módulo no está lista.
- **Respuesta del cliente.** Llega por correo a Jessica y solo se la comenta de palabra a Selena. No queda registrada.
- **Precio en tarifa.** Jessica lo captura a mano y pasa que llega el pedido sin tarifa.

## 4. El proceso como quedó confirmado

### Secuencia

| # | Paso | Quién ejecuta | Quién aprueba o revisa |
|---|---|---|---|
| 1 | Llega la solicitud por correo; abre el proyecto y captura lo que pide el cliente | Jessica | — |
| 2 | Analiza la muestra o la especificación; busca artículos parecidos | Selena (pide pruebas al laboratorio; Ariadna autoriza la solicitud de pruebas) | Jessica revisa |
| 2a | Si un artículo existente cumple todo: se cotiza ese y **no** hay FT | Jessica | — |
| 3 | Define ruta y centros de trabajo; contesta el checklist de factibilidad | Yet | Jessica revisa |
| 4 | Alta de código (generado) y lista de materiales | Selena | — |
| 5 | Cotiza en el módulo de costeo y propone precio | Jessica | **Jose aprueba todas**; regresa a recotizar si el margen está bajo |
| 6 | Envía cotización y aprobación de inicio al cliente | Jessica (el documento se genera solo) | — |
| 7 | Cliente aprueba el inicio por correo, OC, WhatsApp o cotización firmada | Cliente, o quien lo pidió en Dirección si es interno | — |
| 8 | Se asigna folio FT; se elabora la Solicitud de desarrollos | Selena | Jorge aprueba |
| 9 | Aviso a partes interesadas; si falta materia prima, requisición a Compras | Odoo / Selena | Jorge autoriza la requisición |
| 10 | Pide la corrida de muestra y acuerda fecha de máquina | Yet solicita; **Planeación emite la orden** | — |
| 11 | Corre la muestra con parámetros propuestos y reales por área | Yet propone | Valida el supervisor del área |
| 12 | Pruebas de laboratorio a la muestra | Laboratorio (solo mide) | **Selena valida** que cumple |
| 13 | Baja de muestra y envío por paquetería o recoge el cliente | Almacén (Cynthia) | — |
| 14 | Aviso al cliente con dimensiones de rollos y CoA | Jessica | — |
| 15 | Respuesta del cliente: aprueba, pide cambios o rechaza | Jessica registra | — |
| 16 | Pilotajes: son los **tres primeros lotes del primer pedido** | Yet | — |
| 17 | **Verificación** = estudio de habilidad con esos tres lotes | Yet, Selena, Eduardo | — |
| 18 | **Validación** = ficha técnica interna y Especificaciones del producto | Selena | Seis puestos firman la interna (Jessica entre ellos) |
| 19 | Aprobación final: PPAP y PSW si es automotriz; correo de aceptación o primera OC en los demás | Eduardo arma el PPAP; Jessica registra la evidencia | — |
| 20 | Liberación del artículo y cierre del proyecto | Selena y Yet | — |
| 21 | Revisión mensual de desarrollos abiertos | Jessica, Yet y Selena | Sin definir a detalle (sección 8) |

### Reglas confirmadas

- **Tiempo.** De la solicitud del cliente a la cotización enviada (pasos 1 a 6): **máximo 1 día**. De la Solicitud de desarrollos al producto final: **15 días, sin contar el tiempo que la materia prima tarda en llegar a planta**.
- **Escalamiento en dos niveles.** Si Selena o Yet no cumplen, escala a Jessica. Si Jessica no actúa, escala a Jorge. Jorge no recibe avisos desde el inicio.
- **Análisis sin muestra.** Se puede hacer solo con la especificación. El reloj corre desde el correo, no desde la llegada de la muestra física.
- **Qué es producto nuevo.** Cualquier cambio en la especificación respecto a un artículo existente genera proyecto nuevo.

| Situación | Qué se hace |
|---|---|
| Un artículo existente cumple todo | Se cotiza ese; no hay FT |
| Desarrollo abierto y el cliente ajusta lo que pidió | Mismo proyecto, sube revisión |
| Producto ya liberado y hay cualquier cambio | FT nuevo y código nuevo |
| Solicitud parecida a un artículo existente, con alguna diferencia | FT nuevo, con el existente como base |

- **Folio FT.** Consecutivo de todos los proyectos, se reinicia cada año (FT-001-2027). Hoy lo asigna Selena a mano.
- **Código del artículo.** Sale de la regla del DAT de codificación (sección 5). Se da de alta **antes de cotizar**, marcado "en desarrollo" (decisión de Jose, opción A).
- **Costo.** Jose aprueba todas las cotizaciones de desarrollo. **Cualquier cambio que mueva el costo**, por pequeño que sea, regresa a su aprobación. El margen mínimo no está definido.
- **Cantidad de muestra.** La mayor de tres: lo que diga la OC del cliente; lo necesario para entregar 50 m en calidad PQ; el mínimo de carga de la máquina de tintorería si lleva teñido.
- **Sobrante.** Lo PQ se queda en la ubicación 57 (31 Desarrollos Toluca); lo FE va a su ubicación.
- **Muestra física del cliente.** Llega a nombre de Jessica, se la pasa a Selena, se corta en tamaño carta y se guarda en una carpeta.
- **Desarrollos internos.** Mismos pasos; en lugar del cliente va quien lo pide en Dirección.
- **Partes interesadas** que reciben la Solicitud aprobada, siempre las mismas: Diseño de Procesos (225), Jefe de Manufactura (211), Inspección (José Luis Almazán), Coordinador de Laboratorio y MP (188), Jefe de Inventarios y Almacenes (194), Ingeniero de Calidad (199).
- **Validación de corrida por área.** Tintorería: su supervisor. Acabado: su supervisor. Tejido: no hay supervisor, así que valida **solo el Jefe de Manufactura**. Ventas no firma fichas de proceso.
- **Dos juegos de límites.** La ficha interna usa márgenes de control más cerrados; al cliente se le da más tolerancia. Son dos documentos distintos con contenido distinto; no los fusiones.
- **Aprobación final.** Solo en automotriz es necesaria la muestra firmada. Para los demás, Jose aceptó que la evidencia sea el correo de aceptación o la primera orden de compra.
- **Documentos al cliente.** La gran mayoría pide CoA con la muestra. Las Especificaciones del producto las manda Jessica.

## 5. Dos piezas que sostienen todo

### 5.1 La tabla de características

Es el corazón del cambio. Hoy la misma tabla se recaptura en: Análisis de mercado, texto de la descripción, Análisis de muestras del cliente, Solicitud de pruebas para laboratorio, Aprobación para iniciar un proyecto, Solicitud de desarrollos (4 variantes), Resultados de acabado, Resultados de proceso, Modificación de proyectos, Aprobación de proyecto, ficha técnica interna y Especificaciones del producto.

En Odoo es **una tabla por proyecto** (base: sgi.dev.characteristic), un renglón por característica, con estas columnas:

| Columna | Quién la llena | Cuándo |
|---|---|---|
| Característica (de catálogo), dirección, unidad, método o norma | Precargado por tipo de producto | Al elegir el tipo |
| Especificación del cliente: valor nominal y tolerancia | Jessica | Paso 1 |
| Medido en la muestra del cliente | Laboratorio, a solicitud de Selena | Paso 2 |
| Límite de control interno (más cerrado que el del cliente) | Selena | Al emitir la ficha |
| Obtenido en corrida: tres lecturas y promedio | Laboratorio | Paso 12 y lotes de pilotaje |
| Aprobado por el cliente | Jessica | Paso 19 |

Cambios de modelo necesarios:

- **Valores numéricos**, no char, para poder comparar contra tolerancia, calcular diferencias y Cpk. Deja un campo de texto aparte solo para características cualitativas (tacto, color).
- **Catálogo de características** por tipo de desarrollo (general, entretelas V10, carda, tramado), con unidad y norma. Al elegir el tipo se cargan los renglones. Los catálogos de cada formato están en el anexo A.
- **Posición** además de dirección: izquierda, centro, derecha (la solidez al frote lo pide).
- **Rendimiento calculado:** 1000 / (masa g/m² × ancho m).
- **Tres resultados por lote:** dentro del control interno (cumple); fuera del interno pero dentro del cliente (se puede embarcar, con aviso a Calidad y a Yet); fuera del cliente (no conforme).
- **El certificado de calidad nunca muestra el límite interno.** Se emite contra la especificación del cliente. El estudio de habilidad también se calcula contra la del cliente.
- **Mismo catálogo para qb.producto.ficha.spec,** de modo que al liberar el artículo los renglones del proyecto pasen a su ficha técnica sin recaptura. Agrega ahí una marca "aparece en la especificación del cliente", igual a la que ya existe para el certificado.

### 5.2 La regla de codificación

Documento fuente: "DAT P-D02-01 Codificación de artículos para el área de tejido y acabado" (documents.document id 4694). Código de 16 posiciones:

| Posición | Dato | Valores |
|---|---|---|
| 1 | Composición | B poliéster brillante/semi opaco; C cotton; D polipropileno; L lyocell; N nylon 100 %; T poliéster 100 % fibra corta; V poliéster/carbón; W poliéster 100 %; X poliéster/algodón; Y poliéster/mezcla; Z varios |
| 2 | Dibujo | A cardigan; B crepe anulado; C crepe cargas; D desagujado; F flece; J jersey; K interlock; L listado; M punto de Roma; N piqué anulado; P piqué cargas; Q jaquard; R rib; T french terry; V vanizado |
| 3 a 5 | Peso en g/m² | Tres dígitos |
| 6 | Tipo de hilo | Q natural; B preteñido; R reciclado |
| 7 y 8 | Galga, por rango | Galga 14 → 01-10; 16 → 11-20; 18 → 21-30; 22 → 31-45; 24 → 46-55; 26 → 56-65; 28 → 66-75; 30 → 76-85; 32 → 86-95 |
| 9 | Operación | H tejido (crudo); I teñido; J acabado |
| 10 y 11 | Color | AC azul cielo; AM amarillo; AZ azul marino; BL blanco; CF café; GO gris oxford; HU hueso; NA naranja; NG negro; NT natural; RO rojo; CJ café jaspe; AJ azul jaspe; GJ gris jaspe; GR gris rata; GC gris claro; MO morado; MZ mostaza; AN amarillo neón; NN natural negro |
| 12 a 14 | Ancho de tela abierta | Tres dígitos |
| 15 y 16 | Acabado (opcional) | IN ignífugo; AB antibacterial; AF afelpado; ES esmerilado; SU tacto suave; RI tacto rígido; AE antiestático; RE repelente; DI dry; BI biopulido; RA resina; TF termofijado |

Ejemplo: WJ053Q22JNT160 = poliéster 100 %, jersey, 53 g/m², hilo natural, galga 18, acabado, natural, 160 cm.

**Verifica las tablas contra el PDF antes de cargarlas:** se extrajeron de un PDF en columnas y algunas claves del final pueden estar truncadas. Hay otros cuatro DAT que no se revisaron: 4695 (entretela), 4696 (thermobonding), 4697 (maquila) y 4698 (hilo).

> Cotejo contra el PDF (rev. 05, mayo 2025) hecho el 2026-10-06: las tablas
> coinciden; DI es «Dryfit» y RA es «Resina acrílica».

Dos usos:

1. **Generador de código.** Con las características ya capturadas, Odoo propone el código y crea los artículos de la ruta (H crudo, I teñido, J acabado).
2. **Carga inicial de las 1,839 fichas vacías.** Composición, dibujo, peso, tipo de hilo, galga, color, ancho y acabado se pueden derivar del código de cada artículo existente. Eso habilita la búsqueda de parecidos sin captura manual.

## 6. Cambios de código

Ordenados por dependencia. Cada bloque dice qué construir y cómo saber que quedó.

### 6.1 Proyecto único con ciclo de vida

- Un solo proyecto que nace como **análisis** y recibe folio FT al aprobar el cliente. No dos proyectos.
- **Etapas de avance** en lugar de clientes: solicitud, análisis, cotización, aprobación del cliente, muestra, respuesta del cliente, pilotaje, liberado, cerrado sin producto. El cliente vive solo en partner_id.
- **Folio FT por secuencia** con reinicio anual, formato FT-039-2026, asignado al pasar a desarrollo aprobado. Debe permitir capturar a mano un folio histórico.
- **Origen:** cliente o interno. Si es interno, el solicitante se elige de hr.employee.
- **Revisión** como campo entero y bitácora de revisiones (fecha, quién la pidió, característica, valor anterior, valor nuevo, cotización asociada). El nombre se arma con folio, código y revisión; nadie edita el título.
- **Dirección de correo propia por proyecto** (alias estándar de Proyectos) para que los correos del cliente queden pegados.
- **Pestaña comercial** que sustituye al Análisis de mercado Industrial: equipo de ventas, vendedor, cliente actual o prospecto (calculado), programa, aplicación, tiempo de programa, consumo anual y mensual con conversión yardas ↔ metros, tipo de laminado, requisitos legales y reglamentarios (lista, no texto), mercado, fecha requerida, número de especificación.
- **Muestra física:** fecha de recepción, fecha de entrega a Diseño, ubicación en carpeta, y etiqueta imprimible (cliente, proyecto, fecha).
- **Resultado del análisis** como selección: producto de línea (liga al artículo y cierra sin FT), producto nuevo, no factible (con motivo de lista).
- **Reloj en horas** por paso, con hora de inicio y fin. Dos relojes separados: desarrollo (se detiene mientras hay materia prima pendiente) y materia prima.

*Listo cuando:* un proyecto nuevo recorre todas las etapas sin editar su nombre, su folio se asigna solo, y se ve cuántas horas llevó cada paso.

### 6.2 Tabla de características y catálogos

Lo descrito en 5.1, más los catálogos de 5.2 como modelos (composición, dibujo, tipo de hilo, galga, color, acabado).

*Listo cuando:* al elegir el tipo se cargan los renglones; Jessica captura la especificación del cliente; las demás columnas se llenan en su paso sin crear renglones nuevos; Odoo marca qué queda fuera de tolerancia.

### 6.3 Búsqueda de artículos parecidos

- Al capturar la especificación, mostrar artículos de línea y desarrollos anteriores ordenados por cercanía en peso, ancho, composición, dibujo y galga.
- Regla automática: si todas las características caen dentro de la tolerancia de un artículo existente, proponer "producto de línea"; si una sola queda fuera, "producto nuevo". Selena confirma.
- El artículo base queda ligado al proyecto y aporta su ruta y su costo como punto de partida.

*Listo cuando:* sustituye al formato de Experiencias previas y la decisión línea/nuevo queda registrada con el artículo base.

### 6.4 Solicitud de pruebas a laboratorio

- Selena marca qué renglones van a laboratorio y genera la solicitud con un botón.
- Flujo: solicita Selena → autoriza Coordinador de Laboratorio y MP (188) → el laboratorista captura el resultado en el mismo renglón.
- El laboratorio **solo mide**. El dictamen (cumple, no cumple, cumple con desviación) lo da Selena.

*Listo cuando:* no se recaptura ningún resultado y queda el tiempo que tardó el laboratorio.

### 6.5 Checklist de factibilidad

- Catálogo fijo de recursos por línea (tejido circular, entretelas); en cada proyecto aparecen los renglones y Yet marca sí o no con observación.
- Los renglones que Odoo puede contestar se contestan solos: capacidad de máquina (ya la calcula qb.cotizacion.capacity_status) y existencia de materia prima (lista de materiales contra existencias).
- Revisa Jessica. Junta en **una sola pantalla de revisión** el análisis de Selena y la factibilidad de Yet: una aprobación, no dos.
- El AMEF de proceso **no** va antes de cotizar; va después de la aprobación del cliente, con Eduardo.

*El catálogo de recursos está pendiente* (sección 8). Deja el modelo y la vista; no inventes los renglones.

### 6.6 Artículo en desarrollo y generador de código

- Estado del artículo: **en desarrollo → en pilotaje → liberado → archivado**.

| Estado | Se puede vender | Notas |
|---|---|---|
| En desarrollo | No | Solo proyecto, cotización y órdenes de muestra |
| En pilotaje | Sí, con aviso | Entra cuando el cliente aprueba la muestra. Los tres primeros lotes se marcan como pilotaje |
| Liberado | Sí | Tras verificación, validación y aprobación final |
| Archivado | No | Automático si la cotización se pierde o el proyecto se cancela; incluye intermedios y listas de materiales |

- Generador de código según 5.2, que crea crudo, teñido y acabado.
- **Bloquear** los productos 16292 y 16293 (MUESTRA PILOTO) para órdenes nuevas. Coordina la fecha con Jose; hay órdenes abiertas con ellos.

*Listo cuando:* ningún código se arma a mano, lo no ganado desaparece de vista solo, y no se puede emitir una orden de muestra con el genérico.

### 6.7 Ruta y fichas de proceso

- Yet captura la ruta como operaciones de la lista de materiales. El **Diagrama de flujo de proceso se imprime** desde ahí; no se dibuja.
- Cada operación lleva **parámetros propuestos** (Yet) y **parámetros reales** (supervisor), con número de ajuste y motivo de lista.
- Tejido: usar sgi.machine.sheet, que ya existe. Corrige sus firmas a puestos vigentes.
- **Construir las fichas de tintorería y de acabado** con la misma estructura. Campos en el anexo B.
- Acabado maneja hasta **tres pases de rama**. Al tercero sin cumplir, aviso a Selena.
- La gráfica de proceso de tintorería se genera con los datos de gradiente, temperatura y tiempo.
- Firmas iguales en las tres áreas: propone Yet, valida el supervisor del área (en tejido el Jefe de Manufactura), laboratorio mide.
- Los parámetros de la corrida aprobada pasan a ser la ficha de proceso del artículo, sin recaptura.

*Listo cuando:* desaparecen los diez formatos de corrida (anexo C).

### 6.8 Cotización (qb.cotizacion)

- Campo **proyecto** en la cotización; cotización visible desde el proyecto.
- Estado nuevo **"Por aprobar"** entre Borrador y Presentada. Solo el puesto 183 (Director de Finanzas y Administración) la libera. Sin eso no se imprime ni se envía. Botones: aprobar, o regresar a recotizar con motivo obligatorio de lista (margen bajo, costo mal calculado, volumen dudoso, otro).
- **Suplente** configurable para la aprobación. No está definido quién (sección 8).
- **Margen mínimo** como parámetro vacío: mientras no tenga valor, no hay aviso.
- Casillas nuevas: hoja de seguridad, IMDS, certificación ISO.
- **Costo de la muestra visible** antes de aprobar (una muestra con teñido cuesta el baño completo).
- **Aprobación del cliente:** medio (correo, OC, WhatsApp, cotización firmada), fecha y evidencia adjunta.
- **Seguimiento:** actividad automática a Jessica a los 5 días hábiles de presentada y al vencer la vigencia. Al vencer pasa a "Vencida" y obliga a decidir: renovar, ganada o perdida. Motivo de pérdida de lista (precio, tiempo de entrega, especificación, el cliente canceló, sin respuesta).
- Borradores con más de 30 días sin movimiento se archivan.
- Al marcarse **Ganada** y aprobar el cliente la muestra: crear el precio en la tarifa del cliente con precio, moneda y vigencia de la cotización.
- **Recálculo por revisión:** al subir la revisión del proyecto, recalcular el costo. Si cambia, el proyecto se detiene hasta que Jose apruebe la cotización revisada. Si no cambia, pasa.
- **Plantilla PDF** que sustituya al Word F-P-A28-12: bilingüe español/inglés, con los nueve términos (CoA al 100 %, pruebas especiales de laboratorio, LTA, PPAP, inspección total, CPK 3 sigma, APQP, PSCR, evidencia C-TPAT) y la leyenda de muestra menor a 50 m sin costo. Jose dijo que él la va a cambiar; confirma con él antes de tocarla.
- **Aprobación para iniciar un proyecto:** documento generado desde el proyecto y la cotización. No se arma a mano.

*Listo cuando:* ninguna cotización llega al cliente sin aprobación de Jose, y el primer pedido de un artículo nuevo siempre encuentra precio en tarifa.

### 6.9 Solicitud de desarrollos y arranque

- Reporte PDF de la solicitud con la clave nueva (F-C1-01, F-C1-11, F-C1-15, F-C1-16 según el tipo; ver sgi.format.map ids 33 a 36).
- Aprobación de Jorge (puesto 230) como compuerta: sin ella no se puede pedir la corrida.
- Al aprobar: aviso automático con el PDF a los seis puestos de la sección 4, y revisión de existencias de cada componente contra la cantidad de la muestra.
- Si falta algo: requisición a Compras **ligada al proyecto**, generada desde la lista de materiales. Sustituye al Word F-P-D01-16. Los campos de ese formato están en el anexo D.

### 6.10 Orden de muestra

- Yet pide la corrida desde el proyecto con cantidad y fecha deseada.
- A Planeación le llega la orden armada: artículo del proyecto, folio FT en origin, tipo de operación 86 "Tejido Desarrollo", lista de materiales y ruta.
- **Cantidad sugerida** = la mayor entre la OC, lo necesario para 50 m PQ según el rendimiento de primera esperado, y el mínimo de baño de la máquina. Muestra el motivo. Yet puede ajustarla.
- La fecha de máquina se refleja sola en la tarea del proyecto.
- El sobrante entra a la ubicación 57 ligado a su proyecto.

### 6.11 Envío de muestra y respuesta del cliente

- Selena valida la muestra; sin eso no hay baja.
- En la baja (tipo de operación 267 "Baja de Muestras"): medio de entrega, paquetería y guía.
- Al validarse la baja: correo listo para que Jessica lo mande, con rollos y dimensiones, medio y guía. Si la cotización incluye CoA, se adjunta el del lote.
- Respuesta del cliente como selección: aprueba (pasa a pilotaje), pide cambios (nueva revisión), rechaza (cierre con motivo). Seguimiento automático si no contesta.

### 6.12 Pilotaje, verificación, validación y liberación

- Los tres primeros lotes de producción del artículo se marcan como pilotaje. Un lote que no cumple no cuenta.
- **Estudio de habilidad** (Cp y Cpk) calculado con las lecturas de esos lotes contra la especificación del cliente. El número de lecturas por lote y las características que entran están por definir (sección 8).
- **Ficha técnica interna** con dos juegos de límites y aprobación de seis puestos: Jefe de Manufactura, Jefe de Calidad, Supervisor de inspección y empaque, Diseño de Producto, Dirección de Operaciones, Administrador de Ventas.
- **Especificaciones del producto** para el cliente: documento aparte, bilingüe, con instrucciones de cuidado y uso principal del textil. Lo exclusivo de cada documento se captura solo ahí.
- **Aprobación final** según la casilla PPAP de la cotización: con PPAP, el PPAP se arma con lo ya capturado (ruta, AMEF, plan de control, resultados, estudio de habilidad, muestra maestra) y cierra con el PSW; sin PPAP, basta correo de aceptación o primera OC, con evidencia. La Aprobación de proyecto se genera y se envía como documento informativo.
- **Liberación:** lista de materiales con los consumos reales de los tres lotes, ruta confirmada, cambio de estado.
- **Cierre:** decisión obligatoria sobre el sobrante en la ubicación 57 (se entrega, pasa a producto terminado, se ofrece como saldo o se da de baja).

### 6.13 Escalamiento y medición

- Avisos en horas: al responsable a las 2 horas de tener el paso pendiente, a Jessica a las 4, a Jorge solo si se venció el día. Son valores propuestos, no confirmados; déjalos configurables.
- Corrige las mediciones que hoy cuentan otra cosa (sección 7.2).

## 7. Datos por MCP, al final

Hazlo cuando el código esté en la rama que Jose indique y desplegado en la base. **Vuelve a leer cada registro antes de escribirlo:** los ids de este documento son del 6-oct-2026. Revisa primero si las fichas del SGI se cambian directo o por el flujo de propuestas (sgi.activity.change); si existe ese flujo, úsalo. Entrega a Jose la lista de lo que cambiaste.

### 7.1 Fichas de actividad de C1

| Actividad (id) | Cambio |
|---|---|
| C1.01 (564) | Ejecuta Jessica (da de alta) y la estructura sale de la plantilla. Ligar el formato Análisis de mercado Industrial (documento 3884). Quitar el plazo de 2 días y el escalamiento directo a Jorge. El entregable 365 "Solicitud de producto nuevo" no tiene modelo: ligarlo al proyecto |
| C1.02 (565) | Ejecuta Selena; revisa Jessica (hoy solo "se entera"). Quitar el plazo de 3 días. Formatos Análisis de muestras (3959), Experiencias previas (3982) y Solicitud de pruebas (3920) quedan sustituidos |
| C1.03 (566) | Antes de cotizar solo checklist; el AMEF pasa después de la aprobación del cliente. Ejecuta Yet, revisa Jessica. Quitar al Planeador y el plazo de 5 días. Medir por el checklist, no por sgi.fmea. Ligar aquí el Diagrama de flujo (3975), hoy ligado a C1.15 |
| C1.04 (567) | Dos ejecutores: Selena (artículo y lista de materiales) y Yet (ruta y centros de trabajo). Código generado |
| C1.05 (568) | Ejecuta Jessica en el módulo de costeo. Aprueba el puesto 183, todas. Hoy dice Diseño de Producto y "Director Estratégico". Menú: el del módulo de costeo |
| C1.06 (569) | Menú correcto (hoy apunta a Ventas → Cotizaciones). Seguimiento según 6.8. Quitar "muestra sin pedido la autoriza el Director de Operaciones" |
| C1.07 (570) | Quitar el formato F-P-D01-10 (3964): es una hoja de costeo vieja, ya no se usa. Meta: 15 días sin contar espera de materia prima |
| C1.08 (571) | Filtrar la medición a requisiciones ligadas a un proyecto de desarrollo; hoy cuenta todas las de la empresa (approval.request categorías 9 y 13) |
| C1.09 (572) | Solicita Yet; **ejecuta Planeador de Producción (221)**; Selena se entera. Quitar la aprobación del Director de Operaciones (rol id 1919): ya aprueba la Solicitud |
| C1.10 (573) | Ligar las fichas de tejido (4761), tintorería (3976) y acabado (3960), hoy ligadas a C1.15. Validación por supervisor de área |
| C1.11 (574) | El laboratorio mide; quien decide que no cumple es Selena. Corregir el aviso de medición (menú abre quality.check, mide con project.task) |
| C1.12 (575) | Renombrar a algo como "Validar y enviar la muestra al cliente". Quitar "asignar el código". La baja la ejecuta Almacén; avisa Jessica. Quitar los cinco DAT como formatos de esta actividad |
| C1.13 (576) | Formato Modificación de proyectos (3966) sustituido por revisiones. Escalamiento: Jessica es primer nivel |
| C1.14 (577) | Pilotajes = tres primeros lotes del primer pedido. Es la **verificación** (estudio de habilidad). Quitar "firma del Jefe de Calidad" como cierre, o agregarla al documento: hoy la ficha y los formatos se contradicen |
| C1.15 (578) | Es la **validación**. Dos documentos distintos. Quitar de aquí diagrama y fichas de proceso |
| C1.16 (579) | El PPAP se arma con lo capturado antes. Solo aplica a quien lo pide |
| C1.17 (580) | Quitar "pasar a código definitivo" y "antes de su primer pedido". Precio en tarifa automático |
| C1.18 (581) | La hacen Jessica, Yet y Selena. **No configurar más:** Jose lo dejó para después |
| C1.19 (755) | Un cambio a producto liberado abre FT nuevo. Simplificar la ficha |

En todas las actividades de C1 donde ejecutan Selena o Yet: escalamiento a Jessica y después a Jorge.

### 7.2 Otros datos

- Marcar sgi_is_ft en los 77 proyectos FT- que no lo tienen (o corre la migración que lo calcule).
- Reescribir las tareas de las plantillas 480 y 481 con nombres de actividad, en el orden de la sección 4. Reordenar etapas: hoy "Validación" va antes que "Verificación" y ninguna contiene el estudio ni la ficha técnica.
- Unificar el formato de los folios FT existentes a guiones.
- Cargar los catálogos de 5.2 y derivar las fichas técnicas de los artículos existentes desde su código. Hazlo primero en qbtesting y muestra a Jose una muestra del resultado.
- Lista fija de partes interesadas por puesto.
- Formatos que se dan de baja en la lista maestra: anexo C. No borres los documentos; márcalos como obsoletos según el flujo documental del SGI.

### 7.3 Lo que no debes tocar por tu cuenta

- Las 6 cotizaciones presentadas vencidas: las cierra Jessica.
- Los 152 lotes de la ubicación 57: Jose decide qué se hace.
- Usuario de Francisco González y puesto de José Luis Almazán: son altas de RH que decide Jose.
- Órdenes de fabricación abiertas con el artículo genérico.

## 8. Pendientes sin definir

No los resuelvas. Déjalos como parámetro vacío, catálogo sin renglones o tarea abierta.

| Con quién | Qué falta |
|---|---|
| Yet | Lista de recursos del checklist de factibilidad, para tejido circular y para entretelas. Quién integra el comité de pilotajes |
| Laboratorio | Quién hace las pruebas de desarrollo y cuánto tardan. Si el Reporte de prueba o desarrollo (F-P-C06-01, texto libre) aporta algo que la tabla no dé |
| Ingeniería de Calidad | Lecturas por lote y características críticas para el estudio de habilidad (tres lecturas por pilotaje no alcanzan para un Cpk defendible). Cómo se empata "pilotajes = primer pedido" con el PPAP aprobado antes del primer embarque en automotriz. Plan de control por artículo |
| Compras | Quién elige proveedor de materia prima nueva y si se consiguen tres propuestas |
| Jose | Margen mínimo. Suplente para aprobar cotizaciones. Plantilla PDF de la cotización. Usuario de Francisco González (sin él no puede validar corridas de tejido). Puesto de José Luis Almazán (no existe un puesto de supervisor de inspección; el más cercano es 269, auxiliar). Qué hacer con los 152 lotes. Todo lo de la C1.18: día, quién recibe el resultado, pantalla de revisión |
| Areli (SGI) | Tiempo de conservación de las muestras del cliente |
| Sin dueño | Los otros cuatro DAT de codificación. El formato Solicitud de muestras (F-P-A28-03, documento 3874): es una bitácora de muestras de artículos de línea, no un desarrollo; probablemente va a C2 como pedido de muestra. El formato F-P-P04-02 de Manufactura (documento 4023), sin uso aparente. El Word de cotización específico de Seiren Viscotec (documento 848) |

## 9. Orden sugerido de trabajo

1. Leer el código de quimibond_sgi y del módulo de costeo; confirmar o corregir lo que este documento supone.
2. Presentar a Jose un plan corto con lo que cambia respecto a este documento y esperar su visto bueno.
3. Tabla de características y catálogos (6.2): todo lo demás depende de ella.
4. Proyecto único (6.1) y estado del artículo con generador de código (6.6).
5. Cotización (6.8). Es donde está el control de dinero.
6. Análisis, laboratorio y factibilidad (6.3 a 6.5).
7. Solicitud, orden de muestra y fichas de proceso (6.7, 6.9, 6.10).
8. Envío, respuesta, pilotaje y liberación (6.11, 6.12).
9. Escalamiento y medición (6.13).
10. Datos por MCP (sección 7), primero en qbtesting.

Sube a main por bloques que se puedan probar solos. No dejes para el final una entrega única.

---

## Anexo A. Características por formato

Para armar el catálogo. Cada una es un renglón de la tabla de 5.1.

**Solicitud general (tejido circular):** masa por unidad de área (g/m²); ancho (m); espesor (plg/mm); rendimiento (m/kg, calculado); tipo de acabado; tipo y tacto del engomado; elongación estática largo y ancho (%); densidad del tejido en mallas y columnas (plg/10 cm); cambios dimensionales al calor largo y ancho (%); engomado de orillas (sí/no); tamaño de los rollos (m).

**Entretelas V10:** tela base y sus características; masa total (g/m²); cantidad de resina (g/m²); ancho (m); cambios dimensionales al lavado ×2 largo y ancho (%); máquina de proceso; fuerza máxima método de la tira largo y ancho (N/5 cm); tipo de resina; adhesión por fusionado largo y ancho (N/5 cm); tipo de polvo; solidez del color al frote izquierda, centro y derecha; mesh; rendimiento (m/kg); corte de orillas (sí/no); tamaño de los rollos (m).

**Carda:** tipo de fibra; título de fibra; acabado; masa total (g/m²); ancho (m); espesor; fuerza máxima largo y ancho (N/5 cm); rendimiento (m/kg); tamaño de los rollos (m).

**Tramado:** composición; masa total (g/m²); ancho (m); espesor; fuerza máxima largo y ancho (N/5 cm); hilos por pulgada a lo ancho; tipo de hilo; rendimiento (m/kg); tamaño de los rollos (m).

**Análisis de muestras del cliente (agrega):** composición %; título y tipo de hilo; ligamento; número de agujas; galga; corte y engomado de orillas.

**Aprobación para iniciar y Aprobación de proyecto (agregan):** construcción; elongación dinámica largo y ancho (%); tipo de goma (continua o discontinua); tacto (apreciación); color. Especímenes de referencia: elongación estática 5×20 cm a 2.5 kg por 3 min; densidad en 1 in.

**Datos de proyecto de esos dos formatos:** aplicación; fecha de solicitud; fecha requerida; número de especificación; tamaño de la muestra; consumo mensual; forma de empaque; material de línea (número de parte) o nuevo desarrollo; nombre del producto; precio y moneda; unidad de medida. Información requerida: ficha técnica o CoA; hoja de seguridad; certificación ISO 9001 e ISO 14001; PPAP; IMDS; CT-PAT; PSCR; caducidad de materiales (solo entretelas); otros. Capacidad utilizada por recurso: materia prima, tejido, tintorería, acabado, inspección (en entretelas: materia prima, cardado, punteado, inspección).

**Especificaciones del producto, para el cliente (agrega):** instrucciones de cuidado (lavado, blanqueo, secado, planchado, lavado en seco); uso principal del textil.

**Ficha técnica interna (agrega):** familia; dibujo; composición total; tacto; secciones de tejido, teñido y acabado con tolerancia o factor; presentación, empaque y etiqueta.

## Anexo B. Fichas de proceso por construir

**Tintorería** (F-P-D01-33, documento 3976): tipo de acabado; artículo; relación de baño; volumen de baño; peso total de tela; composición de la tela; tiempo empleado; temperatura; porcentaje del brazo del plegador; bomba (velocidad y presión); porcentaje del acumulador (inicial y cargado); gradiente de temperatura (subida y bajada); auxiliares utilizados; gráfica de proceso.

**Acabado** (F-P-D01-05, documento 3960): ruta de proceso en hasta 10 pasos. Teñido: máquina, marca, modelo, fórmula. Por cada pase de rama (hasta tres): color; rama (Bruckner o Unitech); temperatura de 8 campos, cada uno con lado A y B (°C); velocidad (m/min); ancho de entrada 1 y 2, de cadena y de salida (m); rodillo de presión superior (sí/no); alimentación de rodillo superior e inferior y de rueda izquierda y derecha (%); tensión de salida; extracción de humedad; pick up; receta de rama con hasta tres productos (g/l); temperatura del foulard (°C); vaporizar en entrada (sí/no); campo de enfriamiento en salida (sí/no); ancho de entrada y de salida de tela (m); masa de entrada y de salida (g/m²); corte de orillas (sí/no); engomado de orillas (sí/no); tipo de goma (discontinua o continua); productos y número de fórmula.

**Tejido** (F-IT-P-P01-08-05, documento 4761): ya existe como sgi.machine.sheet. Contenido de referencia: materia prima por polea (tipo de hilo, título, torsión, cm por vuelta, % de hilo); datos de máquina con especificación y tolerancia (consumo, tensión, polea de alimentación, punto cilindro, punto plato, altura de plato, número de alimentadores, ancho de bastidor, estiraje, diámetro/galga/agujas); tela acondicionada (peso, ancho, espesor, columnas, mallas, elongación bajo carga largo y ancho).

**Reportes de área** (se sustituyen por parámetros de la orden de trabajo): abridora (porcentaje de tensión, velocidad, presión del foulard, presión del exprimido, ancho de tela abierta); devanado (color, velocidad alta o baja); inspección y empaque (velocidad alta, media o baja); carda y tramado (velocidad). Todos llevan artículo, número de ajuste, lote, máquina y operador.

## Anexo C. Formatos y su destino

| Formato (documento) | Destino |
|---|---|
| Análisis de mercado Industrial F-P-A28-20 (3884; copias sueltas en hoja de cálculo: 1797, 5082, 5091, 5134, 5135) | Pestaña comercial del proyecto |
| Solicitud de desarrollos F-P-D01-02 (3958), V10 (3968), carda (3972), tramado (3973) | Tabla de características; PDF generado |
| Aprobación para iniciar un proyecto F-P-D01-09 (3963) y -19 (3969) | Generado desde proyecto y cotización |
| Análisis de muestras del cliente F-P-D01-03 (3959) | Columna "medido en la muestra" |
| Experiencias previas de entretelas F-P-D01-40 (3982) | Artículo base ligado |
| Solicitud de pruebas para laboratorio F-P-C05-02 (3920) | Botón sobre los renglones |
| Checklist de factibilidad F-P-D01-29 (3974) | Checklist con catálogo |
| Diagrama de flujo de proceso F-P-D01-32 (3975) | Impreso desde la ruta |
| Planeación de recursos F-P-D01-10 (3964) | **Baja.** Era el costeo viejo |
| Cotización de producto F-P-A28-12 (3878) | PDF del módulo de costeo |
| Solicitud de materia prima F-P-D01-16 (3967) | Requisición ligada al proyecto |
| Reportes de carda y tramado (3983), abridora (3980), devanado (3979), almacenaje (3978), inspección y empaque (3977) | Parámetros por operación |
| Resultados de acabado (3961) y de proceso V10-carda-tramado (3970) | Columna "obtenido en corrida" |
| Fichas de tejido (4761), tintorería (3976) y acabado (3960) | Fichas de proceso en Odoo |
| Reporte de prueba o desarrollo F-P-C06-01 (3926) | Impreso desde la tabla. Pendiente confirmar con Laboratorio |
| Modificación de proyectos F-P-D01-12 (3966) | Revisiones del proyecto |
| Acta de revisión F-P-D01-39 (3981) | Acuerdos con responsable y fecha en el proyecto |
| Aprobación de proyecto F-P-D01-11 (3965) | **Se conserva**, generado desde Odoo, para clientes sin PPAP |
| Ficha técnica de producto F-P-D01-24 (3971) | **Se conserva**, como ficha interna en Odoo |
| Especificaciones del producto F-P-D01-08 (3962) | **Se conserva**, como documento al cliente |
| Reporte de conformidad F-P-C07-01 (3931) | Ya está mapeado al lote como CoA |

Dato curioso que conviene limpiar: el mismo PDF "F-P-D01-03 ANALISIS DE MUESTRAS DEL CLIENTE-WP075R46JNT155" está copiado en la carpeta de 72 proyectos. Es un machote, no análisis reales.

## Anexo D. Solicitud de materia prima

Secciones del Word F-P-D01-16 y a qué corresponden en Odoo.

- **Solicitud (Diseño y Desarrollo):** solicitado por, fecha, departamento; tipo de material (fibra, hilo, tela, químicos, colorante); composición (CO, PES, PA, PES/CO, otra) y porcentaje. Según el tipo — fibra: título, largo, color, blanqueo óptico, punto de reblandamiento, punto de fusión, características de plastificación; hilo: título, torsión, color, blanqueo óptico; tela: ligamento, pretratamiento y su tipo, color, maquila; químicos: nombre y características; colorantes: nombre, color y características. Cantidad en kg, conos, m, litros o piezas. → Producto nuevo con sus características.
- **Autorización:** Diseño y Desarrollo y Director de Operaciones. → Aprobación de la requisición.
- **Compras (Auxiliar de Compras):** tres propuestas de proveedor con datos, capacidad de entrega, tiempo de entrega, precio y forma de pago. → Solicitudes de cotización a proveedores sobre la misma requisición.
- **Calidad:** Coordinador de Laboratorio y MP y Diseño y Desarrollo. → Control de calidad en la recepción.
