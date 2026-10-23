# SGI: implementación por proceso, puesto y persona (lo que sigue después de C1)

**Fecha:** 2026-10-23 · **Pide:** Jose Mizrahi · **Base:** datos de producción por MCP
(company_id = 1) del 2026-10-23 · **Modelo a repetir:** C1
(`docs/superpowers/specs/2026-10-06-c1-desarrollo-producto-brief.md` y
`docs/superpowers/plans/2026-10-06-c1-desarrollo-producto-plan.md`).

## 1. Lo que enseñó C1 y se repite en cada proceso

C1 pasó de borrador a vigente (95 % de lo medible con dato) con este ciclo. Se
aplica igual a los otros 13 procesos.

| Paso | Qué | Quién | Sale |
|---|---|---|---|
| 1. Preparación | Por MCP, actividad por actividad: quién la tiene asignada, quién la hace de verdad (usuario que crea el registro), si se mide, si el dato se llena, qué formato de Excel o Word se usa todavía | Claude | Ficha de preparación (como la de C2, sección 3) |
| 2. Sesión con el dueño | Recorrer las actividades del dueño y las de su equipo directo; confirmar quién, cuándo, dónde queda el dato | Jose + dueño | Notas |
| 3. Sesión por puesto | Una sesión corta con cada puesto que tiene actividades del proceso y no reporta al dueño | Jose + puesto (dueño opcional) | Notas |
| 4. Brief | Lo confirmado, las reglas, lo pendiente (sin inventar) | Claude | `docs/superpowers/specs/<fecha>-<proceso>-brief.md` |
| 5. Código por bloques | Cambios de modelo, vistas, reportes; cada bloque sube versión del SGI con su `## <versión>` del CHANGELOG; pruebas en el build de Odoo.sh con `--test-tags /quimibond_sgi` | Claude | PR a `main` → `quimibond` cuando Jose lo indique |
| 6. Datos por MCP | Fichas de actividad, responsables por puesto, entregables y dominios de medición | Claude, con confirmación de Jose | Fichas corregidas |
| 7. Piloto (4 semanas) | Se mide; cada semana se revisa adherencia e indicadores | Dueño | Estado `piloto` |
| 8. Vigente | Cuando se cumplen los criterios de la sección 2 | Dueño + Blanca (SGI) | Estado `vigente` |

Reglas de siempre: asignar por puesto, no por persona; nada de texto libre para
datos; lo que nadie ha definido queda vacío o como pendiente; solo
company_id = 1.

## 2. Criterios para pasar un proceso a vigente

1. Cada actividad tiene **puesto responsable** y ese puesto tiene **titular con
   usuario de Odoo** (o una decisión explícita de quién captura por él).
2. Cada actividad se mide por **entregable** u **Odoo**; las «manuales» que
   quedan tienen motivo escrito (no hay dato en Odoo que las pruebe).
3. **Adherencia ≥ 80 %**: la actividad la hace el puesto que la tiene asignada,
   no otro ni una cuenta compartida.
4. Los **indicadores** del proceso tienen dato (ninguno en «sin datos» ni en
   0 % por falta de captura).
5. No queda formato de Excel o Word que duplique lo que ya está en Odoo.

## 3. Foto de hoy (315 actividades)

| Proceso | Dueño | Estado | Act. | Verde | Rojo | Pendiente (manual) | Sin medición |
|---|---|---|---:|---:|---:|---:|---:|
| C1 Desarrollo y alta de producto | Jessica Francisco | vigente | 20 | 9 | 1 | 0 | 10 |
| C2 Pedido a entrega | Jessica Francisco | piloto | 29 | 15 | 1 | 11 | 2 |
| C3 Planeación y programación | Paris Villordo | borrador | 14 | 4 | 6 | 4 | 0 |
| C4 Producción | Francisco González | borrador | 20 | 12 | 2 | 5 | 1 |
| C5 Calidad de producto | Oscar González | borrador | 26 | 4 | 3 | 8 | 11 |
| C6 Almacén e inventarios | Cynthia Santana | borrador | 23 | 17 | 2 | 4 | 0 |
| E1 Dirección y planeación | Jorge Ortiz | borrador | 10 | 2 | 5 | 2 | 1 |
| E2 Gestión del SGI | Blanca Ballesteros | borrador | 43 | 5 | 19 | 13 | 6 |
| S1 Compra a pago | Jorge Ortiz | borrador | 21 | 8 | 7 | 6 | 0 |
| S2 Facturación, crédito y cobranza | Irma Luna | borrador | 15 | 6 | 5 | 3 | 1 |
| S3 Contabilidad, costos y cierre | Irma Luna | borrador | 22 | 6 | 9 | 7 | 0 |
| S4 Recursos Humanos y nómina | Miguel Medina | borrador | 39 | 6 | 11 | 19 | 3 |
| S5 Mantenimiento e instalaciones | Manuel Juárez | borrador | 22 | 4 | 11 | 4 | 3 |
| S6 Tecnología | Mariano Domínguez | borrador | 11 | 5 | 3 | 1 | 2 |
| **Total** | | | **315** | **103** | **85** | **87** | **40** |

«Verde» dice que el entregable existe, no que lo hizo el puesto correcto: hay
verdes con adherencia 0 % (C2.18, C2.21, C2.24). La adherencia se revisa en la
preparación de cada proceso.

## 4. Bloqueos que cruzan procesos (se resuelven una vez, antes o junto con C2 y C6)

| # | Bloqueo | Dónde pega | Qué se decide |
|---|---|---|---|
| B1 | **Puestos vacantes con actividades** | 260 Encargado de almacén (0 titulares): C2.21, C2.23, C6.02, C6.08, C6.19 · 198 Coordinador de ventas industrial (0): C2.08, C2.16, C2.17 · 246 Operador Jet F (0): C4.09 | Quién hace hoy esas actividades y a qué puesto pasan, o si se cubre la vacante |
| B2 | **Actividades sin puesto** | C2.35 reclamación de entrega, C2.36 junta mensual de entregas, C3.08 órdenes de tejido del programa, S1.01 requisición, S1.18 conformidad del servicio | Puesto responsable de cada una (S1.01 y S1.18 las hace cualquiera que pide: ¿familia de puestos?) |
| B3 | **Titulares sin usuario de Odoo** en actividades que se miden en Odoo | Supervisor TAC (Zaira Hernández, 5 act.), Auditor de calidad (Danashely Rodríguez, 3), laboratoristas (colorimetría, TAC ×4, tejido), auxiliares de supervisor (2), almacenistas (6), choferes (2), electromecánicos y mecánicos, operadores de tejido, jet, rama, carda, cocina e inspección | Por puesto: usuario propio, captura en kiosco/tablet de planta, o el supervisor captura por él (y la actividad pasa a ese puesto) |
| B4 | **Cuentas compartidas** | La adherencia baja cuando el registro lo crea una cuenta que no es del puesto (C2.18, C2.21) | Lista de cuentas compartidas y a quién pertenece cada uso; se identifica en la preparación de cada proceso |
| B5 | **Medición que no cuadra con el indicador** | C2.40 pronóstico mensual en verde con VE-03 cobertura del pronóstico en 7.5 % | Revisar el dominio del entregable contra el indicador en cada preparación |

## 5. Orden propuesto y calendario

Sigue la cadena de valor y junta los procesos que comparten personas. Las
fechas son propuesta; se ajustan a la agenda de cada dueño.

| Semana | Proceso | Sesiones | Por qué en este orden |
|---|---|---|---|
| 26-oct | **C2** Pedido a entrega | Jessica (lunes) · Darío Manríquez (Logística) · Cynthia Santana + Ariadna Lara (almacén, sello, certificado) | Ya está en piloto; tres huecos de captura concretos |
| 2-nov | **C6** Almacén e inventarios | Cynthia · Ana Silvia Colín · almacenistas (con B1 y B3) | Comparte personas y vacante con C2; 17 de 23 ya en verde |
| 2-nov | **C3** Planeación y programación | Paris Villordo · Jorge Ortiz (C3.07) · Francisco González y José Luis Almazán (C3.15, C3.16, C3.18) | 6 rojas; alimenta la fecha compromiso de C2 (OTIF 43 %) |
| 9-nov | **C4** Producción | Francisco González · supervisores (José Luis Almazán, Gerardo Franco, Zaira Hernández, Sergio González) · Carlos Vargas (C4.02) · mecánico de tejido | Muchos puestos operativos sin usuario (B3) |
| 16-nov | **C5** Calidad de producto | Oscar González · Eduardo Santiago · Ariadna Lara · Danashely Rodríguez · laboratoristas | 11 sin medición; dependen de lo que decida C4 sobre captura en planta |
| 23-nov | **S1** Compra a pago | Jorge Ortiz · Elena Delgado · Aurelio Álvarez · Irma Luna · Paris (S1.04) · Ariadna (S1.08) · Darío (S1.14) | 7 rojas; B2 (S1.01, S1.18) |
| 30-nov | **S2** Facturación, crédito y cobranza | Irma Luna · Sandra Dávila · vendedores (S2.17) | Sandra hace 14 de 15 |
| 30-nov | **S3** Contabilidad, costos y cierre | Irma Luna · Aurelio Álvarez | 9 rojas; liga con `qb_costeo` |
| 7-dic | **S5** Mantenimiento e instalaciones | Manuel Juárez · electromecánicos, eléctricos, mecánicos · Darío (S5.11, S5.21, S5.22) · choferes (S5.20) | 11 rojas |
| 7-dic | **S6** Tecnología | Mariano Domínguez | Un solo puesto |
| 14-dic | **E1** Dirección y planeación | Jorge Ortiz · Jose Mizrahi · Jessica (E1.01) | 5 rojas; se apoya en los indicadores ya corregidos |
| 14-dic | **E2** Gestión del SGI | Blanca Ballesteros · Carlos Álvarez | 43 actividades, 19 rojas; conviene al final, con los demás procesos ya medidos |
| por decidir | **S4** Recursos Humanos y nómina | Miguel Medina · Lorena Mondragón · Aurelio Álvarez · Irma Luna | Corte de nómina el 1-ene-2027: RH está ocupado en diciembre. Propuesta: enero 2027, salvo lo que ya cubre `quimibond_nomina` |

## 6. Por persona y puesto

Una fila por titular; los puestos con varios titulares sin usuario van juntos.
«Rojo / pend. / sin» = rojas, manuales pendientes, sin medición.

| Persona | Puesto | Usuario | Act. | Verde | Rojo / pend. / sin | Procesos (actividades) | Sesiones |
|---|---|---|---:|---:|---|---|---|
| Jessica Francisco | 207 Administrador de Ventas y Marketing | sí | 10 | 7 | 1 / 1 / 1 | C1 (01, 05, 06, 13, 18), C2 (04, 40, 41, 43), E1.01; dueña de C1 y C2 | C2, E1 |
| Guadalupe Guerrero | 233 Atención a clientes confección | sí | 3 | 3 | — | C2 (08, 16, 17) | C2 (con Jessica) |
| Juan José Hernández | 227 Representante de ventas | sí | 3 | 3 | — | C2 (08, 16, 17) | C2 (con Jessica) |
| Juan Alberto Hernández, Javier Hernández | 229 Vendedor | sí | 4 | 3 | 0 / 1 / 0 | C2 (08, 16, 17), S2.17 | C2, S2 |
| (vacante) | 198 Coordinador de ventas industrial | — | 3 | 3 | — | C2 (08, 16, 17) | B1 |
| Darío Manríquez | 196 Logística | sí | 16 | 2 | 4 / 8 / 2 | C2 (18, 19, 20, 28–32, 34, 38, 39, 42), S1.14, S5 (11, 21, 22) | C2, S1, S5 |
| Cynthia Santana | 194 Jefe de inventarios y almacenes | sí | 8 | 5 | 2 / 1 / 0 | C2.27, C6 (12, 21, 22, 24, 26, 27, 28); dueña de C6 | C2, C6 |
| Ana Silvia Colín | 245 Aux. registros y entradas de almacén | sí | 6 | 6 | — | C6 (06, 07, 13, 16, 17, 18) | C6 |
| (vacante) | 260 Encargado de almacén | — | 5 | 4 | 0 / 1 / 0 | C2 (21, 23), C6 (02, 08, 19) | B1 |
| 6 titulares sin usuario | 243 Almacenista K | no | 8 | 6 | 0 / 2 / 0 | C2.26, C6 (01, 05, 10, 14, 15, 20, 25) | C6, B3 |
| 2 titulares sin usuario | 203 Chofer | no | 2 | 0 | 1 / 1 / 0 | C2.33, S5.20 | C2, S5, B3 |
| Ariadna Lara | 188 Coordinador de laboratorio y MP | sí | 7 | 2 | 1 / 2 / 2 | C2 (24, 37), C5 (01, 02, 07, 23), S1.08 | C2, C5, S1 |
| Sandra Dávila | 235 Cuentas por cobrar y facturación | sí | 15 | 7 | 5 / 2 / 1 | C2.10, S2 (14 de 15) | C2, S2 |
| Selena Manríquez | 224 Diseño y desarrollo de producto | sí | 7 | 3 | 0 / 0 / 4 | C1 | (C1 hecho; las 4 sin medición se miden al usarse) |
| Yetlanezi Tablas | 225 Diseño y desarrollo de procesos | sí | 5 | 2 | 0 / 0 / 3 | C1 | (ídem) |
| Paris Villordo | 221 Planeador de producción | sí | 11 | 3 | 5 / 2 / 1 | C1.09, C3 (9 de 14), S1.04; dueño de C3 | C3, S1 |
| Francisco González | 211 Jefe de manufactura | sí | 4 | 0 | 3 / 1 / 0 | C3 (16, 18), C4 (23, 25); dueño de C4 | C3, C4 |
| José Luis Almazán | 209 Supervisor de entretelas | sí | 7 | 5 | 1 / 0 / 1 | C3.15, C4 (03, 06, 13, 18, 19, 21) | C3, C4 |
| Gerardo Franco | 205 Supervisor tintorería | sí | 6 | 5 | 0 / 0 / 1 | C4 (03, 06, 11, 13, 19, 21) | C4 |
| Zaira Hernández | 187 Supervisor TAC | **no** | 5 | 4 | 0 / 0 / 1 | C4 (03, 06, 13, 19, 21) | C4, B3 |
| Sergio González | 295 Encargado de producción y control | sí | 5 | 4 | 0 / 0 / 1 | C4 (03, 06, 13, 19, 21) | C4 |
| 2 titulares sin usuario | 218 Auxiliar de supervisor | no | 5 | 4 | 0 / 0 / 1 | C4 (03, 06, 13, 19, 21) | C4, B3 |
| Carlos Vargas | 192 Control de producción | sí | 1 | 0 | 0 / 1 / 0 | C4.02 tarjeta viajera | C4 |
| Mecánicos de tejido (4) | 63 | no | 1 | 0 | 0 / 1 / 0 | C4.04 | C4, B3 |
| Operadores de tejido, jet, rama, carda, cocina, inspección, empacadores | 57–79, 238–300 | no | 8 actividades compartidas | | | C4 (05, 08, 09, 12, 14, 16, 17, 24) | C4, B3 |
| Oscar González | 204 Jefe de calidad | sí | 11 | 2 | 1 / 4 / 4 | C5 (09–11, 14, 15, 18–22, 28); dueño de C5 | C5 |
| Eduardo Santiago | 199 Ingeniero de calidad | sí | 7 | 1 | 1 / 1 / 4 | C1.16, C5 (12, 13, 16, 24, 26, 27) | C5 |
| Danashely Rodríguez | 285 Auditor de calidad | **no** | 3 | 0 | 1 / 0 / 2 | C5 (04, 05, 08) | C5, B3 |
| Laboratoristas (colorimetría 1, TAC 4, tejido 1) | 215, 185, 220 | no | 4 | 1 | 0 / 3 / 0 | C1.11, C4.10, C5.03, C5.25 | C5, B3 |
| Jorge Ortiz | 230 Director de operaciones | sí | 6 | 2 | 2 / 1 / 1 | C3.07, E1 (03, 04, 09), S1 (03, 06); dueño de E1 y S1 | C3, S1, E1 |
| Jose Mizrahi | 183 Director de finanzas y administración | sí | 6 | 1 | 3 / 2 / 0 | E1 (02, 05–08, 10) | E1 |
| Elena Delgado | 214 Auxiliar de compras | sí | 7 | 3 | 2 / 2 / 0 | S1 (05, 07, 09, 12, 13, 17, 28) | S1 |
| Aurelio Álvarez | 226 Auditor interno contable | sí | 19 | 5 | 9 / 5 / 0 | S1 (21, 22, 24–27), S3, S4.12 | S1, S3, S4 |
| Irma Luna | 228 Contador general | sí | 12 | 2 | 4 / 6 / 0 | S1.23, S3, S4 (13, 21); dueña de S2 y S3 | S1, S2, S3, S4 |
| Miguel Medina | 219 Coordinador de RH | sí | 21 | 5 | 4 / 10 / 2 | S4; dueño de S4 | S4 |
| Lorena Mondragón | 231 Responsable de nóminas | sí | 16 | 2 | 6 / 7 / 1 | S3.05, S4 | S4 |
| Manuel Juárez | 213 Jefe de mantenimiento | sí | 14 | 4 | 5 / 2 / 3 | S5; dueño de S5 | S5 |
| Electromecánicos (3), eléctricos (2), mecánicos (5), soldador (1) | 210, 190, 197, 208 | no | 4 | 0 | 2 / 2 / 0 | S5 (02, 08, 15, 16) | S5, B3 |
| Mariano Domínguez | 217 Sistemas TI | sí | 11 | 5 | 3 / 1 / 2 | S6; dueño de S6 | S6 |
| Blanca Ballesteros | 189 Jefe de MAST y SGI | sí | 29 | 4 | 10 / 10 / 5 | E2; dueña de E2 | E2 |
| Carlos Álvarez | 191 Asistente de MAST y SGI | sí | 14 | 1 | 9 / 3 / 1 | E2 | E2 |

## 7. C2, primera semana: tres sesiones, no una

Jessica es la dueña de C2, pero de sus 29 actividades solo 4 son de su puesto.
La mayoría de lo que está mal no es de ella.

### 7.1 Lunes: Jessica (su parte y la de Ventas)

**Sus actividades**

| Act. | Qué | Medición hoy | Para la sesión |
|---|---|---|---|
| C2.04 | Capturar o actualizar el pedido industrial con cantidad, fecha y planta del release | entregable, verde, adherencia 86 % | ¿Quién captura el 14 % restante? Meta: automático desde el release del cliente |
| C2.40 | Pronóstico mensual por cliente y artículo para el S&OP | Odoo, verde | **No cuadra:** VE-03 cobertura del pronóstico 7.5 % y VE-04 cumplimiento 47.5 %. ¿Dónde lo hace hoy? ¿Qué clientes faltan? |
| C2.41 | Cotizar un producto de línea revisando los requisitos del cliente | Odoo, verde, adherencia 0 % | ¿Quién cotiza de verdad? |
| C2.43 | Registrar cada visita al cliente con requisitos, quejas y oportunidades | manual, pendiente | Definir el entregable: actividad o evento en el cliente con tipo «Visita» y requisitos, quejas y oportunidades como renglones, no como texto |

**Las de Ventas (su equipo)**

| Act. | Qué | Puestos | Para la sesión |
|---|---|---|---|
| C2.08 | Pedido de confección con precio de lista y existencia revisada | 233, 198, 227, 229 | 100 %: sin cambios |
| C2.16 | Pedir a Planeación la fecha compromiso con una actividad en el pedido | ídem | Adherencia 0 %. ¿Se pide por otro lado? Meta: automático al confirmar el pedido |
| C2.17 | Registrar la fecha compromiso y enviar la confirmación desde Odoo | ídem | 100 %. **La aprobación del Administrador de ventas no está activa** («ya hay otra regla en el botón»); la actividad «Aprobaciones sin activar» lleva vencida desde el 5-oct. Decidir si esa regla sobra o reemplaza a la otra |
| C2.10 | Alta de cliente nuevo con constancia fiscal y términos de pago | 235 (Sandra Dávila) | 75 %: ¿quién da de alta el otro 25 %? |
| — | Coordinador de ventas industrial (198) vacante | | ¿Se cubre o sus pedidos los toma otro puesto? (B1) |

**Como dueña del proceso**

- **C2.35** (reclamación de entrega) y **C2.36** (junta mensual de entregas a
  tiempo y completas) no tienen puesto (B2). Propuesta a confirmar: C2.36 es
  suya.
- **OTIF 43 %** con entregas completas a la primera en 86 %: el problema es «a
  tiempo», no «completo». Revisar si la fecha compromiso es realista o si
  planta no la cumple (pasa a C3).
- Lo de Logística y Almacén (7.2 y 7.3) se le presenta para que lo sepa; se
  resuelve con esos puestos.

### 7.2 Darío Manríquez (Logística): 12 actividades de C2

- **C2.18** programa de salidas de la semana: verde con adherencia 0 % (lo
  hace otra cuenta).
- **C2.34** acuse firmado: 0 adjuntos `ACUSE-…` en las salidas.
- **Exportación** sin dato en Odoo: C2.28 carpeta del embarque, C2.29 previo y
  pedimento, C2.30 aviso de embarque, C2.31 cruce, C2.32 expediente cerrado,
  C2.39 certificado de origen T-MEC. **C2-05 expedientes cerrados sin datos.**
  Para cada uno: qué documento existe, dónde vive (agente aduanal, correo,
  carpeta) y si se adjunta en la salida con nombre fijo o lleva campo.
- C2.19 cita o ruteo, C2.20 transporte, C2.38 certificado impreso: manuales.
- **C2.42** pedido mensual del desperdicio: rojo.

### 7.3 Cynthia Santana y Ariadna Lara (Almacén y certificado)

- **C2.27** número de sello: 267 salidas validadas desde el 23-sep, **ninguna con
  sello**. C2-06 en 0 %. Opción de sistema: no validar la salida sin sello.
- **C2.21 / C2.23** surtido y tarimas: son del Encargado de almacén (vacante,
  B1). ¿Quién las hace?
- **C2.24** certificado de calidad (Ariadna): 68 embarques que lo requieren,
  **todos en «pendiente»**. C2-04 en 0 %. Opción: marcar «enviado» al mandarlo
  por correo desde la salida.
- **C2.37** etiquetado del lote de Contitech: manual.

## 8. Qué sigue

1. Lunes 26-oct: sesión con Jessica (7.1). Con lo que decida, brief de C2 en
   `docs/superpowers/specs/2026-10-26-c2-pedido-entrega-brief.md`.
2. Misma semana: Darío, y Cynthia con Ariadna. Jose decide si van juntos o por
   separado.
3. Decisiones B1 a B3 con Jorge y Jose antes de C6 y C4 (vacantes, actividades
   sin puesto, captura de quien no tiene usuario).
4. Antes de cada sesión, ficha de preparación del proceso como la sección 7.
5. Código de C2 en bloques, con el mismo flujo que C1.
