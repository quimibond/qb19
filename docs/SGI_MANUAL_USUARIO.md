# Manual de Usuario — SGI en Odoo (Quimibond)

**Para:** todo el personal de PNTQ que usa el Sistema de Gestión Integral.
**Dónde:** en Odoo, menú **SGI** (ícono teal con flechas y palomita).
**Qué es:** nuestro SGI de siempre (ISO 9001 / 14001 / 45001) — los mismos formatos,
claves y flujos que conoces — pero vivo dentro de Odoo: ya no se llenan Excels ni se
buscan archivos en Dropbox; los registros se capturan donde ocurre el trabajo y las
alertas te llegan solas.

---

## 1. Lo básico

### 1.1 Tu rol define lo que ves

| Rol | Quién es | Qué puede hacer |
|---|---|---|
| **Usuario SGI** | Todo el personal | Ver sus procedimientos, dar acuse de lectura, levantar NCs e incidentes, capturar mediciones que le tocan, cerrar sus acciones |
| **Auditor SGI** | Auditores internos | + crear auditorías y hallazgos |
| **Jefe de MAST y SGI** | Administración del sistema | Todo: aprobar cambios, cierres forzados, migración de formatos, configuración |
| **Dirección** | Dirección de Operaciones | + Revisión por la Dirección |

### 1.2 Cómo te avisa el sistema

Todo aviso llega como **Actividad** (el reloj 🕐 arriba a la derecha en Odoo): acciones
por vencer, documentos por revisar, mediciones por capturar, acuses pendientes. Revisa
tus actividades a diario — es tu lista de pendientes del SGI. Cada registro tiene
además su **chatter** (historial de quién hizo qué y cuándo): esa es la evidencia que
antes daban las firmas.

---

## 2. Documentos (el Dropbox se jubiló)

### 2.1 Consultar un documento
App **Documentos** → carpeta **SGI** → secciones 00-23 (las mismas de siempre). Cada
documento trae su **clave** (P-A02, IT-P-P01-08, F-P-G05-01…), revisión, estado
(**Vigente**, Prueba piloto, Obsoleto) y a qué proceso pertenece.

### 2.2 Mis procedimientos
Menú **SGI → Mi trabajo → Mis procedimientos**: SOLO los documentos vigentes que aplican a TU
puesto. Es tu biblioteca personal de trabajo.

### 2.3 Acuse de lectura
Cuando se publica o cambia un documento que te aplica, te llega un acuse pendiente.
Léelo y presiona **"Marcar leído y entendido"**. Es tu firma de difusión (la que pide
el auditor). Si lo dejas más de 7 días, el sistema insiste.

### 2.4 Solicitar un cambio de documento (el F-P-G01-06 de siempre)
App **Aprobaciones** → Nueva solicitud → categoría **"Modificación de documento SGI"**:
elige el documento, tipo (alta/modificación/baja), la revisión nueva, el motivo y los
cambios; si va a prueba piloto, sus fechas (máximo 90 días). La solicitud viaja sola:
**Jefe de MAST y SGI → Dirección de Operaciones**. Al aprobarse, el documento se
versiona automáticamente, y el cambio sale en el **NEWS** del mes (que ahora se genera
solo). Nada de correos ni Excel de solicitudes.

---

## 3. No Conformidades (adiós al F-P-G05-01 en Excel)

### 3.1 Levantar una NC
- Desde la operación (lo ideal): en la **recepción** o la **orden de fabricación**
  donde viste el problema → botón de alerta de calidad / "Levantar NC".
- O directo: **SGI → Mejora continua → No Conformidades → Tablero → Nuevo**.

Llena: qué se desvió (descripción), proceso donde se detectó, clasificación
(Mayor/Menor/Observación), responsables de contestar. El **folio** (NCI-2026-0001) se
asigna solo — ya no hay lista de folios manual.

### 3.2 El ciclo de la NC (etapas del tablero)
**Abierta → Seguimiento → Cerrada** (las mismas A/S/C del concentrado de siempre).

En la NC capturas: **5 Porqués** (pestaña "Desviación y análisis" — ahora sí es
obligatorio: sin causa raíz no hay cierre), las **correcciones y acciones** (cada una
con responsable y fecha compromiso), y al final la **verificación de eficacia**.

### 3.3 Los candados (por qué "no me deja cerrar")
El sistema NO permite pasar a Cerrada si falta: ① causa raíz, ② alguna acción sin
fecha de terminación, ③ la verificación de eficacia. Es a propósito: era nuestra
falla histórica (45% de NCs abiertas). Solo el Jefe de MAST puede hacer un cierre
forzado, y queda registrado con motivo.

### 3.4 Si no actúas, el sistema escala
NC sin acciones a los 5 días (3 si es externa o de cliente) → actividad automática al
responsable y aviso a MAST.

### 3.5 El concentrado
**SGI → Mejora continua → No Conformidades → Concentrado**: la vista tipo F-P-G05-02 (folio, hallazgo,
responsables, estatus, cierre) con filtros por año y equipo. Se arma sola; exportable
para el auditor.

---

## 4. Reclamaciones de clientes

App **Helpdesk → Reclamaciones de clientes** (o el correo del buzón crea el ticket
solo). Captura pedido, producto, lote y metros afectados. Etapas: Nueva → Contención →
Análisis → Respuesta al cliente → Cerrada, con **SLA de 3 días** para primera
respuesta (el semáforo del ticket te lo marca). Si amerita tratamiento de fondo:
botón **"Generar NC"** — crea la NC ligada con todo pre-llenado.

## 5. Mejoras (F-P-A10-02)

App **Proyecto → Mejora Continua SGI**. Cada mejora es una tarjeta: tipo (ambiental/
proceso/recurso), área, tareas. Para pasarla a **Terminada** exige fecha límite y al
menos **una evidencia adjunta** (foto/archivo) — los dos hallazgos clásicos de
auditoría, resueltos por diseño.

## 6. Mapa de procesos

**SGI → Procesos**: los 5 macroprocesos y sus procesos, cada uno con su ficha (dueño,
puestos, documentos, y su salud: NCs abiertas, acciones vencidas). **SGI → Procesos → Flujos**:
cada flecha del mapa es un entregable real — "Programa semanal (F-P-A12-01)" va de
Planeación a Producción — y el botón **"Ver registros"** te abre las órdenes/pedidos
reales que fluyen por ella. El output de un proceso ES el input del siguiente, y ahora
se puede navegar.

## 7. Indicadores (los 18 Excels de F-P-A10-03, jubilados)

**SGI → Medición → Indicadores**: cada KPI con su Objetivo y su Aceptable.
Cada mes el sistema crea las mediciones: **las automáticas se calculan solas** de los
datos reales (entregas, producción, desperdicio del SALDO, reclamos…) y las manuales
te llegan como actividad (captura antes del día 5). El responsable **valida** su
medición; el semáforo (verde/amarillo/rojo) sale solo.

⚠️ **La regla de oro:** una medición **roja validada** de un indicador con "Generar NC
en rojo" activo crea la No Conformidad **automáticamente** — la tanda mensual de NCs
por indicador que MAST levantaba a mano, ya no se levanta: nace sola.

### 7.1 Presupuesto maestro de ventas (F-P-A28-18/17)

> **Solo cantidades (v19.0.13).** Tú capturas **únicamente cantidades**. El precio,
> los importes y las divisas los pone Odoo desde la **lista de precios** — la única
> fuente de verdad. Si un precio está mal, **se corrige LA LISTA** (y con eso se
> arreglan cotizaciones, pedidos y presupuesto de un golpe), nunca un número suelto
> en el presupuesto. Doble moneda: ves el precio/importe en la moneda de la lista
> (lo que el cliente conoce) y en pesos (moneda de la compañía, convertida con el
> tipo presupuestal de Ajustes). Los totales de cantidad son **por unidad** y los de
> divisa **por moneda** (nunca se suman entre sí); el único total global es en pesos.
> Si un producto no tiene precio en la lista, la ficha lo avisa y te manda a la lista.


**Ventas → Presupuesto de ventas** (o **SGI → Medición → Presupuesto de ventas**):
un presupuesto por **mercado** (equipo: industrial, confección, especiales) y **año**.

- **Capturar la matriz.** Abre el presupuesto y usa **Capturar cantidades** y
  **Capturar importes**: una cuadrícula producto (filas) × meses (columnas), como
  el Excel. Cada celda se edita directo. También puedes capturar en la lista de la
  pestaña *Matriz*.
- **Importar el año desde Excel.** En la lista de líneas, **Favoritos → Importar
  registros**. Columnas: presupuesto, producto, unidad (opcional — por defecto la
  de venta del producto), mes (día 1), cantidad e importe.
- **Aprobar.** Cuando la matriz está lista, MAST la **aprueba**: a partir de ahí es
  evidencia y no se edita.
- **Revisar en junio.** La reprogramación de mitad de año se hace con **Revisar
  (nueva Rev.)**: crea la Rev. siguiente en borrador copiando las líneas y deja la
  anterior como obsoleta. **La historia se conserva; nunca se pisa lo aprobado.**
- **El real se calcula solo** desde lo facturado (la cifra dura) y, como apoyo, lo
  pedido. La comparación (ppto vs facturado vs pedido) está en **Comparación**.

📏 **Por qué los totales de cantidad son por unidad.** Vendemos en metros, kg,
rollos y piezas. Sumar "12,500 m + 3,200 kg" no significa nada, así que las
cantidades se totalizan **por unidad** ("12,500 m · 3,200 kg"). El **único total
global es el de dinero** (en la moneda de la compañía; contabilidad ya convirtió
cada factura a su tipo de cambio, no se reconvierte). Si una factura viene en una
unidad de otra categoría, cuenta en el importe pero no en la cantidad, y el aviso
aparece en la ficha para corregir la unidad.

El KPI **VE-02 (Cumplimiento de presupuesto)** lee este presupuesto aprobado, y al
cierre de mes el sistema **avisa al responsable del equipo** si el acumulado va por
debajo del umbral configurado en Ajustes.

## 8. Auditorías internas

**SGI → Auditorías y riesgos**: programa anual → auditoría (con su folio AUD-) → checklist en
**Encuesta** (contestable en tableta, como siempre trabajó el auditor) → **hallazgos**.
Un clic convierte el hallazgo en NC. La auditoría no cierra hasta que todo hallazgo
tenga disposición. El sistema avisa al auditor líder 15 días antes de cada auditoría
del programa. Regla dura: nadie audita su propio proceso (el sistema lo impide).

## 9. Riesgos y oportunidades

**SGI → Auditorías y riesgos → Riesgos**: los 5 instrumentos en un solo lugar — **RyO** (5×5, niveles
Inmediata/Media/Intermedia/Baja), **IPER** de SST (3×3), **aspectos ambientales**,
**patrimonial** y **FODA**. Con acciones de mitigación, riesgo residual y revisión
programada (adiós a las "matrices desactualizadas" de cada auditoría: el sistema te
recuerda revisarlas).

## 10. Proveedores

Cada trimestre el sistema evalúa solo a los proveedores con recepciones: entregas a
tiempo + NCs → calificación y clase **Acreditado / Condicionado / Baja** (visible en la
ficha del proveedor y en la orden de compra, junto al contador de sus NCs). Proveedor
condicionado → actividad automática a Compras.

## 11. Revisión por la Dirección

**SGI → Revisión por la Dirección** → Nueva → botón **"Cargar entradas"**: las 10
entradas de la norma (acuerdos previos y su % de cumplimiento, NCs, reclamaciones,
auditorías, KPIs en rojo, proveedores, riesgos altos, ambiental, cambios) se llenan
SOLAS con los datos del periodo. Los **acuerdos** se capturan con responsable y fecha
— y se convierten en tareas rastreables: el % de cumplimiento de la próxima revisión
también se calculará solo. El acta se imprime con un clic.

## 12. Calidad de planta y herramientas automotrices

- **Planes de control** (SGI → Automotriz): las características a inspeccionar por
  producto (con criticidad F/R/S tipo Continental); las inspecciones reales de
  recepción/producción salen de aquí, y lo rechazado se va solo a **Cuarentena**.
- **Certificado de Calidad (CoA):** en el lote → botón "Certificado de calidad" → PDF
  bilingüe con los resultados reales de inspección; se puede publicar al portal del
  cliente junto a su entrega.
- **Metrología:** cada equipo de medición con su semáforo de calibración. Equipo
  vencido o fuera de tolerancia queda **"NO USAR"** automáticamente, y fuera de
  tolerancia genera la NC de evaluación de impacto (qué producto se midió con él).
- **AMEF:** modos de falla con NPR calculado; NPR alto exige acción antes de que el
  AMEF sea vigente. Al cerrar una NC mayor, el sistema recuerda actualizar el AMEF.
- **PPAP:** expediente por cliente/producto con los 18 elementos generados
  automáticamente; no se puede marcar "enviado" con elementos pendientes ni "aprobado"
  sin el PSW.
- **Pesaje y revisado** (piso): se sigue trabajando igual con la báscula y los wizards
  de siempre; la novedad es que un rollo confirmado fuera de tolerancia de peso genera
  su alerta de calidad solo, y los defectos TEJIDO-* alimentan el pareto del tablero.

## 13. Incidentes de seguridad (SST)

**SGI → Auditorías y riesgos → SST → Incidentes**: cualquier empleado puede reportar (lesión, casi-accidente,
daño, ambiental). La investigación usa la metodología **SCAT** de siempre (causas
inmediatas → básicas → falta de control) y el cierre exige las 3 capas + acciones
terminadas. Un incidente grave avisa de inmediato a MAST y Dirección.

## 14. Migración de formatos (para MAST)

**SGI → Documental → Migración de formatos**: el tablero que dice exactamente **qué formatos ya
viven en Odoo y cuáles faltan**. Cada formato trae su clase (A: lo sustituye una
transacción de Odoo · B: se vuelve hoja de trabajo de Calidad · C: Odoo lo imprime ·
D: sigue como documento), su **destino** y su estado — arrastra la tarjeta conforme
avanza (Pendiente → En curso → Migrado / Baja tramitada). Viene pre-clasificado; MAST
solo valida y ejecuta por tandas.

**¿Y la clave del formato?** Cuando una transacción de Odoo sustituye a un
formato (la cotización es el F-P-A28-04, la remisión el F-P-A16-01…), el
registro lo dice él mismo: una franja azul arriba del formulario muestra
**"Formato controlado del SGI: F-P-A28-04 · Rev. 03"**, y el PDF impreso lleva
la misma clave y revisión en el pie de página. La revisión se lee en vivo del
documento vigente en Documentos: si sube de revisión por el flujo de cambio,
pantalla y PDF se actualizan solos. El mapeo se administra en SGI →
Configuración → *Formatos en documentos de Odoo* (solo Jefe de MAST).

## 15. Preguntas frecuentes

**"No me deja cerrar la NC"** → te falta causa raíz, una acción sin terminar o la
eficacia. Es el candado, no un error.
**"No veo el menú SGI"** → no estás en el grupo Usuario SGI; pídelo a Sistemas/MAST.
**"¿Dónde quedó el formato X?"** → búscalo por clave en Documentos; si su estado de
migración es "Migrado", ya no se llena: usa su destino en Odoo (lo dice la pestaña
Migración). Si está "Baja tramitada", desapareció por una razón.
**"Me llegó una actividad que no entiendo"** → ábrela: siempre apunta al registro
(NC, documento, medición) con la explicación.
**"¿Y los registros viejos (actas, concentrados, Excels de años pasados)?"** → quedaron
archivados en el respaldo (Dropbox de solo lectura). El sistema arranca la historia
nueva desde Odoo.
**"Quiero proponer una queja/sugerencia"** → el buzón QR de planta (crea un ticket
directo) o con tu jefe/MAST.

## 16. Regla de convivencia durante la transición

Cuando un formato se declara **Migrado**, el Excel viejo se deja de llenar — máximo un
mes de doble captura mientras se valida. Un dato, un lugar: si está en Odoo, Odoo es
la verdad.
