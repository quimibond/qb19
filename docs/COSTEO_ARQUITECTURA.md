# Cómo yo rediseñaría el costeo

**Fecha:** agosto 2026 · Recomendación de arquitectura, no de parche.

## El diagnóstico en una frase

El sistema construye **proxies para datos que ya existen**. Cada proxy necesita
un factor de calibración, cada factor necesita una banda de cordura, cada banda
necesita un parámetro, y cada parámetro necesita un chequeo en el panel. Por eso
una pregunta conceptualmente simple —¿cuánto cuesta este metro de tela?— vive en
~10,000 líneas y 30 parámetros.

Mira el patrón:

| Capa | Lo que hace el módulo | Lo que ya existe en Odoo |
|---|---|---|
| Materia prima | Explota la receta al último precio de compra, y le aplica un factor de ajuste contra el costo primo del mayor | Valuación real-time con AVCO en `Materia Prima` y `Producto en Proceso` — Odoo ya sabe el valor exacto de cada consumo |
| Aduana | Prorrateo sobre una base promedio | `stock.landed.cost`, configurado y con 195 registros históricos |
| Conversión | Pool de planta repartido 67/33 entre kilos y metros | `mrp.workcenter.costs_hour` × tiempo real del workorder |
| Renta | Un número contractual capturado a mano | El mayor (irregular, pero es el hecho) |
| Energía | $/kg promedio de planta | Cuentas ya asignadas a Tintorería y Acabado |
| Operación | % del precio de venta | — (aquí sí hace falta una decisión de reparto) |

Un proxy es la respuesta correcta cuando el dato primario no existe. Aquí existe
casi siempre, y el proxy es lo que produce la brecha.

## Lo que yo haría, en orden

### 1. Que Odoo valúe el inventario, y el módulo lo lea

`Materia Prima` y `Producto en Proceso` ya están en `real_time` + `average`. Eso
significa que cada movimiento de inventario genera su capa de valuación con el
valor real, y que el costo de ventas se postea solo. **El consumo real de cada
orden de producción es dato primario, no hay que estimarlo.**

Con eso, la MP del costeo deja de ser «receta × último precio × factor de
ajuste» y pasa a ser «lo que la orden consumió, al valor con que Odoo lo
descargó». Desaparecen de un golpe: la explosión recursiva de recetas, el mapa
de últimas compras, el factor de ajuste, su banda de cordura, sus dos
parámetros y su chequeo de panel.

La receta sigue sirviendo, pero para lo que sirve una receta: **estándar contra
el cual medir**. Consumo real vs. receta = variación de rendimiento, que es
información accionable — «esta tela se está yendo 8% arriba de receta» — en vez
de un factor global que solo dice que algo no cuadra.

### 2. Capturar los pedimentos con landed cost

Ya está configurado. En los últimos doce meses se aplicó **uno** ($111,355 en
mayo 2026) mientras ~$963,000/mes de aduana se quedaban en resultados.

El pedimento sabe a qué embarque pertenece. Capturarlo en la recepción hace que
el arancel de una máquina se capitalice en la máquina y el del hilo en el hilo
—que es exactamente lo que ningún prorrateo puede adivinar—, y de paso el costo
del hilo importado llega a cada tela por la receta, sin trucos.

Es trabajo operativo, no de código: alguien captura el pedimento cuando llega
el embarque.

### 3. Tarifa por hora en los centros de trabajo

Los 38 workcenters tienen `costs_hour = 0`. Poner ahí la tarifa (MOD + overhead
del centro ÷ horas normales) hace que **cada orden absorba conversión por el
tiempo que realmente usó en cada centro**. Eso es costear por ruta: el producto
que se vende crudo deja de pagar acabado, y el que da tres pasadas paga tres.

Es un campo por centro. El reparto 67/33 de planta existe únicamente porque ese
campo está vacío.

### 4. Reducir el módulo a lo que solo él puede hacer

Con 1–3, al módulo le queda un alcance mucho más chico y mucho más claro:

- **Conciliar** el costo de Odoo contra el estado de resultados, y señalar qué
  no cuadra (esto ya existe y es lo más valioso que tiene).
- **Repartir el overhead genuinamente indirecto** —renta, administración,
  ventas— con drivers declarados. Esto sí es una decisión gerencial y no la
  puede tomar Odoo.
- **Reportar margen y decidir precio**: contribución, cuello de botella,
  pisos, escalera de volumen. El cotizador ya es bueno.

De ~10,000 líneas a algo del orden de 2,000, y de 30 parámetros a unos 5.

## Los errores de diseño que más cuestan

### Configuración que le gana al dato, sin avisar

`renta_contractual_mxn`, `capacidad_normal`, `std_output_per_hour`,
`denominador_kg_override`, `energia_por_kg`, `op_pct_override`. Un número
tecleado que sobrescribe lo observado es una mina: nadie recuerda que está ahí.
El bug de la renta fue exactamente eso — el número existía, estaba bien, y no lo
usaba nadie.

**Regla:** todo parámetro o (a) se valida contra su fuente de datos y avisa
cuando se separa, o (b) no existe. Un override sin fecha de caducidad y sin
alarma es deuda garantizada.

### Cuatro caminos para calcular un costo

Hoy: `qb.costo.producto` (mensual), `quote_product` (en vivo),
`product.standard_price` (el de Odoo) y `comparador._metrics` (un cuarto). Ya
encontramos una divergencia real: el comparador derivaba `op_pct` de un
cociente y con el driver nuevo daba un piso falso.

**Regla:** una función. Todo lo demás la llama. Si dos caminos tienen que
existir (mensual cerrado vs. en vivo), que compartan el núcleo y que haya un
test que fije que dan lo mismo.

### Promedios de promedios con ventanas distintas

Pools suavizados a 12 meses ÷ producción de 3 meses, aplicados a las ventas de 1
mes. Cada desajuste de ventana fabrica varianza que no existe: la cobertura de
fabricación oscilando entre 80% y 117% es en buena parte eso.

**Regla:** un período de costeo = un mes, y los dos lados de cualquier cociente
se miden sobre la misma ventana. Si hace falta suavizar, se suaviza el
resultado, no cada lado por su cuenta.

### La historia se reescribe

Cualquier recálculo sobrescribe meses ya cerrados. El número que presentaste el
mes pasado puede cambiar sin que nadie se entere.

**Regla:** el período de costeo tiene estado. Borrador → cerrado. Un período
cerrado no se recalcula sin reabrirlo explícitamente, y la reapertura deja
rastro. Sin esto, nadie puede defender un número frente a dirección.

### Seis banderas donde deberían ir tres ejes

`bucket`, `es_variable`, `es_renta`, `centro_id`, `driver`, `allocation_pct` —
todas sobre la misma fila de clasificación, y algunas significan cosas que se
traslapan.

**Mejor práctica:** tres dimensiones limpias.

- **Elemento de costo** — qué es: material, mano de obra, energía, depreciación,
  renta, servicios.
- **Centro de costo** — dónde ocurrió.
- **Comportamiento** — fijo o variable.

El resto (driver, %) es la *regla de reparto*, que es otra tabla: de qué centro,
con qué driver, a qué destino. Separar «qué es» de «cómo se reparte» es lo que
hace que el sistema se pueda explicar en cinco minutos.

### Se reporta la brecha, pero no de qué está hecha

El punto del costeo estándar son las **variaciones**. Hoy el sistema da un
número y un hueco.

**Mejor práctica:** descomponer el hueco.

- **Variación de precio** — pagamos distinto de lo esperado por el insumo.
- **Variación de rendimiento** — consumimos más o menos que la receta.
- **Variación de tarifa** — la hora de centro costó distinto.
- **Variación de volumen** — produjimos menos que la capacidad normal (esto ya
  está: `fab_ocioso_month`).

Eso convierte «faltan $2M» en «$1M es rendimiento en tejido, $600k es precio de
hilo, $400k es ociosidad» — que sí se puede accionar.

### Nadie puede explicar un costo en una pantalla

Treinta parámetros y catorce chequeos significan que una sola persona sabe
operarlo. Un sistema de costeo en el que la gente confía es uno donde el costo
de un producto se explica completo, de un vistazo: *este metro cuesta $X — $a de
hilo (estas compras), $b de conversión (estas horas en estos centros), $c de
overhead (este driver)*.

El módulo ya tiene el «desglose explicado» del cotizador, que es exactamente el
instinto correcto. **Esa debería ser la interfaz principal**, no un anexo del
PDF.

## Las tres preguntas que hoy están mezcladas

Buena parte de los errores vienen de mezclar tres cosas que responden a
preguntas distintas y admiten respuestas distintas:

| Pregunta | Regla | Quién manda |
|---|---|---|
| ¿Cuánto vale mi inventario? | IAS 2: costo de adquisición y conversión, ociosidad al resultado | La contabilidad / Odoo |
| ¿Cuánto me cuesta este producto? | Costeo gerencial: contribución, costo por ruta, cuello de botella | El módulo |
| ¿A cuánto lo vendo? | Pisos + capacidad + mercado | El cotizador |

`op = op_pct × precio` existió porque las tres estaban en la misma fórmula. Un
costo de inventario **no puede** depender del precio de venta; un piso de precio
**sí** tiene que resolverse contra un gasto que es porcentaje de la venta. Son
respuestas distintas y ahora cada una usa su driver — pero conviene que la
separación sea explícita en el diseño, no una nota al pie.

## Por dónde empezar

Ordenado por relación valor/esfuerzo:

1. **Capturar pedimentos con landed cost** (operativo, empieza mañana). Cierra
   ~$963k/mes con el dato correcto en lugar de un prorrateo.
2. **Tarifa por hora en los 38 workcenters** (un campo por centro). Desbloquea
   el costeo por ruta, que es el hallazgo #12 de la revisión.
3. **Cerrar períodos** (código chico, alto valor de confianza).
4. **Leer el consumo real de las órdenes** en lugar de explotar recetas (código
   medio). Convierte el factor de ajuste de MP en variación de rendimiento.
5. **Reestructurar la clasificación** a elemento × centro × comportamiento
   (código medio, migración de datos).

Los puntos 1 y 2 no son de programación, y son los dos que más mueven la aguja.
