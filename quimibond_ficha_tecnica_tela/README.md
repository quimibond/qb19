# Quimibond - Ficha Técnica de Tela

Módulo Odoo 19 con **2 modelos separados**:

- `ficha.tecnica.tejido` — datos de tejido (máquina, hilos,
  especificaciones, tela acondicionada). Se vincula 1:1 con el producto
  "Tela en Proceso" (kg).
- `ficha.tecnica.acabado` — datos de acabado (rendimiento, peso, ancho,
  espesor, encogimiento, elongación). **Cada producto de tela acabada**
  (ej. cada color) tiene su propia ficha, con un `Many2one` obligatorio
  hacia la `ficha.tecnica.tejido` que le sirve de base. Una ficha de
  tejido puede ser la base de varias fichas de acabado.

## Instalación / actualización

1. Copiar la carpeta `quimibond_ficha_tecnica_tela` a la carpeta de addons.
2. El modelo único `ficha.tecnica.tela` de la primera versión ya no existe
   en el código (2.1.1 retiró también su importador; Jose Sacramento
   confirmó que no lo usa). Su tabla `ficha_tecnica_tela` sigue en la base
   hasta que se borre a mano; no se migra al par tejido/acabado.
3. Requiere `mrp` y `openpyxl` en el servidor.

## Uso

- **Menú → Fichas Técnicas de Tela**:
  - **Fichas Técnicas de Tejido** — alta/edición manual.
  - **Fichas Técnicas de Acabado** — alta/edición manual (requiere elegir
    primero la ficha de tejido base y el producto de tela acabada).
  - **Importar Tejido desde Excel** — importación masiva tabular.
  - **Importar Acabado desde Excel** — importación masiva tabular.
- Desde la ficha del producto (`product.template`), 2 botones inteligentes
  separados: "Ficha de Tejido" y "Ficha de Acabado".
- Desde una ficha de tejido, un botón muestra todas las fichas de acabado
  que la usan como base.

## Importación masiva (tabular)

Ambos wizards leen la **primera fila como encabezados** y cada fila
siguiente como un artículo distinto. El match de encabezado es tolerante a
mayúsculas/acentos/espacios (ej. "Rendimiento", "rendimiento", "RENDIMIENTO"
matchean igual). Columnas no reconocidas se ignoran — puedes incluir
columnas extra sin que falle la importación.

### Columnas reconocidas — Importar Tejido

`Artículo` (requerida), `Revisión`, `Producto Proceso` (referencia interna
o nombre del producto "Tela en Proceso", opcional), `Máquina`, `Marca
Máquina`, `Galga`, `Diámetro`, `No Agujas`, `No Alimentadores`,
`Velocidad`, `Vueltas por rollo`, `Notas`.

Tabla de especificaciones por polea/hilo: `Longitud Malla Polea1` /
`Polea2` / `Tolerancia` / `Tolerancia Unidad`, igual para `Consumo cm vta`
y `Polea Alimentación`.

Tabla de especificaciones generales: `Tensión`, `Punto Cilindro`, `Punto
Plato`, `Altura Plato`, `Ancho Bastidor`, `Estiraje`, `Ancho Rollo`,
`Peso Promedio Rollo` — cada uno con su columna `<Dato> Tolerancia` y
`<Dato> Tolerancia Unidad`.

Tela acondicionada: `Peso Acondicionado`, `Ancho Acondicionado`, `Espesor
Acondicionado`, `Columnas`, `Mallas`, `Elongación Carga Largo`,
`Elongación Carga Ancho` — cada uno con su columna `<Dato> Tolerancia` y
`<Dato> Tolerancia Unidad` (el de Peso admite tolerancia asimétrica en
texto libre, ej. "+12 / -6").

Hasta 2 hilos por columnas `Hilo1 Tipo` / `Hilo1 Título` / `Hilo1
Torsión` / `Hilo1 Pct` / `Hilo1 Lote` / `Hilo1 Proveedor` (e igual para
`Hilo2`). **`Hilo1 Proveedor` / `Hilo2 Proveedor` deben ser el nombre
exacto de un contacto ya existente en Odoo marcado como proveedor**
(`supplier_rank > 0`); si no se encuentra, la fila se importa igual pero
sin vincular el proveedor, y se reporta como aviso.

### Columnas reconocidas — Importar Acabado

`Artículo` (requerida), `Revisión`, `Artículo Tejido` (requerida — debe
existir ya como ficha de tejido), `Producto Acabado` (requerida —
referencia interna o nombre del producto, debe existir ya en Odoo),
`Rendimiento`, `Notas`.

Tabla de datos de tela acabada: `Peso`, `Ancho`, `Encogimiento a lo
Largo`, `Encogimiento a lo Ancho`, `Espesor`, `Elongación Largo`,
`Elongación Ancho` — cada uno con su columna `<Dato> Tolerancia` y
`<Dato> Tolerancia Unidad` (el de Peso admite tolerancia asimétrica en
texto libre, ej. "+12 / -6").

Si una fila referencia un artículo de tejido o producto que no existe
todavía en Odoo, esa fila se omite y se reporta en el resumen de avisos
al final de la importación — el resto de filas válidas sí se procesan.

## Los dos rendimientos (regla de Jose, 2026-10-06)

Hay dos rendimientos en metros por kilogramo y **los dos se conservan como
campo**:

- **Tejido** (`rendimiento_tela_tejida` en `ficha.tecnica.tejido`, rama
  `consolti`): rendimiento de la tela en proceso. **Es el que alimenta el
  cálculo de tamaño de orden y split de Tintorería**, junto con
  `tintoreria.capacidad.rendimiento` (módulo `quimibond_tintoreria_rendimiento`).
- **Acabado** (`rendimiento_tela_acabada` en `ficha.tecnica.acabado`):
  rendimiento teórico del producto terminado. **Valida los metros finales**
  contra el rendimiento real del pesaje de cada rollo. No alimenta a
  Tintorería.

## Columnas fijas

Las columnas fijas de `ficha.tecnica.tejido` y `ficha.tecnica.acabado`
(máquina, hilos, poleas, tela acondicionada, peso, ancho, encogimiento,
elongación, rendimientos…) **no se eliminan ni se renombran**: el código de
Consolti las lee. Los renglones de características de 2.1.0 conviven con
ellas.

## Características con dos juegos de límites (2.1.0)

Desde 2.1.0 el módulo es dueño del **catálogo de características** y de las
**claves de codificación de artículos**, y cada ficha de tejido y de acabado
lleva una pestaña «Características» con un renglón por característica:

| Columna | Qué guarda |
|---|---|
| Característica | Del catálogo (`ficha.tecnica.caracteristica`: clave estable, nombre en español e inglés, tipo de dato numérico / cualitativo / sí-no, unidad, método o norma); un renglón sin catálogo se escribe libre |
| Dirección y posición | Largo / ancho; izquierda / centro / derecha |
| Especificación del cliente | Nominal y límite: nominal ± tolerancia (en unidades o en %), máximo o mínimo. Texto solo en las cualitativas |
| Control interno | Margen más cerrado sobre el mismo nominal. **Nunca se imprime al cliente**; el certificado y las Especificaciones del producto salen de la especificación del cliente |
| En especificación del cliente / En certificado | Qué renglones van a cada documento |

Los límites viven en el mixin `ficha.tecnica.caracteristica.mixin`
(`_result_for(valor)` devuelve `cumple`, `desviacion` o `no_conforme`;
`_limit_vals()` copia un renglón de un documento a otro). El SGI de
`quimibond_sgi` (tabla de características del proyecto de desarrollo, C1)
usa el mismo catálogo y el mismo mixin, de modo que al liberar un artículo
los renglones del proyecto pasan a su ficha sin recaptura.

Las claves de codificación (`ficha.tecnica.clave.codigo`) transcriben el DAT
P-D02-01 rev. 05: composición, dibujo, tipo de hilo, galga por rango
(`gauge_code(18)` → `'21'`, `gauge_from_code('22')` → `18`), operación, color
y acabado. Catálogo y claves se siembran con `noupdate` y se editan en
**Fichas Técnicas de Tela → Configuración** (gerentes de Fabricación).
Pruebas: `tests/test_caracteristicas.py` (corren en el CI).

## Máquina y tolerancias numéricas (2.2.0, 2026-10-13)

Decisión de Jose: una sola ficha por proceso, la de este módulo. El SGI
(`quimibond_sgi` 57.155.0) le agrega la capa de control del desarrollo
(proyecto, orden, quién propuso, validó y midió, y el valor real de cada
parámetro) y deja de usar su ficha de proceso por máquina.

- **Máquina** (`maquina_tejido`) es el centro de trabajo de Odoo
  (`mrp.workcenter`). Lo que se capturó como texto antes de 2.2.0 queda en
  `maquina_tejido_texto` (solo lectura, dato histórico). Al importar del
  Excel, la columna `Máquina` se liga al centro de trabajo con ese nombre o
  código; si no hay uno solo, se guarda el texto y la importación lo avisa.
- **Tolerancias**: las que eran texto ahora son número. La de peso (tela
  acondicionada y acabada) tiene dos: arriba (`*_tol`) y abajo
  (`*_tol_minus`); con la de abajo en 0 la tolerancia es ± la de arriba. En
  el Excel se sigue aceptando «+12 / -6».
- La migración `19.0.2.2.0/pre-migrate.py` conserva los textos que no se
  puedan leer como número en una columna `<campo>_texto_2_1` y los reporta
  en el log.

## Ficha de la máquina: configuraciones por fecha (2.3.0, 2026-10-13)

Decisión de Jose: los datos de una circular son de la máquina, no del
artículo, y cambian con el cilindro.

- **En el centro de trabajo** (pestaña «Ficha de la máquina»): marca y
  modelo, y las **configuraciones con fecha** (`ficha.tecnica.maquina.config`):
  diámetro (in), galga, agujas, alimentadores y velocidad máxima, «vigente
  desde» y el motivo (alta, cambio de cilindro). La vigente es la de fecha más
  reciente que ya empezó. También en Ficha técnica → Configuración →
  Configuraciones de máquina.
- **La ficha de tejido** guarda la configuración con la que se elaboró: al
  poner la máquina y la fecha de elaboración se elige sola la vigente a esa
  fecha, y de ella salen galga, diámetro, agujas, alimentadores y marca (en
  solo lectura). Un cambio de cilindro posterior no altera las fichas
  anteriores. La velocidad sigue siendo de la ficha (es del proceso); si
  supera la máxima de la configuración, la ficha lo avisa.
- **Galga del artículo**: si se captura, la lista de máquinas solo ofrece las
  que hoy corren esa galga.
- Una configuración que ya usa alguna ficha no se borra: se registra otra con
  su fecha.
- `diametro` de la ficha de tejido pasa de texto a número
  (`migrations/19.0.2.3.0/pre-migrate.py`).

## Ficha de la máquina para cualquier tipo de máquina (2.4.0, 2026-10-13)

Decisión de Jose: la ficha de la máquina no es solo de tejido; cambia con el
tipo de máquina (tejido, tintorería, acabado, inspección, abridora,
devanadora…).

- **Tipos de máquina** (Configuración → Tipos de máquina,
  `ficha.tecnica.maquina.tipo`): cada tipo tiene su lista de **parámetros**
  (`ficha.tecnica.maquina.parametro`: clave, nombre, unidad, numérico o
  texto, cómo se mide). El módulo trae seis tipos (tejido circular,
  tintorería, acabado, inspección, abridora, devanadora) y **solo los
  parámetros de tejido circular** (diámetro, galga, agujas, alimentadores,
  velocidad máxima). Los de los demás tipos los da de alta la planta: no se
  inventan. Se pueden agregar tipos (carda, fusionadora, rama…).
- **Centro de trabajo**: campo «Tipo de máquina» en la pestaña «Ficha de la
  máquina», junto a marca y modelo. Si una máquina sin tipo recibe su
  primera configuración, toma el tipo de esa configuración.
- **Configuración con fecha**: un renglón por parámetro activo de su tipo
  (`ficha.tecnica.maquina.valor`), con valor numérico o texto. Al cambiar el
  tipo se rehacen los renglones. Un parámetro nuevo aparece en las
  configuraciones que se creen después; uno con valores capturados no se
  borra (se archiva).
- **Tejido**: galga, diámetro, agujas, alimentadores y velocidad máxima
  siguen como columnas de la configuración, calculadas desde los valores con
  las claves `galga`, `diametro`, `no_agujas`, `no_alimentadores` y
  `velocidad_max` (y se pueden escribir directo: crean o ajustan el valor).
  La ficha de tejido y la búsqueda de máquinas por galga no cambian.
- La migración `19.0.2.4.0/post-migrate.py` expone al MCP los cuatro
  modelos (lectura, alta y edición). No asigna tipos ni crea configuraciones.

## Avisos de máquina en producción (2.5.0, 2026-10-13)

Primeros usos de la ficha de la máquina en la planta. Son **avisos**, no
bloqueos: la decisión sigue siendo de la planta.

- **Galga al programar tejido** (`mrp.workorder.ft_aviso_maquina`): si la
  orden de trabajo va a una circular cuya configuración vigente (a la fecha
  de inicio) corre otra galga que la del artículo, la orden lo avisa en la
  ficha, en la lista de órdenes de trabajo (renglón amarillo y columna
  «Aviso de máquina»), al cambiar la máquina (ventana de aviso) y con el
  filtro «Galga distinta a la máquina». La orden de fabricación repite el
  aviso. La galga del artículo sale de su ficha de tejido («Galga del
  artículo», o la galga con la que se elaboró) y, si no hay ficha, de las
  posiciones 7 y 8 del código con el catálogo de claves de galga
  (DAT P-D02-01: 21 a 30 = galga 18, 46 a 55 = galga 24, 66 a 75 = galga 28…).
- **Cargas por jet en tintorería** (`mrp.production`): las órdenes de
  tintorería no llevan órdenes de trabajo, así que la orden de fabricación
  trae la pestaña «Cargas por jet»: por cada jet, la capacidad por carga que
  aplica y cuántas cargas son. La capacidad sale de la configuración vigente
  del jet: la del grupo de rendimiento de la tela (A de 3 a menos de 7, B de
  7 a menos de 11, C de 11 a 15 m/kg; hoja «CAP DE CARGAS») y, sin
  rendimiento, la capacidad general. El rendimiento sale de la ficha de
  tejido: 1000 / (peso g/m² × ancho m); un ancho fuera de 0.3 a 4 m no se
  usa. Si se elige el **Jet** de la orden, avisa cuando la orden no cabe en
  una carga o el jet no tiene capacidad para ese grupo.
- Qué órdenes son de tintorería lo dice el tipo de operación («Calcular
  cargas por jet»). La migración `19.0.2.5.0/post-migrate.py` lo prende en
  los tipos de fabricación cuyo nombre dice «Tintorería».

## Listo para el producto único de acabado en kg (2.6.0, 2026-10-13)

Decisión de Jose: los productos de tintorería se van a dejar de usar; habrá
**un producto de acabado en kg** con todas las operaciones en su ruta, desde
que se recibe de tejido hasta que sale de inspección. Sin cambiar lo que hoy
funciona con las órdenes TL/OP-TIN:

- **Aviso de carga en la orden de trabajo del jet**: si una orden de trabajo
  cae en un jet (tipo de máquina «Tintorería») y la orden de fabricación trae
  más kg de los que caben en una carga (según el grupo de rendimiento o la
  capacidad general), la orden de trabajo lo avisa como el de galga: banner,
  renglón amarillo, ventana al cambiar la máquina y el filtro «Con aviso de
  máquina» (antes «Galga distinta a la máquina»).
- **Pestaña «Cargas por jet»** también en una orden con orden de trabajo en un
  jet; el jet resaltado es el de la orden de trabajo y el campo «Jet» manual
  se oculta.
- **Rendimiento por la ficha de acabado**: si el producto no tiene ficha de
  tejido propia (caso del producto de acabado), se toma la de tejido en que
  se basa su ficha de acabado. El rendimiento de tintorería sigue siendo el
  de tejido (regla de los dos rendimientos).
- Pendiente para cuando exista el producto único: la corrida de muestra del
  SGI (57.154.0) arma crudo → teñido → acabado y tendrá que pasar a crudo →
  acabado con la ruta completa.
