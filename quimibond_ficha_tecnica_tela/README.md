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
