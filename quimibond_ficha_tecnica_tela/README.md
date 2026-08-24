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
2. Si ya tenías instalada una versión anterior de este módulo (con el
   modelo único `ficha.tecnica.tela`), usa **Actualizar**, no
   desinstalar — Odoo mantiene los datos ya cargados en las tablas que
   siguen existiendo, pero el modelo antiguo `ficha.tecnica.tela` deja de
   existir en esta versión. **Antes de actualizar en producción, exporta
   o respalda los datos de fichas ya capturadas con el modelo anterior**,
   ya que este cambio de esquema no migra automáticamente los datos del
   modelo viejo al nuevo par tejido/acabado — hay que recapturarlos o
   escribir un script de migración a la medida si ya tienes muchos
   registros cargados.
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

## Métodos disponibles para desarrollos futuros

En `ficha.tecnica.acabado`, el campo `rendimiento_tela_acabada` es el que
alimenta el cálculo de tamaño de orden / split de Tintorería descrito en
el documento de Diseño Técnico general, junto con
`tintoreria.capacidad.rendimiento` (módulo separado
`quimibond_tintoreria_rendimiento`).
