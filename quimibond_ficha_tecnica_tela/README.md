# Quimibond - Ficha Técnica de Tela

Módulo Odoo 19 para capturar, importar y administrar la ficha técnica
completa de cada artículo de tela (Tejido + Acabado).

## Instalación

1. Copiar la carpeta `quimibond_ficha_tecnica_tela` a la carpeta de
   addons de tu instancia (o al repo de custom addons que se despliega
   vía Git push a Odoo SH).
2. Actualizar la lista de aplicaciones (Ajustes → Técnico → Actualizar
   lista de apps) e instalar "Quimibond - Ficha Técnica de Tela".
3. Requiere el módulo `mrp` instalado (dependencia declarada en el manifest).
4. Requiere que el servidor tenga `openpyxl` disponible (ya lo usan
   otros módulos de importación en Odoo; normalmente ya está instalado).

## Uso

- **Menú → Fichas Técnicas de Tela → Fichas Técnicas**: alta/edición manual.
- **Menú → Fichas Técnicas de Tela → Importar desde Excel**: sube el
  archivo con el formato actual de "Ficha Técnica de Proceso Tejido
  Circular" y extrae automáticamente los datos de tejido (máquina,
  hilos, especificaciones, tela acondicionada). Los campos de la
  sección **Acabado** (rendimiento mts/kg, peso, ancho, espesor,
  encogimiento, elongación) no existen en el formato de origen actual
  — deben capturarse manualmente en la ficha después de importar, o
  editarse en cualquier momento desde el formulario.
- Cada ficha se puede vincular a los 2 productos de la arquitectura de
  manufactura: **Tela en Proceso (kg)** y **Tela Acabada (m)**. Desde
  la ficha del producto (`product.template`), un botón inteligente
  "Ficha(s) Técnica(s)" muestra las fichas vinculadas.
- Los campos de encogimiento tienen un indicador calculado
  "dentro de norma (≤5%)" — es informativo, no bloquea el guardado,
  para no impedir capturar datos históricos fuera de tolerancia.

## Nota sobre el mapa de celdas del importador

El archivo `wizard/ficha_tecnica_import_wizard.py` define un
diccionario `CELL_MAP` con las coordenadas fijas de celda del formato
Excel actual (ej. `G9` = artículo, `AF28` = galga, etc.), extraído y
validado contra el archivo real
`Ficha_tecnica_tejido_WJ044Q22HNT235.xlsx`. **Si el formato de la
plantilla Excel cambia** (se agregan/mueven filas o columnas), solo
hay que actualizar ese diccionario — el resto del wizard no requiere
modificarse.

## Pendiente / siguiente iteración sugerida

- Cuando exista un archivo tabular con múltiples artículos y su
  sección de Acabado ya poblada (a diferencia del formato actual que
  es 1 archivo = 1 artículo, solo Tejido), se puede extender el wizard
  para importación masiva fila por fila en vez de celda fija.
- Conectar `rendimiento_tela_acabada` con la lógica de cálculo de
  tamaño de orden / split de Tintorería, y con la BOM de "Tela
  Acabada" (cantidad de componente = 1/rendimiento), como está descrito
  en el documento de Diseño Técnico general.
