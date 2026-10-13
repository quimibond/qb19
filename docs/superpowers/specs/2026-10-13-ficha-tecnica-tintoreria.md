# Ficha técnica de tintorería: lo que falta para cerrar «una sola ficha por proceso»

Para Jose Sacramento (Consolti). Pedido de Dirección General, 2026-10-13.

## Contexto

Decisión de Jose: **una sola ficha por proceso, la de
`quimibond_ficha_tecnica_tela`**. Desde `quimibond_sgi` 57.155.0 el SGI ya no usa su
ficha de proceso por máquina (`sgi.machine.sheet`). Hereda las fichas de tejido y de
acabado de este módulo y les agrega solo la capa de control del desarrollo:

- el proyecto y la orden de la corrida;
- quién propuso, quién midió y quién validó, con fechas;
- el valor real de cada parámetro, con su número de ajuste y su motivo.

Ya funcionan así la ficha de tejido (crudo) y la de acabado. **Falta la de
tintorería** (teñido). Mientras no exista, «Validar corrida» de una orden de teñido
pasa sin ficha y lo deja escrito en la orden.

En 2.2.0 la ficha de tejido ya guarda la máquina como centro de trabajo
(`mrp.workcenter`) y las tolerancias como números. La de tintorería debería nacer
igual.

## El formato a sustituir

**F-P-D01-33 «Ficha técnica de tintorería»**, hoy **F-C1-19** en Documentos
(documento 3976, revisión 00). Estos son sus campos, tal como están en la
plantilla.

| Bloque | Campos |
|---|---|
| Datos generales | Ficha técnica, fecha, nombre del solicitante, cliente, correo, teléfono, dirección, descripción del producto |
| Ficha técnica de tintorería | Tipo de acabado, artículo, composición de la tela |
| Baño y carga | Relación de baño, volumen de baño, peso total de tela |
| Condiciones | Tiempo empleado, temperatura |
| Máquina | Porcentaje del brazo del plegador; bomba: velocidad y presión; porcentaje del acumulador: inicial y cargado |
| Gradiente de temperatura | Subida y bajada |
| Auxiliares utilizados | Lista |
| Gráfica de proceso | Curva de temperatura contra tiempo |
| Observaciones | Texto |
| Firmas | Analizó, Revisó, Supervisor de Tintorería, Administrador de Ventas y Marketing, Jefe de Manufactura, Jefe de Calidad |

## Propuesta de modelo

Un `ficha.tecnica.tintoreria` en `quimibond_ficha_tecnica_tela`, con las mismas
convenciones que tejido y acabado. A confirmar con Jose Sacramento:

- **Encabezado:** `articulo`, `revision`, `active`, `fecha_elaboracion`, como las
  otras dos fichas.
- **Artículo teñido:** `product_tenido_id` (Many2one `product.product`). Es el campo
  con el que el SGI encuentra la ficha del artículo.
- **Ficha base:** `tejido_id`, la ficha de tejido del crudo, como en acabado.
- **Máquina:** `maquina_id` (Many2one `mrp.workcenter`), la máquina de tintorería.
- **Parámetros numéricos:** cada uno con valor, tolerancia (Float) y unidad.
  - Relación de baño y volumen de baño.
  - Peso de tela.
  - Temperatura y tiempo.
  - Plegador (%).
  - Bomba: velocidad y presión.
  - Acumulador inicial y cargado (%).
  - Gradiente de subida y de bajada.
- **Composición y tipo de acabado:** texto o, mejor, las claves que ya tiene el
  catálogo de códigos.
- **Auxiliares:** renglones con producto (`product.product`), cantidad y unidad. Es
  lista, no texto.
- **Gráfica de proceso:** renglones de la curva (minuto, temperatura) o un adjunto.
  Pendiente de decidir.
- **Firmas:** no van en el modelo. Las firmas por puesto las pone la capa del SGI
  (propuso, midió, validó). Jose debe decidir qué puestos del formato se conservan.

## Cómo se engancha el SGI (lo hace el SGI cuando el modelo exista)

En `addons/quimibond_sgi/models/sgi_dev_fabric_sheet.py`:

1. `SHEET_MODELS['tenido'] = ('ficha.tecnica.tintoreria', 'product_tenido_id')`.
2. Una lista `TINTORERIA_PARAMS`, como `TEJIDO_PARAMS`: campo del valor, tolerancia,
   unidad, etiqueta y sección.
3. Una clase con `_name = 'ficha.tecnica.tintoreria'` e
   `_inherit = ['ficha.tecnica.tintoreria', 'sgi.dev.process.sheet.mixin']`, más
   `sgi_reading_ids` y la vista heredada con la pestaña «Control del desarrollo».

Con eso, «Fichas de proceso» del proyecto deja crear la de teñido, y «Validar
corrida» la exige como exige las de tejido y acabado.

## Preguntas abiertas

1. ¿Una ficha por artículo teñido (un color) o por proceso de color, compartida
   entre artículos?
2. ¿La gráfica se captura como renglones o se adjunta?
3. ¿Qué firmas del formato se conservan, además de propuso, midió y validó del
   SGI?
