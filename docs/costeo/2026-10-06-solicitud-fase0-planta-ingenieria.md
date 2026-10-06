# Solicitud de datos — Fase 0 del costeo nuevo (qb_costeo v2)

**Para:** Planta (tintorería y acabado), Ingeniería, Contabilidad
**De:** Dirección
**Fecha:** 2026-10-06
**Spec:** `docs/superpowers/specs/2026-10-06-qb-costeo-v2-diseno.md`

El costeo nuevo cobra a cada tela las horas que de verdad usa en cada centro,
en lugar de repartir una bolsa pareja por metro. Para arrancar necesitamos
tres cosas que solo planta e ingeniería saben. Todo se captura **en Odoo**, en
las operaciones de la receta, para que sirva igual cuando entren las órdenes de
trabajo y el pesaje de rollos en noviembre.

## 1. Tintorería (responsable: jefe de tintorería)

### 1.1 Familias de color y su ciclo

| Familia | Colores de la nomenclatura (posiciones 10-11) | Ciclo por carga (h:mm) | Confirmar |
|---|---|---|---|
| Natural / claro | NT, BL, OW, GO | 2:20 (supuesto) | ☐ |
| Medio | ¿cuáles? | ¿? | ☐ |
| Obscuro | NG, AZ, RO, HU, CO | 10:20 (supuesto) | ☐ |

- Los ciclos de arriba salieron de una estimación. Corríjanlos con el tiempo
  real de puerta a puerta de la máquina (carga, teñido, lavados, descarga).
- Si GO, BL u OW van en un ciclo distinto al natural, sepárenlos.
- Si hay colores que no están en la lista (p. ej. códigos nuevos), agréguenlos
  con su familia.

### 1.2 Kilos por carga

Por máquina de tintorería: capacidad en kg por carga y, si aplica, por banda de
rendimiento de la tela (A 3-6, B 7-10, C 11-15 m/kg). Si la tabla de
`quimibond_tintoreria_rendimiento` ya está al día, basta con decirlo.

### 1.3 Relación de baño

Litros de agua por kg de tela por máquina (sirve para validar el agua y los
auxiliares de las recetas).

## 2. Acabado (responsable: jefe de acabado)

### 2.1 Velocidad de rama por producto

Metros por hora reales (no nominales) en la rama para cada producto de la
lista de abajo. Si varios productos corren igual, indiquen la familia y la
velocidad una sola vez. Si un producto pasa dos veces por rama, anótenlo.

### 2.2 Centros de trabajo del pesaje

Confirmar qué centros de trabajo de Odoo van a usar las órdenes de trabajo y
el pesaje de rollos desde noviembre (nombre exacto en Odoo). Las operaciones
de la receta deben apuntar a esos mismos centros.

## 3. Ingeniería (responsable: ingeniería de producto)

### 3.1 Recetas que ya sabemos mal

| Producto | Problema | Qué hacer |
|---|---|---|
| WK284R46ING166 (teñido de WK300R46JNG155) | Lleva **0.632 kg de NEGRO016 por kg de tela** (63 %). Lo normal es 4-8 %; seguramente es 0.0632 | Corregir la cantidad en la receta |
| WK300R50HNG165 (crudo de WK300R50JNG165) | Precio de costo **0** en Odoo, se consume en receta activa | Revisar con Contabilidad el costo promedio del crudo |
| WC090Q11JNT168 | El costo promedio del crudo está roto en Odoo ($17.46/m de MP + tejido contra $6.66 del modelo) | Revisar con Contabilidad el AVCO del crudo WC090Q11HNT168 |

### 3.2 Operaciones de receta (tiempos estándar)

Capturar en Odoo, en la receta de cada producto de la lista, la operación de
**tintorería** (centro de trabajo, kg por carga de su banda, ciclo de su
familia de color) y la de **acabado** (centro de trabajo, velocidad de rama).
El equipo de sistemas carga las operaciones a partir de las tablas de 1 y 2; a
Ingeniería le toca validar que la ruta de cada producto sea la correcta (p. ej.
productos que pasan dos veces por rama o que no se tiñen).

### 3.3 Rango plausible de colorante y auxiliares

Confirmar los límites para la validación automática de recetas: colorante
≤ 12 % del peso de tela, auxiliares ≤ 25 %. Si hay recetas legítimas fuera de
esos rangos, indicarlas.

## 4. Contabilidad (responsable: contador general)

- Revisar los dos costos promedio del punto 3.1.
- Pregunta aparte, sin urgencia: ¿Contabilidad puede etiquetar los gastos de
  fábrica (cuentas 501 y 504) con su centro (tejido, tintorería, acabado…)
  como distribución analítica al capturar cada factura? Hoy el costeo lo
  adivina con una tabla manual.

## 5. Lista de productos (40 principales de septiembre + cotizados en septiembre-octubre)

Fabricados, vendidos en metros. Los importados (`IW…`, en kg) no entran.

| # | Producto | Ventas sep-26 | Tintorería (familia / ciclo / kg carga) | Acabado (m/h) |
|---|---|---|---|---|
| 1 | WJ053Q22JNT160 | $2,081,703 | | |
| 2 | X140NT165 | $1,975,506 | | |
| 3 | WJ042Q22JNT160 | $915,205 | | |
| 4 | A55BL172 | $678,184 | | |
| 5 | WJ060Q21JNT165 | $618,129 | | |
| 6 | WN075Q66JBL205 | $575,350 | | |
| 7 | WC090Q11JNT168 | $389,207 | | |
| 8 | XJ14021GO165 | $300,295 | | |
| 9 | A55BL86 | $270,628 | | |
| 10 | WM4032BL152 | $255,850 | | |
| 11 | WJ032Q22JNT160 | $242,430 | | |
| 12 | WN055Q66JNT162 | $236,603 | | |
| 13 | WJ060Q21JNT160 | $160,984 | | |
| 14 | WD038Q46JNT175 | $153,855 | | |
| 15 | WTT403266NG152 | $139,270 | | |
| 16 | ZN4032NG152 | $125,025 | | |
| 17 | WJ053Q22JNT170 | $123,084 | | |
| 18 | AS4032BL152 | $105,723 | | |
| 19 | WNS403266NG152 | $102,064 | | |
| 20 | ZN4032BL152 | $86,965 | | |
| 21 | AT9032BL152 | $75,205 | | |
| 22 | WP4032OW152 | $71,789 | | |
| 23 | WM4032OW152 | $66,713 | | |
| 24 | WJ060Q21JNT170 | $61,395 | | |
| 25 | WM4032NG152 | $59,387 | | |
| 26 | AP4032BL152 | $52,189 | | |
| 27 | WP4032NG152 | $48,279 | | |
| 28 | WP4032RO152 | $41,472 | | |
| 29 | WNS403266BL152 | $38,797 | | |
| — | **Cotizados sep-oct** | | | |
| 30 | WK300R46JNG155 (Bowen) | cotizado | | |
| 31 | WK300R50JNG165 (Bowen) | cotizado | | |
| 32 | WK140R46JNT165 (Bowen) | cotizado | | |
| 33 | WK120B68JNG165 (ContiTech) | cotizado | | |
| 34 | WK135B66JNG165 (ContiTech) | cotizado | | |
| 35 | NN040Q66JNT163 (ContiTech) | cotizado | | |
| 36 | WJ080Q21JNT165 (ContiTech / Menchaca) | cotizado | | |
| 37 | WJ080Q21JNT155 (Menchaca) | cotizado | | |
| 38 | WJ053Q22JNT195 (Shawmut) | cotizado | | |
| 39 | WJ053Q22JNT178 (Shawmut) | cotizado | | |
| 40 | WJ150R66JNT170 / WJ150R67JNT170 / WN130R22JNT170 (Seiren) | cotizado | | |

Los productos `A…`, `WM…`, `WP…`, `ZN…`, `AS…`, `AT…`, `WNS…`, `WTT…` son
entretelas y otras familias: si no pasan por tintorería o rama, basta con
marcarlo.

## Qué pasa con esto

Con las tablas 1 y 2 y la lista 5 llena, sistemas carga las operaciones en las
recetas y arranca el costeo nuevo en paralelo con el actual sobre septiembre y
octubre. Lo que no llegue se costea con el promedio del centro, marcado como
*estimado*, y así se ve en cada cotización hasta que llegue el dato.
