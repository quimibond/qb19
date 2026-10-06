# Nomenclatura de artículos de tejido y acabado

Fuente: **DAT P-D02-01 «Codificación de productos para el área de tejido y
acabado», revisión 05 (mayo 2025)**, documento controlado del SGI
(`documents.document` 4694). Esta es la referencia para cualquier código que
parsee la referencia interna; el parser de `qb_capacidad_costeo`
(`qb.producto.ficha`) llamaba «calidad» al bloque de hilo + galga y está mal.

```
 I  W  J  080  Q  21  J  NT  165  AF
 │  │  │   │   │   │  │   │   │    └─ 16     acabado (opcional)
 │  │  │   │   │   │  │   │   └────── 13-15  ancho de tela abierta, cm
 │  │  │   │   │   │  │   └────────── 11-12  color
 │  │  │   │   │   │  └────────────── 10     operación: H tejido · I teñido · J acabado
 │  │  │   │   │   └───────────────── 8-9    rango de galga
 │  │  │   │   └───────────────────── 7      tipo de hilo
 │  │  │   └───────────────────────── 4-6    peso, g/m² (3 dígitos)
 │  │  └───────────────────────────── 3      dibujo (tipo de tejido)
 │  └──────────────────────────────── 2      composición
 └─────────────────────────────────── 1      unidad: I = kilogramos; sin letra = metros
```

## Posición 2 — composición

| Clave | Composición |
|---|---|
| B | Poliéster brillante (BTE) / poliéster semi opaco (SO) |
| C | Cotton |
| D | Polipropileno 100 % (PP) |
| L | Lyocell |
| N | Nylon 100 % |
| T | Poliéster 100 % fibra corta |
| V | Poliéster / carbón |
| W | Poliéster 100 % |
| X | Poliéster / algodón |
| Y | Poliéster / mezcla |
| Z | Varios |

## Posición 3 — dibujo

| Clave | Dibujo |
|---|---|
| A | Cardigan |
| B | Crepe anulado |
| C | Crepe cargas |
| D | Desagujado |
| F | Fleece |
| J | Jersey |
| K | Interlock |
| L | Listado |
| M | Punto de Roma |
| N | Piqué anulado |
| P | Piqué cargas |
| Q | Jaquard |
| R | Rib |
| T | French terry |
| V | Vanizado |

## Posición 7 — tipo de hilo

| Clave | Hilo |
|---|---|
| Q | Natural |
| B | Preteñido |
| R | Reciclado |

## Posiciones 8-9 — rango de galga

El número no es la galga: es un **rango** que identifica la galga de la
máquina. Dentro del rango se numeran variantes.

| Galga | Rango |
|---|---|
| 14 | 01-10 |
| 16 | 11-20 |
| 18 | 21-30 |
| 22 | 31-45 |
| 24 | 46-55 |
| 26 | 56-65 |
| 28 | 66-75 |
| 30 | 76-85 |
| 32 | 86-95 |

Ejemplos: `WJ080Q21JNT165` → hilo natural, **galga 18**; `WJ053Q22JNT160` →
galga 18; `WR135Q46JNT165` → galga 24; `WN075Q66JBL205` → galga 28;
`WC090Q11JNT168` → galga 16; `WK120B68JNG165` → preteñido, galga 28;
`WK300R50JNG165` → reciclado, galga 24.

El diámetro de la máquina **no** va en el código (las etiquetas de los
centros de trabajo en Odoo sí lo traen: `DIAMETRO 30`, `DIAMETRO 32`…).

## Posición 10 — operación

| Clave | Operación |
|---|---|
| H | Tejido (crudo) |
| I | Teñido |
| J | Acabado (terminado) |

## Posiciones 11-12 — color

AC azul cielo · AM amarillo · AZ azul marino · BL blanco · CF café · GO gris
oxford · HU hueso · NA naranja · NG negro · NT natural · RO rojo · CJ café
jaspe · AJ azul jaspe · GJ gris jaspe · GR gris rata · GC gris claro · MO
morado · MZ mostaza · AN amarillo neón · NN natural negro.

## Posición 16 — acabado (opcional)

IN ignífugo · AB antibacterial · AF afelpado · ES esmerilado · SU tacto suave
· RI tacto rígido · AE antiestático · RE repelente · DI dryfit · BI biopulido ·
RA resina acrílica · TF termofijado.

## Ejemplos del documento

| Código | Lectura |
|---|---|
| `IWT105Q56JNT155AF` | kilos, poliéster 100 %, french terry, 105 g/m², hilo natural, galga 26, acabado, natural, 155 cm, afelpado |
| `WN066B46HNG099` | metros, poliéster 100 %, piqué anulado, 66 g/m², preteñido, galga 24, tejido, negro, 99 cm |
| `XJ130Q21IGO096` | poliéster/algodón, jersey, 130 g/m², natural, galga 18, teñido, gris oxford, 96 cm |
| `WB045Q46JBL166` | poliéster 100 %, crepe anulado, 45 g/m², natural, galga 24, acabado, blanco, 166 cm |

## Fuera de esta nomenclatura

Entretelas y no tejidos (`A55BL172`, `WM4032BL152`, `ZN4032NG152`, `K…`,
`P…`) siguen las DAT P-D02-02 y P-D02-03 (bloque numérico de 4 dígitos =
código de resina, no gramaje). Maquila: DAT P-D02-04. Hilo: DAT P-D02-05.

## Uso en el costeo v2

- La **galga** sale del código (posiciones 8-9) y las máquinas de Odoo traen
  la etiqueta `GALGA nn`: un crudo nuevo sin órdenes se estima con la
  historia de **las máquinas de su galga**, no con el promedio del centro
  (spec §5.2, fuente `galga`, entre `hermano` y `estimado`).
- El **diámetro** no está en el código; cuando importa (galga 18: Ø30 teje
  ~21.8 kg/h, Ø32 ~13.3 kg/h en 2025-26) la cotización de una especificación
  nueva debe preguntarlo, o la ruta de la receta debe fijar las máquinas.
- Las posiciones 2, 3 y 7 (composición, dibujo, hilo) permiten agrupar
  productos para rendimiento y velocidad de rama cuando no hay historia
  propia.
