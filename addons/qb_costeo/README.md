# qb_costeo — Costeo Quimibond (segunda generación)

Motor de costeo que sustituye a `qb_capacidad_costeo`. Diseño completo en
`docs/superpowers/specs/2026-10-06-qb-costeo-v2-diseno.md`. Esta es la
**Fase 1**: tarifas por centro, períodos con compuerta y validación de datos.
El costo por producto, la conciliación y el cotizador llegan en las versiones
siguientes; mientras, corre en paralelo con el módulo anterior sobre la misma
base.

## La idea

> El módulo calcula la tarifa ($/h) de cada centro a partir del mayor y de la
> capacidad normal; Odoo la aplica en las órdenes; el módulo solo agrega lo
> que Odoo no ve.

Por cada centro directo y período:

```
pool_fijo     = fijo del centro en el mayor (suavizado N meses)
                + su parte de la nómina de fábrica (por sueldos de RH)
                + su parte de los centros indirectos (por horas, nómina o partes iguales)
pool_variable = energía y consumibles del centro (mes real)
tarifa_fija   = pool_fijo ÷ horas normales        (calendario × eficiencia, o capturada con motivo)
tarifa_var    = pool_variable ÷ horas reales      (órdenes de trabajo; si no hay, estándar de la receta × producido)
tarifa        = tarifa_fija + tarifa_var
ociosidad     = pool_fijo − tarifa_fija × horas reales       → al resultado del mes (IAS 2)
op_pct        = administración y ventas ÷ ventas (suavizados)
```

Al **cerrar** el período, la tarifa se escribe en «Costo por hora» de los
centros de trabajo marcados con *Publicar tarifa*. Odoo costea con ella las
órdenes del mes siguiente.

## Compuerta de cierre

Un período no cierra si:

- no se ha calculado;
- algún centro directo no tiene horas normales o no tuvo horas reales ni
  estándar en el mes;
- hay más de la tolerancia en cuentas de resultados sin clasificar;
- algún producto de los N principales del mes (por venta) tiene una
  validación abierta.

Las razones se ven en el formulario («Qué impide cerrar»). Reabrir exige
motivo y queda en el chatter.

## Validaciones

`qb.producto.validacion` corre semanalmente (y a mano en Configuración →
*Validar recetas y precios ahora*) sobre los productos con receta activa:

| Regla | Dispara cuando |
|---|---|
| `precio_cero` | un componente con unidad de peso cuesta 0 (el agua se excluye) |
| `precio_fuera_banda` | el costo promedio se aleja más del % de banda de la última compra |
| `receta_implausible` | una línea de colorante/auxiliar pasa del % máximo del peso, o todas juntas pasan del total |

Una fila se cierra sola cuando deja de disparar, se acepta con motivo, y se
reabre si el valor cambia más de 10 %.

## Configuración mínima

1. **Centros** (Manufactura → Costos → Configuración): código, naturaleza,
   driver, centros de trabajo de Odoo, departamentos de RH, *Publicar tarifa*.
2. **Clasificación de cuentas**: cada cuenta de resultados a su bucket; las
   de fábrica con centro y %. *Cuentas sin clasificar* lista lo que falta.
   Si la base trae `qb_capacidad_costeo`, la instalación importa su
   clasificación.
3. **Parámetros**: todos traen ayuda; los defaults sirven.
4. **Períodos**: crear el mes, *Calcular*, revisar tarifas, *Cerrar*.

## Pruebas

```
docker run … odoo -i qb_costeo --test-enable --test-tags /qb_costeo
```

`tests/test_periodo.py` (reparto, capacidad, estándar sin órdenes, compuerta,
publicación, reapertura, refs excluidas, para_cotizar) y
`tests/test_validacion.py` (colorante, autocierre, aceptar, precio cero y
banda).
