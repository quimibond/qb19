# website_quimibond_proveedores — Portal de proveedores

Extiende «Mi cuenta» del sitio B2B (`website_quimibond`) para el proveedor.
Se enciende con la misma casilla **Diseño B2B de Quimibond** del sitio web;
apagada, todas las rutas dan 404.

## Qué ve el proveedor

| Ruta | Qué hay |
|---|---|
| `/mi-cuenta/proveedor` | Resumen: órdenes por entregar, órdenes que esperan su factura, por cobrar por moneda y próximo pago |
| `/mi-cuenta/proveedor/ordenes` | Órdenes de compra de 12 meses y las que faltan por facturar |
| `/mi-cuenta/proveedor/orden/<id>` | Lo pedido, lo recibido y lo facturado; **subir factura** (XML y PDF); facturas de la orden |
| `/mi-cuenta/proveedor/pagos` | Facturas en revisión, con pago programado (y su fecha), rechazadas y pagadas |
| `/proveedores` | Página pública: cómo funciona, requisitos y compromisos |
| `/proveedores/alta` | Alta en línea con documentos |

Quien solo es proveedor entra directo a su portal desde `/mi-cuenta`; quien
además es cliente ve la pestaña «Portal de proveedores» en su cuenta.

**Vista previa:** un gerente de Compras abre `/mi-cuenta/proveedor?proveedor=<id del contacto>`
y ve el portal como ese proveedor (solo lectura: no puede subir facturas).

## Seguridad

Igual que «Mi cuenta» del cliente: todo se lee con `sudo()` y siempre filtrado
por `partner_id child_of` la empresa del usuario. Una orden o factura de otro
proveedor responde 404. Los usuarios de portal no tienen permisos sobre los
modelos del módulo; solo los toca el controlador.

## Subir factura

El proveedor sube XML y PDF contra una orden confirmada. Al momento:

1. Se lee el CFDI (3.3 o 4.0, sin resolver entidades). Debe ser de tipo ingreso y estar timbrado.
2. **Se rechaza** si el receptor no es la compañía, si el emisor no es el RFC del
   proveedor, si el folio fiscal ya se subió, o si el CFDI aparece **cancelado en
   el SAT** (`sat.cfdi` de `quimibond_sat`, si está instalado).
3. **Se recibe con aviso** si la moneda no es la de la orden, si aún no hay
   recepción o si el importe antes de impuestos no coincide con lo recibido y no
   facturado (tolerancia de $1).

Queda una **Factura recibida** (`qb.proveedor.factura`, Compras → Portal de
proveedores → Facturas recibidas) con una actividad para Cuentas por pagar.
**No crea asientos.** Desde ahí:

- **Crear factura de proveedor**: usa el botón nativo de la orden (borrador),
  le pone referencia y fecha y le adjunta el XML y el PDF.
- **Buscar factura ya registrada**: liga la que tenga el mismo folio fiscal. Un
  cron lo hace solo cada hora.
- **Rechazar** con motivo: el proveedor lo ve en el portal, recibe un correo y
  puede subir otra.

Mientras una factura de la orden está en revisión no se puede subir otra.

## Estado de pago

| Lo que ve el proveedor | Cuándo |
|---|---|
| En revisión | Factura subida sin registrar, factura en borrador, o no liberada para pago (`release_to_pay` de la conciliación a tres vías, si existe) |
| Pago programado + fecha | Publicada y liberada. La fecha es el vencimiento, o el primer día de pago en o después de él |
| En proceso de pago | Ya venció y no hay día de pago fijo: no se promete fecha |
| Pago parcial / Pagada | Según los pagos conciliados |

## Alta en línea

`/proveedores/alta` pide datos de la empresa, contacto, cuenta bancaria y
documentos (constancia fiscal, opinión de cumplimiento, carátula bancaria;
certificados opcionales) en PDF, JPG o PNG de hasta 5 MB. Valida RFC, correo y
el dígito verificador de la CLABE; trae campo trampa contra robots y tope de 30
solicitudes por hora.

Llega a Compras → Portal de proveedores → **Altas de proveedores**
(`qb.proveedor.alta`). **Aprobar** crea el contacto (o liga el que ya existe
con ese RFC). La cuenta bancaria **no** se copia sola a la ficha: la captura
Cuentas por pagar después de validar la carátula. El acceso al portal se da
desde el contacto, con la acción nativa.

Los requisitos y los compromisos del proveedor son texto fijo en
`controllers/alta.py`.

**Inglés (desde 1.1.0).** Con la casilla «Versión en inglés» del sitio,
`/en/proveedores` y el alta salen en inglés: los textos de las plantillas y
los de `controllers/alta.py` (requisitos, compromisos, avisos del formulario)
llevan su traducción en `i18n/en_CA.po`. Un texto nuevo va en español en el
código y con su inglés en el `.po`. El portal con sesión
(`/mi-cuenta/proveedor`) sigue solo en español.

## Configuración

En Sitio web → Configuración → Sitios web:

- **Recibe las facturas de proveedores**: quién recibe la actividad por cada factura (vacío: el comprador de la orden).
- **Recibe las altas de proveedores**.
- **Día de pago a proveedores**: si se paga un día fijo de la semana.

## Pruebas

Corren en CI (Community): `--test-tags /website_quimibond_proveedores`. Lo que
depende de Enterprise (`release_to_pay`, `l10n_mx_edi_cfdi_uuid`) y de
`quimibond_sat` se lee solo si el campo o el modelo existe.
