# qb_pedimento — Pedimentos

Un expediente por pedimento, de importación o exportación, dentro de Odoo. Sustituye
a los Excel F-P-A-16-05 IMPORTACIONES y F-P-A-16-06 EXPORTACIONES que lleva Logística.
El diseño y la evidencia de cada decisión están en
[`docs/superpowers/specs/2026-10-23-pedimentos-design.md`](../../docs/superpowers/specs/2026-10-23-pedimentos-design.md).

## Qué guarda

| Pestaña | Contenido |
|---|---|
| Encabezado | Número (`26 16 3774 6000206`, validado: año, aduana, patente, consecutivo), clave (A1, V1, V5, R1), aduana y patente (salen del número), proveedor o cliente, agencia, referencia de la agencia, fechas de cada etapa. |
| Documentos | Checklist según la operación: importación, exportación o virtual. Cada documento se cumple con su archivo o con «No aplica». |
| Ligado a | Compras, ventas, recepciones o entregas, facturas de la mercancía y facturas de la agencia (muchos a muchos). |
| Contribuciones y partidas | Valor comercial y en aduana, tipo de cambio; DTA, IGI, IVA, PRV, IVA/PRV y multas; partidas con fracción arancelaria y tasa de IGI. |
| Cuenta de gastos y garantías | Total de la cuenta de gastos, anticipo y saldo (negativo = a favor de Quimibond, como en el Excel); garantías navieras, depósitos y cuenta aduanera hasta que se devuelven. |
| Embarque | Incoterm, BL o guía, booking, contenedores, COVE, DODA y Entry. |

## Etapas

`Preparación → En tránsito → En aduana → Despachado → Entregado / Cruzado → Cerrado`.

- **Despachado** exige el número de pedimento.
- **Cerrado** exige además la fecha de entrega o cruce, todos los documentos obligatorios y la cuenta de gastos (el total o la factura de la agencia).
- «Días hábiles a cierre» mide el indicador C2-05 (meta: 5).

## Lo que hace solo

- **Copia el número a la localización mexicana** cuando `l10n_mx_edi` está instalado. Va a
  `stock.picking.l10n_mx_edi_pedimento_number` y a `stock.landed.cost.l10n_mx_edi_customs_number`
  en borrador, con dos espacios. De ahí Odoo lo lleva a los lotes y al CFDI.
- **Crea el costo en destino en borrador** («Crear costo en destino», solo importación). Pone IGI, DTA,
  PRV y los gastos sin IVA sobre las recepciones validadas. Contabilidad lo revisa y lo valida. Los
  productos se configuran en Pedimentos → Configuración → Ajustes.
- **Avisa cada día con una actividad al responsable**, y no repite el aviso mientras siga abierto:
  - despachado sin cruce ni llegada en 3 días hábiles (C2.31);
  - entregado sin cerrar en 5 días hábiles (C2.32);
  - garantía sin devolver a los 60 días.

  Los plazos se cambian en Ajustes. Al cerrar el expediente o devolver la garantía, el aviso se marca hecho.
- **Facturas sin pedimento** (Reportes): facturas de venta publicadas con complemento de comercio exterior,
  o a un cliente marcado «Venta con pedimento virtual» (ficha del contacto, Ventas y compras), que no
  tienen pedimento ligado. Es el cruce que pidió la auditoría del Edomex.
- **Arancel por fracción** (Reportes): valor en aduana e IGI por fracción y año.

Desde una compra, una recepción o entrega, una venta o una factura, el botón **Pedimentos** abre los
expedientes ligados. También crea uno nuevo con el documento ya ligado.

## Permisos

Privilegio **Pedimentos**:

- **Usuario** (Logística, Contabilidad, CxC): captura expedientes y garantías.
- **Administrador**: configura catálogos y plazos, reabre y borra.

## Fuera de esta versión

- Leer el PDF del pedimento desde el correo «PEDIMENTO PAGADO» (la memoria ya extrae su texto).
- Señales a Supabase.
- Anexo 24/30: Quimibond no tiene IMMEX propio.
