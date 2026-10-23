# Pedimentos en Odoo: diseño con evidencia

Pedido de Dirección General (Jose), 2026-10-23: «toda la parte de pedimentos
para llevar el control en Odoo», con cada decisión respaldada en procedimientos,
correos y datos. Módulo: `addons/qb_pedimento`.

Fuentes (todas de solo lectura, 2026-10-23):

- **SGI en producción**: actividades C2.28–C2.32 y C2.39 (proceso C2 «Pedido a
  entrega», etapa E. Exportación), S1.14 y S1.06 (proceso S1 «Compra a pago»),
  indicadores C2-05 y EX-17, procedimientos P-A16 Logística (doc 3520) y P-A13
  Contador General (doc 3516), formatos F-P-A-16-05 IMPORTACIONES (doc 3772),
  F-P-A-16-06 EXPORTACIONES (doc 3773) y F-P-A13-02 RESUMEN DE IMPORTACIONES
  (doc 3726).
- **Memoria de correo** (Supabase, 2025-10 a 2026-10): buzones de Dario
  Manriquez (logistica@), Irma Luna, Aurelio Álvarez, Sandra Dávila (cxcobrar@)
  y las agencias (SISA, Space Aduanas, Enrique Reyes SC, Jorge Reyes Hidalgo y
  Sucesores SC, DHL, UPS). Ids de correo (`emails.id`) y de adjunto
  (`email_attachments.id`) citados abajo.
- **Odoo producción por MCP**: compras, ventas, costos en destino, cuentas y
  campos nativos de la localización mexicana.

## 1. Cómo se opera hoy

### Quién

| Persona | Qué hace | Evidencia |
|---|---|---|
| Dario Manriquez (Logística, puesto 196) | Ejecuta C2.28–C2.32 y S1.14. Sigue el embarque físico y avisa a almacén con ETA y número de pedimento. Lleva los Excel de importaciones y exportaciones. Manda cada mes a la contadora externa los pedimentos de exportación y las facturas de Space que faltan. | SGI; correos 80430, 103386 y 102432; adjunto 488612 |
| Irma Luna (Contador General, puesto 228, jefa de Dario) | Según P-A13, «coordina las importaciones». Da de alta los encargos conferidos, paga anticipos y garantías, firma las cartas (encomienda, 3.1.8, VUCEM) y entrega pedimentos a auditores y abogados. | P-A13; correos 81762, 102453 y 526310 |
| Aurelio Álvarez (Contabilidad) | Persigue las cuentas de gastos y lleva el auxiliar «DEUDA AGENTE ADUANAL» de los depósitos en garantía. | Correo 93573; adjunto 205695 |
| Sandra Dávila (CxC) | Opera el cierre mensual de los pedimentos virtuales V1 con Space. | Correos 69747 y 259996 |

### Con quién

**Agencias:**

- SISA (Servicio Internacional), referencias 83xxx. Desde marzo de 2026 se
  paga directo a sus agencias por aduana: Enrique Reyes SC (patente 1740,
  Veracruz) y Jorge Reyes Hidalgo y Sucesores SC (patente 3774: Manzanillo,
  Laredo, Altamira, AIFA y AICM). Correo 102369.
- Space Aduanas (Especialistas en Aduanas y Comercio Exterior SC), con las
  patentes 1612 y 3066. Hace los V1 y los A1 de exportación.
- Villarreal y Villarreal (patente 3457).
- DHL (patente 3547) y UPS (patentes 3407 y 1864) para courier.

**Una agencia opera con varias patentes.** Por eso la patente es un dato del
pedimento, no del proveedor.

### Volumen

- **Importaciones:** de 1 a 3 pedimentos al mes, más unos 6 de courier al año.
  Hay 46 compras al extranjero en 2025-2026, en USD y EUR. El Excel de Dario
  tiene 13 filas de enero a septiembre de 2026.
- **Exportaciones:** de 5 a 6 pedimentos A1 al mes, más el cierre mensual de
  V1 de cada cliente IMMEX: Lear, NAPS/Shawmut e Inoac. La hoja de Dario de
  2025 suma 63 pedimentos: 28 A1 y 35 V1. Adjuntos 277078 y 361586.

### Régimen

Quimibond importa en definitiva (clave A1, régimen IMD) y vende a empresas
IMMEX con pedimento virtual V1, como «proveedor nacional a empresa IMMEX» (RGCE
5.2.5 fr. II, correo 69747). En la evidencia **no aparece ningún programa IMMEX
propio**, así que no se diseña control de Anexo 24/30.

### Dónde vive hoy el control

**Fuera de Odoo**, en Excel:

- F-P-A-16-05 IMPORTACIONES: unas 100 columnas, una fila por cuenta de gastos.
- F-P-A-16-06 EXPORTACIONES.

En Odoo, el número de pedimento solo aparece como texto en `ref` («PED:26 16
3774 6000675/…», 8 facturas en 2026). Los campos nativos casi no se usan:

| Campo nativo | Uso hoy |
|---|---|
| `stock.picking.l10n_mx_edi_pedimento_number` | 0 registros, aunque C2.29 manda capturarlo ahí |
| `stock.landed.cost.l10n_mx_edi_customs_number` | 1 registro (LC/2026/0254) |
| `account.move.line.l10n_mx_edi_customs_number` | 0 registros |

### Dolores con evidencia

| # | Dolor | Evidencia |
|---|---|---|
| D1 | Garantías navieras y depósitos que no regresan. SISA debía **$427,359** al 2026-09-15, y los depósitos se acumulan en la cuenta 184.03.0004. | Hilo 73903; adjunto 205695 |
| D2 | Contabilidad atrasada por las cuentas de gastos y las facturas de agencia que faltan. | Correos 93573 y 103386 |
| D3 | Auditoría de la Secretaría de Finanzas del Edomex: «facturación a clientes del extranjero vs pedimentos virtuales, jun-2021 a mar-2026». | Correo 126240; adjunto 338835 |
| D4 | Almacenajes y demoras: $150,750 en el contenedor DFSU6973437, condonación negada. | Hilo 94628 |
| D5 | Documentos inconsistentes (factura contra orden de compra, cartas sin firmar) y rectificaciones R1 por errores de la agencia. | Hilo 33980; correo 102733 |
| D6 | Dirección pidió la fracción y el % de arancel pagado de 2025-2026, y no hay de dónde sacarlo. | Correo 342305 |
| D7 | El indicador C2-05 («expedientes de exportación cerrados en 5 días hábiles») no se puede medir. | `sgi.indicator` 119, en prueba, sin datos |

## 2. Decisiones de diseño

| # | Decisión | Por qué (evidencia) |
|---|---|---|
| 1 | **Un registro = un pedimento** (`qb.pedimento`), el expediente completo. | Cada fila de F-P-A-16-05 y F-P-A-16-06 es un pedimento con su cuenta de gastos. Las agencias titulan sus correos «PEDIMENTO PAGADO 3774-6000417 // BL … // factura». |
| 2 | **Relaciones muchos a muchos** con compras, recepciones o entregas, y facturas. | LC/2026/0254: un pedimento para 3 recepciones. Icomatex: una compra en 6 contenedores. Un V1 mensual cubre las facturas de todo el mes. |
| 3 | **El número se guarda como lo escribe Dario** («26 16 3774 6000206») y se valida como el SAT: año, aduana, patente, consecutivo; el primer dígito del consecutivo es el último dígito del año. | Excel de Dario; formato del Anexo 22. Aduana y patente se calculan del número. |
| 4 | **El número se copia a los campos nativos** en el formato de la localización, con dos espacios: la entrega o recepción (`l10n_mx_edi_pedimento_number`) y el costo en destino (`l10n_mx_edi_customs_number`). | C2.29 manda capturarlo en la entrega. LC/2026/0254 trae «26  16  3774  6000417». La localización lo propaga a los lotes y al nodo InformacionAduanera del CFDI de venta de primera mano. Si `l10n_mx_edi` no está instalado (CI), no se copia y no falla. |
| 5 | **Número opcional hasta «Despachado».** Antes se identifica por la referencia de la agencia (83272, SIN-0661/26). | Hay borrador de pedimento, solicitud de gastos y anticipo antes del pago (correos 216951 y 101664). |
| 6 | **Etapas**: Preparación, En tránsito, En aduana, Despachado, Entregado / Cruzado, Cerrado (y Cancelado). Cada cambio guarda su fecha. | Fechas del Excel: arribo, liberación y entrega en planta. C2.31 pide anotar fecha y hora del cruce. C2.32 cierra con el pedimento pagado. C2-05 mide de «entregado» a «cerrado». |
| 7 | **Cerrar exige**: número, documentos obligatorios completos y la cuenta de gastos capturada. | C2.32 y S1.14: «adjuntar pedimento y cuenta de gastos». La queja de Contabilidad, D2. |
| 8 | **Checklist de documentos por tipo**, sembrado de las columnas del Excel (un juego para importación y otro para exportación). Cada renglón se cumple con su adjunto o con «No aplica». | Columnas de checklist de F-P-A-16-05 (pedimento, COVE, factura, DODA, BL, origen, packing, especificaciones, carta encomienda, 3.1.8, MV, pago de derechos) y de F-P-A-16-06 (carta porte, BL, Entry, DODA, acuse de digitalización, e-documents, certificado de origen, CSF). |
| 9 | **Contribuciones como campos fijos**: DTA, IGI, IVA, PRV, IVA/PRV, multas y recargos. Lo depositado en cuenta aduanera de garantía (forma de pago «otros») va como garantía (decisión 12), no como contribución. | Son las columnas del Excel y del pedimento 26 16 3774 6000206 (adjunto 271227): 344,760 de contribuciones en efectivo más 14,225 en «otros» (cuenta aduanera por precio estimado, correo 81248). |
| 10 | **Partidas** con fracción arancelaria, cantidad, valor y tasa de IGI. La fracción viene por omisión del producto. | D6. 112 productos ya tienen `l10n_mx_edi_tariff_fraction_id`. |
| 11 | **Cuenta de gastos**: referencia, facturas de la agencia ligadas, total, anticipo y fecha. El saldo se calcula, y un saldo a favor se sigue hasta que se devuelve. | Columnas finales del Excel: total 543,084.01 contra anticipo 587,000, saldo -43,915.99. Devolución del saldo a favor en el correo 316442. |
| 12 | **Garantías como renglones propios** (naviera, cuenta aduanera, depósito): beneficiario, importe, moneda, fechas de pago y devolución, y estado. Lista propia con «sin devolver». | D1. Es el dolor de más dinero con evidencia directa. |
| 13 | **Rectificación**: el pedimento R1 apunta al original. | Correo 102733. |
| 14 | **«Cliente con pedimento virtual»** en el contacto. Las facturas de venta a esos clientes, o con comercio exterior, sin pedimento ligado, quedan en un filtro. | D3: la auditoría cruza exactamente eso. Los V1 van a clientes nacionales IMMEX (Lear Mexican Trim, NAPS), así que `l10n_mx_edi_external_trade` no basta. |
| 15 | **Avisos con actividades al responsable** (cron diario, sin duplicar). Tres casos: despachado sin cruzar en 3 días hábiles; entregado sin cerrar en 5 días hábiles; garantía sin devolver en 60 días. | C2.31 («si no cruza en 3 días hábiles… avisar el mismo día») y C2.32 («si el agente no lo manda en 5 días hábiles…»). Los 60 días son una propuesta (hilo 73903: meses sin devolución) y se configuran por compañía. |
| 16 | **Botones de pedimentos** en compra, entrega o recepción y factura. | S1.14: «adjuntar pedimento y cuenta de gastos a la orden de compra». |
| 17 | **Sin dependencias Enterprise**: `purchase_stock`, `sale_stock` y `stock_landed_costs`. Lo de `l10n_mx_edi` se usa solo si está instalado. | Así el CI lo instala y prueba (regla del repo). |
| 18 | **No se crean asientos.** El costo en destino se crea en **borrador** desde el pedimento y Contabilidad lo valida. | Contabilidad registra hoy IGI, DTA y PRV en la factura del proveedor (FACTU/2026/07/128) y en el diario 355. Cambiar eso le toca a Irma, no al módulo. |
| 19 | **Permisos**: Usuario (Logística, Contabilidad y CxC capturan) y Administrador (configura y borra). | Capturan Dario, Irma, Aurelio y Sandra. |

### Lo que se dejó fuera, a propósito

- **Anexo 24/30, control de saldos IMMEX:** no hay programa IMMEX propio (correo 69747).
- **Previo y semáforo:** no aparecen en la operación propia (sin evidencia). Si se
  necesitan, son una nota y una fecha más.
- **Leer el PDF o el XML del pedimento automáticamente:** fase 2. Las agencias mandan
  NORMAL, SIMPDEC y HOJACALCULO en PDF (adjunto 271227), y la memoria ya extrae su
  texto; se puede proponer el expediente desde el correo «PEDIMENTO PAGADO».
- **Señales a Supabase** (garantías sin devolver, expedientes sin cerrar): fase 3,
  con el patrón `@senal`.

## 3. Huecos que debe confirmar Comercio exterior

1. ¿El Excel F-P-A-16-05 sigue siendo el control, o ya se deja de llenar al
   arrancar el módulo? Se propone dejarlo y migrar las 13 filas de 2026.
2. ¿Quién es el responsable por defecto del expediente: Dario en todo, o Irma
   en las importaciones? Se propone Dario.
3. ¿Los 60 días para avisar de una garantía sin devolver?
4. Nombres de aduana: se siembran solo los códigos que aparecen en los números
   de pedimento de los correos (2025-10 a 2026-10), cada uno cotejado con la
   aduana que la agencia nombra:

   | Código | Aduana | Cotejo | Correos |
   |---|---|---|---|
   | 07 | Cd. Juárez | Lear, patentes 3066 y 3459 | 86 |
   | 16 | Manzanillo | patente 3774 | 71 |
   | 43 | Veracruz | patente 1740, Enrique Reyes | 12 |
   | 47 | AICM | patentes 3774 y 3547 | 11 |
   | 85 | AIFA | patentes 3547 y 3774 | 9 |
   | 24 | Nuevo Laredo | MW Canada, adjunto 79936 | — |

   Un código desconocido (p.ej. el 39 del V1 de NAPS) se da de alta solo, con
   el nombre por llenar.
