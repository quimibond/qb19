# Nómina — paralelo Odoo vs NOI, corridas piloto de septiembre 2026

Comparación de las tres corridas piloto de Odoo (en borrador, nunca
validadas ni timbradas) contra los CFDI que NOI sí timbró, leídos de la
memoria de Supabase (`email_attachments`, XML resumidos del buzón de RH).
Cruce por **RFC + periodo**; tolerancia 5 centavos en cada concepto y 10
centavos en el neto.

| Corrida Odoo | Periodo | Recibos Odoo | XML de NOI encontrados | Comparados | Gravado | ISR | IMSS | Neto |
|---|---|---|---|---|---|---|---|---|
| 117 Semana 38 | 7 al 13 sep | 87 | 88 | 86 | 86/86 | 86/86 | 86/86 | 86/86 |
| 118 Quincena 18 Toluca | 1 al 15 sep | 49 | 26 (+12 de CDMX) | 26 | 25/26 | 24/26 | 26/26 | 25/26 |
| 119 Semana 39 | 14 al 20 sep | 89 | 86 | 87 | 87/87 | 87/87 | 87/87 | 87/87 |

Sumas de los recibos comparados (Odoo vs NOI):

| Corrida | ISR Odoo | ISR NOI | Neto Odoo | Neto NOI |
|---|---|---|---|---|
| Semana 38 | 23,634.85 | 23,635.00 | 298,785.65 | 298,785.85 |
| Quincena 18 Toluca | 40,285.28 | 40,286.08 | 242,836.20 | 242,837.61 |
| Semana 39 | 37,714.47 | 37,715.19 | 283,316.63 | 283,316.27 |

Segunda pasada (misma tarde): la memoria tenía 6 XML más que la primera
lectura no había tomado (UUID en minúsculas): Escandón (Q18), Pliego
Cabrera (S38), Mejía Morales, Nava Hernández, Agustín Crescencio y Modesto
Medina (S39). Los 6 coinciden al centavo; ya están en las cifras de arriba.

## Cómo se comparó

- **Gravado:** `gross_wage` de Odoo contra la suma de percepciones gravadas
  del XML. Coincide en todos; las diferencias que salen a simple vista
  contra el total de percepciones del XML son la parte exenta (horas extra
  al 50 %, fondo de ahorro patrón, vales) que el resumen no lista aparte.
- **ISR:** NOI reporta el ISR ya neto de subsidio al empleo. En Odoo son dos
  líneas (`ISR` y `SUBSIDY`); se compara `ISR − SUBSIDY`. 38 recibos llevan
  subsidio (123.34 semanal, 267.82 quincenal) y coinciden al centavo.
- **IMSS:** `IMSS_EMPLOYEE_TOTAL` contra la deducción IMSS del XML. 199/199.
- **Neto:** `net_wage` contra el neto del XML.

## Diferencias reales (2 de 199)

| Recibo | Empleado | Diferencia | Causa |
|---|---|---|---|
| 4454, quincena 18 | Jessica Francisco Sánchez (244) | Neto −1.59 | `INFONAVIT` Odoo 914.98 vs NOI 913.33 (+1.65); ISR −0.08 de redondeo. El crédito está capturado en el contrato como cuota fija de 3,659.92 (Odoo la reparte entre 4 quincenas: 914.98). NOI descuenta 913.33, que corresponde a una cuota de 3,653.32: 6.60 pesos menos. Cotejar el importe contra el aviso de retención del INFONAVIT y corregir el que esté mal |
| 4452, quincena 18 | Manuel Antonio Juárez Matías (99) | Gravado −0.06, ISR −0.11, neto +0.06 | Redondeo de horas extra (Odoo 1,504.29 vs NOI 1,504.30) |

Todo lo demás está a ±5 centavos de redondeo.

## Lo que NO se pudo comparar

- **23 recibos de la quincena 18 de Toluca** no tienen XML en la memoria de
  Supabase (Manríquez ×2, Huerta, Cárdenas, Martínez Aguilar, Romero
  Martínez, Villordo, Jiménez Bartolo, Martínez González, Tablas, Esquivel
  Isidro, Domínguez Castillo, González Domínguez, Zavala, Velázquez Flores,
  Bernal Quintana, Álvarez Cruz, Valdés Ocampo, González Pérez, Vargas
  Figueroa, Rodríguez Puerta, Terrón, González Ruiz). Los XML llegan a la
  memoria porque NOI manda desde `rhmexico@` un correo por trabajador a su
  correo personal ("Envío del Comprobante Fiscal Digital: NOM_NOMINA…") y
  el buzón guarda el enviado; a estos 23 no se les mandó (sin correo en
  NOI, probablemente). Pedirle a RH que exporte los XML de la quincena 18
  desde NOI, no que reenvíe correos.
- **Quincena 18 de CDMX** (12 XML en NOI): no hay corrida piloto en Odoo.
  La corrida 118 es sólo Toluca; falta armar la de CDMX (ver
  `HANDOFF_NOMINA_QUINCENA_CDMX_2026-09-18.md`).
- **4 recibos semanales sin XML:** Carmona Medina (S38 y S39), Fuentes
  Fernández y Ramírez López (S39, estos dos en cero en Odoo). Faltan en la
  memoria, no necesariamente en NOI.
- **2 XML sin recibo en Odoo:** Uriel Martínez López (S38, finiquito, baja)
  y Gael García Ramírez (S38; en Odoo entró hasta la S39).

## Quincena 19 de Toluca (2026-09-29): ISR mensual y H_SENC

Con `quimibond_nomina` 19.0.1.7.0 en producción (ISR sobre el acumulado del
mes en la última nómina del mes; `H_SENC` con el divisor 15.2083) y las
corridas 122 (Q18) y 121 (Q19) recalculadas contra el reporte de NOI
`NOMINA 19 QNTL.xlsx`:

| | Recibos | Dentro de 2 pesos |
|---|---|---|
| 122 Quincena 18 Toluca | 48 con XML | 48 |
| 121 Quincena 19 Toluca | 48 con reporte | 41 |

Los 7 fuera de rango de la quincena 19 son datos, no cálculo: 276 Vallejo,
275 Cruz Apolinar, 471 Esquivel, 99 Juárez Matías y 498 Velázquez tienen el
gravable distinto a NOI (horas extra u otras percepciones no capturadas en
Odoo); 499 Bernal y 270 Escandón tienen el mismo gravable pero deducciones o
percepciones exentas distintas (Escandón: prima dominical exenta 98.33 que
Odoo no trae). Jessica Francisco (244) queda en 6,622.08 vs 6,623.82: el
crédito INFONAVIT. Cynthia Santana (11) 11,908.71 vs 11,908.78; Villordo
(464) 9,707.31 vs 9,707.35; Mairani Hernández (345) 6,104.34 vs 6,104.38.

El parámetro `quimibond_nomina.isr_mensual_incluye_borrador = 1` está puesto
para que las pilotos (en borrador) acumulen; **quitarlo antes de la primera
nómina real**. La corrida 120 (semana 40, última de septiembre) no se
recalculó: si se recalcula, el ajuste mensual tomará las semanas 37 a 39 en
borrador, incluida la corrida 116 (semana 37, superada), así que primero
cancelar la 116.

## Conclusión

El cálculo de Odoo reproduce a NOI en los 199 recibos comparados: ISR,
IMSS, subsidio, fondo de ahorro, cuotas sindicales, FONACOT e INFONAVIT
salen iguales, con una sola diferencia real de 1.65 pesos en un crédito
INFONAVIT. Lo que sigue para timbrar es lo que no depende del cálculo:
timbrado de prueba en staging, la quincena de CDMX, y aguinaldo, finiquitos
y vacaciones.
