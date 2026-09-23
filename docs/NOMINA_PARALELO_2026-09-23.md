# Nómina — paralelo Odoo vs NOI, corridas piloto de septiembre 2026

Comparación de las tres corridas piloto de Odoo (en borrador, nunca
validadas ni timbradas) contra los CFDI que NOI sí timbró, leídos de la
memoria de Supabase (`email_attachments`, XML resumidos del buzón de RH).
Cruce por **RFC + periodo**; tolerancia 5 centavos en cada concepto y 10
centavos en el neto.

| Corrida Odoo | Periodo | Recibos Odoo | XML de NOI encontrados | Comparados | Gravado | ISR | IMSS | Neto |
|---|---|---|---|---|---|---|---|---|
| 117 Semana 38 | 7 al 13 sep | 87 | 87 | 85 | 85/85 | 85/85 | 85/85 | 85/85 |
| 118 Quincena 18 Toluca | 1 al 15 sep | 49 | 25 (+12 de CDMX) | 25 | 24/25 | 23/25 | 25/25 | 24/25 |
| 119 Semana 39 | 14 al 20 sep | 89 | 82 | 83 | 83/83 | 83/83 | 83/83 | 83/83 |

Sumas de los recibos comparados (Odoo vs NOI):

| Corrida | ISR Odoo | ISR NOI | Neto Odoo | Neto NOI |
|---|---|---|---|---|
| Semana 38 | 23,413.51 | 23,413.67 | 295,987.13 | 295,987.30 |
| Quincena 18 Toluca | 39,639.05 | 39,639.83 | 237,059.19 | 237,060.60 |
| Semana 39 | 36,358.00 | 36,358.69 | 271,114.04 | 271,113.68 |

## Cómo se comparó

- **Gravado:** `gross_wage` de Odoo contra la suma de percepciones gravadas
  del XML. Coincide en todos; las diferencias que salen a simple vista
  contra el total de percepciones del XML son la parte exenta (horas extra
  al 50 %, fondo de ahorro patrón, vales) que el resumen no lista aparte.
- **ISR:** NOI reporta el ISR ya neto de subsidio al empleo. En Odoo son dos
  líneas (`ISR` y `SUBSIDY`); se compara `ISR − SUBSIDY`. 38 recibos llevan
  subsidio (123.34 semanal, 267.82 quincenal) y coinciden al centavo.
- **IMSS:** `IMSS_EMPLOYEE_TOTAL` contra la deducción IMSS del XML. 193/193.
- **Neto:** `net_wage` contra el neto del XML.

## Diferencias reales (2 de 193)

| Recibo | Empleado | Diferencia | Causa |
|---|---|---|---|
| 4454, quincena 18 | Jessica Francisco Sánchez (244) | Neto −1.59 | `INFONAVIT` Odoo 914.98 vs NOI 913.33 (+1.65); ISR −0.08 de redondeo. Revisar cómo está capturado el crédito (porcentaje, VSM o cuota fija) contra el aviso del INFONAVIT |
| 4452, quincena 18 | Manuel Antonio Juárez Matías (99) | Gravado −0.06, ISR −0.11, neto +0.06 | Redondeo de horas extra (Odoo 1,504.29 vs NOI 1,504.30) |

Todo lo demás está a ±5 centavos de redondeo.

## Lo que NO se pudo comparar

- **24 recibos de la quincena 18 de Toluca** no tienen XML en la memoria de
  Supabase (Escandón, Manríquez ×2, Huerta, Cárdenas, Martínez Aguilar,
  Romero Martínez, Villordo, Jiménez Bartolo, Martínez González, Tablas,
  Esquivel Isidro, Domínguez Castillo, González Domínguez, Zavala,
  Velázquez Flores, Bernal Quintana, Álvarez Cruz, Valdés Ocampo, González
  Pérez, Vargas Figueroa, Rodríguez Puerta, Terrón, González Ruiz). El
  buzón de RH sólo mandó 37 de los 61 XML de ese periodo; pedirle a RH la
  carpeta completa de la quincena 18 o volver a correr la extracción.
- **Quincena 18 de CDMX** (12 XML en NOI): no hay corrida piloto en Odoo.
  La corrida 118 es sólo Toluca; falta armar la de CDMX (ver
  `HANDOFF_NOMINA_QUINCENA_CDMX_2026-09-18.md`).
- **7 recibos semanales sin XML:** Pliego Cabrera (S38), Carmona Medina
  (S38 y S39), Nava Hernández, Mejía Morales, Agustín Crescencio, Fuentes
  Fernández y Ramírez López (S39, estos dos en cero en Odoo). Faltan en la
  memoria, no necesariamente en NOI.
- **2 XML sin recibo en Odoo:** Uriel Martínez López (S38, finiquito, baja)
  y Gael García Ramírez (S38; en Odoo entró hasta la S39).

## Conclusión

El cálculo de Odoo reproduce a NOI en los 193 recibos comparados: ISR,
IMSS, subsidio, fondo de ahorro, cuotas sindicales, FONACOT e INFONAVIT
salen iguales, con una sola diferencia real de 1.65 pesos en un crédito
INFONAVIT. Lo que sigue para timbrar es lo que no depende del cálculo:
timbrado de prueba en staging, la quincena de CDMX, y aguinaldo, finiquitos
y vacaciones.
