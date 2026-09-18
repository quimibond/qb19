# Handoff — nómina: cierre del CFDI y quincena de CDMX

**Para:** la siguiente sesión de Claude Code sobre la nómina de Quimibond.
**Fecha:** 18 de septiembre de 2026.
**Sustituye a:** `PROMPT_horas_extra_cfdi_v2.md`. Esa tarea ya está cerrada; aquí
va el estado verificado y lo que sigue.

---

## 1. Lo que quedó bien (verificado contra el XML renderizado, no contra mocks)

`quimibond_nomina` **19.0.1.3.0** está en `main` e instalado en la staging de
`main` (`https://quimibond-qb19-main-38167860.dev.odoo.com`, neutralizada, se
borra el 16-oct-2026). Se comprobó rendereando el CFDI del recibo **4358**
(Genaro Gerónimo Bartolo, semana 38, 9 horas dobles), sin firmar:

| Pieza | Sale | NOI |
|---|---|---|
| `nomina12:Emisor/@RegistroPatronal` | el del contrato (`hr.version.l10n_mx_employer_registration`), la compañía como respaldo | — |
| `@SalarioDiarioIntegrado` / `@SalarioBaseCotApor` | **889.43 / 889.43** (líneas `INT_DAY_WAGE_BASE` / `INT_DAY_WAGE`, por código) | 889.43 |
| `@ClaveEntFed` | **MEX** (estado de la dirección laboral) | MEX |
| `@NumEmpleado` | **325** (id de Odoo; referencia de empleado vacía) | 1 (otra numeración) |
| Percepción 019 | **una sola**, `ImporteGravado=581.77` `ImporteExento=581.77` (antes salían dos, P19_2 exenta y P19 gravada) | una |
| `nomina12:HorasExtra` | `Dias="3" TipoHoras="01" HorasExtra="9" ImportePagado="1163.54"` dentro de la 019 | igual |
| Conceptos | en español (releídos de `l10n.mx.concept` en `es_MX`) | español |

El neto de la corrida 117 sigue en **302,757.31**; el centinela de reglas está
en verde con `SUBSIDY` e `INT_DAY_WAGE`. Los cuatro PR: #310 (módulo), #311,
#313 (correcciones de instalación en Odoo 19), #314 (diccionario del CFDI,
`ClaveEntFed`, `NumEmpleado`), #315 (fusión de las 019, nodo, conceptos).

Detalle de cada parche, criterio de `Dias` (142 nodos de NOI) y verificación:
`addons/quimibond_nomina/README.md`. **No toques el cálculo ni las reglas.**

### Lo que sigue siendo dato, no código (para RH)

- `TipoJornada`: Odoo manda `01` y NOI `03`. Es *Tipo de jornada* del
  contrato (`l10n_mx_shift_type`); ponerlo en los contratos de planta.
- `CuentaBancaria`: Odoo `11323066620`, NOI la CLABE `012180011323066628`.
  Con CLABE de 18 dígitos el SAT no exige `Banco` y es la que ya aceptó: va en
  la cuenta bancaria del empleado.
- `NumEmpleado`: capturar la *Referencia de empleado* con el número de NOI
  (hoy sale el id de Odoo). Un solo empleado la tiene (id 287, "83").
- `Antigüedad`: Odoo `P1365W`, NOI `P1367W`; definir la fecha de ingreso.

---

## 2. La tarea: armar la quincena de CDMX en Odoo

Es el **único paralelo que nunca se ha corrido**. Semanal (37, 38, 39) y
quincenal Toluca (17, 18) ya cuadran con NOI al centavo; la quincena de CDMX
no se ha armado ni una vez. De paso prueba con datos reales dos cosas del
módulo que hasta hoy sólo tienen test: **`Dias="6"`** (quincenal con horas
extra) y el **registro patronal por contrato** (`Y6087828106`, el de México;
154 timbres en NOI contra 1,916 de Toluca `C-675994510-1`).

### Qué periodo

La **quincena 18 (1–15 de septiembre)**: NOI ya la timbró para CDMX, así que
hay contra qué comparar hoy. Después, la 19 (16–30 sep), que cierra el
criterio de dos periodos seguidos en la quincenal.

### Quiénes (verificado en producción por MCP, 18-sep-2026)

Hay **71** empleados activos con calendario de pago quincenal
(`hr.version.schedule_pay = 'bi-weekly'`, compañía 1). **49** fueron a la
quincena 18 de Toluca (corrida 118). Los **22** restantes, por exclusión:

**Con contrato vigente y sueldo — la quincena de CDMX candidata (14):**

| id | Nombre | Puesto | Sueldo (quincenal en el contrato) |
|---|---|---|---|
| 2 | José Jaime Mizrahi | Director de finanzas y administración | 52,000.00 |
| 6 | Jorge Manuel Eduardo Ortiz Velázquez | Director de operaciones | 42,523.78 |
| 8 | Irma Luna Ángeles | Contador general | 15,136.21 |
| 13 | Aurelio Álvarez García | Auditor interno contable | 10,633.80 |
| 19 | Lorena Mondragón Reinoso | Responsable de nóminas | 8,639.23 |
| 539 | Sandra Dávila Centeno | Cuentas por cobrar y facturación | 8,311.49 |
| 9 | Ma. Guadalupe Guerrero García | Atención a clientes y vendedores | 6,997.80 |
| 255 | Ricardo Salgado Saldo | Chofer | 5,922.87 |
| 541 | Zaira Hamdan Pérez | Asistente administrativo | 5,697.03 |
| 538 | Nelly Esquivel Hermenegildo | Limpieza | 4,998.06 |
| 556 | Verónica Luna Vázquez | Auxiliar de oficina | 4,805.82 |
| 557 | Juan Alberto Hernández Hernández | Vendedor | 4,805.82 |
| 535 | Juan José Hernández López | Representante de ventas | 4,805.82 |
| 339 | Javier Hernández Ramírez | Representante de ventas | 3,750.00 |

Once de los catorce tienen CURP de la Ciudad de México; cuadra con una
oficina de CDMX. **Confírmalo contra la quincena 18 de CDMX en NOI** antes de
armar nada: la lista sale por exclusión, no de un campo "CDMX" (no existe).

**Con contrato pero sin sueldo ni fecha de inicio (8) — no entran hasta que
RH los complete o los archive:** 4 José Mizrahi Daniel, 5 Jacobo Mizrahi
Penhos, 24 José Juan Aramiz, 27 Gilberto López Rangel, 277 Reynaldo González,
527 "Fatima Bustamante" (duplicado de 254), 563 José Gómez, 564 Francisco
González. Ninguno debería estar en NOI con sueldo; si alguno sí, es hallazgo.

### Antes de crear recibos: tres datos que el módulo necesita y hoy no están

Los tres son de contrato/empleado, no de código. **Cámbialos en staging**, que
es donde está instalado el módulo (producción no lo tiene todavía):

1. **Registro patronal.** En cada uno de los 14 contratos (`hr.version` de la
   versión vigente), `l10n_mx_employer_registration = 'Y6087828106'`. Es lo
   que prueba el parche con datos reales: el CFDI debe traer ese y no el de la
   compañía (`C-675994510-1`).
2. **Dirección laboral en CDMX.** Hoy los 14 tienen `address_id` = la
   compañía (Toluca), así que `ClaveEntFed` saldría `MEX`. Crear un
   `res.partner` "Quimibond — oficina CDMX" con `state_id` = Ciudad de México
   (id 493, código `CMX`) y ponerlo como *Dirección laboral* de los 14. Con
   eso `ClaveEntFed` sale `CMX` sin tocar código. Verifica contra NOI qué
   entidad manda para ellos.
3. **Tipo de jornada** (`l10n_mx_shift_type`): lo que mande NOI para CDMX.

### Cómo armar la corrida

Igual que las anteriores (instrucciones completas en la tarea programada
«Nómina Quimibond — corrida paralela en Odoo», que trae las tres fuentes y el
mapeo):

- Corrida nueva `hr.payslip.run`, nombre
  `Quincena 18 2026 CDMX — PILOTO, NO VALIDAR NI TIMBRAR (ya timbrada en NOI)`,
  1–15 de septiembre, compañía 1.
- Recibos por lote sobre `slip_ids` con `[0, 0, {...}]`, `struct_id = 4`
  («Paga regular», `MX_REGULAR`), en tandas de 15 a 20. Si truena con
  `'>' not supported between instances of 'bool' and 'datetime.date'`, a
  alguien le falta `contract_date_start`: partir la tanda.
- Incidencias con los tipos de entrada de siempre (21 faltas, 22 incapacidad,
  16 HE dobles, 17 triples, 13 prima dominical, 18 sencillas, 15 puntualidad,
  14 objetivos, 20 prima vacacional, 32 vales, 29 préstamo, 33 retroactivo).
- Faltas con el criterio de NOI (`(48 − horas) ÷ 48 × 6`, pendiente de
  confirmar con Aurelio) si aplica a oficina; lo normal es que no haya.

### Qué comparar contra NOI

1. **Neto por recibo y total**, como en las otras corridas. La cuenta
   `501.06.27 Diferencias de redondeo de nómina` sigue siendo la alarma.
2. **CFDI de un recibo** con el snippet del README (sección "Cómo
   verificarlo"): `RegistroPatronal="Y6087828106"`, `ClaveEntFed` la de NOI,
   `PeriodicidadPago="04"`, SDI/SBC contra el CFDI de NOI de esa persona.
3. **`Dias="6"`**: hace falta un quincenal con horas extra. En oficina es
   raro; los 19 casos de `Dias=6` medidos en NOI salieron de la quincena
   16–31 jul, casi seguro de mantenimiento (mecánicos quincenales de Toluca,
   corrida 118). Si ningún CDMX trae tiempo extra en la 18, prueba `Dias="6"`
   sobre un recibo de la **corrida 118** que sí traiga: es el mismo código.
4. Un recibo **sin** horas extra no debe traer nodo ni cambiar en nada.

### Regla que no se rompe nunca

**No validar, no contabilizar y no timbrar nada en producción.** El PAC de
producción apunta al SAT real. Staging sí (`l10n_mx_edi_pac_test_env = true`,
llave dummy, correo y crons apagados).

---

## 3. Lo que sigue después

- Quincena 19 CDMX y Toluca (cierra el 30-sep).
- Timbrado de prueba con el PAC en ambiente de pruebas: ya no está bloqueado
  por `HorasExtra`.
- Aguinaldo contra diciembre; un finiquito real contra su CFDI.
- Corte de nómina en Odoo: **1 de enero de 2027**.
