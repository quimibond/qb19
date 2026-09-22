# Handoff — nómina: cierre del CFDI y quincena de CDMX

**Para:** la siguiente sesión de Claude Code sobre la nómina de Quimibond.
**Fecha:** 18 de septiembre de 2026.
**Sustituye a:** `PROMPT_horas_extra_cfdi_v2.md`. Esa tarea ya está cerrada; aquí
va el estado verificado y lo que sigue. **Corregido el mismo día:** la quincena
de CDMX son 11 personas, no 14 (sección 2).

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

Detalle de cada parche y verificación: `addons/quimibond_nomina/README.md`.
**`Dias` no es una fórmula** (corregido en la 1.4.0): es la entrada `HE_DIAS` que
captura RH; sin ella se estima y queda avisado en el log. Detalle en el README:
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

### Quiénes: 11 personas (confirmado contra los CFDI de NOI, 18-sep-2026)

La primera versión de este handoff traía 14 candidatos sacados **por
exclusión** en Odoo (71 quincenales activos − 49 de la quincena 18 de Toluca
= 22, de los cuales 14 con sueldo). Se confirmó contra los CFDI que NOI
emitió y el SAT certificó, leídos de los correos de `rhmexico@quimibond.com`:
para el periodo 01/Sep/2026–15/Sep/2026 hay **exactamente 11 comprobantes con
registro patronal `Y6087828106`**, y son éstos:

| id Odoo | Nombre | Puesto | Sueldo quincenal en el contrato |
|---|---|---|---|
| 8 | Irma Luna Ángeles | Contador general | 15,136.21 |
| 13 | Aurelio Álvarez García | Auditor interno contable | 10,633.80 |
| 19 | Lorena Mondragón Reinoso | Responsable de nóminas | 8,639.23 |
| 539 | Sandra Dávila Centeno | Cuentas por cobrar y facturación | 8,311.49 |
| 9 | Ma. Guadalupe Guerrero García | Atención a clientes y vendedores | 6,997.80 |
| 255 | Ricardo Salgado Saldo | Chofer | 5,922.87 |
| 541 | Zaira Hamdán Pérez | Asistente administrativo | 5,697.03 |
| 538 | Nelly Esquivel Hermenegildo | Limpieza | 4,998.06 |
| 556 | Verónica Luna Vázquez | Auxiliar de oficina | 4,805.82 |
| 557 | Juan Alberto Hernández Hernández | Vendedor | 4,805.82 |
| 535 | Juan José Hernández López | Representante de ventas | 4,805.82 |

**Esta es la corrida.** En staging los 11 ya tienen
`l10n_mx_employer_registration = 'Y6087828106'` (se cargó el 18-sep junto con
dos que resultaron no ir; sobra, no estorba).

**Los tres de la lista original que NO entran:**

- **José Jaime Mizrahi (id 2).** Activo, contrato desde 2023-03-31, sueldo
  52,000, y **cero CFDI de nómina en todo 2026**: no cobra por nómina, presta
  servicios bajo RESICO y factura a Quimibond. No entra en ninguna corrida.
  RH debería archivarlo o marcarlo de otra forma: hoy cualquier barrido de
  "quincenales con sueldo" lo incluye.
- **Javier Hernández Ramírez (id 339).** Activo, representante de ventas,
  sueldo 3,750, **cero CFDI en 2026**. Cobra por otra vía o el registro está
  obsoleto; no entra hasta que RH diga cuál.
- **Jorge Manuel Eduardo Ortiz Velázquez (id 6).** Éste sí cobraba: 16 CFDI
  en 2026 con `Y6087828106`, el último de la quincena 16–31 de agosto; en la
  del 1–15 de septiembre ya no aparece. En Odoo sigue activo y sin fecha de
  baja: es una baja de finales de agosto sin registrar, o algo cambió.
  **No entra en la 18** y hay que aclararlo antes de enero. Además está
  **duplicado**: empleado 6 (activo, alta 18-ago-2025) y empleado 34
  (archivado, alta 1-ene-2026), mismo RFC `OIVJ6912293Q5` (verificado por
  MCP). Decidir cuál se queda.

**Una que no está en ninguna lista, para que nadie la "recupere":** Silvia
Carmen Chávez Carreón (id 540), archivada en producción sin fecha de baja. Su
último CFDI es la quincena 1–15 de agosto, con `Y6087828106`. Es baja y está
bien excluida.

**Con contrato pero sin sueldo ni fecha de inicio (8) — tampoco entran:** 4
José Mizrahi Daniel, 5 Jacobo Mizrahi Penhos, 24 José Juan Aramiz, 27
Gilberto López Rangel, 277 Reynaldo González, 527 "Fatima Bustamante"
(duplicado de 254), 563 José Gómez, 564 Francisco González. Ninguno debería
estar en NOI con sueldo; si alguno sí, es hallazgo.

### Antes de crear recibos: tres datos que el módulo necesita y hoy no están

Los tres son de contrato/empleado, no de código. **Cámbialos en staging**, que
es donde está instalado el módulo (producción no lo tiene todavía). El
primero ya está hecho para los 11:

1. **Registro patronal.** En cada uno de los 11 contratos (`hr.version` de la
   versión vigente), `l10n_mx_employer_registration = 'Y6087828106'`. Es lo
   que prueba el parche con datos reales: el CFDI debe traer ese y no el de la
   compañía (`C-675994510-1`).
2. **Dirección laboral en CDMX.** Hoy los 11 tienen `address_id` = la
   compañía (Toluca), así que `ClaveEntFed` saldría `MEX`. Crear un
   `res.partner` "Quimibond — oficina CDMX" con `state_id` = Ciudad de México
   (id 493, código `CMX`) y ponerlo como *Dirección laboral* de los 11. Con
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
3. **`Dias="6"`**: hace falta un quincenal con horas extra. Con 11 personas
   de oficina es casi seguro que ninguna traiga; los 19 casos de `Dias=6`
   medidos en NOI salieron de la quincena 16–31 jul, de mantenimiento
   (mecánicos quincenales de Toluca, corrida 118). Prueba `Dias="6"` sobre un
   recibo de la **corrida 118** que sí traiga tiempo extra: es el mismo código.
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
