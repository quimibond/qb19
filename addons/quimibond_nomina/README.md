# quimibond_nomina — CFDI de nómina y centinela de reglas

Quimibond migra su nómina de NOI/CONTPAQi a Odoo 19 (Odoo.sh). **Fecha de
corte: 1 de enero de 2027**, elegida para que el arrastre de provisiones sea
cero (validar una sola semana hoy cargaría 724,629 pesos de provisión de golpe
contra una nómina de 284,892).

El cálculo ya cuadra contra el despacho: 87 de 87 en la semana 38 y 89 de 89 en
la semana 39 (59 centavos de diferencia en toda la nómina). La póliza contable
está probada de punta a punta en staging. Lo que falta para timbrar son tres
defectos de `l10n_mx_hr_payroll_account_edi`; este módulo resuelve dos.

> **Regla que no se rompe nunca:** no validar, no contabilizar y no timbrar
> nada en producción. El PAC apunta a producción y validar es el paso previo a
> timbrar un CFDI real ante el SAT. Las corridas piloto están marcadas
> `PILOTO, NO VALIDAR NI TIMBRAR`. En staging sí se puede: está neutralizada y
> la llave privada es una dummy.

## Qué trae

### 1. Registro patronal por contrato

Odoo guarda **uno solo**, en `res.company.l10n_mx_imss_id` (verificado: el
campo existe únicamente en `res.company` y `res.config.settings`). Quimibond
tiene dos, confirmados contra los CFDI que hoy timbra el despacho:
`C-675994510-1` (Toluca, 1,916 timbres) y `Y6087828106` (México, 154).

El módulo agrega `l10n_mx_employer_registration` a `hr.version` (y su reflejo
en `hr.employee`, que es donde Odoo 19 edita el contrato) y lo inyecta en
`nomina12:Emisor/@RegistroPatronal`. Vacío = el de la compañía.

### 2. SalarioDiarioIntegrado y SalarioBaseCotApor

El módulo de Odoo los alimenta al revés: manda `l10n_mx_daily_salary`, el
salario diario simple, al atributo del integrado. Aquí salen del recibo ya
calculado:

| Atributo | Línea del recibo | Qué es |
|---|---|---|
| `nomina12:Receptor/@SalarioDiarioIntegrado` | `INT_DAY_WAGE_BASE` | SDI declarado en la ficha |
| `nomina12:Receptor/@SalarioBaseCotApor` | `INT_DAY_WAGE` | el mismo, topado a 25 UMA |

**Las líneas se buscan POR CÓDIGO, nunca con `env.ref()`.** Es deliberado: el
módulo resuelve las reglas por XML id, y cuando la regla de fábrica está
archivada y en su lugar corre una copia activa con el mismo código (la
situación de esta base) el `env.ref()` devuelve la archivada, no produce
línea, y el CFDI sale con `0.00` sin error, sin advertencia y sin nada en el
log. Si el recibo no trae la línea, se cae al salario diario simple (lo que
hacía el módulo) y se escribe un aviso en el log.

Cómo está hecho: `hr.payslip._l10n_mx_edi_add_payslip_cfdi_values` llama al
original y agrega `cfdi_values['qb_nomina']` (valores planos, porque
`_clean_cfdi_values` destruye los recordsets antes de renderizar); la
plantilla `cfdiv40_nomina_quimibond` hereda
`l10n_mx_hr_payroll_account_edi.cfdiv40_nomina` y sobrescribe los tres
atributos. **Revisar esa herencia en cada actualización del módulo de Odoo.**

### 3. Centinela de reglas

`SUBSIDY` (id 42) e `INT_DAY_WAGE` (id 95) de «Paga regular» **no son copias
propias**: son registros del módulo de Odoo con el código de Quimibond escrito
encima (creados con la instalación del módulo, editados el 16-sep-2026). Las
demás reglas «(Quimibond)» sí son registros nuevos y están a salvo.

**Ya pasó una vez:** el 16-sep-2026 quince reglas volvieron solas al código de
fábrica a media corrida de la quincena 18. El daño no es que truene, es que no
truena: la nómina sigue calculando con la lógica de Odoo y nadie se entera.

El centinela (Nómina → Configuración → Centinela de reglas) guarda un SHA-256
de la condición y la fórmula de cada regla vigilada, la revisa a diario (cron
de las 7:00 CDMX) y manda correo a los responsables de nómina (grupo
*Nómina / Administrador*; más direcciones en el parámetro
`quimibond_nomina.sentinel_email`). **No revierte solo, a propósito**: esas
reglas se editan deliberadamente y revertir por detrás sería peor que el
problema. Cuando el cambio es querido, «Aceptar huella actual». Al instalar se
siembra con las dos reglas y toma como línea base lo que esté vivo: el estado
ya verificado contra el despacho. Se pueden agregar más reglas a mano.

## Lo que NO trae, y por qué

El nodo **`nomina12:HorasExtra`**, obligatorio cuando hay percepción `019`.
Hoy la plantilla emite `nomina12:Percepcion` como elemento vacío, así que
ningún recibo con tiempo extra se puede timbrar (en una semana normal, 39 de
87). Dos razones para no escribirlo todavía:

1. **Falta el dato.** El nodo pide `Dias`: en cuántos días se generaron las
   horas extra. El archivo de incidencias de RH trae horas trabajadas por día
   y el total semanal de horas extra, pero no los días, y no se deduce.
   Pendiente: que RH lo capture o que se acuerde una convención.
2. **Hay un ticket abierto con Odoo** (18-sep-2026). Si ellos lo implementan
   en la plantilla y nosotros la sobrescribimos, salen dos nodos `HorasExtra`
   y el CFDI queda inválido. Revisar el estado del ticket antes de escribirlo.

Cuando se escriba, va como herencia de la misma plantilla, en
`data/cfdi_nomina_templates.xml`.

## Cómo verificarlo después del build

1. Instalar `quimibond_nomina` en la staging de `main`.
2. **El campo aparece:** abrir un empleado de planta, pestaña de nómina: el
   campo *Registro patronal IMSS* debe estar junto a *Fondo de ahorro*.
3. **El centinela arrancó:** Nómina → Configuración → Centinela de reglas
   trae dos renglones (`SUBSIDY`, `INT_DAY_WAGE`) con huella aceptada y sin
   marca de cambio.
4. **El CFDI sale bien.** En staging, sobre el recibo 4358 (semana 38, ya en
   estado `paid` con su póliza publicada, que es lo que el render exige):

```python
p = env['hr.payslip'].with_context(lang='es_MX').browse(4358)
D = env['l10n_mx_edi.document'].with_context(lang='es_MX')
cv = D._get_company_cfdi_values(p.company_id)
D._add_certificate_cfdi_values(cv)
p._l10n_mx_edi_add_payslip_cfdi_values(cv)
D._clean_cfdi_values(cv)          # SIEMPRE al final: destruye los recordsets
print(env['ir.qweb'].with_context(lang='es_MX')._render(
    'l10n_mx_hr_payroll_account_edi.cfdiv40_nomina', cv))
```

   Qué mirar en el XML: `nomina12:Emisor/@RegistroPatronal` (si al contrato
   se le puso `Y6087828106`, debe salir ese y no el de la compañía);
   `nomina12:Receptor/@SalarioDiarioIntegrado` con el SDI y
   `@SalarioBaseCotApor` el mismo topado a 25 UMA.
5. **La nómina no se movió.** Recalcular la corrida 117 (semana 38, 87
   recibos) y confirmar que el neto sigue en 302,757.31. El módulo no toca el
   cálculo, sólo el CFDI.
6. **Tests** (no corren en el CI porque dependen de Enterprise):
   `odoo-bin ... --test-tags /quimibond_nomina --stop-after-init`.

## Datos que cuesta trabajo redescubrir

**Corridas piloto en producción** (todas en borrador, ninguna validada):

| id | Qué | Recibos | Neto Odoo | Neto NOI |
|---|---|---|---|---|
| 119 | Semana 39 (14–20 sep) | 89 | 284,892.47 | 284,891.88 |
| 118 | Quincena 18 Toluca | 49 | 407,646.58 | 407,648.16 |
| 117 | Semana 38 | 87 | 302,757.31 | 302,757.28 |
| 116 | Semana 37 | 86 | — | superada, no usar |

- **Estructura** `struct_id = 4` («Paga regular»), `code = 'MX_REGULAR'`.
- **Diario** id 26 («Nominas»), cuenta por defecto `501.06.27 DIFERENCIAS DE
  REDONDEO DE NOMINA`, que funciona como alarma: mientras ahí sólo caigan
  centavos, el modelo está sano.
- **Tipos de incidencia** (`hr.payslip.input.type`): 21 faltas (días), 22
  incapacidad (días), 16 horas extra dobles, 17 triples, 13 prima dominical
  (horas), 18 horas sencillas, 15 premio de puntualidad (importe), 14 logro de
  objetivos (importe), 20 prima vacacional (importe), 32 vales, 29 préstamo,
  33 retroactivo.
- **Crear recibos en lote:** crear `hr.payslip` directo falla; hay que escribir
  sobre el lote (`hr.payslip.run`, `slip_ids` con `[0, 0, {...}]`) en tandas de
  15 a 20. Si truena con `'>' not supported between instances of 'bool' and
  'datetime.date'`, a alguien le falta `contract_date_start` en su versión:
  partir la tanda a la mitad para encontrarlo.
- **Criterio de faltas de NOI** (pendiente de confirmar con el contador): paga
  el sueldo semanal en proporción a las horas trabajadas sobre 48, no
  descontando días completos. La fórmula que lo reproduce al centavo es
  `faltas (días) = (48 − horas trabajadas) ÷ 48 × 6`. La incapacidad sí va por
  días completos, y una semana entera se paga en cero porque la cubre el IMSS.
- Una tarea programada diaria («Nómina Quimibond — corrida paralela en Odoo»)
  arma la corrida semanal sola a partir del correo de RH.

## Lo que sigue

- Timbrado de prueba con el PAC en ambiente de pruebas (bloqueado por
  `HorasExtra` para quien tiene tiempo extra).
- Nómina de aguinaldo contra la de diciembre; un finiquito real contra su CFDI;
  quincena 19 (cierra el 30 de septiembre) para cumplir dos periodos seguidos
  también en la quincenal.
- Pendientes de terceros: RFC, CURP y NSS de un empleado; destrabar la app de
  Ausencias («Debe configurar al menos una cuenta analítica», sin eso no hay
  nodo de Incapacidades); la cuenta archivada `201.01.02 Reembolso empleados`
  que usan las reglas *Gastos* y *Reembolso*; el 41% de prima vacacional de
  cinco personas contra IDSE.
