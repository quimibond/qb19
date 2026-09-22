# quimibond_nomina — CFDI de nómina y centinela de reglas

Quimibond migra su nómina de NOI/CONTPAQi a Odoo 19 (Odoo.sh). **Fecha de
corte: 1 de enero de 2027**, elegida para que el arrastre de provisiones sea
cero (validar una sola semana hoy cargaría 724,629 pesos de provisión de golpe
contra una nómina de 284,892).

El cálculo ya cuadra contra el despacho: 87 de 87 en la semana 38 y 89 de 89 en
la semana 39 (59 centavos de diferencia en toda la nómina). La póliza contable
está probada de punta a punta en staging. Lo que falta para timbrar son tres
defectos de `l10n_mx_hr_payroll_account_edi`; este módulo resuelve los tres.

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
original y corrige por dos vías. Primero **sobrescribe las llaves del
diccionario** que arma el módulo (`salario_diario_integrado`,
`salario_base_cot_apor`, `registro_patronal`, `clave_ent_fed`,
`num_empleado`, estén en `nomina_receptor`, `nomina_emisor` o donde estén;
nunca inventa llaves), así al inspeccionar `cfdi_values` en staging se ve lo
corregido. Segundo, agrega `cfdi_values['qb_nomina']` (valores planos, porque
`_clean_cfdi_values` destruye los recordsets) y la plantilla
`cfdiv40_nomina_quimibond`, que hereda
`l10n_mx_hr_payroll_account_edi.cfdiv40_nomina`, sobrescribe los atributos
con eso: es la red de seguridad si el módulo cambia el nombre de una llave.
**Revisar esa herencia en cada actualización del módulo de Odoo.**

### 3. ClaveEntFed y NumEmpleado del Receptor

El módulo los deja vacíos y los dos son obligatorios en el Anexo 20
(`ClaveEntFed` además lo exige el PAC cuando hay ISR retenido).

| Atributo | De dónde sale |
|---|---|
| `ClaveEntFed` | estado de la **dirección laboral** del empleado → de la ubicación de trabajo → de la compañía → parámetro `quimibond_nomina.clave_ent_fed`. Los códigos de estado de México en Odoo son los del SAT (`MEX`, `CMX`). La compañía está en `MEX`, que es lo que manda NOI |
| `NumEmpleado` | *Referencia de empleado* (`registration_number`): el número de trabajador de NOI con el prefijo de su nómina, `S-1` semanal, `Q-14` quincenal Toluca, `C-26` CDMX (NOI repite números entre nóminas y en Odoo la referencia es única por compañía; capturada en producción el 22-sep-2026 para los 153 activos). El atributo es **requerido** en Nómina 1.2 (1 a 15 caracteres, todo menos `\|`): **sin referencia el módulo detiene el CFDI con un error** ("El empleado X no tiene Referencia de empleado…") en vez de emitir uno que el PAC rechaza. Nunca se inventa: antes se rellenaba con la credencial o el id de Odoo (la credencial de Ricardo es `041460744711` y NOI manda `32`; el id de Genaro es 325 y NOI manda 1) |

### 4. Centinela de reglas

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

### 5. El nodo `nomina12:HorasExtra`

El módulo de Odoo **nunca lo emite**: escribe cada `nomina12:Percepcion` como
elemento vacío, incluidas las de `TipoPercepcion="019"`. El Anexo 20 obliga a
que toda percepción 019 lleve al menos un `HorasExtra`; sin él el PAC rechaza
el CFDI, y en una semana normal 39 de 86 recibos traen horas extra. Hay ticket
abierto con Odoo; este parche funciona solo y **se apaga solo** si Odoo lo
corrige de origen (ver abajo).

Así lo timbra NOI hoy (empleado 325, semana 39):

```xml
<nomina12:Percepcion Clave="P003" Concepto="HORAS EXTRAS DOBLES"
                     ImporteExento="373.28" ImporteGravado="373.28" TipoPercepcion="019">
  <nomina12:HorasExtra Dias="3" TipoHoras="01" HorasExtra="9" ImportePagado="746.55"/>
</nomina12:Percepcion>
```

| Atributo | De dónde sale |
|---|---|
| `TipoHoras` | entrada `HE_DOBLE` (id 16) → `01`; `HE_TRIPLE` (id 17) → `02`. Las horas sencillas (`H_SENCILLA`) **no llevan nodo**: van al concepto 038 |
| `HorasExtra` | `amount` de esas entradas (son horas) |
| `ImportePagado` | líneas `HE_EXEMPT` (id 463) + `HE_TAX` (id 464) del recibo, por código. Con dobles y triples en el mismo recibo se reparte en proporción a horas × factor (2 y 3) y la suma queda exacta |
| `Dias` | entrada `HE_DIAS` (días con tiempo extra, captura de RH); si falta, se estima (abajo) |

**`Dias` es un dato capturado, no una fórmula.** Es el número real de
días en que se generó el tiempo extra, y NOI lo captura: 3 horas en un solo
día son `Dias=1` (Ricardo Salgado, quincena 18 de CDMX:
`Dias="1" HorasExtra="3"`); las mismas 3 horas en tres días son `Dias=3`
(semanal de Toluca). No se deduce de las horas. La primera versión del módulo
traía una regla sacada de medir 142 nodos de NOI (3 por semana del periodo, 1
si fueron una o dos horas): describía el caso más común y lo generalizó de
más; la quincena de CDMX la desmintió.

Hoy sale así:

1. **Entrada `HE_DIAS` ("Días con tiempo extra")**, cantidad, en la
   estructura `MX_REGULAR`. Si el recibo la trae con cantidad > 0, **ese
   valor es `Dias`**, sin tocarlo. Es el dato que RH captura junto con las
   horas (`HE_DOBLE`, `HE_TRIPLE`). Aplica a **todos** los nodos del recibo,
   dobles y triples: son los días en que hubo tiempo extra, no los días de
   cada tipo.
2. **Sin `HE_DIAS`, `Dias` se estima** con `max(1, min(3 × semanas,
   ceil(horas)))`: 3 horas dan 3, 9 dan 3, 18 en quincena dan 6, 1 da 1.
   Nunca emite un valor imposible (más días que horas, cero, más de 3 por
   semana), pero **es una estimación**: en el caso de Ricardo saldría 3 y NOI
   timbra 1. Queda un `info` en el log cada vez que se estima. El dato bueno
   es la captura de RH.

**Primero se fusionan las dos percepciones 019.** Odoo genera una percepción
por regla, así que las horas extra salen partidas: `P19_2` exenta
(`HE_EXEMPT`) y `P19` gravada (`HE_TAX`). El SAT exige que *toda* percepción
019 lleve un hijo `HorasExtra`, y con la lista partida sólo hay dos salidas,
ambas malas: colgar el nodo a las dos declara el doble de horas; colgarlo a
una deja a la otra sin hijo y el PAC rechaza. NOI emite una sola con los dos
importes dentro, y eso es lo que el SAT ya acepta.
`_qb_fusionar_percepciones_019` suma `importe_gravado` e `importe_exento`,
conserva clave y concepto de la gravada, no toca las demás percepciones y es
idempotente (si Odoo deja de partirlas, no hace nada). `ImportePagado` del
nodo = gravado + exento de la percepción fusionada (Genaro, semana 38:
581.77 + 581.77 = 1163.54); si no se puede leer, se cae a las líneas
`HE_EXEMPT` + `HE_TAX`.

**Cómo está hecho.** `hr.payslip._qb_add_horas_extra` anota los nodos en
`cfdi_values['qb_horas_extra_por_indice']` (índice de la percepción 019 dentro
de `percepcion_list` → lista de nodos) y la vista
`quimibond_nomina.cfdiv40_nomina_horas_extra` los imprime dentro de cada
`nomina12:Percepcion` leyendo `<variable>_index` del `t-foreach`. Esa vista
**se configura sola** (`qb_nomina_ensure_horas_extra_view`, llamada por
`<function>` en `data/cfdi_horas_extra.xml` en cada instalación/actualización):
lee la plantilla real de Odoo, saca el nombre de la variable del `t-foreach`
sobre `percepcion_list` y escribe el arch. Si la plantilla ya trae
`HorasExtra` (Odoo corrigió el defecto) o no tiene la forma esperada, la vista
queda apagada y se avisa en el log: sale un CFDI sin nodo, nunca uno con dos ni
una instalación rota por un xpath que no resuelve.

**Falla en silencio hacia el lado seguro.** Horas capturadas sin percepción
019, percepción 019 sin horas, o sin líneas `HE_EXEMPT`/`HE_TAX`: aviso en el
log y el CFDI sale como estaba. Más vale un CFDI sin nodo (el PAC lo rechaza y
se ve) que uno con un nodo inventado. Desinstalar el módulo basta para
quitarlo: no toca reglas, entradas, conceptos ni registros de fábrica.

### 6. Conceptos en español

Los conceptos del CFDI son lo que el trabajador lee en su recibo y NOI los
manda en español. El módulo los toma del nombre del concepto
(`l10n.mx.concept.name`) en el idioma del contexto, y cuando el CFDI se arma
en `en_US` (shell de Odoo, cron) salen "Overtime", "Savings Fund". Los
registros ya están traducidos a `es_MX`; `_qb_conceptos_en_espanol` vuelve a
leer cada `concepto` de percepciones, deducciones y otros pagos desde su
registro (por `clave` = `payroll_code`) en `es_MX`. No hay diccionario en el
código: un concepto sin traducción se queda como estaba. Si algún concepto
sigue saliendo en inglés, es que a su registro le falta la traducción: se
corrige en Nómina → Configuración → Conceptos CFDI, en español.

**El módulo sólo LEE `l10n.mx.concept`; no escribe ni traduce registros.**
`_qb_conceptos_en_espanol` hace un `search` por `payroll_code` y lee `name`
con `lang='es_MX'`; el resultado va al diccionario del CFDI de ese recibo y
nada más. `l10n.mx.concept` lo usa todo el CFDI mexicano y no se toca.

## Qué escribe el módulo en la base (inventario completo)

Para revisar el riesgo antes de instalarlo en producción. Todo lo demás es
lectura o valores en memoria del CFDI de cada recibo.

| Qué | Cuándo | Registro |
|---|---|---|
| Campo `l10n_mx_employer_registration` en `hr.version` (y su reflejo en `hr.employee`) | al instalar | columna nueva, nace vacía: inerte hasta que alguien la llene |
| Modelo `qb.nomina.rule.sentinel` (tabla nueva) y sus dos filas `SUBSIDY` / `INT_DAY_WAGE` | al instalar (`post_init_hook`) | sólo lee las reglas para sacar la huella; **nunca escribe en `hr.salary.rule`** |
| Cron «Nómina Quimibond - Centinela de reglas salariales» | al instalar | diario, 7:00 CDMX; manda correo, no modifica nada |
| Tipo de entrada `HE_DIAS` (`hr.payslip.input.type`) | al instalar; en cada actualización se liga a la estructura `MX_REGULAR` (`struct_ids`) | registro propio del módulo (`noupdate`) |
| Vista QWeb `cfdiv40_nomina_quimibond` y vista `cfdiv40_nomina_horas_extra` (herencias de la plantilla del CFDI) | al instalar; la segunda reescribe su propio `arch`/`active` en cada actualización | registros propios del módulo; la plantilla de Odoo no se modifica |
| Vistas del empleado, plantilla de contrato y del centinela; menú del centinela (se cuelga de Nómina → Configuración en cada actualización) | al instalar | registros propios del módulo |
| Fila de `hr.payslip.line`, `hr.payslip`, `hr.salary.rule`, `l10n.mx.concept`, `res.company`, `res.partner` | nunca | — |

Desinstalar el módulo borra lo propio (modelo, vistas, cron, tipo de entrada,
campo) y no deja nada cambiado en registros de Odoo.

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
   `@SalarioBaseCotApor` el mismo topado a 25 UMA (Genaro, recibo 4358:
   889.43 en los dos, que es lo que NOI le timbra; 517.13 es el salario
   diario simple, o sea el bug sin parchar); `@ClaveEntFed="MEX"` y
   `@NumEmpleado` con el número del empleado.

   Y en el diccionario, antes de renderizar, lo mismo ya corregido:

```python
print(cv['qb_nomina'])                       # lo que calcula el módulo
for k, v in cv.items():                      # las llaves del módulo de Odoo, ya parchadas
    if isinstance(v, dict) and 'salario_diario_integrado' in v:
        print(k, {j: v[j] for j in ('salario_diario_integrado', 'salario_base_cot_apor', 'clave_ent_fed', 'num_empleado') if j in v})
```
5. **El nodo HorasExtra.** En el log de la instalación debe aparecer
   `herencia HorasExtra activa (variable del t-foreach: …)`. En el
   diccionario del recibo 4358, `percepcion_list` trae **una sola**
   percepción 019 con `importe_gravado = 581.77` e `importe_exento = 581.77`
   (antes salían dos, P19_2 exenta y P19 gravada), y
   `cv['qb_horas_extra_por_indice']` trae `{<índice>: [{'dias': 3,
   'tipo_horas': '01', 'horas_extra': 9, 'importe_pagado': '1163.54'}]}`. En
   el XML: `<nomina12:HorasExtra Dias="3" TipoHoras="01" HorasExtra="9"
   ImportePagado="1163.54"/>` dentro de la 019. En el recibo **4501**
   (Ricardo Salgado, quincena 18 de CDMX, 3 horas): con una entrada
   `HE_DIAS = 1` sale `dias: 1` (lo que timbra NOI) y sin ella `dias: 3`
   (estimado). `num_empleado` sale la referencia con prefijo (`C-32`); sin
   referencia `_qb_nomina_cfdi_values()` levanta `UserError`. Ojo: el diccionario sólo se arma con el recibo `paid` y su
   asiento `posted`; en borrador truena con `'bool' object has no attribute
   'rpartition'` y validado sin pagar con `... 'isoformat'`. Un recibo sin horas extra no
   cambia en nada. Un recibo quincenal con horas extra emite `Dias="6"`. Los
   conceptos salen en español aunque el shell esté en `en_US`.
6. **La nómina no se movió.** Recalcular la corrida 117 (semana 38, 87
   recibos) y confirmar que el neto sigue en 302,757.31. El módulo no toca el
   cálculo, sólo el CFDI.
7. **Tests** (no corren en el CI porque dependen de Enterprise):
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

- Timbrado de prueba con el PAC en ambiente de pruebas.
- Nómina de aguinaldo contra la de diciembre; un finiquito real contra su CFDI;
  quincena 19 (cierra el 30 de septiembre) para cumplir dos periodos seguidos
  también en la quincenal.
- **Datos que hay que corregir en Odoo antes de timbrar** (no es código):
  `TipoJornada` sale `01` (Diurna) y NOI manda `03` (Mixta): es el campo *Tipo
  de jornada* del contrato (`l10n_mx_shift_type`), hay que ponerlo en los
  contratos de planta. `CuentaBancaria`: Odoo trae `11323066620` y NOI manda
  la CLABE `012180011323066628`; con CLABE de 18 dígitos el SAT no exige el
  atributo `Banco`, y es la que el SAT ya aceptó, así que en la cuenta
  bancaria del empleado debe ir la CLABE (la de Odoo parece una captura
  trunca: le falta el `0` inicial y le sobra un `0` final). `NumEmpleado`:
  la *Referencia de empleado* con prefijo de nómina (`S-1`, `Q-14`, `C-26`),
  capturada en producción el 22-sep-2026; sin ella el módulo detiene el CFDI
  (`UserError`), porque el atributo es requerido. **`HE_DIAS`:** capturar en
  cada recibo con tiempo extra los días en que se generó (mientras falte,
  `Dias` se estima). `Antigüedad`:
  Odoo manda `P1365W` y NOI `P1367W`, dos semanas de diferencia por la fecha
  de corte que usa cada uno; definir con RH cuál es la fecha de ingreso buena.
- Pendientes de terceros: RFC, CURP y NSS de un empleado; destrabar la app de
  Ausencias («Debe configurar al menos una cuenta analítica», sin eso no hay
  nodo de Incapacidades); la cuenta archivada `201.01.02 Reembolso empleados`
  que usan las reglas *Gastos* y *Reembolso*; el 41% de prima vacacional de
  cinco personas contra IDSE.
