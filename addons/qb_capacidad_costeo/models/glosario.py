# -*- coding: utf-8 -*-
"""Glosario de términos del cotizador — UNA sola fuente.

El mismo glosario se muestra en la calculadora (wizard), en la cotización
guardada y en el PDF, para que cualquiera que lea una cotización entienda
cada término sin preguntar. Si una definición cambia, se cambia aquí y
cambia en los tres lados a la vez.
"""

GLOSARIO_HTML = """
<table class="table table-sm" style="font-size:12px;">
  <tbody>
    <tr><th colspan="2" style="background:#f2f2f2;">💱 Moneda y tipo de cambio</th></tr>
    <tr><td style="width:28%;"><b>MXN / divisa</b></td>
        <td>Todos los <b>costos</b> del modelo se calculan en pesos mexicanos
        (MXN). «Divisa» es la moneda extranjera de la cotización (USD, EUR).
        Cada cifra indica su moneda; si no la indica, es MXN.</td></tr>
    <tr><td><b>Tipo de cambio (TC)</b></td>
        <td>Pesos por 1 unidad de la divisa (ej. TC 18.50 = 1 USD cuesta
        $18.50 MXN). Se toma de Odoo el día de cotizar y queda guardado en
        la cotización para trazabilidad.</td></tr>

    <tr><th colspan="2" style="background:#f2f2f2;">🏭 Costos (por unidad, MXN)</th></tr>
    <tr><td><b>Materia prima (MP)</b></td>
        <td>La receta (BOM) explotada hasta sus componentes comprados, cada
        uno a su <b>último costo de compra</b> convertido a MXN.</td></tr>
    <tr><td><b>Energía variable</b></td>
        <td>Luz, gas y agua que consume producir la unidad ($/kg × peso).
        Variable = solo se gasta si se produce.</td></tr>
    <tr><td><b>Costo variable</b></td>
        <td>= MP + energía. Lo que sale de la bolsa por producir UNA unidad
        más. Es el piso absoluto de cualquier precio.</td></tr>
    <tr><td><b>Fabricación absorbida</b></td>
        <td>La parte del gasto <b>fijo</b> de fábrica (sueldos de planta,
        renta, depreciación, arrendamiento de maquinaria) que le toca a cada
        unidad, repartida por peso (tejido/tintorería) y por metros
        (acabado).</td></tr>
    <tr><td><b>Costo de producción</b></td>
        <td>= costo variable + fabricación absorbida.</td></tr>
    <tr><td><b>Operación (%)</b></td>
        <td>Gastos de administración y ventas (cuentas 6xx) como % de las
        ventas. Se cobra como % del precio porque escala con cuánto se
        vende, no con lo que se produce.</td></tr>

    <tr><th colspan="2" style="background:#f2f2f2;">💲 Precios</th></tr>
    <tr><td><b>Precio objetivo</b></td>
        <td>El precio que TÚ propones o que el cliente pide. Se captura en
        la moneda de la cotización; el sistema lo convierte a MXN con el TC
        del día y sobre él evalúa semáforo y márgenes.</td></tr>
    <tr><td><b>Precio de mercado</b></td>
        <td>El precio promedio al que este producto REALMENTE se facturó en
        los últimos 12 meses (todos los clientes, en MXN). Es el ancla
        realista para cotizar: los pisos dicen debajo de qué no bajar; el
        mercado dice qué se está logrando hoy.</td></tr>
    <tr><td><b>Precio evaluado</b></td>
        <td>El precio sobre el que se calculan semáforo y márgenes: el
        objetivo si se capturó; si no, el de mercado; si tampoco hay ventas,
        el piso a planta llena.</td></tr>
    <tr><td><b>Escalera de volumen</b></td>
        <td>Precios estandarizados por tramo (½×, 1×, 2×, 4× del volumen
        cotizado) con un descuento fijo por cada duplicación. El volumen
        justifica el precio menor porque absorbe mejor los costos fijos y
        alarga las corridas (menos cambios de máquina, menos merma). Dos
        reglas duras: el precio nunca baja del piso a planta llena, y la
        contribución total $/mes nunca baja al crecer el tramo. Al cliente
        solo se le ofrecen tramos que caben en capacidad.</td></tr>
    <tr><td><b>Piso con capacidad ociosa</b></td>
        <td>= costo variable. Si hay máquinas paradas, cualquier precio
        arriba de esto aporta algo para pagar los fijos (que se pagan
        igual, se venda o no). Nunca vender debajo de él.</td></tr>
    <tr><td><b>Piso a planta llena</b></td>
        <td>= (variable + fabricación) ÷ (1 − %operación). Margen CERO
        cubriendo todo. Si la planta está llena, aceptar menos es regalar
        capacidad.</td></tr>

    <tr><th colspan="2" style="background:#f2f2f2;">📈 Márgenes (al precio evaluado)</th></tr>
    <tr><td><b>Margen de contribución</b></td>
        <td>= precio − costo variable (en $ y en % del precio). Lo que cada
        unidad aporta para pagar los costos fijos.</td></tr>
    <tr><td><b>Margen bruto %</b></td>
        <td>= (precio − costo de producción) ÷ precio. Utilidad después de
        fabricar, ANTES de administración y ventas.</td></tr>
    <tr><td><b>Margen neto %</b></td>
        <td>= margen bruto − %operación. Lo que queda de verdad después de
        TODO.</td></tr>
    <tr><td><b>Contribución por hora-máquina</b></td>
        <td>= contribución ÷ horas del centro más lento de la ruta. Para
        decidir qué producto conviene cuando las máquinas son el
        límite.</td></tr>

    <tr><th colspan="2" style="background:#f2f2f2;">⚙️ Capacidad y semáforo</th></tr>
    <tr><td><b>Capacidad</b></td>
        <td>Horas-máquina disponibles por mes en cada centro (calendarios
        reales × eficiencia).</td></tr>
    <tr><td><b>¿Cabe en capacidad?</b></td>
        <td>Compara las horas que exige el volumen cotizado contra las horas
        LIBRES de cada centro de la ruta; si no cabe, dice cuántas horas /
        máquinas / turnos faltan.</td></tr>
    <tr><td><b>Ociosidad</b></td>
        <td>Capacidad instalada que NO se está usando. Su costo fijo se paga
        aunque las máquinas estén paradas — por eso, con ociosidad, un
        precio «ámbar» puede convenir.</td></tr>
    <tr><td><b>Semáforo</b></td>
        <td>🔴 debajo del costo variable: destruye valor, no tomar ·
        🟡 entre los dos pisos: aporta a fijos, conviene SOLO con capacidad
        ociosa · 🟢 arriba del piso lleno: cubre todo y deja margen.</td></tr>
    <tr><td><b>Volumen</b></td>
        <td>Unidades por mes que se cotizan. Si el cliente compra regular,
        se precarga con su promedio histórico.</td></tr>
    <tr><td><b>Validez</b></td>
        <td>Fecha límite de la cotización; después el TC y los costos de MP
        pueden haber cambiado y hay que re-cotizar.</td></tr>
  </tbody>
</table>
"""
