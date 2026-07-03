/** @odoo-module **/

import { registry } from "@web/core/registry";
import { FloatField, floatField } from "@web/views/fields/float/float_field";
import { useState } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { _t } from "@web/core/l10n/translation";

/**
 * Widget de captura de peso vía báscula IoT ("Rhino") para wizards Owl.
 *
 * IMPORTANTE - Por qué existe este widget y no un simple <a> + JS global:
 * En Odoo 17/18/19 los campos de formulario (incluidos los de wizards
 * TransientModel) son componentes Owl cuyo valor visible proviene del
 * estado reactivo de `record` (ORM en memoria), NO del DOM. Escribir
 * `input.value = x` y disparar `dispatchEvent` genera una discrepancia
 * entre el DOM real y el estado reactivo: Owl vuelve a renderizar con el
 * valor "correcto" (ignorando la inyección) o, en el peor caso, interpreta
 * la inconsistencia como un estado inválido del modal y lo cierra.
 *
 * La única vía soportada para escribir un valor en un campo es a través
 * de `this.props.record.update({...})`, que sí pasa por el ciclo de vida
 * reactivo de Owl y del ORM.
 */
export class ScaleCaptureField extends FloatField {
    static template = "mrp_revisado_telas.ScaleCaptureField";

    setup() {
        super.setup();
        this.state = useState({ loading: false });
        // FloatField/CharField ya declaran su propio setup(); nos aseguramos
        // de tener el servicio de notificaciones disponible aquí también.
        this.notification = useService("notification");
    }

    get iotScaleUrl() {
        // TODO: idealmente mover esta URL a un parámetro de sistema
        // (ir.config_parameter) o al registro iot.device relacionado
        // (iot_device_id) en vez de tenerla fija en el JS.
        return "https://192-168-100-30.3991e8c5.odoo-iot.com/hw_proxy/scale_read";
    }

    async onCapturarClick() {
        if (this.state.loading) {
            return;
        }
        this.state.loading = true;
        try {
            const response = await fetch(this.iotScaleUrl, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    jsonrpc: "2.0",
                    method: "call",
                    params: {},
                    id: Math.floor(Math.random() * 100000),
                }),
            });
            const payload = await response.json();
            const weightValue = this._extractWeight(payload);

            if (isNaN(weightValue) || weightValue < 0) {
                this.notification.add(
                    _t("La báscula no devolvió un peso válido."),
                    { type: "warning" }
                );
                return;
            }

            // ESTA es la línea clave: actualiza el estado reactivo del
            // record de Owl/ORM, en vez de tocar el DOM directamente.
            await this.props.record.update({
                [this.props.name]: weightValue,
            });
        } catch (err) {
            console.error("[REVISADO] Error al leer báscula:", err);
            this.notification.add(
                _t("No se pudo conectar con la báscula. Verifique la IoT Box."),
                { type: "danger" }
            );
        } finally {
            this.state.loading = false;
        }
    }

    _extractWeight(payload) {
        let weightValue;
        if (payload && payload.result !== undefined) {
            if (
                typeof payload.result === "number" ||
                (!isNaN(parseFloat(payload.result)) && typeof payload.result !== "object")
            ) {
                weightValue = parseFloat(payload.result);
            } else if (typeof payload.result === "object" && payload.result !== null) {
                const resData = payload.result;
                weightValue =
                    resData.weight !== undefined
                        ? resData.weight
                        : resData.value !== undefined
                        ? resData.value
                        : resData.net;
            }
        }
        return parseFloat(weightValue);
    }
}

export const scaleCaptureField = {
    ...floatField,
    component: ScaleCaptureField,
};

registry.category("fields").add("peso_bascula", scaleCaptureField);
