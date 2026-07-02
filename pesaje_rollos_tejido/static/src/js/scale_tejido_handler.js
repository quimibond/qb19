/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.tejidoAbortController = null;

        onMounted(() => {
            const root = this.model && this.model.root;
            if (!root) return;
            
            if (root.resModel === 'mrp.weigh.roll.wizard' || root.resModel === 'mrp.subproduct.wizard') {
                this._onXmlScaleButtonClickBound = this._onXmlScaleButtonClick.bind(this);
                document.addEventListener('click', this._onXmlScaleButtonClickBound, true);
            }
        });

        onWillDestroy(() => {
            if (this._onXmlScaleButtonClickBound) {
                document.removeEventListener('click', this._onXmlScaleButtonClickBound, true);
            }
            if (this.tejidoAbortController) {
                this.tejidoAbortController.abort();
            }
        });
    },

    _onXmlScaleButtonClick(ev) {
        const btn = ev.target.closest('.btn_trigger_rhino');
        if (!btn) return;

        ev.preventDefault();
        ev.stopPropagation();

        console.log("--> [TEJIDO] ¡Clic interceptado con éxito! Bloqueando viaje a Python para evitar cierre.");

        const root = this.model && this.model.root;
        btn.disabled = true;
        btn.innerHTML = '⏳ Leyendo báscula...';

        const iotUrl = "https://192-168-100-30.3991e8c5.odoo-iot.com/hw_proxy/scale_read";
        this.tejidoAbortController = new AbortController();

        fetch(iotUrl, {
            method: "POST",
            // VOLVEMOS A LAS CABECERAS LIMPIAS QUE EL IOT SÍ PERMITE
            headers: { 
                "Content-Type": "application/json"
            },
            signal: this.tejidoAbortController.signal,
            body: JSON.stringify({ 
                jsonrpc: "2.0", 
                method: "call", 
                params: {}, 
                // EL ID ALEATORIO EVITA QUE CHROME RECICLE EL PESO SIN ROMPER EL CORS
                id: Math.floor(Math.random() * 100000)
            })
        })
        .then(response => response.json())
        .then(payload => {
            console.log("--> [TEJIDO] Respuesta cruda completa de la báscula:", payload);
            
            let weightValue = undefined;

            if (payload && payload.result !== undefined) {
                if (typeof payload.result === 'number' || (!isNaN(parseFloat(payload.result)) && typeof payload.result !== 'object')) {
                    weightValue = parseFloat(payload.result);
                } 
                else if (typeof payload.result === 'object' && payload.result !== null) {
                    const resData = payload.result;
                    weightValue = resData.weight !== undefined ? resData.weight : (resData.value !== undefined ? resData.value : resData.net);
                }
            }

            weightValue = parseFloat(weightValue);

            if (!isNaN(weightValue) && weightValue >= 0) {
                root.update({ weight: weightValue });
                console.log("--> [TEJIDO] ¡Peso insertado con éxito en Odoo! Valor:", weightValue);
            } else {
                console.log("--> [TEJIDO] No se pudo extraer un peso numérico válido.");
            }
        })
        .catch(err => {
            console.log("--> Petición finalizada o error de red:", err.name);
        })
        .finally(() => {
            if (btn) {
                btn.disabled = false;
                btn.innerHTML = '⚖️ Capturar';
            }
        });
    }
});