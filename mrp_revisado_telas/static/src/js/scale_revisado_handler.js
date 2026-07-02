/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.revisadoAbortController = null;

        onMounted(() => {
            const root = this.model && this.model.root;
            if (!root) return;
            
            // Validamos que se aplique únicamente al modelo del wizard de Revisado
            if (root.resModel === 'mrp.revisado.wizard') {
                this._onXmlScaleButtonClickBound = this._onXmlScaleButtonClick.bind(this);
                document.addEventListener('click', this._onXmlScaleButtonClickBound, true);
            }
        });

        onWillDestroy(() => {
            if (this._onXmlScaleButtonClickBound) {
                document.removeEventListener('click', this._onXmlScaleButtonClickBound, true);
            }
            if (this.revisadoAbortController) {
                this.revisadoAbortController.abort();
            }
        });
    },

    _onXmlScaleButtonClick(ev) {
        // Buscamos el botón de capturar (revisa que la clase coincida con tu XML, ej. btn_trigger_rhino)
        const btn = ev.target.closest('.btn_trigger_rhino');
        if (!btn) return;

        // CRUCIAL: Bloqueamos el viaje a Python para evitar que Odoo destruya y cierre el wizard
        ev.preventDefault();
        ev.stopPropagation();

        console.log("--> [REVISADO] ¡Clic interceptado con éxito! Bloqueando viaje a Python para evitar cierre.");

        const root = this.model && this.model.root;
        btn.disabled = true;
        btn.innerHTML = '⏳ Leyendo báscula...';

        const iotUrl = "https://192-168-100-30.3991e8c5.odoo-iot.com/hw_proxy/scale_read";
        this.revisadoAbortController = new AbortController();

        fetch(iotUrl, {
            method: "POST",
            headers: { 
                "Content-Type": "application/json"
            },
            signal: this.revisadoAbortController.signal,
            body: JSON.stringify({ 
                jsonrpc: "2.0", 
                method: "call", 
                params: {}, 
                // ID aleatorio dinámico para que Chrome jamás recicle el peso anterior
                id: Math.floor(Math.random() * 100000)
            })
        })
        .then(response => response.json())
        .then(payload => {
            console.log("--> [REVISADO] Respuesta cruda completa de la báscula:", payload);
            
            let weightValue = undefined;

            if (payload && payload.result !== undefined) {
                // Si el IoT Box responde con el formato plano (número puro)
                if (typeof payload.result === 'number' || (!isNaN(parseFloat(payload.result)) && typeof payload.result !== 'object')) {
                    weightValue = parseFloat(payload.result);
                } 
                // Si responde como un diccionario/objeto JSON
                else if (typeof payload.result === 'object' && payload.result !== null) {
                    const resData = payload.result;
                    weightValue = resData.weight !== undefined ? resData.weight : (resData.value !== undefined ? resData.value : resData.net);
                }
            }

            weightValue = parseFloat(weightValue);

            // Inyectamos el número extraído en el campo 'peso_actual' de tu wizard de Revisado
            if (!isNaN(weightValue) && weightValue >= 0) {
                root.update({ peso_actual: weightValue });
                console.log("--> [REVISADO] ¡Peso insertado con éxito en Odoo! Valor:", weightValue);
            } else {
                console.log("--> [REVISADO] No se pudo extraer un peso numérico válido.");
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