/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.revisadoScaleInterval = null;
        this.revisadoAbortController = null;
        this.lastRawWeight = null;
        this.readCounter = 0;

        onMounted(() => {
            const root = this.model && this.model.root;
            if (!root) return;

            if (root.resModel === 'mrp.revisado.wizard') {
                this.lastRawWeight = null;
                this.readCounter = 0;

                setTimeout(() => {
                    const inputWeight = document.querySelector("input[name='peso_actual']");
                    if (inputWeight) inputWeight.value = "0.00";
                }, 200);

                this._connectToRevisadoScale();
            }
        });

        onWillDestroy(() => {
            if (this.revisadoScaleInterval) {
                clearInterval(this.revisadoScaleInterval);
                this.revisadoScaleInterval = null;
            }
            if (this.revisadoAbortController) {
                this.revisadoAbortController.abort();
                this.revisadoAbortController = null;
            }
        });
    },

    _connectToRevisadoScale() {
        const root = this.model && this.model.root;
        if (!root || root.data.weighing_mode !== 'iot' || !root.data.iot_device_id) return;
            
        const iotUrl = "https://192-168-100-30.3991e8c5.odoo-iot.com/hw_proxy/scale_read";

        this.revisadoScaleInterval = setInterval(() => {
            if (!this.revisadoScaleInterval) return;

            this.revisadoAbortController = new AbortController();

            fetch(iotUrl, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                signal: this.revisadoAbortController.signal,
                body: JSON.stringify({ 
                    jsonrpc: "2.0", 
                    method: "call", 
                    params: {}, 
                    id: Math.floor(Math.random() * 1000)
                })
            })
            .then(response => response.json())
            .then(payload => {
                if (!this.revisadoScaleInterval || !root) return;

                const data = payload.result || payload;
                if (data) {
                    let weightValue = data.weight !== undefined ? data.weight : data.value;
                    weightValue = parseFloat(weightValue);

                    if (!isNaN(weightValue) && weightValue >= 0) {
                        
                        if (weightValue === this.lastRawWeight) {
                            this.readCounter++;
                        } else {
                            this.lastRawWeight = weightValue;
                            this.readCounter = 1;
                        }

                        if (this.readCounter >= 2) {
                            if (root.data && root.data.peso_actual !== weightValue) {
                                root.update({ peso_actual: weightValue });
                            }
                        }
                    }
                }
            })
            .catch(err => {
                if (err.name !== 'AbortError') {
                    console.log("Leyendo peso dinámico (Revisado)...");
                }
            });
        }, 800);
    }
});