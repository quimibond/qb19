/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.revisadoScaleInterval = null;

        onMounted(() => {
            if (this.model.root.resModel === 'mrp.revisado.wizard') {
                this._connectToRevisadoScale();
            }
        });

        onWillDestroy(() => {
            if (this.revisadoScaleInterval) {
                clearInterval(this.revisadoScaleInterval);
                this.revisadoScaleInterval = null;
            }
        });
    },

    _connectToRevisadoScale() {
        const root = this.model.root;
        if (root.data.weighing_mode === 'iot' && root.data.iot_device_id) {
            
            // 🔒 CONEXIÓN DIRECTA HTTPS LOCAL (Sin extensiones de Chrome ni CORS)
            this.revisadoScaleInterval = setInterval(() => {
                fetch("https://192-168-100-30.3991e8c5.odoo-iot.com/hw_proxy/perform_action", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ 
                        jsonrpc: "2.0", 
                        params: { "action": "read_scale" } 
                    })
                })
                .then(response => response.json())
                .then(payload => {
                    const data = payload.result || payload;
                    if (data) {
                        let weightValue = data.weight !== undefined ? data.weight : data.value;
                        weightValue = parseFloat(weightValue);

                        if (!isNaN(weightValue) && weightValue >= 0) {
                            if (root.data.peso_actual !== weightValue) {
                                root.update({ peso_actual: weightValue });
                            }
                        }
                    }
                })
                .catch(err => console.log("Conectando con IoT Box local..."));
            }, 1000);
        }
    }
});