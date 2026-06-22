/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.tejidoScaleInterval = null;

        onMounted(() => {
            const modelName = this.model.root.resModel;
            if (modelName === 'mrp.weigh.roll.wizard' || modelName === 'mrp.subproduct.wizard') {
                this._connectToTejidoScale();
            }
        });

        onWillDestroy(() => {
            if (this.tejidoScaleInterval) {
                clearInterval(this.tejidoScaleInterval);
                this.tejidoScaleInterval = null;
            }
        });
    },

    _connectToTejidoScale() {
        const root = this.model.root;
        if (root.data.weighing_mode === 'iot' && root.data.iot_device_id) {
            
            // 🔒 Endpoint nativo exclusivo para lectura de básculas en Odoo IoT
            const iotUrl = "https://192-168-100-30.3991e8c5.odoo-iot.com/hw_proxy/scale_read";

            this.tejidoScaleInterval = setInterval(() => {
                fetch(iotUrl, {
                    method: "POST",
                    headers: { 
                        "Content-Type": "application/json" 
                    },
                    body: JSON.stringify({ 
                        jsonrpc: "2.0", 
                        method: "call", 
                        params: {}, // El endpoint scale_read no requiere acciones, lee directo el driver activo
                        id: Math.floor(Math.random() * 1000)
                    })
                })
                .then(response => response.json())
                .then(payload => {
                    const data = payload.result || payload;
                    if (data) {
                        // Formato de respuesta estándar de Odoo IoT para básculas (weight o value)
                        let weightValue = data.weight !== undefined ? data.weight : data.value;
                        weightValue = parseFloat(weightValue);

                        if (!isNaN(weightValue) && weightValue >= 0) {
                            const targetField = 'weight';
                            if (root.data[targetField] !== weightValue) {
                                root.update({ [targetField]: weightValue });
                            }
                        }
                    }
                })
                .catch(err => console.log("Conectando con canal nativo de báscula (Tejido)..."));
            }, 1000);
        }
    }
});