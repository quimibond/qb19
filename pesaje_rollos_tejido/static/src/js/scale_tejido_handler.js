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
            
            // Subdominio HTTPS oficial generado para tu planta (Puerto 443 nativo por SSL)
            const iotUrl = "https://192-168-100-30.3991e8c5.odoo-iot.com/hw_proxy/perform_action";

            this.tejidoScaleInterval = setInterval(() => {
                fetch(iotUrl, {
                    method: "POST",
                    headers: { 
                        "Content-Type": "application/json" 
                    },
                    body: JSON.stringify({ 
                        jsonrpc: "2.0", 
                        method: "call", // Odoo requiere el método "call" para sus controladores web
                        params: { 
                            "action": "read_scale" 
                        },
                        id: Math.floor(Math.random() * 1000) // ID de transacción obligatorio en JSON-RPC
                    })
                })
                .then(response => response.json())
                .then(payload => {
                    const data = payload.result || payload;
                    if (data) {
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
                .catch(err => console.log("Conectando de forma segura con IoT Box local (Tejido)..."));
            }, 1000);
        }
    }
});