/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { useService } from "@web/core/utils/hooks";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        // Usamos el servicio de red unificado y oficial de Odoo 19
        this.jsonrpcService = useService("jsonrpc"); 
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
            
            // 🔒 CONEXIÓN ESTABLE ODOO 19
            this.tejidoScaleInterval = setInterval(() => {
                this.jsonrpcService("/quimibond/scale/read_weight", {})
                .then(data => {
                    if (data && data.status === 'success' && data.weight !== undefined) {
                        const weightValue = parseFloat(data.weight);
                        const targetField = 'weight';
                        if (root.data[targetField] !== weightValue && weightValue >= 0) {
                            root.update({ [targetField]: weightValue });
                        }
                    }
                })
                .catch(err => console.log("Sincronizando peso con Python..."));
            }, 1000);
        }
    }
});