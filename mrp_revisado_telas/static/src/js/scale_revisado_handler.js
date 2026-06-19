/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";
import { useService } from "@web/core/utils/hooks";
import { onMounted, onWillDestroy } from "@odoo/owl";

patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        this.ormService = useService("orm");
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
            
            // 🔒 COMPATIBLE ODOO 19: Usamos el servicio RPC del entorno global nativo de OWL
            this.revisadoScaleInterval = setInterval(() => {
                this.env.services.rpc("/quimibond/scale/read_weight", {})
                .then(data => {
                    if (data && data.status === 'success' && data.weight !== undefined) {
                        const weightValue = parseFloat(data.weight);
                        if (root.data.peso_actual !== weightValue && weightValue >= 0) {
                            root.update({ peso_actual: weightValue });
                        }
                    }
                })
                .catch(err => console.log("Reconectando canal RPC de báscula..."));
            }, 1000);
        }
    }
});