/** @odoo-module */
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";

// Dejamos el patch activo para que Odoo compile el archivo correctamente,
// pero vaciamos la lógica de conexión para evitar que salte el error del Longpolling.
patch(FormController.prototype, {
    setup() {
        super.setup(...arguments);
        // No ejecutamos ninguna conexión automática a la IoT Box
    },

    _connectToTejidoScale() {
        // Desactivado temporalmente: el sistema operará en modo manual
    }
});