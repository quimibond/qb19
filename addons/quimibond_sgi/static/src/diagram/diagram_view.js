/** @odoo-module **/
// Envolturas del motor de diagramas (54.0.0):
//  · vista «sgi_diagram» (registro "views"): entra al selector de vistas de
//    una acción junto a lista, kanban y formulario. Arquitectura:
//    <sgi_diagram kind="risk_matrix" kinds="…"/>.
//  · acción cliente `sgi_diagram` (registro "actions"): la abren los botones
//    «Diagrama» de las fichas con el diagrama y el registro en el contexto.
import { Component } from "@odoo/owl";
import { registry } from "@web/core/registry";
import { Layout } from "@web/search/layout";
import { standardViewProps } from "@web/views/standard_view_props";
import { SgiDiagram } from "./diagram";

function kindsFrom(value) {
    return String(value || "").split(",").map((k) => k.trim()).filter(Boolean);
}

function diagramPropsFromContext(ctx, fallbackKind) {
    const props = {
        kind: ctx.sgi_diagram_kind || fallbackKind,
        // OWL valida los props: `null` no es Number ni Boolean, así que lo
        // que falta se manda como `false` (las vistas de lista sin proceso
        // activo llegaban con null y reventaban el mapa de procesos).
        resId: Number(ctx.sgi_diagram_res_id || ctx.default_process_id || ctx.active_process_id) || false,
        params: ctx.sgi_diagram_params || {},
        selected: ctx.sgi_diagram_selected ? String(ctx.sgi_diagram_selected) : false,
    };
    const kinds = kindsFrom(ctx.sgi_diagram_kinds);
    if (kinds.length) {
        props.kinds = kinds;
    }
    return props;
}

export class SgiDiagramViewController extends Component {
    static template = "quimibond_sgi.DiagramView";
    static components = { Layout, SgiDiagram };
    static props = { ...standardViewProps, archInfo: Object };

    setup() {
        const ctx = this.props.context || {};
        this.diagramProps = diagramPropsFromContext(ctx, this.props.archInfo.kind);
        if (!ctx.sgi_diagram_kind) {
            this.diagramProps.kind = this.props.archInfo.kind;
        }
        if (!this.diagramProps.kinds && this.props.archInfo.kinds.length) {
            this.diagramProps.kinds = this.props.archInfo.kinds;
        }
    }
}

export const sgiDiagramView = {
    type: "sgi_diagram",
    Controller: SgiDiagramViewController,
    searchMenuTypes: [],
    props: (genericProps) => {
        const { arch } = genericProps;
        const kind = arch.getAttribute("kind");
        const kinds = kindsFrom(arch.getAttribute("kinds"));
        return { ...genericProps, archInfo: { kind, kinds: kinds.length ? kinds : [kind] } };
    },
};

registry.category("views").add("sgi_diagram", sgiDiagramView);

export class SgiDiagramAction extends Component {
    static template = "quimibond_sgi.DiagramAction";
    static components = { Layout, SgiDiagram };
    static props = ["*"];

    setup() {
        const ctx = (this.props.action && this.props.action.context) || {};
        this.diagramProps = diagramPropsFromContext(ctx, "process_map");
        this.display = { controlPanel: {} };
    }
}

registry.category("actions").add("sgi_diagram", SgiDiagramAction);
