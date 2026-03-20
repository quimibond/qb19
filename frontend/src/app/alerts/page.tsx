"use client";

import { useEffect, useState } from "react";
import { supabase } from "@/lib/supabase";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { timeAgo } from "@/lib/utils";
import { AlertTriangle, Check, Eye } from "lucide-react";

interface Alert {
  id: string;
  alert_type: string;
  severity: string;
  title: string;
  description: string;
  contact_name: string;
  created_at: string;
  state: string;
  is_read: boolean;
}

const severityVariant: Record<string, "destructive" | "warning" | "info"> = {
  critical: "destructive",
  high: "destructive",
  medium: "warning",
  low: "info",
};

const typeLabel: Record<string, string> = {
  no_response: "Sin respuesta",
  sentiment: "Sentimiento",
  opportunity: "Oportunidad",
  risk: "Riesgo",
  accountability: "Cumplimiento",
  communication_gap: "Comunicacion",
};

export default function AlertsPage() {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [loading, setLoading] = useState(true);
  const [stateFilter, setStateFilter] = useState<string>("new");
  const [error, setError] = useState<string | null>(null);
  const [feedback, setFeedback] = useState<string | null>(null);

  useEffect(() => {
    async function fetchAlerts() {
      setError(null);
      try {
        let query = supabase
          .from("alerts")
          .select("*")
          .order("created_at", { ascending: false })
          .limit(50);

        if (stateFilter !== "all") {
          query = query.eq("state", stateFilter);
        }

        const { data, error: queryError } = await query;
        if (queryError) {
          setError(queryError.message);
        }
        setAlerts(data || []);
      } catch {
        setError("Error de conexion con Supabase");
      } finally {
        setLoading(false);
      }
    }
    fetchAlerts();
  }, [stateFilter]);

  function showFeedback(msg: string) {
    setFeedback(msg);
    setTimeout(() => setFeedback(null), 2000);
  }

  async function markRead(id: string) {
    const { error } = await supabase.from("alerts").update({ is_read: true, state: "acknowledged" }).eq("id", id);
    if (error) {
      showFeedback("Error al marcar como vista");
      return;
    }
    setAlerts((prev) =>
      prev.map((a) => (a.id === id ? { ...a, is_read: true, state: "acknowledged" } : a))
    );
    showFeedback("Alerta marcada como vista");
  }

  async function resolve(id: string) {
    const { error } = await supabase.from("alerts").update({ state: "resolved" }).eq("id", id);
    if (error) {
      showFeedback("Error al resolver alerta");
      return;
    }
    setAlerts((prev) => prev.map((a) => (a.id === id ? { ...a, state: "resolved" } : a)));
    showFeedback("Alerta resuelta");
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-bold">Alertas</h1>
          <p className="text-sm text-[var(--muted-foreground)]">Alertas de inteligencia sobre clientes y operaciones</p>
        </div>
        <div className="flex gap-1">
          {["new", "acknowledged", "resolved", "all"].map((f) => (
            <button
              key={f}
              onClick={() => { setStateFilter(f); setLoading(true); }}
              className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${
                stateFilter === f
                  ? "bg-[var(--primary)] text-white"
                  : "text-[var(--muted-foreground)] hover:bg-[var(--accent)]"
              }`}
            >
              {f === "all" ? "Todas" : f === "new" ? "Nuevas" : f === "acknowledged" ? "Vistas" : "Resueltas"}
            </button>
          ))}
        </div>
      </div>

      {feedback && (
        <div className="rounded-lg border border-emerald-500/30 bg-emerald-500/10 p-3 text-sm text-emerald-400">
          {feedback}
        </div>
      )}

      {error && (
        <div className="rounded-lg border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">
          {error}
        </div>
      )}

      {loading ? (
        <div className="flex justify-center py-12">
          <div className="animate-pulse text-[var(--muted-foreground)]">Cargando alertas...</div>
        </div>
      ) : alerts.length === 0 ? (
        <Card>
          <CardContent className="flex flex-col items-center justify-center py-12">
            <AlertTriangle className="mb-3 h-10 w-10 text-[var(--muted-foreground)] opacity-50" />
            <p className="text-sm text-[var(--muted-foreground)]">No hay alertas en esta categoria.</p>
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-3">
          {alerts.map((alert) => (
            <Card key={alert.id} className={!alert.is_read ? "border-l-2 border-l-[var(--primary)]" : ""}>
              <CardContent className="flex items-start justify-between gap-4 p-4">
                <div className="flex-1 min-w-0">
                  <div className="flex flex-wrap items-center gap-2 mb-1">
                    <Badge variant={severityVariant[alert.severity] || "info"}>
                      {alert.severity}
                    </Badge>
                    <Badge variant="outline">
                      {typeLabel[alert.alert_type] || alert.alert_type}
                    </Badge>
                    {alert.contact_name && (
                      <span className="text-xs text-[var(--muted-foreground)]">{alert.contact_name}</span>
                    )}
                  </div>
                  <p className="font-medium text-sm">{alert.title}</p>
                  <p className="mt-1 text-sm text-[var(--muted-foreground)] line-clamp-2">{alert.description}</p>
                  <p className="mt-1 text-xs text-[var(--muted-foreground)]">{timeAgo(alert.created_at)}</p>
                </div>
                <div className="flex shrink-0 gap-1">
                  {alert.state === "new" && (
                    <Button variant="ghost" size="icon" onClick={() => markRead(alert.id)} title="Marcar como vista">
                      <Eye className="h-4 w-4" />
                    </Button>
                  )}
                  {alert.state !== "resolved" && (
                    <Button variant="ghost" size="icon" onClick={() => resolve(alert.id)} title="Resolver">
                      <Check className="h-4 w-4" />
                    </Button>
                  )}
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
