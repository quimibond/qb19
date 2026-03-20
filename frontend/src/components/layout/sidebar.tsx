"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState } from "react";
import { cn } from "@/lib/utils";
import {
  Brain,
  LayoutDashboard,
  MessageSquare,
  FileText,
  AlertTriangle,
  CheckSquare,
  Users,
  Menu,
  X,
} from "lucide-react";

const navItems = [
  { href: "/dashboard", label: "Dashboard", icon: LayoutDashboard },
  { href: "/chat", label: "Preguntar", icon: MessageSquare },
  { href: "/briefings", label: "Briefings", icon: FileText },
  { href: "/alerts", label: "Alertas", icon: AlertTriangle },
  { href: "/actions", label: "Acciones", icon: CheckSquare },
  { href: "/contacts", label: "Contactos", icon: Users },
];

export function Sidebar() {
  const pathname = usePathname();
  const [open, setOpen] = useState(false);

  return (
    <>
      {/* Mobile toggle */}
      <button
        onClick={() => setOpen(true)}
        className="fixed left-4 top-4 z-50 rounded-md p-2 hover:bg-[var(--accent)] lg:hidden"
        aria-label="Abrir menu"
      >
        <Menu className="h-5 w-5" />
      </button>

      {/* Overlay */}
      {open && (
        <div
          className="fixed inset-0 z-40 bg-black/50 lg:hidden"
          onClick={() => setOpen(false)}
        />
      )}

      {/* Sidebar */}
      <aside
        className={cn(
          "fixed inset-y-0 left-0 z-50 flex w-64 flex-col border-r border-[var(--border)] bg-[var(--background)] transition-transform duration-200",
          open ? "translate-x-0" : "-translate-x-full lg:translate-x-0"
        )}
      >
        {/* Logo */}
        <div className="flex h-16 items-center justify-between px-6 border-b border-[var(--border)]">
          <div className="flex items-center gap-3">
            <Brain className="h-7 w-7 text-[var(--primary)]" />
            <div>
              <p className="font-bold text-sm">Quimibond</p>
              <p className="text-xs text-[var(--muted-foreground)]">Intelligence</p>
            </div>
          </div>
          <button
            onClick={() => setOpen(false)}
            className="rounded-md p-1 hover:bg-[var(--accent)] lg:hidden"
            aria-label="Cerrar menu"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        {/* Navigation */}
        <nav className="flex-1 space-y-1 p-3">
          {navItems.map((item) => {
            const isActive =
              pathname === item.href || pathname?.startsWith(item.href + "/");
            return (
              <Link
                key={item.href}
                href={item.href}
                onClick={() => setOpen(false)}
                className={cn(
                  "flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm font-medium transition-colors",
                  isActive
                    ? "bg-[var(--primary)]/10 text-[var(--primary)]"
                    : "text-[var(--muted-foreground)] hover:bg-[var(--accent)] hover:text-[var(--foreground)]",
                )}
              >
                <item.icon className="h-4 w-4" />
                {item.label}
              </Link>
            );
          })}
        </nav>

        {/* Footer */}
        <div className="border-t border-[var(--border)] p-4">
          <p className="text-xs text-[var(--muted-foreground)]">
            Intelligence System v5.1
          </p>
        </div>
      </aside>
    </>
  );
}
