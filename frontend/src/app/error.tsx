"use client";

import { useEffect } from "react";
import { Button } from "@/components/ui/button";

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error("Unhandled error:", error);
  }, [error]);

  return (
    <div className="flex min-h-[50vh] flex-col items-center justify-center text-center">
      <h2 className="text-xl font-bold mb-2">Algo salio mal</h2>
      <p className="text-sm text-[var(--muted-foreground)] mb-4 max-w-md">
        Ocurrio un error inesperado. Intenta recargar la pagina.
      </p>
      <Button onClick={reset}>Reintentar</Button>
    </div>
  );
}
