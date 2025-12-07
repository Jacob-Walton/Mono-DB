import { useCallback, useState, useEffect, useRef } from "react";

// Types matching Tauri backend
export interface ConnectionInfo {
  host: string;
  port: number;
  connected: boolean;
}

export interface QueryResult {
  columns: string[];
  rows: Record<string, unknown>[];
  row_count: number;
  execution_time: number;
  result_type: string;
}

export interface TableInfo {
  name: string;
  type: string;
}

// Lazy check for Tauri
function checkIsTauri(): boolean {
  if (typeof window === "undefined") return false;
  return "__TAURI_INTERNALS__" in window || "__TAURI__" in window;
}

// Dynamic import for Tauri API
async function invoke<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  const { invoke: tauriInvoke } = await import("@tauri-apps/api/core");
  return tauriInvoke<T>(cmd, args);
}

export function useDatabase() {
  const [connection, setConnection] = useState<ConnectionInfo | null>(null);
  const [isConnecting, setIsConnecting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [isTauri, setIsTauri] = useState(false);
  const initialized = useRef(false);

  // Check if we're in Tauri on mount
  useEffect(() => {
    if (initialized.current) return;
    initialized.current = true;

    const inTauri = checkIsTauri();
    setIsTauri(inTauri);

    if (inTauri) {
      invoke<ConnectionInfo | null>("get_connection_status")
        .then((info) => setConnection(info ?? null))
        .catch(() => setConnection(null));
    }
  }, []);

  const connect = useCallback(async (host: string, port: number) => {
    if (!checkIsTauri()) {
      setError("Not running in Tauri environment");
      return false;
    }

    setIsConnecting(true);
    setError(null);

    try {
      const info = await invoke<ConnectionInfo>("connect", { host, port });
      setConnection(info);
      return true;
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      return false;
    } finally {
      setIsConnecting(false);
    }
  }, []);

  const disconnect = useCallback(async () => {
    if (!checkIsTauri()) return;

    try {
      await invoke("disconnect");
      setConnection(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  const executeQuery = useCallback(async (query: string): Promise<QueryResult | null> => {
    if (!checkIsTauri()) {
      setError("Not running in Tauri environment");
      return null;
    }

    if (!connection?.connected) {
      setError("Not connected to database");
      return null;
    }

    try {
      const result = await invoke<QueryResult>("execute_query", { query });
      return result;
    } catch (e) {
      const errorMsg = e instanceof Error ? e.message : String(e);
      setError(errorMsg);
      throw new Error(errorMsg);
    }
  }, [connection]);

  const listTables = useCallback(async (): Promise<TableInfo[]> => {
    if (!checkIsTauri()) {
      return [];
    }

    if (!connection?.connected) {
      return [];
    }

    try {
      return await invoke<TableInfo[]>("list_tables");
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      return [];
    }
  }, [connection]);

  const clearError = useCallback(() => setError(null), []);

  return {
    connection,
    isConnecting,
    isConnected: connection?.connected ?? false,
    error,
    connect,
    disconnect,
    executeQuery,
    listTables,
    clearError,
    isTauri,
  };
}
