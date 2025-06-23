import { invoke } from "@tauri-apps/api/core";

export interface QueryResult {
  columns: string[];
  rows: any[][];
  row_count: number;
  execution_time: number;
}

export interface ConnectionConfig {
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
}

export class DatabaseDriver {
  private isConnected = false;
  private connectionConfig: ConnectionConfig | null = null;

  async connect(config: ConnectionConfig): Promise<boolean> {
    try {
      await invoke("db_connect", { config });
      this.connectionConfig = config;
      this.isConnected = true;
      return true;
    } catch (error) {
      console.error("Connection failed:", error);
      return false;
    }
  }

  async disconnect(): Promise<void> {
    if (this.isConnected) {
      await invoke("db_disconnect");
      this.isConnected = false;
      this.connectionConfig = null;
    }
  }

  async executeQuery(query: string): Promise<QueryResult> {
    if (!this.isConnected) {
      throw new Error("No database connection");
    }

    try {
      return await invoke("db_execute_query", { query });
    } catch (error) {
      // Re-throw the error with the original message from the server
      throw new Error(String(error));
    }
  }

  async getTables(): Promise<string[]> {
    if (!this.isConnected) {
      return [];
    }

    return await invoke("db_get_tables");
  }

  getConnectionStatus(): boolean {
    return this.isConnected;
  }

  getConnectionConfig(): ConnectionConfig | null {
    return this.connectionConfig;
  }
}

export const dbDriver = new DatabaseDriver();