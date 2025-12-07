export interface TreeNode {
    id: string;
    name: string;
    type: "database" | "schema" | "table" | "column";
    children?: TreeNode[];
    isKey?: boolean;
    dataType?: string;
}

export interface Tab {
    id: string;
    name: string;
    content: string;
    saved: boolean;
}

export interface QueryResult {
    columns: string[];
    rows: Record<string, unknown>[];
    rowCount: number;
    executionTime: number;
}

export interface ConnectionStatus {
    connected: boolean;
    host: string;
    port: number;
    database: string;
    user: string;
}