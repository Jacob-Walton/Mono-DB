import React, { useEffect, useState } from "react";
import { RefreshCw, Database } from "lucide-react";
import Editor from "@monaco-editor/react";
import { dbDriver, QueryResult } from "../services/DatabaseDriver";

interface TableViewerProps {
  tableName: string;
  onQueryResult: (result: QueryResult | null) => void;
  onError: (error: string) => void;
}

export const TableViewer: React.FC<TableViewerProps> = ({ tableName, onQueryResult, onError }) => {
  const [loading, setLoading] = useState(false);
  const [limit, setLimit] = useState(100);
  const [currentQuery, setCurrentQuery] = useState("");

  const loadTableData = async () => {
    if (!dbDriver.getConnectionStatus()) {
      onError("Not connected to database");
      return;
    }

    const query = `SELECT * FROM ${tableName} LIMIT ${limit};`;
    setCurrentQuery(query);
    
    // Clear any previous errors when starting new load
    onError("");
    
    setLoading(true);
    try {
      const result = await dbDriver.executeQuery(query);
      onQueryResult(result);
      onError("");
    } catch (error) {
      // Show the error message from the server
      const errorMessage = error instanceof Error ? error.message : String(error);
      onError(errorMessage);
      onQueryResult(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadTableData();
  }, [tableName, limit]);

  return (
    <div className="query-editor">
      <div className="editor-toolbar">
        <div className="limit-control">
          <label>Rows:</label>
          <select
            value={limit}
            onChange={(e) => setLimit(parseInt(e.target.value))}
          >
            <option value={50}>50</option>
            <option value={100}>100</option>
            <option value={500}>500</option>
            <option value={1000}>1000</option>
          </select>
        </div>
        
        <button
          className="toolbar-button"
          onClick={loadTableData}
          disabled={loading}
        >
          <RefreshCw size={16} className={loading ? "spinning" : ""} />
          Refresh
        </button>
        
        <div className="connection-status">
          <Database size={16} />
          <span className={dbDriver.getConnectionStatus() ? "connected" : "disconnected"}>
            Table: {tableName}
          </span>
        </div>
      </div>
      
      <div className="editor-container">
        <Editor
          height="300px"
          language="sql"
          theme="vs"
          value={currentQuery}
          options={{
            readOnly: true,
            minimap: { enabled: false },
            fontSize: 14,
            lineNumbers: "on",
            wordWrap: "on",
            automaticLayout: true,
            scrollBeyondLastLine: false,
            contextmenu: false,
            selectOnLineNumbers: false,
            domReadOnly: true,
            cursorStyle: "line",
            readOnlyMessage: { value: "This query is read-only" },
            links: false,
            colorDecorators: false,
            selectionHighlight: false,
            renderLineHighlight: "none",
            hover: { enabled: false },
            parameterHints: { enabled: false },
            suggestOnTriggerCharacters: false,
            acceptSuggestionOnEnter: "off",
            acceptSuggestionOnCommitCharacter: false,
            quickSuggestions: false,
          }}
        />
      </div>
    </div>
  );
};