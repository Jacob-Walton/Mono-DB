import React, { useState, useRef } from "react";
import Editor from "@monaco-editor/react";
import { Play, Save, Database } from "lucide-react";
import { dbDriver, QueryResult } from "../services/DatabaseDriver";

interface QueryEditorProps {
  onQueryResult: (result: QueryResult | null) => void;
  onError: (error: string) => void;
}

export const QueryEditor: React.FC<QueryEditorProps> = ({ onQueryResult, onError }) => {
  const [query, setQuery] = useState("SELECT * FROM users LIMIT 10;");
  const [isExecuting, setIsExecuting] = useState(false);
  const editorRef = useRef<any>(null);

  const handleEditorDidMount = (editor: any) => {
    editorRef.current = editor;
    editor.focus();
  };

  const executeQuery = async () => {
    if (!query.trim()) return;

    // Clear any previous errors when starting new execution
    onError("");
    
    setIsExecuting(true);
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
      setIsExecuting(false);
    }
  };

  const saveQuery = () => {
    console.log("Saving query:", query);
  };

  return (
    <div className="query-editor">
      <div className="editor-toolbar">
        <button 
          onClick={executeQuery} 
          disabled={isExecuting || !dbDriver.getConnectionStatus()}
          className="toolbar-button primary"
        >
          <Play size={16} />
          {isExecuting ? "Executing..." : "Execute"}
        </button>
        <button onClick={saveQuery} className="toolbar-button">
          <Save size={16} />
          Save
        </button>
        <div className="connection-status">
          <Database size={16} />
          <span className={dbDriver.getConnectionStatus() ? "connected" : "disconnected"}>
            {dbDriver.getConnectionStatus() ? "Connected" : "Disconnected"}
          </span>
        </div>
      </div>
      
      <div className="editor-container">
        <Editor
          height="300px"
          language="sql"
          theme="vs"
          value={query}
          onChange={(value) => setQuery(value || "")}
          onMount={handleEditorDidMount}
          options={{
            minimap: { enabled: false },
            fontSize: 14,
            lineNumbers: "on",
            wordWrap: "on",
            automaticLayout: true,
            scrollBeyondLastLine: false,
          }}
        />
      </div>
    </div>
  );
};