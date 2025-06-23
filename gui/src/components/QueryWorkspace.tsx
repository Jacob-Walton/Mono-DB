import React, { useState } from "react";
import { QueryEditor } from "./QueryEditor";
import { TableViewer } from "./TableViewer";
import { QueryResult } from "../services/DatabaseDriver";

interface QueryWorkspaceProps {
  selectedTable: string | null;
  onQueryResult: (result: QueryResult | null) => void;
  onError: (error: string) => void;
}

export const QueryWorkspace: React.FC<QueryWorkspaceProps> = ({ 
  selectedTable, 
  onQueryResult, 
  onError 
}) => {
  const [mode, setMode] = useState<'query' | 'table'>('query');

  React.useEffect(() => {
    if (selectedTable) {
      setMode('table');
      // Clear any existing errors when switching to table view
      onError("");
    }
  }, [selectedTable, onError]);

  const handleModeChange = (newMode: 'query' | 'table') => {
    setMode(newMode);
    // Clear errors when switching modes
    onError("");
  };

  return (
    <div className="query-workspace">
      <div className="workspace-header">
        <div className="workspace-tabs">
          <button 
            className={`workspace-tab ${mode === 'query' ? 'active' : ''}`}
            onClick={() => handleModeChange('query')}
          >
            Query Editor
          </button>
          {selectedTable && (
            <button 
              className={`workspace-tab ${mode === 'table' ? 'active' : ''}`}
              onClick={() => handleModeChange('table')}
            >
              {selectedTable}
            </button>
          )}
        </div>
      </div>
      
      <div className="workspace-content">
        {mode === 'query' ? (
          <QueryEditor
            onQueryResult={onQueryResult}
            onError={onError}
          />
        ) : (
          selectedTable && (
            <TableViewer
              tableName={selectedTable}
              onQueryResult={onQueryResult}
              onError={onError}
            />
          )
        )}
      </div>
    </div>
  );
};
