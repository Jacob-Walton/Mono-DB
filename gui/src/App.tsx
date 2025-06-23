import { useState } from "react";
import { Sidebar } from "./components/Sidebar";
import { QueryWorkspace } from "./components/QueryWorkspace";
import { ResultsTable } from "./components/ResultsTable";
import { QueryResult } from "./services/DatabaseDriver";
import "./App.css";

function App() {
  const [queryResult, setQueryResult] = useState<QueryResult | null>(null);
  const [queryError, setQueryError] = useState("");
  const [isConnected, setIsConnected] = useState(false);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);

  const handleNewQuery = () => {
    setSelectedTable(null);
    setQueryResult(null);
    setQueryError("");
  };

  const handleTableSelect = (tableName: string) => {
    setSelectedTable(tableName);
    setQueryError("");
  };

  const clearError = () => {
    setQueryError("");
  };

  return (
    <div className="app">
      <header className="app-header">
        <h1>MonoDB</h1>
        <span className="subtitle">Database Management Interface</span>
        {isConnected && (
          <div className="connection-indicator">
            <div className="status-dot connected"></div>
            <span>Connected</span>
          </div>
        )}
      </header>
      
      <div className="app-layout">
        <aside className="app-sidebar">
          <Sidebar 
            onConnectionChange={setIsConnected}
            onNewQuery={handleNewQuery}
            onTableSelect={handleTableSelect}
          />
        </aside>
        
        <main className="app-main">
          <div className="workspace">
            {isConnected ? (
              <QueryWorkspace
                selectedTable={selectedTable}
                onQueryResult={setQueryResult}
                onError={setQueryError}
              />
            ) : (
              <div className="welcome-workspace">
                <div className="welcome-content">
                  <h2>Welcome to MonoDB</h2>
                  <p>Connect to your database to get started</p>
                </div>
              </div>
            )}
          </div>
          
          <div className="results-panel">
            <ResultsTable 
              result={queryResult}
              error={queryError}
              onClearError={clearError}
            />
          </div>
        </main>
      </div>
    </div>
  );
}

export default App;
