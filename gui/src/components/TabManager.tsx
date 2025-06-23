import React, { useState } from "react";
import { X } from "lucide-react";
import { QueryEditor } from "./QueryEditor";
import { TableViewer } from "./TableViewer";
import { QueryResult } from "../services/DatabaseDriver";

interface Tab {
  id: string;
  title: string;
  type: string;
  content?: any;
  isActive?: boolean;
  hasChanges?: boolean;
}

interface TabManagerProps {
  onQueryResult: (result: QueryResult | null) => void;
  onError: (error: string) => void;
}

export const TabManager: React.FC<TabManagerProps> = ({ onQueryResult, onError }) => {
  const [tabs, setTabs] = useState<Tab[]>([
    { id: "welcome", title: "Welcome", type: "welcome", isActive: true }
  ]);
  const [activeTabId, setActiveTabId] = useState("welcome");

  const addTab = (tab: Tab) => {
    const existingTab = tabs.find(t => t.id === tab.id);
    if (existingTab) {
      setActiveTabId(tab.id);
      return;
    }

    setTabs(prev => [
      ...prev.map(t => ({ ...t, isActive: false })),
      { ...tab, isActive: true }
    ]);
    setActiveTabId(tab.id);
  };

  const closeTab = (tabId: string) => {
    const tabIndex = tabs.findIndex(t => t.id === tabId);
    if (tabIndex === -1) return;

    const newTabs = tabs.filter(t => t.id !== tabId);
    
    if (activeTabId === tabId && newTabs.length > 0) {
      const newActiveIndex = Math.min(tabIndex, newTabs.length - 1);
      setActiveTabId(newTabs[newActiveIndex].id);
    }
    
    setTabs(newTabs);
  };

  const activateTab = (tabId: string) => {
    setActiveTabId(tabId);
    setTabs(prev => prev.map(t => ({ ...t, isActive: t.id === tabId })));
  };

  const activeTab = tabs.find(t => t.id === activeTabId);

  const renderTabContent = (tab: Tab) => {
    switch (tab.type) {
      case "query":
        return (
          <QueryEditor
            onQueryResult={onQueryResult}
            onError={onError}
          />
        );
      case "table":
        return (
          <TableViewer
            tableName={tab.content?.tableName}
            onQueryResult={onQueryResult}
            onError={onError}
          />
        );
      case "welcome":
        return (
          <div className="welcome-tab">
            <div className="welcome-content">
              <h2>Welcome to MonoDB</h2>
              <p>Connect to your database and start exploring.</p>
              <div className="welcome-actions">
                <button 
                  className="welcome-button"
                  onClick={() => addTab({ id: `query-${Date.now()}`, title: "New Query", type: "query" })}
                >
                  Create New Query
                </button>
              </div>
            </div>
          </div>
        );
      default:
        return <div>Unknown tab type</div>;
    }
  };

  // Expose addTab function globally for sidebar
  React.useEffect(() => {
    (window as any).addTab = addTab;
  }, []);

  return (
    <div className="tab-manager">
      <div className="tab-bar">
        <div className="tab-list">
          {tabs.map(tab => (
            <div
              key={tab.id}
              className={`tab ${tab.id === activeTabId ? 'active' : ''}`}
              onClick={() => activateTab(tab.id)}
            >
              <span className="tab-title">{tab.title}</span>
              {tab.hasChanges && <div className="tab-indicator" />}
              {tab.id !== "welcome" && (
                <button
                  className="tab-close"
                  onClick={(e) => {
                    e.stopPropagation();
                    closeTab(tab.id);
                  }}
                >
                  <X size={12} />
                </button>
              )}
            </div>
          ))}
        </div>
      </div>

      <div className="tab-content">
        {activeTab && renderTabContent(activeTab)}
      </div>
    </div>
  );
};
