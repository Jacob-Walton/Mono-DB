import React, { useState } from "react";
import { Server, Database, Table, ChevronRight, ChevronDown, Plus } from "lucide-react";
import { ConnectionPanel } from "./ConnectionPanel";
import { dbDriver } from "../services/DatabaseDriver";

interface SidebarProps {
  onConnectionChange: (connected: boolean) => void;
  onNewQuery: () => void;
  onTableSelect: (tableName: string) => void;
}

export const Sidebar: React.FC<SidebarProps> = ({ onConnectionChange, onNewQuery, onTableSelect }) => {
  const [expandedSections, setExpandedSections] = useState<Set<string>>(new Set(["connection"]));
  const [tables, setTables] = useState<string[]>([]);
  const [isConnected, setIsConnected] = useState(false);

  const toggleSection = (section: string) => {
    const newExpanded = new Set(expandedSections);
    if (newExpanded.has(section)) {
      newExpanded.delete(section);
    } else {
      newExpanded.add(section);
    }
    setExpandedSections(newExpanded);
  };

  const handleConnectionChange = async (connected: boolean) => {
    setIsConnected(connected);
    onConnectionChange(connected);
    
    if (connected) {
      try {
        const tableList = await dbDriver.getTables();
        setTables(tableList);
        if (tableList.length > 0) {
          setExpandedSections(prev => new Set([...prev, "database", "tables"]));
        }
      } catch (error) {
        console.error("Failed to load tables:", error);
        setTables([]);
      }
    } else {
      setTables([]);
    }
  };

  return (
    <div className="sidebar">
      {/* Connection Section */}
      <div className="sidebar-section">
        <div 
          className="sidebar-section-header"
          onClick={() => toggleSection("connection")}
        >
          {expandedSections.has("connection") ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
          <Server size={16} />
          <span>Connection</span>
        </div>
        {expandedSections.has("connection") && (
          <div className="sidebar-section-content">
            <ConnectionPanel onConnectionChange={handleConnectionChange} />
          </div>
        )}
      </div>

      {/* Database Explorer */}
      {isConnected && (
        <div className="sidebar-section">
          <div 
            className="sidebar-section-header"
            onClick={() => toggleSection("database")}
          >
            {expandedSections.has("database") ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
            <Database size={16} />
            <span>Schema</span>
          </div>
          {expandedSections.has("database") && (
            <div className="sidebar-section-content">
              <div className="database-tree">
                {tables.length > 0 ? (
                  <div>
                    <div 
                      className="tree-item"
                      onClick={() => toggleSection("tables")}
                    >
                      {expandedSections.has("tables") ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                      <Table size={14} />
                      <span>Tables ({tables.length})</span>
                    </div>
                    {expandedSections.has("tables") && (
                      <div className="tree-children">
                        {tables.map(table => (
                          <div 
                            key={table}
                            className="tree-leaf"
                            onClick={() => onTableSelect(table)}
                          >
                            <Table size={12} />
                            <span>{table}</span>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                ) : (
                  <div className="empty-schema">No tables found</div>
                )}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Quick Actions */}
      <div className="sidebar-section">
        <div className="sidebar-section-header">
          <Plus size={16} />
          <span>Actions</span>
        </div>
        <div className="sidebar-section-content">
          <button 
            className="sidebar-button" 
            onClick={onNewQuery}
            disabled={!isConnected}
          >
            New Query
          </button>
        </div>
      </div>
    </div>
  );
};
