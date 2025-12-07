"use client";

import { TreeNode } from "@/types/database";
import { useState, useCallback } from "react";
import { TreeView } from "@/components/ui/TreeView/TreeView";
import styles from "./Sidebar.module.css";
import { Loader2, Plug, RefreshCw, Search } from "lucide-react";

interface SidebarProps {
  treeData: TreeNode[];
  onTableSelect: (node: TreeNode) => void;
  onContextAction?: (action: string, node: TreeNode) => void;
  isConnected: boolean;
  isLoading?: boolean;
  onRefresh?: () => void;
  onConnectClick?: () => void;
}

export function Sidebar({
  treeData,
  onTableSelect,
  onContextAction,
  isConnected,
  isLoading = false,
  onRefresh,
  onConnectClick,
}: SidebarProps) {
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedId, setSelectedId] = useState<string | null>(null);

  const filterTree = (nodes: TreeNode[], query: string): TreeNode[] => {
    if (!query) return nodes;

    return nodes.reduce<TreeNode[]>((acc, node) => {
      const matchesQuery = node.name.toLowerCase().includes(query.toLowerCase());
      const filteredChildren = node.children ? filterTree(node.children, query) : [];

      if (matchesQuery || filteredChildren.length > 0) {
        acc.push({
          ...node,
          children: filteredChildren.length > 0 ? filteredChildren : node.children,
        });
      }

      return acc;
    }, []);
  };

  const filteredData = filterTree(treeData, searchQuery);

  const handleSelect = useCallback((node: TreeNode) => {
    setSelectedId(node.id);
    // Don't auto-select table on click, only via context menu
  }, []);

  const handleContextAction = useCallback((action: string, node: TreeNode) => {
    if (action === "select") {
      onTableSelect(node);
    } else if (onContextAction) {
      onContextAction(action, node);
    }
  }, [onTableSelect, onContextAction]);

  return (
    <aside className={styles.sidebar}>
      <div className={styles.header}>
        <div className={styles.titleRow}>
          <span className={styles.title}>Object Explorer</span>
          {isConnected && onRefresh && (
            <button
              className={styles.refreshBtn}
              onClick={onRefresh}
              disabled={isLoading}
              title="Refresh"
            >
              <RefreshCw size={14} className={isLoading ? styles.spinning : ""} />
            </button>
          )}
        </div>
        {isConnected && (
          <div className={styles.searchBox}>
            <Search size={14} className={styles.searchIcon} />
            <input
              type="text"
              placeholder="Search..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
          </div>
        )}
      </div>

      <div className={styles.treeContainer}>
        {!isConnected ? (
          <div className={styles.notConnected}>
            <Plug size={24} className={styles.notConnectedIcon} />
            <span>Not connected</span>
            {onConnectClick && (
              <button className={styles.connectBtn} onClick={onConnectClick}>
                Connect to Server
              </button>
            )}
          </div>
        ) : isLoading ? (
          <div className={styles.loading}>
            <Loader2 size={20} className={styles.spinner} />
            <span>Loading...</span>
          </div>
        ) : (
          <TreeView
            data={filteredData}
            selectedId={selectedId}
            onSelect={handleSelect}
            onContextAction={handleContextAction}
          />
        )}
      </div>
    </aside>
  );
}
