"use client";

import { TreeNode } from "@/types/database";
import { useState, useCallback } from "react";
import { ContextMenu, ContextMenuItem } from "@/components/ui/ContextMenu/ContextMenu";
import styles from "./TreeView.module.css";
import {
  ChevronRight,
  Database,
  Table2,
  Columns3,
  KeyRound,
  FolderOpen,
  Folder,
  Eye,
  Copy,
  Trash2,
  RefreshCw
} from "lucide-react";

interface TreeItemProps {
  node: TreeNode;
  level?: number;
  selectedId: string | null;
  onSelect: (node: TreeNode) => void;
  onContextAction?: (action: string, node: TreeNode) => void;
}

function TreeItem({ node, level = 0, selectedId, onSelect, onContextAction }: TreeItemProps) {
  const [isOpen, setIsOpen] = useState(level < 1);
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number } | null>(null);
  const hasChildren = node.children && node.children.length > 0;

  const getIcon = () => {
    const size = 14;
    if (node.isKey) return <KeyRound size={size} className={`${styles.icon} ${styles.key}`} />;

    switch (node.type) {
      case "database":
        return <Database size={size} className={`${styles.icon} ${styles.database}`} />;
      case "schema":
        return isOpen
          ? <FolderOpen size={size} className={`${styles.icon} ${styles.schema}`} />
          : <Folder size={size} className={`${styles.icon} ${styles.schema}`} />;
      case "table":
        return <Table2 size={size} className={`${styles.icon} ${styles.table}`} />;
      case "column":
        return <Columns3 size={size} className={`${styles.icon} ${styles.column}`} />;
      default:
        return <Database size={size} className={styles.icon} />;
    }
  };

  const handleClick = () => {
    if (hasChildren) {
      setIsOpen(!isOpen);
    }
    onSelect(node);
  };

  const handleDoubleClick = () => {
    // Toggle open/close on double click for items with children
    if (hasChildren) {
      setIsOpen(!isOpen);
    }
  };

  const handleContextMenu = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setContextMenu({ x: e.clientX, y: e.clientY });
  }, []);

  const getContextMenuItems = (): ContextMenuItem[] => {
    if (node.type === "table") {
      return [
        { id: "select", label: "Select Top 100", icon: <Eye size={14} /> },
        { id: "copy", label: "Copy Name", icon: <Copy size={14} /> },
        { id: "sep1", label: "", separator: true },
        { id: "refresh", label: "Refresh", icon: <RefreshCw size={14} /> },
        { id: "sep2", label: "", separator: true },
        { id: "drop", label: "Drop Table", icon: <Trash2 size={14} />, danger: true },
      ];
    }
    if (node.type === "column") {
      return [
        { id: "copy", label: "Copy Name", icon: <Copy size={14} /> },
      ];
    }
    return [
      { id: "refresh", label: "Refresh", icon: <RefreshCw size={14} /> },
    ];
  };

  const handleContextAction = (actionId: string) => {
    if (actionId === "copy") {
      navigator.clipboard.writeText(node.name);
    } else if (onContextAction) {
      onContextAction(actionId, node);
    }
  };

  return (
    <div className={styles.node}>
      <div
        className={`${styles.item} ${selectedId === node.id ? styles.selected : ""}`}
        style={{ paddingLeft: `${level * 16 + 8}px` }}
        onClick={handleClick}
        onDoubleClick={handleDoubleClick}
        onContextMenu={handleContextMenu}
      >
        {hasChildren ? (
          <ChevronRight
            size={14}
            className={`${styles.toggle} ${isOpen ? styles.open : ""}`}
          />
        ) : (
          <span className={styles.togglePlaceholder} />
        )}
        {getIcon()}
        <span className={styles.name}>{node.name}</span>
        {node.dataType && <span className={styles.dataType}>{node.dataType}</span>}
        {node.type === "table" && node.children && (
          <span className={styles.count}>{node.children.length}</span>
        )}
      </div>
      {hasChildren && isOpen && (
        <div className={styles.children}>
          {node.children!.map((child) => (
            <TreeItem
              key={child.id}
              node={child}
              level={level + 1}
              selectedId={selectedId}
              onSelect={onSelect}
              onContextAction={onContextAction}
            />
          ))}
        </div>
      )}
      <ContextMenu
        items={getContextMenuItems()}
        position={contextMenu}
        onClose={() => setContextMenu(null)}
        onSelect={handleContextAction}
      />
    </div>
  );
}

interface TreeViewProps {
  data: TreeNode[];
  selectedId: string | null;
  onSelect: (node: TreeNode) => void;
  onContextAction?: (action: string, node: TreeNode) => void;
}

export function TreeView({ data, selectedId, onSelect, onContextAction }: TreeViewProps) {
  if (data.length === 0) {
    return (
      <div className={styles.empty}>
        <Database size={24} className={styles.emptyIcon} />
        <span>No tables found</span>
      </div>
    );
  }

  return (
    <div className={styles.tree}>
      {data.map((node) => (
        <TreeItem
          key={node.id}
          node={node}
          selectedId={selectedId}
          onSelect={onSelect}
          onContextAction={onContextAction}
        />
      ))}
    </div>
  );
}
