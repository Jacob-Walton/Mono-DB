"use client";

import { TreeNode } from "@/types/database";
import { useState } from "react";
import styles from "@/styles/components/TreeView.module.css";
import { ChevronRight, Database, KeyRound } from "lucide-react";

interface TreeItemProps {
    node: TreeNode;
    level?: number;
    selectedId: string | null;
    onSelect: (node: TreeNode) => void;
}

function TreeItem({ node, level = 0, selectedId, onSelect }: TreeItemProps) {
    const [isOpen, setIsOpen] = useState(level < 2);
    const hasChildren = node.children && node.children.length > 0;

    const getIcon = () => {
        const iconProps = { size: 14, className: `${styles.icon} ${styles[node.type]}` };
        if (node.isKey) return <KeyRound {...iconProps} className={`${styles.icon} ${styles.key}`} />;
        switch (node.type) {
            case "database": return <Database {...iconProps} />;
            case "schema": return <Database {...iconProps} />;
            case "table": return <Database {...iconProps} />;
            case "column": return <Database {...iconProps} />;
        }
    };

    const handleClick = () => {
        if (hasChildren) {
            setIsOpen(!isOpen);
        }
        onSelect(node);
    };

  return (
    <div className={styles.node}>
      <div
        className={`${styles.item} ${selectedId === node.id ? styles.selected : ""}`}
        style={{ paddingLeft: `${level * 12 + 8}px` }}
        onClick={handleClick}
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
            />
          ))}
        </div>
      )}
    </div>
  );
}

interface TreeViewProps {
    data: TreeNode[];
    selectedId: string | null;
    onSelect: (node: TreeNode) => void;
}

export function TreeView({ data, selectedId, onSelect }: TreeViewProps) {
    return (
        <div className={styles.tree}>
            {data.map((node) => (
                <TreeItem
                    key={node.id}
                    node={node}
                    selectedId={selectedId}
                    onSelect={onSelect}
                />
            ))}
        </div>
    );
}