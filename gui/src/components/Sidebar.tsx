"use client";

import { TreeNode } from "@/types/database";
import { useState } from "react";
import { TreeView } from "@/components/TreeView";
import styles from "@/styles/components/Sidebar.module.css";
import { Plus, Search } from "lucide-react";

interface SidebarProps {
    treeData: TreeNode[];
    onTableSelect: (node: TreeNode) => void;
}

export function Sidebar({ treeData, onTableSelect }: SidebarProps) {
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

    const handleSelect = (node: TreeNode) => {
        setSelectedId(node.id);
        if (node.type === "table") {
            onTableSelect(node);
        }
    };

    return (
        <aside className={styles.sidebar}>
            <div className={styles.header}>
                <div className={styles.searchBox}>
                    <Search size={14} className={styles.searchIcon} />
                    <input
                        type="text"
                        placeholder="Search tables..."
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                    />
                </div>
            </div>

            <div className={styles.treeContainer}>
                <TreeView
                    data={filteredData}
                    selectedId={selectedId}
                    onSelect={handleSelect}
                />
            </div>

            <div className={styles.footer}>
                <button className={styles.addBtn}>
                    <Plus size={14} />
                    <span>New Connection</span>
                </button>
            </div>
        </aside>
    );
}