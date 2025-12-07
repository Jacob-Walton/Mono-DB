"use client";

import { Tab } from "@/types/database";
import styles from "@/styles/components/QueryTabs.module.css";
import { FileCode, Plus, X } from "lucide-react";

interface QueryTabsProps {
    tabs: Tab[];
    activeTabId: string;
    onTabSelect: (tabId: string) => void;
    onTabClose: (tabId: string) => void;
    onTabCreate: () => void;
}

export function QueryTabs({
    tabs,
    activeTabId,
    onTabSelect,
    onTabClose,
    onTabCreate,
}: QueryTabsProps) {
  return (
    <div className={styles.tabs}>
      {tabs.map((tab) => (
        <button
          key={tab.id}
          className={`${styles.tab} ${activeTabId === tab.id ? styles.active : ""}`}
          onClick={() => onTabSelect(tab.id)}
        >
          <FileCode size={14} className={styles.icon} />
          <span className={styles.name}>{tab.name}</span>
          {!tab.saved && <span className={styles.unsavedDot} />}
          <span
            className={styles.close}
            onClick={(e) => {
              e.stopPropagation();
              onTabClose(tab.id);
            }}
          >
            <X size={14} />
          </span>
        </button>
      ))}
      <button className={styles.newTab} onClick={onTabCreate}>
        <Plus size={14} />
      </button>
    </div>
  );
}