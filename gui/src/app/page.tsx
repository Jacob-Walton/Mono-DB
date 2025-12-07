"use client";

import { EditorToolbar } from "@/components/EditorToolbar";
import { Header } from "@/components/Header";
import { QueryTabs } from "@/components/QueryTabs";
import { Resizer } from "@/components/Resizer";
import { ResultsPanel } from "@/components/ResultsPanel";
import { Sidebar } from "@/components/Sidebar";
import { StatusBar } from "@/components/StatusBar";
import { useQueryExecution } from "@/hooks/useQueryExecution";
import { useQueryTabs } from "@/hooks/useQueryTable";
import { mockConnection, mockTreeData } from "@/lib/mockData";

import styles from "@/styles/app/page.module.css";
import { TreeNode } from "@/types/database";
import dynamic from "next/dynamic";
import { useCallback, useState } from "react";

const QueryEditor = dynamic(
  () => import('@/components/QueryEditor').then(mod => mod.QueryEditor),
  { ssr: false, loading: () => <div className={styles.editorLoading}>Loading Editor...</div> }
);

export default function Home() {
  const {
    tabs,
    activeTab,
    activeTabId,
    setActiveTabId,
    createTab,
    closeTab,
    updateTabContent,
    saveTab,
  } = useQueryTabs();

  const {
    result,
    isExecuting,
    error,
    executeQuery,
  } = useQueryExecution();

  const [resultsPanelHeight, setResultsPanelHeight] = useState(250);
  const [cursorPosition] = useState({ line: 1, column: 1 });

  const handleExecute = useCallback(() => {
    if (activeTab) {
      executeQuery(activeTab.content);
    }
  }, [activeTab, executeQuery]);

  const handleFormat = useCallback(() => {
    // In a real app, you'd use a SQL formatter library
    console.log("Format SQL");
  }, []);

  const handleSave = useCallback(() => {
    if (activeTab) {
      saveTab(activeTab.id);
    }
  }, [activeTab, saveTab]);

  const handleTableSelect = useCallback((node: TreeNode) => {
    if (node.type === "table") {
      const query = `SELECT * FROM ${node.name} LIMIT 100;`;
      if (activeTab) {
        updateTabContent(activeTab.id, query);
      }
    }
  }, [activeTab, updateTabContent]);

  const handleResize = useCallback((delta: number) => {
    setResultsPanelHeight((prev) => Math.max(100, Math.min(600, prev + delta)));
  }, []);

  return (
    <div className={styles.app}>
      <Header connection={mockConnection} />
      
      <Sidebar 
        treeData={mockTreeData} 
        onTableSelect={handleTableSelect}
      />

      <main className={styles.main}>
        <QueryTabs
          tabs={tabs}
          activeTabId={activeTabId}
          onTabSelect={setActiveTabId}
          onTabClose={closeTab}
          onTabCreate={createTab}
        />

        <div className={styles.editorContainer}>
          <EditorToolbar
            onExecute={handleExecute}
            onFormat={handleFormat}
            onSave={handleSave}
            isExecuting={isExecuting}
            executionTime={result?.executionTime ?? null}
          />

          <div className={styles.editorWrapper}>
            {activeTab && (
              <QueryEditor
                value={activeTab.content}
                onChange={(value) => updateTabContent(activeTab.id, value)}
                onExecute={handleExecute}
              />
            )}
          </div>

          <Resizer onResize={handleResize} />

          <div 
            className={styles.resultsPanel}
            style={{ height: `${resultsPanelHeight}px` }}
          >
            <ResultsPanel
              result={result}
              error={error}
              isExecuting={isExecuting}
            />
          </div>
        </div>
      </main>

      <StatusBar 
        connection={mockConnection} 
        cursorPosition={cursorPosition}
      />
    </div>
  );
}