"use client";

import { EditorToolbar } from "@/components/editor/EditorToolbar/EditorToolbar";
import { Header } from "@/components/layout/Header/Header";
import { QueryTabs } from "@/components/editor/QueryTabs/QueryTabs";
import { Resizer } from "@/components/ui/Resizer/Resizer";
import { ResultsPanel } from "@/components/results/ResultsPanel/ResultsPanel";
import { Sidebar } from "@/components/layout/Sidebar/Sidebar";
import { StatusBar } from "@/components/layout/StatusBar/StatusBar";
import { ConnectionDialog } from "@/components/dialogs/ConnectionDialog";
import { useQueryTabs } from "@/hooks/useQueryTable";
import { useDatabase } from "@/hooks/useDatabase";

import styles from "./page.module.css";
import { TreeNode, QueryResult } from "@/types/database";
import dynamic from "next/dynamic";
import { useCallback, useState, useEffect } from "react";
import { FileCode, Plus } from "lucide-react";

const QueryEditor = dynamic(
  () => import('@/components/editor/QueryEditor/QueryEditor').then(mod => mod.QueryEditor),
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
    renameTab,
    closeOthers,
    closeAll,
  } = useQueryTabs();

  const {
    connection,
    isConnecting,
    isConnected,
    error: dbError,
    connect,
    disconnect,
    executeQuery: dbExecuteQuery,
    listTables,
    clearError,
  } = useDatabase();

  const [showConnectionDialog, setShowConnectionDialog] = useState(false);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [queryError, setQueryError] = useState<string | null>(null);
  const [isExecuting, setIsExecuting] = useState(false);
  const [resultsPanelHeight, setResultsPanelHeight] = useState(250);
  const [cursorPosition] = useState({ line: 1, column: 1 });

  // Table data state
  const [treeData, setTreeData] = useState<TreeNode[]>([]);
  const [isLoadingTables, setIsLoadingTables] = useState(false);

  // Load tables when connected
  const loadTables = useCallback(async () => {
    if (!isConnected) {
      setTreeData([]);
      return;
    }

    setIsLoadingTables(true);
    try {
      const tables = await listTables();
      const tableNodes: TreeNode[] = tables.map((table) => ({
        id: `table-${table.name}`,
        name: table.name,
        type: "table" as const,
        children: [],
      }));

      setTreeData([
        {
          id: "database",
          name: connection?.host ?? "Database",
          type: "database",
          children: tableNodes.length > 0 ? tableNodes : undefined,
        },
      ]);
    } catch (e) {
      console.error("Failed to load tables:", e);
      setTreeData([]);
    } finally {
      setIsLoadingTables(false);
    }
  }, [isConnected, listTables, connection?.host]);

  // Load tables when connection changes
  useEffect(() => {
    if (isConnected) {
      loadTables();
    } else {
      setTreeData([]);
    }
  }, [isConnected, loadTables]);

  const handleExecute = useCallback(async () => {
    if (!activeTab) return;

    setIsExecuting(true);
    setQueryError(null);

    try {
      if (isConnected) {
        const dbResult = await dbExecuteQuery(activeTab.content);
        if (dbResult) {
          setResult({
            columns: dbResult.columns,
            rows: dbResult.rows.map((row) => {
              const obj: Record<string, unknown> = {};
              if (typeof row === 'object' && row !== null) {
                for (const [key, value] of Object.entries(row)) {
                  obj[key] = value;
                }
              }
              return obj;
            }),
            rowCount: Number(dbResult.row_count),
            executionTime: Number(dbResult.execution_time),
          });
        }
      } else {
        // Mock execution for demo
        await new Promise((resolve) => setTimeout(resolve, 300));
        setResult({
          columns: ["id", "name", "status"],
          rows: [
            { id: 1, name: "Example", status: "active" },
            { id: 2, name: "Demo", status: "pending" },
          ],
          rowCount: 2,
          executionTime: 42,
        });
      }
    } catch (e) {
      setQueryError(e instanceof Error ? e.message : String(e));
    } finally {
      setIsExecuting(false);
    }
  }, [activeTab, isConnected, dbExecuteQuery]);

  const handleFormat = useCallback(() => {
    console.log("Format SQL");
  }, []);

  const handleSave = useCallback(() => {
    if (activeTab) {
      saveTab(activeTab.id);
    }
  }, [activeTab, saveTab]);

  // Create new tab with query for table
  const handleTableSelect = useCallback((node: TreeNode) => {
    if (node.type === "table") {
      const query = `SELECT * FROM ${node.name} LIMIT 100;`;
      createTab(query, node.name);
    }
  }, [createTab]);

  const handleResize = useCallback((delta: number) => {
    setResultsPanelHeight((prev) => Math.max(100, Math.min(600, prev + delta)));
  }, []);

  const handleConnect = useCallback(async (host: string, port: number) => {
    clearError();
    return await connect(host, port);
  }, [connect, clearError]);

  const handleDisconnect = useCallback(async () => {
    await disconnect();
    setResult(null);
    setTreeData([]);
  }, [disconnect]);

  const handleOpenConnectionDialog = useCallback(() => {
    setShowConnectionDialog(true);
  }, []);

  const handleNewQuery = useCallback(() => {
    createTab();
  }, [createTab]);

  const hasOpenTabs = tabs.length > 0;

  return (
    <div className={styles.app}>
      <Header
        connection={connection}
        onConnectClick={handleOpenConnectionDialog}
        onDisconnectClick={handleDisconnect}
      />

      <Sidebar
        treeData={treeData}
        onTableSelect={handleTableSelect}
        isConnected={isConnected}
        isLoading={isLoadingTables}
        onRefresh={loadTables}
        onConnectClick={handleOpenConnectionDialog}
      />

      <main className={styles.main}>
        {hasOpenTabs ? (
          <>
            <QueryTabs
              tabs={tabs}
              activeTabId={activeTabId ?? ""}
              onTabSelect={setActiveTabId}
              onTabClose={closeTab}
              onTabCreate={handleNewQuery}
              onTabRename={renameTab}
              onCloseOthers={closeOthers}
              onCloseAll={closeAll}
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
                  error={queryError}
                  isExecuting={isExecuting}
                />
              </div>
            </div>
          </>
        ) : (
          <div className={styles.emptyState}>
            <div className={styles.emptyContent}>
              <FileCode size={48} className={styles.emptyIcon} />
              <h2>No Query Open</h2>
              <p>Create a new query to get started, or select a table from the Object Explorer.</p>
              <button className={styles.newQueryBtn} onClick={handleNewQuery}>
                <Plus size={16} />
                New Query
              </button>
            </div>
          </div>
        )}
      </main>

      <StatusBar
        isConnected={isConnected}
        connectionString={isConnected ? `${connection?.host}:${connection?.port}` : undefined}
        cursorPosition={cursorPosition}
        lastQueryTime={result?.executionTime}
      />

      <ConnectionDialog
        open={showConnectionDialog}
        onClose={() => {
          setShowConnectionDialog(false);
          clearError();
        }}
        onConnect={handleConnect}
        isConnecting={isConnecting}
        error={dbError}
      />
    </div>
  );
}
