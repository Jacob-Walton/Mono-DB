"use client";

import styles from "./ResultsPanel.module.css";
import { AlertCircle, Check, Copy, Download, MoreHorizontal, TableProperties } from "lucide-react";
import { useState } from "react";
import { QueryResult } from "@/types/database";

interface ResultsPanelProps {
  result: QueryResult | null;
  error: string | null;
  isExecuting: boolean;
}

type ResultTab = "results" | "messages" | "explain";

export function ResultsPanel({ result, error, isExecuting }: ResultsPanelProps) {
  const [activeTab, setActiveTab] = useState<ResultTab>("results");

  const copyToClipboard = () => {
    if (!result) return;
    const csv = [
      result.columns.join(","),
      ...result.rows.map((row) =>
        result.columns.map((col) => String(row[col] ?? "")).join(",")
      ),
    ].join("\n");
    navigator.clipboard.writeText(csv);
  };

  const getStatusBadgeClass = (status: unknown) => {
    if (typeof status !== "string") return "";
    switch (status) {
      case "active": return styles.active;
      case "inactive": return styles.inactive;
      case "pending": return styles.pending;
      default: return "";
    }
  };

  return (
    <div className={styles.panel}>
      <div className={styles.header}>
        <div className={styles.tabs}>
          <button
            className={`${styles.tab} ${activeTab === "results" ? styles.active : ""}`}
            onClick={() => setActiveTab("results")}
          >
            <TableProperties size={12} />
            <span>Results</span>
          </button>
          <button
            className={`${styles.tab} ${activeTab === "messages" ? styles.active : ""}`}
            onClick={() => setActiveTab("messages")}
          >
            Messages
          </button>
          <button
            className={`${styles.tab} ${activeTab === "explain" ? styles.active : ""}`}
            onClick={() => setActiveTab("explain")}
          >
            Explain
          </button>
        </div>

        <div className={styles.actions}>
          {result && (
            <span className={styles.meta}>
              <Check size={12} />
              <span>{result.rowCount} rows</span> returned
            </span>
          )}
          <button className={styles.actionBtn} onClick={copyToClipboard} title="Copy as CSV">
            <Copy size={12} />
          </button>
          <button className={styles.actionBtn} title="Export">
            <Download size={12} />
          </button>
          <button className={styles.actionBtn}>
            <MoreHorizontal size={12} />
          </button>
        </div>
      </div>

      <div className={styles.content}>
        {isExecuting && (
          <div className={styles.placeholder}>
            <div className={styles.loader} />
            <span>Executing query...</span>
          </div>
        )}

        {error && !isExecuting && (
          <div className={styles.error}>
            <AlertCircle size={16} />
            <span>{error}</span>
          </div>
        )}

        {!isExecuting && !error && !result && (
          <div className={styles.placeholder}>
            <span>Run a query to see results</span>
          </div>
        )}

        {!isExecuting && !error && result && activeTab === "results" && (
          <div className={styles.tableContainer}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th>#</th>
                  {result.columns.map((col) => (
                    <th key={col}>{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {result.rows.map((row, i) => (
                  <tr key={i}>
                    <td>{i + 1}</td>
                    {result.columns.map((col) => (
                      <td key={col}>
                        {col === "status" ? (
                          <span className={`${styles.badge} ${getStatusBadgeClass(row[col])}`}>
                            {String(row[col])}
                          </span>
                        ) : row[col] === null ? (
                          <span className={styles.null}>NULL</span>
                        ) : (
                          String(row[col])
                        )}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {activeTab === "messages" && (
          <div className={styles.messages}>
            {result && (
              <p className={styles.success}>
                Query executed successfully. {result.rowCount} rows returned in {result.executionTime}ms.
              </p>
            )}
          </div>
        )}

        {activeTab === "explain" && (
          <div className={styles.explain}>
            <p className={styles.placeholder}>Run explain analyze to see query plan (unimplemented)</p>
          </div>
        )}
      </div>
    </div>
  );
}