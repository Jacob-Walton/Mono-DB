"use client";

import styles from "@/styles/components/EditorToolbar.module.css";
import { Loader2, Play, RotateCcw, Save, Sparkles, Zap } from "lucide-react";

interface EditorToolbarProps {
    onExecute: () => void;
    onFormat: () => void;
    onSave: () => void;
    isExecuting: boolean;
    executionTime: number | null;
}

export function EditorToolbar({
    onExecute,
    onFormat,
    onSave,
    isExecuting,
    executionTime,
}: EditorToolbarProps) {
  return (
    <div className={styles.toolbar}>
      <div className={styles.left}>
        <button
          className={`${styles.btn} ${styles.run}`}
          onClick={onExecute}
          disabled={isExecuting}
        >
          {isExecuting ? (
            <Loader2 size={14} className={styles.spinner} />
          ) : (
            <Play size={14} />
          )}
          <span>{isExecuting ? "Running..." : "Run Query"}</span>
        </button>
        <button className={styles.btn} onClick={onFormat}>
          <Sparkles size={14} />
          <span>Format</span>
        </button>
        <button className={styles.btn} onClick={onSave}>
          <Save size={14} />
          <span>Save</span>
        </button>
        <div className={styles.divider} />
        <button className={styles.btnIcon}>
          <RotateCcw size={14} />
        </button>
      </div>

      <div className={styles.right}>
        {executionTime !== null && (
          <div className={styles.executionInfo}>
            <Zap size={12} />
            <span>
              Query executed in <strong>{executionTime}ms</strong>
            </span>
          </div>
        )}
      </div>
    </div>
  );
}