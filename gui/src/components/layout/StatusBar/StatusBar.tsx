"use client";

import { Check, X, Clock } from "lucide-react";
import styles from "./StatusBar.module.css";

interface StatusBarProps {
  isConnected: boolean;
  connectionString?: string;
  cursorPosition: { line: number; column: number };
  lastQueryTime?: number | null;
}

const APP_VERSION = "0.1.0";

export function StatusBar({
  isConnected,
  connectionString,
  cursorPosition,
  lastQueryTime
}: StatusBarProps) {
  return (
    <footer className={styles.statusbar}>
      <div className={styles.left}>
        <div className={`${styles.item} ${isConnected ? styles.success : styles.disconnected}`}>
          {isConnected ? <Check size={12} /> : <X size={12} />}
          <span>{isConnected ? "Connected" : "Disconnected"}</span>
        </div>
        {isConnected && connectionString && (
          <>
            <div className={styles.divider} />
            <div className={styles.item}>
              <span>{connectionString}</span>
            </div>
          </>
        )}
      </div>

      <div className={styles.center}>
        <span className={styles.appName}>MonoDB Admin {APP_VERSION}</span>
      </div>

      <div className={styles.right}>
        {lastQueryTime !== null && lastQueryTime !== undefined && (
          <>
            <div className={styles.item}>
              <Clock size={12} />
              <span>{lastQueryTime}ms</span>
            </div>
            <div className={styles.divider} />
          </>
        )}
        <div className={styles.item}>
          <span>Ln {cursorPosition.line}, Col {cursorPosition.column}</span>
        </div>
      </div>
    </footer>
  );
}
