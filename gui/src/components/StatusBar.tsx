"use client";

import { Check, Database, Clock } from "lucide-react";
import { ConnectionStatus } from "@/types/database";
import styles from "@/styles/components/StatusBar.module.css";

interface StatusBarProps {
  connection: ConnectionStatus;
  cursorPosition: { line: number; column: number };
}

export function StatusBar({ connection, cursorPosition }: StatusBarProps) {
  return (
    <footer className={styles.statusbar}>
      <div className={styles.left}>
        <div className={`${styles.item} ${connection.connected ? styles.success : ""}`}>
          <Check size={12} />
          <span>{connection.connected ? "Connected" : "Disconnected"}</span>
        </div>
        <div className={styles.divider} />
        <div className={styles.item}>
          <Database size={12} />
          <span>{connection.database}</span>
        </div>
        <div className={styles.divider} />
        <div className={styles.item}>
          <span>MonoDB 0.2.0</span>
        </div>
      </div>

      <div className={styles.right}>
        <div className={styles.item}>
          <Clock size={12} />
          <span>Last query: just now</span>
        </div>
        <div className={styles.divider} />
        <div className={styles.item}>
          <span>Ln {cursorPosition.line}, Col {cursorPosition.column}</span>
        </div>
      </div>
    </footer>
  );
}