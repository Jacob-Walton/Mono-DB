"use client";

import { ConnectionStatus } from "@/types/database";
import styles from "@/styles/components/Header.module.css";
import { Database, Download, Server, Settings, Upload } from "lucide-react";

interface HeaderProps {
    connection: ConnectionStatus;
}

export function Header({ connection }: HeaderProps) {
  return (
    <header className={styles.header}>
      <div className={styles.left}>
        <div className={styles.logo}>
          <div className={styles.logoIcon}>
            <Database size={16} />
          </div>
          <span>MonoDB Admin</span>
        </div>
      </div>

      <div className={styles.center}>
        <div className={`${styles.connectionStatus} ${connection.connected ? styles.connected : styles.disconnected}`}>
          <span className={styles.statusDot} />
          <Server size={12} />
          <span>
            {connection.user}@{connection.host}:{connection.port}
          </span>
        </div>
      </div>

      <div className={styles.actions}>
        <button className={styles.btn}>
          <Download size={14} />
          <span>Export</span>
        </button>
        <button className={styles.btn}>
          <Upload size={14} />
          <span>Import</span>
        </button>
        <button className={styles.btnIcon}>
          <Settings size={14} />
        </button>
      </div>
    </header>
  );
}