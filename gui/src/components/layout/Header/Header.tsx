"use client";

import { ConnectionInfo } from "@/hooks/useDatabase";
import styles from "./Header.module.css";
import { Database, Download, LogOut, Server, Settings, Upload, Plug } from "lucide-react";

interface HeaderProps {
  connection: ConnectionInfo | null;
  onConnectClick: () => void;
  onDisconnectClick: () => void;
}

export function Header({ connection, onConnectClick, onDisconnectClick }: HeaderProps) {
  const isConnected = connection?.connected ?? false;

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
        {isConnected ? (
          <button
            className={`${styles.connectionStatus} ${styles.connected}`}
            onClick={onDisconnectClick}
            title="Click to disconnect"
          >
            <span className={styles.statusDot} />
            <Server size={12} />
            <span>
              {connection!.host}:{connection!.port}
            </span>
            <LogOut size={12} className={styles.disconnectIcon} />
          </button>
        ) : (
          <button
            className={`${styles.connectionStatus} ${styles.disconnected}`}
            onClick={onConnectClick}
            title="Click to connect"
          >
            <span className={styles.statusDot} />
            <Plug size={12} />
            <span>Not Connected</span>
          </button>
        )}
      </div>

      <div className={styles.actions}>
        <button className={styles.btn} disabled={!isConnected}>
          <Download size={14} />
          <span>Export</span>
        </button>
        <button className={styles.btn} disabled={!isConnected}>
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
