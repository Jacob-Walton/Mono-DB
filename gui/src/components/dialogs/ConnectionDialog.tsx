"use client";

import { useState, useCallback } from "react";
import { Dialog, DialogButton, DialogInput } from "@/components/ui/Dialog/Dialog";
import styles from "./ConnectionDialog.module.css";

interface ConnectionDialogProps {
  open: boolean;
  onClose: () => void;
  onConnect: (host: string, port: number) => Promise<boolean>;
  isConnecting: boolean;
  error: string | null;
}

export function ConnectionDialog({
  open,
  onClose,
  onConnect,
  isConnecting,
  error,
}: ConnectionDialogProps) {
  const [host, setHost] = useState("localhost");
  const [port, setPort] = useState("7899");

  const handleConnect = useCallback(async () => {
    const portNum = parseInt(port, 10);
    if (isNaN(portNum) || portNum < 1 || portNum > 65535) {
      return;
    }

    const success = await onConnect(host, portNum);
    if (success) {
      onClose();
    }
  }, [host, port, onConnect, onClose]);

  const isValid = host.trim() !== "" && !isNaN(parseInt(port, 10));

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="Connect to Server"
      width="md"
      actions={
        <>
          <DialogButton onClick={onClose} disabled={isConnecting}>
            Cancel
          </DialogButton>
          <DialogButton
            variant="primary"
            onClick={handleConnect}
            disabled={!isValid || isConnecting}
          >
            {isConnecting ? "Connecting..." : "Connect"}
          </DialogButton>
        </>
      }
    >
      <form
        className={styles.form}
        onSubmit={(e) => {
          e.preventDefault();
          handleConnect();
        }}
      >
        <div className={styles.row}>
          <div className={styles.hostField}>
            <DialogInput
              label="Host"
              value={host}
              onChange={setHost}
              placeholder="localhost"
              autoFocus
            />
          </div>
          <div className={styles.portField}>
            <DialogInput
              label="Port"
              value={port}
              onChange={setPort}
              placeholder="7899"
              type="number"
            />
          </div>
        </div>
        {error && <div className={styles.error}>{error}</div>}
      </form>
    </Dialog>
  );
}
