"use client";

import { useState, useCallback } from "react";
import { Tab } from "@/types/database";
import { ContextMenu, ContextMenuItem } from "@/components/ui/ContextMenu/ContextMenu";
import { Dialog, DialogButton, DialogInput } from "@/components/ui/Dialog/Dialog";
import styles from "./QueryTabs.module.css";
import { Edit3, FileCode, Plus, Trash2, X, XCircle } from "lucide-react";

interface QueryTabsProps {
  tabs: Tab[];
  activeTabId: string;
  onTabSelect: (tabId: string) => void;
  onTabClose: (tabId: string) => void;
  onTabCreate: () => void;
  onTabRename?: (tabId: string, name: string) => void;
  onCloseOthers?: (tabId: string) => void;
  onCloseAll?: () => void;
}

export function QueryTabs({
  tabs,
  activeTabId,
  onTabSelect,
  onTabClose,
  onTabCreate,
  onTabRename,
  onCloseOthers,
  onCloseAll,
}: QueryTabsProps) {
  const [contextMenu, setContextMenu] = useState<{
    position: { x: number; y: number } | null;
    tabId: string | null;
  }>({ position: null, tabId: null });

  const [renameDialog, setRenameDialog] = useState<{
    open: boolean;
    tabId: string | null;
    name: string;
  }>({ open: false, tabId: null, name: "" });

  const handleContextMenu = useCallback((e: React.MouseEvent, tabId: string) => {
    e.preventDefault();
    setContextMenu({
      position: { x: e.clientX, y: e.clientY },
      tabId,
    });
  }, []);

  const handleMiddleClick = useCallback(
    (e: React.MouseEvent, tabId: string) => {
      if (e.button === 1) {
        e.preventDefault();
        onTabClose(tabId);
      }
    },
    [onTabClose]
  );

  const handleDoubleClick = useCallback(
    (tabId: string, currentName: string) => {
      if (onTabRename) {
        setRenameDialog({ open: true, tabId, name: currentName });
      }
    },
    [onTabRename]
  );

  const handleRename = useCallback(() => {
    if (renameDialog.tabId && renameDialog.name.trim() && onTabRename) {
      onTabRename(renameDialog.tabId, renameDialog.name.trim());
    }
    setRenameDialog({ open: false, tabId: null, name: "" });
  }, [renameDialog, onTabRename]);

  const contextMenuItems: ContextMenuItem[] = [
    {
      id: "rename",
      label: "Rename",
      icon: <Edit3 size={14} />,
      disabled: !onTabRename,
    },
    { id: "separator1", label: "", separator: true },
    {
      id: "close",
      label: "Close",
      icon: <X size={14} />,
      shortcut: "Ctrl+W",
    },
    {
      id: "closeOthers",
      label: "Close Others",
      icon: <XCircle size={14} />,
      disabled: tabs.length <= 1 || !onCloseOthers,
    },
    {
      id: "closeAll",
      label: "Close All",
      icon: <Trash2 size={14} />,
      disabled: !onCloseAll,
      danger: true,
    },
  ];

  const handleContextMenuSelect = useCallback(
    (id: string) => {
      if (!contextMenu.tabId) return;

      const tab = tabs.find((t) => t.id === contextMenu.tabId);

      switch (id) {
        case "rename":
          if (tab && onTabRename) {
            setRenameDialog({ open: true, tabId: tab.id, name: tab.name });
          }
          break;
        case "close":
          onTabClose(contextMenu.tabId);
          break;
        case "closeOthers":
          onCloseOthers?.(contextMenu.tabId);
          break;
        case "closeAll":
          onCloseAll?.();
          break;
      }
    },
    [contextMenu.tabId, tabs, onTabClose, onTabRename, onCloseOthers, onCloseAll]
  );

  return (
    <>
      <div className={styles.tabs}>
        {tabs.map((tab) => (
          <button
            key={tab.id}
            className={`${styles.tab} ${activeTabId === tab.id ? styles.active : ""}`}
            onClick={() => onTabSelect(tab.id)}
            onMouseDown={(e) => handleMiddleClick(e, tab.id)}
            onContextMenu={(e) => handleContextMenu(e, tab.id)}
            onDoubleClick={() => handleDoubleClick(tab.id, tab.name)}
            title={`${tab.name}${!tab.saved ? " (unsaved)" : ""}\nDouble-click to rename\nMiddle-click to close`}
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
        <button
          className={styles.newTab}
          onClick={onTabCreate}
          title="New Query (Ctrl+T)"
        >
          <Plus size={14} />
        </button>
      </div>

      <ContextMenu
        items={contextMenuItems}
        position={contextMenu.position}
        onClose={() => setContextMenu({ position: null, tabId: null })}
        onSelect={handleContextMenuSelect}
      />

      <Dialog
        open={renameDialog.open}
        onClose={() => setRenameDialog({ open: false, tabId: null, name: "" })}
        title="Rename Tab"
        width="sm"
        actions={
          <>
            <DialogButton
              onClick={() => setRenameDialog({ open: false, tabId: null, name: "" })}
            >
              Cancel
            </DialogButton>
            <DialogButton
              variant="primary"
              onClick={handleRename}
              disabled={!renameDialog.name.trim()}
            >
              Rename
            </DialogButton>
          </>
        }
      >
        <form
          onSubmit={(e) => {
            e.preventDefault();
            handleRename();
          }}
        >
          <DialogInput
            label="Tab Name"
            value={renameDialog.name}
            onChange={(name) => setRenameDialog((prev) => ({ ...prev, name }))}
            placeholder="Enter tab name..."
            autoFocus
          />
        </form>
      </Dialog>
    </>
  );
}
