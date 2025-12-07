import { Tab } from "@/types/database";
import { useCallback, useState } from "react";

interface TabState {
  tabs: Tab[];
  activeTabId: string | null;
}

function getNextTabNumber(tabs: Tab[]): number {
  const numbers = tabs
    .map(t => parseInt(t.id.replace('tab-', ''), 10))
    .filter(n => !isNaN(n));
  return Math.max(0, ...numbers) + 1;
}

export function useQueryTabs() {
  const [state, setState] = useState<TabState>({
    tabs: [],
    activeTabId: null,
  });

  const activeTab = state.activeTabId
    ? state.tabs.find((t) => t.id === state.activeTabId) ?? null
    : null;

  const createTab = useCallback((content?: string, name?: string) => {
    setState((prev) => {
      const newCount = getNextTabNumber(prev.tabs);
      const newTab: Tab = {
        id: `tab-${newCount}`,
        name: name ?? `Query ${newCount}`,
        content: content ?? "",
        saved: false,
      };
      return {
        tabs: [...prev.tabs, newTab],
        activeTabId: newTab.id,
      };
    });
  }, []);

  const closeTab = useCallback((tabId: string) => {
    setState((prev) => {
      const filtered = prev.tabs.filter((t) => t.id !== tabId);

      // Allow closing all tabs - show empty state
      if (filtered.length === 0) {
        return { tabs: [], activeTabId: null };
      }

      let newActiveTabId = prev.activeTabId;
      if (prev.activeTabId === tabId) {
        const currentIndex = prev.tabs.findIndex((t) => t.id === tabId);
        const newIndex = Math.min(currentIndex, filtered.length - 1);
        newActiveTabId = filtered[newIndex]?.id ?? filtered[0]?.id ?? null;
      }

      return { tabs: filtered, activeTabId: newActiveTabId };
    });
  }, []);

  const setActiveTabId = useCallback((tabId: string) => {
    setState((prev) => ({ ...prev, activeTabId: tabId }));
  }, []);

  const updateTabContent = useCallback((tabId: string, content: string) => {
    setState((prev) => ({
      ...prev,
      tabs: prev.tabs.map((t) =>
        t.id === tabId ? { ...t, content, saved: false } : t
      ),
    }));
  }, []);

  const saveTab = useCallback((tabId: string) => {
    setState((prev) => ({
      ...prev,
      tabs: prev.tabs.map((t) =>
        t.id === tabId ? { ...t, saved: true } : t
      ),
    }));
  }, []);

  const renameTab = useCallback((tabId: string, name: string) => {
    setState((prev) => ({
      ...prev,
      tabs: prev.tabs.map((t) =>
        t.id === tabId ? { ...t, name } : t
      ),
    }));
  }, []);

  const closeOthers = useCallback((tabId: string) => {
    setState((prev) => ({
      tabs: prev.tabs.filter((t) => t.id === tabId),
      activeTabId: tabId,
    }));
  }, []);

  const closeAll = useCallback(() => {
    setState({ tabs: [], activeTabId: null });
  }, []);

  return {
    tabs: state.tabs,
    activeTab,
    activeTabId: state.activeTabId,
    setActiveTabId,
    createTab,
    closeTab,
    updateTabContent,
    saveTab,
    renameTab,
    closeOthers,
    closeAll,
  };
}
