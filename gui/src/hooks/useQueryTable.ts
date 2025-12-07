import { defaultQuery } from "@/lib/mockData";
import { Tab } from "@/types/database";
import { useCallback, useState } from "react";

interface TabState {
  tabs: Tab[];
  activeTabId: string;
}

function getNextTabNumber(tabs: Tab[]): number {
  const numbers = tabs
    .map(t => parseInt(t.id.replace('tab-', ''), 10))
    .filter(n => !isNaN(n));
  return Math.max(0, ...numbers) + 1;
}

export function useQueryTabs() {
  const [state, setState] = useState<TabState>({
    tabs: [{ id: "tab-1", name: "Query 1", content: defaultQuery, saved: true }],
    activeTabId: "tab-1",
  });

  const activeTab = state.tabs.find((t) => t.id === state.activeTabId) ?? state.tabs[0];

  const createTab = useCallback(() => {
    setState((prev) => {
      const newCount = getNextTabNumber(prev.tabs);
      const newTab: Tab = {
        id: `tab-${newCount}`,
        name: `Query ${newCount}`,
        content: defaultQuery,
        saved: true,
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

      if (filtered.length === 0) {
        const newCount = getNextTabNumber(prev.tabs);
        const newTab = { id: `tab-${newCount}`, name: `Query ${newCount}`, content: "", saved: false };
        return { tabs: [newTab], activeTabId: newTab.id };
      }

      let newActiveTabId = prev.activeTabId;
      if (prev.activeTabId === tabId) {
        const currentIndex = prev.tabs.findIndex((t) => t.id === tabId);
        const newIndex = Math.min(currentIndex, filtered.length - 1);
        newActiveTabId = filtered[newIndex]?.id ?? filtered[0]?.id;
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
  };
}
