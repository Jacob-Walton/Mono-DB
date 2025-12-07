"use client";

import { useRef, useCallback } from "react";
import Editor, { OnMount, OnChange } from "@monaco-editor/react";
import type { editor } from "monaco-editor";
import styles from "@/styles/components/QueryEditor.module.css";

interface QueryEditorProps {
  value: string;
  onChange: (value: string) => void;
  onExecute: () => void;
}

export function QueryEditor({ value, onChange, onExecute }: QueryEditorProps) {
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);

  const handleEditorMount: OnMount = useCallback(async (editor, monaco) => {
    editorRef.current = editor;

    // Register custom MonoDB language
    monaco.languages.register({ id: 'monodb' });

    // Set language configuration (brackets, comments, auto-closing, etc.)
    monaco.languages.setLanguageConfiguration('monodb', {
      comments: {
        lineComment: '//',
      },
      brackets: [
        ['{', '}'],
        ['[', ']'],
        ['(', ')'],
      ],
      autoClosingPairs: [
        { open: '{', close: '}' },
        { open: '[', close: ']' },
        { open: '(', close: ')' },
        { open: '"', close: '"', notIn: ['string'] },
      ],
      surroundingPairs: [
        { open: '{', close: '}' },
        { open: '[', close: ']' },
        { open: '(', close: ')' },
        { open: '"', close: '"' },
      ],
      folding: {
        markers: {
          start: /^\s*\/\/\s*#?region\b/,
          end: /^\s*\/\/\s*#?endregion\b/,
        },
      },
      wordPattern: /(-?\d*\.\d\w*)|([^\`\~\!\@\#\%\^\&\*\(\)\-\=\+\[\{\]\}\\\|\;\:\'\"\,\.\<\>\/\?\s]+)/g,
      indentationRules: {
        increaseIndentPattern: /^.*\{[^}"']*$/,
        decreaseIndentPattern: /^\s*\}/,
      },
    });

    // Load TextMate grammar
    try {
      const { loadWASM } = await import('onigasm');
      const { Registry } = await import('monaco-textmate');
      const { wireTmGrammars } = await import('monaco-editor-textmate');

      // Load onigasm WASM (must be done before any grammar operations)
      try {
        const wasmResponse = await fetch('/onigasm.wasm');
        const wasmBin = await wasmResponse.arrayBuffer();
        await loadWASM(wasmBin);
      } catch (e) {
        // Ignore if already loaded (onigasm throws if called twice)
        if (!(e instanceof Error && e.message.includes('already'))) {
          console.warn('Onigasm WASM load issue:', e);
        }
      }

      // Create grammar registry
      const registry = new Registry({
        getGrammarDefinition: async (scopeName: string) => {
          if (scopeName === 'source.monodb') {
            const response = await fetch('/grammars/monodb.tmLanguage.json');
            const grammar = await response.json();
            return {
              format: 'json',
              content: grammar
            };
          }
          return null as any;
        }
      });

      // Map of language ids to scope names
      const grammars = new Map();
      grammars.set('monodb', 'source.monodb');

      // Wire it all together
      await wireTmGrammars(monaco, registry, grammars, editor);

    } catch (error) {
      console.error('Failed to load TextMate grammar:', error);
    }

    // Get CSS custom properties from root
    const root = getComputedStyle(document.documentElement);
    const bgPrimary = root.getPropertyValue('--bg-primary').trim();
    const bgSecondary = root.getPropertyValue('--bg-secondary').trim();
    const border = root.getPropertyValue('--border').trim();
    const borderLight = root.getPropertyValue('--border-light').trim();
    const textPrimary = root.getPropertyValue('--text-primary').trim();
    const textMuted = root.getPropertyValue('--text-muted').trim();
    const accent = root.getPropertyValue('--accent').trim();
    const accentText = root.getPropertyValue('--accent-text').trim();

    // Define custom theme
    monaco.editor.defineTheme("dataforge-dark", {
      base: "vs-dark",
      inherit: true,
      rules: [
        { token: "keyword", foreground: accentText.replace('#', ''), fontStyle: "bold" },
        { token: "string", foreground: "4ade80" },
        { token: "number", foreground: "fb923c" },
        { token: "comment", foreground: textMuted.replace('#', ''), fontStyle: "italic" },
        { token: "operator", foreground: "f472b6" },
        { token: "identifier", foreground: textPrimary.replace('#', '') },
        { token: "type", foreground: accentText.replace('#', '') },
        { token: "function", foreground: "22d3ee" },
      ],
      colors: {
        "editor.background": bgPrimary,
        "editor.foreground": textPrimary,
        "editor.lineHighlightBackground": bgSecondary,
        "editor.selectionBackground": accent + "40",
        "editorLineNumber.foreground": border,
        "editorLineNumber.activeForeground": accent,
        "editorCursor.foreground": accent,
        "editor.selectionHighlightBackground": accent + "20",
        "editorIndentGuide.background": border,
        "editorIndentGuide.activeBackground": borderLight,
      },
    });

    monaco.editor.setTheme("dataforge-dark");

    // Add keyboard shortcut for execute
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
      onExecute();
    });

    editor.focus();
  }, [onExecute]);

  const handleChange: OnChange = useCallback((value) => {
    onChange(value ?? "");
  }, [onChange]);

  return (
    <div className={styles.editorWrapper}>
      <Editor
        height="100%"
        defaultLanguage="monodb"
        value={value}
        onChange={handleChange}
        onMount={handleEditorMount}
        options={{
          fontSize: 13,
          fontFamily: "'JetBrains Mono', monospace",
          fontLigatures: true,
          lineHeight: 1.6,
          padding: { top: 0, bottom: 0 },
          minimap: { enabled: false },
          scrollBeyondLastLine: false,
          renderLineHighlight: "line",
          cursorBlinking: "smooth",
          cursorSmoothCaretAnimation: "on",
          smoothScrolling: true,
          tabSize: 2,
          wordWrap: "on",
          automaticLayout: true,
          bracketPairColorization: { enabled: true },
          guides: {
            indentation: true,
            bracketPairs: true,
          },
        }}
        loading={<div className={styles.loading}>Loading editor...</div>}
      />
    </div>
  );
}