"use client";

import { useCallback, useEffect, useRef } from "react";
import { X } from "lucide-react";
import styles from "./Dialog.module.css";

interface DialogProps {
  open: boolean;
  onClose: () => void;
  title?: string;
  description?: string;
  children: React.ReactNode;
  actions?: React.ReactNode;
  width?: "sm" | "md" | "lg";
  closeOnBackdrop?: boolean;
  closeOnEscape?: boolean;
  showCloseButton?: boolean;
}

export function Dialog({
  open,
  onClose,
  title,
  description,
  children,
  actions,
  width = "md",
  closeOnBackdrop = true,
  closeOnEscape = true,
  showCloseButton = true,
}: DialogProps) {
  const dialogRef = useRef<HTMLDivElement>(null);
  const initialFocusRef = useRef(false);

  const handleBackdropClick = useCallback(
    (e: React.MouseEvent) => {
      if (closeOnBackdrop && e.target === e.currentTarget) {
        onClose();
      }
    },
    [closeOnBackdrop, onClose]
  );

  // Handle escape key separately to avoid effect re-runs
  useEffect(() => {
    if (!open) return;

    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape" && closeOnEscape) {
        onClose();
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    document.body.style.overflow = "hidden";

    // Only focus on initial open
    if (!initialFocusRef.current) {
      dialogRef.current?.focus();
      initialFocusRef.current = true;
    }

    return () => {
      document.removeEventListener("keydown", handleKeyDown);
      document.body.style.overflow = "";
    };
  }, [open, closeOnEscape, onClose]);

  // Reset focus ref when dialog closes
  useEffect(() => {
    if (!open) {
      initialFocusRef.current = false;
    }
  }, [open]);

  if (!open) return null;

  return (
    <div className={styles.backdrop} onClick={handleBackdropClick}>
      <div
        ref={dialogRef}
        className={`${styles.dialog} ${styles[width]}`}
        role="dialog"
        aria-modal="true"
        aria-labelledby={title ? "dialog-title" : undefined}
        tabIndex={-1}
      >
        {(title || showCloseButton) && (
          <div className={styles.header}>
            {title && (
              <div className={styles.titleWrapper}>
                <h2 id="dialog-title" className={styles.title}>
                  {title}
                </h2>
                {description && (
                  <p className={styles.description}>{description}</p>
                )}
              </div>
            )}
            {showCloseButton && (
              <button
                className={styles.closeButton}
                onClick={onClose}
                aria-label="Close dialog"
              >
                <X size={16} />
              </button>
            )}
          </div>
        )}

        <div className={styles.content}>{children}</div>

        {actions && <div className={styles.actions}>{actions}</div>}
      </div>
    </div>
  );
}

interface DialogButtonProps {
  children: React.ReactNode;
  onClick?: () => void;
  variant?: "primary" | "secondary" | "danger";
  disabled?: boolean;
  type?: "button" | "submit";
}

export function DialogButton({
  children,
  onClick,
  variant = "secondary",
  disabled = false,
  type = "button",
}: DialogButtonProps) {
  return (
    <button
      type={type}
      className={`${styles.button} ${styles[variant]}`}
      onClick={onClick}
      disabled={disabled}
    >
      {children}
    </button>
  );
}

interface DialogInputProps {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  type?: "text" | "password" | "number";
  autoFocus?: boolean;
  error?: string;
}

export function DialogInput({
  label,
  value,
  onChange,
  placeholder,
  type = "text",
  autoFocus = false,
  error,
}: DialogInputProps) {
  return (
    <div className={styles.inputGroup}>
      <label className={styles.label}>{label}</label>
      <input
        type={type}
        className={`${styles.input} ${error ? styles.inputError : ""}`}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        autoFocus={autoFocus}
      />
      {error && <span className={styles.error}>{error}</span>}
    </div>
  );
}
