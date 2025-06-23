import React from "react";
import { X } from "lucide-react";
import { QueryResult } from "../services/DatabaseDriver";

interface ResultsTableProps {
  result: QueryResult | null;
  error: string;
  onClearError?: () => void;
}

export const ResultsTable: React.FC<ResultsTableProps> = ({ result, error, onClearError }) => {
  if (error) {
    return (
      <div className="results-container">
        <div className="error-message">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '8px' }}>
            <div style={{ fontWeight: 'bold' }}>Query Error:</div>
            {onClearError && (
              <button
                onClick={onClearError}
                style={{
                  background: 'none',
                  border: 'none',
                  color: 'var(--error-color)',
                  cursor: 'pointer',
                  padding: '0',
                  display: 'flex',
                  alignItems: 'center'
                }}
                title="Clear error"
              >
                <X size={16} />
              </button>
            )}
          </div>
          <div style={{ whiteSpace: 'pre-wrap', fontFamily: 'monospace', fontSize: '12px' }}>
            {error}
          </div>
        </div>
      </div>
    );
  }

  if (!result) {
    return (
      <div className="results-container">
        <div className="empty-state">
          <span>Execute a query to see results</span>
        </div>
      </div>
    );
  }

  return (
    <div className="results-container">
      <div className="results-header">
        <span>{result.row_count} rows returned in {result.execution_time}ms</span>
      </div>
      
      <div className="table-wrapper">
        <table className="results-table">
          <thead>
            <tr>
              {result.columns.map((column, index) => (
                <th key={index}>{column}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {result.rows.map((row, rowIndex) => (
              <tr key={rowIndex}>
                {row.map((cell, cellIndex) => (
                  <td key={cellIndex}>
                    {cell === null ? <span className="null-value">NULL</span> : String(cell)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};
