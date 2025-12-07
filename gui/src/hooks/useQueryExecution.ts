import { useState, useCallback } from "react";
import { QueryResult } from "@/types/database";
import { mockQueryResult } from "@/lib/mockData";

export function useQueryExecution() {
    const [result, setResult] = useState<QueryResult | null>(null);
    const [isExecuting, setIsExecuting] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const executeQuery = useCallback(async (_query: string) => {
        setIsExecuting(true);
        setError(null);

        // Simulate query execution
        await new Promise((resolve) => setTimeout(resolve, 300 + Math.random() * 500));

        try {
            // TODO: Link with our database
            const result: QueryResult = {
                ...mockQueryResult,
                executionTime: Math.floor(15 + Math.random() * 50),
            };
            setResult(result);
        } catch (err) {
            setError(err instanceof Error ? err.message : "Query execution failed");
        } finally {
            setIsExecuting(false);
        }
    }, []);

    const clearResult = useCallback(() => {
        setResult(null);
        setError(null);
    }, []);

    return {
        result,
        isExecuting,
        error,
        executeQuery,
        clearResult,
    };
}