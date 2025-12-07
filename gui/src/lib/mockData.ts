import { TreeNode, QueryResult, ConnectionStatus } from "@/types/database";

export const mockTreeData: TreeNode[] = [
  {
    id: "1",
    name: "production_db",
    type: "database",
    children: [
      {
        id: "1-1",
        name: "public",
        type: "schema",
        children: [
          {
            id: "1-1-1",
            name: "users",
            type: "table",
            children: [
              { id: "1-1-1-1", name: "id", type: "column", isKey: true, dataType: "uuid" },
              { id: "1-1-1-2", name: "email", type: "column", dataType: "varchar(255)" },
              { id: "1-1-1-3", name: "name", type: "column", dataType: "varchar(100)" },
              { id: "1-1-1-4", name: "created_at", type: "column", dataType: "timestamp" },
              { id: "1-1-1-5", name: "status", type: "column", dataType: "varchar(20)" },
            ],
          },
          {
            id: "1-1-2",
            name: "orders",
            type: "table",
            children: [
              { id: "1-1-2-1", name: "id", type: "column", isKey: true, dataType: "uuid" },
              { id: "1-1-2-2", name: "user_id", type: "column", dataType: "uuid" },
              { id: "1-1-2-3", name: "total", type: "column", dataType: "decimal(10,2)" },
              { id: "1-1-2-4", name: "status", type: "column", dataType: "varchar(20)" },
              { id: "1-1-2-5", name: "created_at", type: "column", dataType: "timestamp" },
            ],
          },
          {
            id: "1-1-3",
            name: "products",
            type: "table",
            children: [
              { id: "1-1-3-1", name: "id", type: "column", isKey: true, dataType: "uuid" },
              { id: "1-1-3-2", name: "name", type: "column", dataType: "varchar(255)" },
              { id: "1-1-3-3", name: "price", type: "column", dataType: "decimal(10,2)" },
              { id: "1-1-3-4", name: "inventory", type: "column", dataType: "integer" },
            ],
          },
          {
            id: "1-1-4",
            name: "sessions",
            type: "table",
          },
        ],
      },
      {
        id: "1-2",
        name: "analytics",
        type: "schema",
        children: [
          { id: "1-2-1", name: "events", type: "table" },
          { id: "1-2-2", name: "pageviews", type: "table" },
        ],
      },
    ],
  },
  {
    id: "2",
    name: "staging_db",
    type: "database",
    children: [
      {
        id: "2-1",
        name: "public",
        type: "schema",
        children: [
          { id: "2-1-1", name: "test_users", type: "table" },
        ],
      },
    ],
  },
];

export const mockQueryResult: QueryResult = {
  columns: ["id", "email", "name", "created_at", "status"],
  rows: [
    { id: 1, email: "sarah.chen@email.com", name: "Sarah Chen", created_at: "2024-01-15 09:23:41", status: "active" },
    { id: 2, email: "marcus.johnson@email.com", name: "Marcus Johnson", created_at: "2024-01-14 14:52:18", status: "active" },
    { id: 3, email: "elena.rodriguez@email.com", name: "Elena Rodriguez", created_at: "2024-01-14 11:07:33", status: "inactive" },
    { id: 4, email: "james.wilson@email.com", name: "James Wilson", created_at: "2024-01-13 16:45:02", status: "active" },
    { id: 5, email: "aiko.tanaka@email.com", name: "Aiko Tanaka", created_at: "2024-01-12 08:31:55", status: "active" },
    { id: 6, email: "omar.hassan@email.com", name: "Omar Hassan", created_at: "2024-01-11 13:22:47", status: "pending" },
    { id: 7, email: "lisa.mueller@email.com", name: "Lisa Müller", created_at: "2024-01-10 10:15:29", status: "active" },
    { id: 8, email: "raj.patel@email.com", name: "Raj Patel", created_at: "2024-01-09 17:08:14", status: "inactive" },
  ],
  rowCount: 8,
  executionTime: 23,
};

export const mockConnection: ConnectionStatus = {
  connected: true,
  host: "localhost",
  port: 7899,
  database: "production_db",
  user: "test",
};

export const defaultQuery = `// Fetch active users from the last 30 days
get from users where
    status = 'Active'
    created_at >= now() - interval '30 days'
    `;