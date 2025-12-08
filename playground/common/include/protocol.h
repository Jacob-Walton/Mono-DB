#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include "value.h"  // Already there, should work

typedef enum
{
    REQUEST_CONNECT,
    REQUEST_EXECUTE,
    REQUEST_LIST,
    REQUEST_BEGINTX,
    REQUEST_COMMITTX,
    REQUEST_ROLLBACKTX,
} RequestTag;

typedef enum
{
    RESPONSE_CONNECT_ACK,
    RESPONSE_SUCCESS,
    RESPONSE_ERROR,
    RESPONSE_STREAM,
    RESPONSE_ACK,
} ResponseTag;

typedef enum IsolationLevel
{
    ISOLATION_READ_UNCOMMITTED = 0,
    ISOLATION_READ_COMMITTED = 1,
    ISOLATION_REPEATABLE_READ = 2,
    ISOLATION_SERIALIZABLE = 3,
} IsolationLevel;

typedef enum ExecutionResultTag
{
    EXECUTION_RESULT_OK,
    EXECUTION_RESULT_CREATED,
    EXECUTION_RESULT_MODIFIED,
} ExecutionResultTag;

typedef struct ExecutionResult
{
    ExecutionResultTag tag;

    union
    {
        // OK
        // data: Value
        // time: u64
        // commit_timestamp: Option<u64>
        // time_elapsed: Option<u64>
        // row_count: Option<u64>
        struct
        {
            struct Value *data;
            uint64_t time;
            struct
            {
                uint8_t is_some;
                uint64_t value;
            } commit_timestamp;
            struct
            {
                uint8_t is_some;
                uint64_t value;
            } time_elapsed;
            struct
            {
                uint8_t is_some;
                uint64_t value;
            } row_count;
        } ok;

        // Created
        // time: u64
        // commit_timestamp: u64
        struct
        {
            uint64_t time;
            uint64_t commit_timestamp;
        } created;

        // Modified
        // time: u64
        // commit_timestamp: Option<u64>
        // rows_affected: Option<u64>
        struct
        {
            uint64_t time;
            struct
            {
                uint8_t is_some;
                uint64_t value;
            } commit_timestamp;
            struct
            {
                uint8_t is_some;
                uint64_t value;
            } rows_affected;
        } modified;
    } body;
} ExecutionResult;

typedef struct Request
{
    RequestTag tag;

    union
    {
        // Connect
        // protocol_version: uint8_t
        // auth_token: Option<String>
        struct
        {
            uint8_t protocol_version;
            struct
            {
                char *ptr;
                uint64_t len;
            } auth_token;
        } connect;

        // Execute
        // query: String
        // params: Vec<Value>,
        // snapshot_timestamp: Option<u64>,
        // user_id: Option<String>,
        struct
        {
            struct
            {
                char *ptr;
                uint64_t len;
            } query;
            struct
            {
                struct Value *ptr;
                uint64_t len;
            } params;
            struct
            {
                uint8_t is_some;
                uint64_t value;
            } snapshot_timestamp;
            struct
            {
                uint8_t is_some;
                struct
                {
                    char *ptr;
                    uint64_t len;
                } value;
            } user_id;
        } execute;

        // List (no fields)
        
        // BeginTx
        // isolation: IsolationLevel (uint8_t)
        // user_id: Option<String>,
        // read_timestamp: Option<u64>,
        struct
        {
            IsolationLevel isolation;
            struct
            {
                uint8_t is_some;
                struct
                {
                    char *ptr;
                    uint64_t len;
                } value;
            } user_id;
            struct
            {
                uint8_t is_some;
                uint64_t value;
            } read_timestamp;
        } begintx;

        // CommitTx
        // tx_id: uint64_t
        struct
        {
            uint64_t tx_id;
        } committx;

        // RollbackTx
        // tx_id: uint64_t
        struct
        {
            uint64_t tx_id;
        } rollbacktx;
    } body;
} Request;

typedef struct Response
{
    ResponseTag tag;

    union
    {
        // ConnectAck
        // protocol_version: uint8_t
        // server_timestamp: Option<u64>
        // user_permissions: Option<Vec<String>>
        struct 
        {
            uint8_t protocol_version;
            struct
            {
                uint8_t is_some;
                uint64_t value;
            } server_timestamp;
            struct
            {
                uint8_t is_some;
                struct
                {
                    char **ptr;
                    uint64_t len;
                } value;
            } user_permissions;
        } connect_ack;

        // Success
        // result: Vec<ExecutionResult>
        struct
        {
            struct
            {
                struct ExecutionResult *ptr;
                uint64_t len;
            } result;
        } success;

        // Error
        // code: uint16_t
        // message: String
        struct
        {
            uint16_t code;
            struct
            {
                char *ptr;
                uint64_t len;
            } message;
        } error;

        // Stream (no fields)
        // Ack (no fields)
    } body;
} Response;

Request *request_new_connect(uint8_t protocol_version, const char *auth_token);
Request *request_new_execute(const char *query, struct Value *params, uint64_t *snapshot_timestamp, const char *user_id);
Request *request_new_list();
Request *request_new_begintx(IsolationLevel isolation, const char *user_id, uint64_t *read_timestamp);
Request *request_new_committx(uint64_t tx_id);
Request *request_new_rollbacktx(uint64_t tx_id);

Response *response_new_connect_ack(uint8_t protocol_version, uint64_t *server_timestamp, char **user_permissions, uint64_t user_permissions_len);
Response *response_new_success(struct ExecutionResult *results, uint64_t results_len);
Response *response_new_error(uint16_t code, const char *message);
Response *response_new_stream();
Response *response_new_ack();

// Encode the request into a buffer. The caller is responsible for freeing the returned buffer.
// The length of the buffer is returned via the out_length parameter.
uint8_t *request_encode(const Request *request, uint64_t *out_length);
// Decode a request from a buffer. The caller is responsible for freeing the returned Request.
Request *request_decode(const uint8_t *buffer, uint64_t length);

// Encode the response into a buffer. The caller is responsible for freeing the returned buffer.
// The length of the buffer is returned via the out_length parameter.
uint8_t *response_encode(const Response *response, uint64_t *out_length);
// Decode a response from a buffer. The caller is responsible for freeing the returned Response.
Response *response_decode(const uint8_t *buffer, uint64_t length);

void request_free(Request *request);
void response_free(Response *response);

#endif