#include "protocol.h"
#include "value.h"
#include <stddef.h>
#include <string.h>
#include <stdlib.h>

static inline void write_u32_le(uint8_t *buf, uint32_t val) {
    buf[0] = val & 0xFF;
    buf[1] = (val >> 8) & 0xFF;
    buf[2] = (val >> 16) & 0xFF;
    buf[3] = (val >> 24) & 0xFF;
}

static inline void write_u64_le(uint8_t *buf, uint64_t val) {
    buf[0] = val & 0xFF;
    buf[1] = (val >> 8) & 0xFF;
    buf[2] = (val >> 16) & 0xFF;
    buf[3] = (val >> 24) & 0xFF;
    buf[4] = (val >> 32) & 0xFF;
    buf[5] = (val >> 40) & 0xFF;
    buf[6] = (val >> 48) & 0xFF;
    buf[7] = (val >> 56) & 0xFF;
}

static inline uint32_t read_u32_le(const uint8_t *buf) {
    return (uint32_t)buf[0] | ((uint32_t)buf[1] << 8) | 
           ((uint32_t)buf[2] << 16) | ((uint32_t)buf[3] << 24);
}

static inline uint64_t read_u64_le(const uint8_t *buf) {
    return (uint64_t)buf[0] | ((uint64_t)buf[1] << 8) | 
           ((uint64_t)buf[2] << 16) | ((uint64_t)buf[3] << 24) |
           ((uint64_t)buf[4] << 32) | ((uint64_t)buf[5] << 40) |
           ((uint64_t)buf[6] << 48) | ((uint64_t)buf[7] << 56);
}

Request *request_new_connect(uint8_t protocol_version, const char *auth_token)
{
    Request req;
    req.tag = REQUEST_CONNECT;
    req.body.connect.protocol_version = protocol_version;
    if (auth_token) {
        uint64_t len = strlen(auth_token);
        req.body.connect.auth_token.len = len;
        req.body.connect.auth_token.ptr = (char *)malloc(len + 1);
        if (req.body.connect.auth_token.ptr) {
            memcpy(req.body.connect.auth_token.ptr, auth_token, len);
            req.body.connect.auth_token.ptr[len] = '\0';
        }
    } else {
        req.body.connect.auth_token.ptr = NULL;
        req.body.connect.auth_token.len = 0;
    }

    Request *req_ptr = (Request *)malloc(sizeof(Request));
    if (req_ptr) {
        *req_ptr = req;
    }
    return req_ptr;
}

Request *request_new_execute(const char *query, struct Value *params, uint64_t *snapshot_timestamp, const char *user_id)
{
    Request req;
    req.tag = REQUEST_EXECUTE;
    
    // Duplicate query string
    uint64_t query_len = strlen(query);
    req.body.execute.query.len = query_len;
    req.body.execute.query.ptr = (char *)malloc(query_len + 1);
    if (req.body.execute.query.ptr) {
        memcpy(req.body.execute.query.ptr, query, query_len);
        req.body.execute.query.ptr[query_len] = '\0';
    }
    
    // Get length of params
    uint64_t params_len = 0;
    if (params) {
        while (params[params_len].tag != VALUE_NULL) {
            params_len++;
        }
    }
    req.body.execute.params.ptr = params;
    req.body.execute.params.len = params_len;

    if (snapshot_timestamp) {
        req.body.execute.snapshot_timestamp.is_some = 1;
        req.body.execute.snapshot_timestamp.value = *snapshot_timestamp;
    } else {
        req.body.execute.snapshot_timestamp.is_some = 0;
        req.body.execute.snapshot_timestamp.value = 0;
    }

    if (user_id) {
        uint64_t user_id_len = strlen(user_id);
        req.body.execute.user_id.is_some = 1;
        req.body.execute.user_id.value.len = user_id_len;
        req.body.execute.user_id.value.ptr = (char *)malloc(user_id_len + 1);
        if (req.body.execute.user_id.value.ptr) {
            memcpy(req.body.execute.user_id.value.ptr, user_id, user_id_len);
            req.body.execute.user_id.value.ptr[user_id_len] = '\0';
        }
    } else {
        req.body.execute.user_id.is_some = 0;
        req.body.execute.user_id.value.ptr = NULL;
        req.body.execute.user_id.value.len = 0;
    }

    Request *req_ptr = (Request *)malloc(sizeof(Request));
    if (req_ptr) {
        *req_ptr = req;
    }
    return req_ptr;
}

Request *request_new_list()
{
    Request req;
    req.tag = REQUEST_LIST;
    // No body fields

    Request *req_ptr = (Request *)malloc(sizeof(Request));
    if (req_ptr) {
        *req_ptr = req;
    }
    return req_ptr;
}

Request *request_new_begintx(IsolationLevel isolation, const char *user_id, uint64_t *read_timestamp)
{
    Request req;
    req.tag = REQUEST_BEGINTX;
    req.body.begintx.isolation = isolation;

    if (user_id) {
        uint64_t user_id_len = strlen(user_id);
        req.body.begintx.user_id.is_some = 1;
        req.body.begintx.user_id.value.len = user_id_len;
        req.body.begintx.user_id.value.ptr = (char *)malloc(user_id_len + 1);
        if (req.body.begintx.user_id.value.ptr) {
            memcpy(req.body.begintx.user_id.value.ptr, user_id, user_id_len);
            req.body.begintx.user_id.value.ptr[user_id_len] = '\0';
        }
    } else {
        req.body.begintx.user_id.is_some = 0;
        req.body.begintx.user_id.value.ptr = NULL;
        req.body.begintx.user_id.value.len = 0;
    }

    if (read_timestamp) {
        req.body.begintx.read_timestamp.is_some = 1;
        req.body.begintx.read_timestamp.value = *read_timestamp;
    } else {
        req.body.begintx.read_timestamp.is_some = 0;
        req.body.begintx.read_timestamp.value = 0;
    }

    Request *req_ptr = (Request *)malloc(sizeof(Request));
    if (req_ptr) {
        *req_ptr = req;
    }
    return req_ptr;
}

Request *request_new_committx(uint64_t tx_id)
{
    Request req;
    req.tag = REQUEST_COMMITTX;
    req.body.committx.tx_id = tx_id;

    Request *req_ptr = (Request *)malloc(sizeof(Request));
    if (req_ptr) {
        *req_ptr = req;
    }
    return req_ptr;
}

Request *request_new_rollbacktx(uint64_t tx_id)
{
    Request req;
    req.tag = REQUEST_ROLLBACKTX;
    req.body.rollbacktx.tx_id = tx_id;

    Request *req_ptr = (Request *)malloc(sizeof(Request));
    if (req_ptr) {
        *req_ptr = req;
    }
    return req_ptr;
}

/*
+-------------------------+
| frame_len      : u32 LE | - total bytes after this field 
+-------------------------+
| version        : u8     | - protocol version (currently 1)
+-------------------------+
| kind           : u8     | - 0 = Request, 1 = Response
+-------------------------+
| msg_type       : u8     | - which Request/Response variant
+-------------------------+
| flags          : u8     | - bitfield, 0 for now
+-------------------------+
| correlation_id : u32 LE | - client-chosen id (0 allowed)
+-------------------------+
| body...                 | - message-specific payload
+-------------------------+
*/
uint8_t *request_encode(const Request *request, uint64_t *out_length)
{
    // Create a buffer for header and one for body
    uint8_t *header = (uint8_t *)malloc(8);
    uint8_t *body = NULL;
    uint64_t body_length = 0;
    uint64_t head_length = 8; // version(1) + kind(1) + msg_type(1) + flags(1) + correlation_id(4)

    if (!header) return NULL;

    // Write header (except msg_type which depends on request type)
    header[0] = 1; // version
    header[1] = 0; // kind = 0 (Request)
    // header[2] = msg_type (set in switch)
    header[3] = 0; // flags
    write_u32_le(&header[4], 0); // correlation_id = 0

    // Encode body based on request type
    switch (request->tag) {
        case REQUEST_CONNECT:
        {
            header[2] = 0x01; // msg_type
            body_length = 1 + 1 + (request->body.connect.auth_token.ptr ? 4 + request->body.connect.auth_token.len : 0);
            body = (uint8_t *)malloc(body_length);
            if (!body) { free(header); return NULL; }
            
            uint64_t offset = 0;
            body[offset++] = request->body.connect.protocol_version;
            
            // Option<String> for auth_token
            if (request->body.connect.auth_token.ptr) {
                body[offset++] = 1; // Some
                write_u32_le(&body[offset], (uint32_t)request->body.connect.auth_token.len);
                offset += 4;
                memcpy(&body[offset], request->body.connect.auth_token.ptr, request->body.connect.auth_token.len);
            } else {
                body[offset++] = 0; // None
            }
            break;
        }
        case REQUEST_EXECUTE:
        {
            header[2] = 0x02; // msg_type
            
            // Calculate body size
            body_length = 4 + request->body.execute.query.len; // query
            body_length += 4; // params count
            for (uint64_t i = 0; i < request->body.execute.params.len; i++) {
                uint64_t param_len = 0;
                uint8_t *param_bytes = value_to_bytes(&request->body.execute.params.ptr[i], NULL, &param_len);
                body_length += 4 + param_len;
                free(param_bytes);
            }
            body_length += 1 + (request->body.execute.snapshot_timestamp.is_some ? 8U : 0U); // snapshot_timestamp
            body_length += 1 + (request->body.execute.user_id.is_some ? 4 + request->body.execute.user_id.value.len : 0U); // user_id
            
            body = (uint8_t *)malloc(body_length);
            if (!body) { free(header); return NULL; }
            
            uint64_t offset = 0;
            // query
            write_u32_le(&body[offset], (uint32_t)request->body.execute.query.len);
            offset += 4;
            memcpy(&body[offset], request->body.execute.query.ptr, request->body.execute.query.len);
            offset += request->body.execute.query.len;
            
            // params
            write_u32_le(&body[offset], (uint32_t)request->body.execute.params.len);
            offset += 4;
            for (uint64_t i = 0; i < request->body.execute.params.len; i++) {
                uint64_t param_len = 0;
                uint8_t *param_bytes = value_to_bytes(&request->body.execute.params.ptr[i], NULL, &param_len);
                write_u32_le(&body[offset], (uint32_t)param_len);
                offset += 4;
                memcpy(&body[offset], param_bytes, param_len);
                offset += param_len;
                free(param_bytes);
            }
            
            // snapshot_timestamp
            body[offset++] = request->body.execute.snapshot_timestamp.is_some;
            if (request->body.execute.snapshot_timestamp.is_some) {
                write_u64_le(&body[offset], request->body.execute.snapshot_timestamp.value);
                offset += 8;
            }
            
            // user_id
            body[offset++] = request->body.execute.user_id.is_some;
            if (request->body.execute.user_id.is_some) {
                write_u32_le(&body[offset], (uint32_t)request->body.execute.user_id.value.len);
                offset += 4;
                memcpy(&body[offset], request->body.execute.user_id.value.ptr, request->body.execute.user_id.value.len);
            }
            break;
        }
        case REQUEST_LIST:
        {
            header[2] = 0x03; // msg_type
            body_length = 0;
            body = NULL;
            break;
        }
        case REQUEST_BEGINTX:
        {
            header[2] = 0x04; // msg_type
            body_length = 1 + 1 + (request->body.begintx.user_id.is_some ? 4 + request->body.begintx.user_id.value.len : 0)
                          + 1 + (request->body.begintx.read_timestamp.is_some ? 8 : 0);
            body = (uint8_t *)malloc(body_length);
            if (!body) { free(header); return NULL; }
            
            uint64_t offset = 0;
            body[offset++] = (uint8_t)request->body.begintx.isolation + 1; // +1 to match Rust encoding
            
            // user_id
            body[offset++] = request->body.begintx.user_id.is_some;
            if (request->body.begintx.user_id.is_some) {
                write_u32_le(&body[offset], (uint32_t)request->body.begintx.user_id.value.len);
                offset += 4;
                memcpy(&body[offset], request->body.begintx.user_id.value.ptr, request->body.begintx.user_id.value.len);
                offset += request->body.begintx.user_id.value.len;
            }
            
            // read_timestamp
            body[offset++] = request->body.begintx.read_timestamp.is_some;
            if (request->body.begintx.read_timestamp.is_some) {
                write_u64_le(&body[offset], request->body.begintx.read_timestamp.value);
            }
            break;
        }
        case REQUEST_COMMITTX:
        {
            header[2] = 0x05; // msg_type
            body_length = 8;
            body = (uint8_t *)malloc(body_length);
            if (!body) { free(header); return NULL; }
            write_u64_le(body, request->body.committx.tx_id);
            break;
        }
        case REQUEST_ROLLBACKTX:
        {
            header[2] = 0x06; // msg_type
            body_length = 8;
            body = (uint8_t *)malloc(body_length);
            if (!body) { free(header); return NULL; }
            write_u64_le(body, request->body.rollbacktx.tx_id);
            break;
        }
        default:
            free(header);
            *out_length = 0;
            return NULL;
    }

    // Build final buffer: [frame_len(4)][header(8)][body(body_length)]
    uint32_t frame_len = (uint32_t)(head_length + body_length);
    uint64_t total_length = 4 + frame_len;
    uint8_t *result = (uint8_t *)malloc(total_length);
    if (!result) {
        free(header);
        free(body);
        return NULL;
    }
    
    write_u32_le(result, frame_len);
    memcpy(result + 4, header, head_length);
    if (body_length > 0) {
        memcpy(result + 4 + head_length, body, body_length);
    }
    
    *out_length = total_length;
    free(header);
    free(body);
    return result;
}

Request *request_decode(const uint8_t *buffer, uint64_t length)
{
    if (length < 4) return NULL;
    
    uint32_t frame_len = read_u32_le(buffer);
    
    if (length < 4 + frame_len) return NULL;
    
    const uint8_t *data = buffer + 4;
    uint64_t offset = 0;
    
    // Parse header
    uint8_t version = data[offset++];
    if (version != 1) return NULL;
    
    uint8_t kind = data[offset++];
    if (kind != 0) return NULL; // Must be Request
    
    uint8_t msg_type = data[offset++];
    offset++; // Skip flags
    offset += 4; // Skip correlation_id
    
    Request *req = (Request *)malloc(sizeof(Request));
    if (!req) return NULL;
    
    switch (msg_type) {
        case 0x01: // Connect
        {
            req->tag = REQUEST_CONNECT;
            req->body.connect.protocol_version = data[offset++];
            
            uint8_t has_token = data[offset++];
            if (has_token) {
                uint32_t token_len = read_u32_le(&data[offset]);
                offset += 4;
                req->body.connect.auth_token.len = token_len;
                req->body.connect.auth_token.ptr = (char *)malloc(token_len + 1);
                memcpy(req->body.connect.auth_token.ptr, &data[offset], token_len);
                req->body.connect.auth_token.ptr[token_len] = '\0';
                offset += token_len;
            } else {
                req->body.connect.auth_token.ptr = NULL;
                req->body.connect.auth_token.len = 0;
            }
            break;
        }
        case 0x02: // Execute
        {
            req->tag = REQUEST_EXECUTE;
            
            // query
            uint32_t query_len;
            memcpy(&query_len, &data[offset], 4);
            offset += 4;
            req->body.execute.query.len = query_len;
            req->body.execute.query.ptr = (char *)malloc(query_len + 1);
            memcpy(req->body.execute.query.ptr, &data[offset], query_len);
            req->body.execute.query.ptr[query_len] = '\0';
            offset += query_len;
            
            // params
            uint32_t params_len;
            memcpy(&params_len, &data[offset], 4);
            offset += 4;
            req->body.execute.params.len = params_len;
            req->body.execute.params.ptr = (struct Value *)malloc(sizeof(struct Value) * params_len);
            
            for (uint32_t i = 0; i < params_len; i++) {
                uint32_t param_len;
                memcpy(&param_len, &data[offset], 4);
                offset += 4;
                
                uint64_t consumed = 0;
                Value *param_value = value_from_bytes(&data[offset], param_len, &consumed);
                if (!param_value) {
                    // Cleanup on failure
                    for (uint32_t j = 0; j < i; j++) {
                        value_free(&req->body.execute.params.ptr[j]);
                    }
                    free(req->body.execute.params.ptr);
                    free(req->body.execute.query.ptr);
                    free(req);
                    return NULL;
                }
                
                req->body.execute.params.ptr[i] = *param_value;
                free(param_value);
                offset += consumed;
            }
            
            // snapshot_timestamp
            req->body.execute.snapshot_timestamp.is_some = data[offset++];
            if (req->body.execute.snapshot_timestamp.is_some) {
                memcpy(&req->body.execute.snapshot_timestamp.value, &data[offset], 8);
                offset += 8;
            }
            
            // user_id
            req->body.execute.user_id.is_some = data[offset++];
            if (req->body.execute.user_id.is_some) {
                uint32_t user_id_len;
                memcpy(&user_id_len, &data[offset], 4);
                offset += 4;
                req->body.execute.user_id.value.len = user_id_len;
                req->body.execute.user_id.value.ptr = (char *)malloc(user_id_len + 1);
                memcpy(req->body.execute.user_id.value.ptr, &data[offset], user_id_len);
                req->body.execute.user_id.value.ptr[user_id_len] = '\0';
            }
            break;
        }
        case 0x03: // List
        {
            req->tag = REQUEST_LIST;
            break;
        }
        case 0x04: // BeginTx
        {
            req->tag = REQUEST_BEGINTX;
            req->body.begintx.isolation = (IsolationLevel)(data[offset++] - 1);
            
            // user_id
            req->body.begintx.user_id.is_some = data[offset++];
            if (req->body.begintx.user_id.is_some) {
                uint32_t user_id_len;
                memcpy(&user_id_len, &data[offset], 4);
                offset += 4;
                req->body.begintx.user_id.value.len = user_id_len;
                req->body.begintx.user_id.value.ptr = (char *)malloc(user_id_len + 1);
                memcpy(req->body.begintx.user_id.value.ptr, &data[offset], user_id_len);
                req->body.begintx.user_id.value.ptr[user_id_len] = '\0';
                offset += user_id_len;
            }
            
            // read_timestamp
            req->body.begintx.read_timestamp.is_some = data[offset++];
            if (req->body.begintx.read_timestamp.is_some) {
                memcpy(&req->body.begintx.read_timestamp.value, &data[offset], 8);
            }
            break;
        }
        case 0x05: // CommitTx
        {
            req->tag = REQUEST_COMMITTX;
            memcpy(&req->body.committx.tx_id, &data[offset], 8);
            break;
        }
        case 0x06: // RollbackTx
        {
            req->tag = REQUEST_ROLLBACKTX;
            memcpy(&req->body.rollbacktx.tx_id, &data[offset], 8);
            break;
        }
        default:
            free(req);
            return NULL;
    }
    
    return req;
}

uint8_t *response_encode(const Response *response, uint64_t *out_length)
{
    // Implementation would go here
    (void)response;    // Mark as unused
    (void)out_length;  // Mark as unused
    return NULL;
}

static void execution_result_free(ExecutionResult *result) {
    if (!result) return;
    
    switch (result->tag) {
        case EXECUTION_RESULT_OK:
            if (result->body.ok.data) {
                value_free(result->body.ok.data);
                free(result->body.ok.data);
            }
            break;
        default:
            break;
    }
}

Response *response_decode(const uint8_t *buffer, uint64_t length)
{
    if (length < 4) return NULL;
    
    uint32_t frame_len = read_u32_le(buffer);
    
    if (length < 4 + frame_len) return NULL;
    
    const uint8_t *data = buffer + 4;
    uint64_t offset = 0;
    
    // Parse header
    uint8_t version = data[offset++];
    if (version != 1) return NULL;
    
    uint8_t kind = data[offset++];
    if (kind != 1) return NULL; // Must be Response
    
    uint8_t msg_type = data[offset++];
    offset++; // Skip flags
    offset += 4; // Skip correlation_id
    
    Response *resp = (Response *)malloc(sizeof(Response));
    if (!resp) return NULL;
    
    switch (msg_type) {
        case 0x01: // ConnectAck
        {
            resp->tag = RESPONSE_CONNECT_ACK;
            resp->body.connect_ack.protocol_version = data[offset++];
            
            // server_timestamp Option<u64>
            resp->body.connect_ack.server_timestamp.is_some = data[offset++];
            if (resp->body.connect_ack.server_timestamp.is_some) {
                resp->body.connect_ack.server_timestamp.value = read_u64_le(&data[offset]);
                offset += 8;
            }
            
            // user_permissions Option<Vec<String>>
            resp->body.connect_ack.user_permissions.is_some = data[offset++];
            if (resp->body.connect_ack.user_permissions.is_some) {
                uint32_t perm_count = read_u32_le(&data[offset]);
                offset += 4;
                
                resp->body.connect_ack.user_permissions.value.len = perm_count;
                resp->body.connect_ack.user_permissions.value.ptr = (char **)malloc(sizeof(char *) * perm_count);
                
                for (uint32_t i = 0; i < perm_count; i++) {
                    uint32_t str_len = read_u32_le(&data[offset]);
                    offset += 4;
                    
                    resp->body.connect_ack.user_permissions.value.ptr[i] = (char *)malloc(str_len + 1);
                    memcpy(resp->body.connect_ack.user_permissions.value.ptr[i], &data[offset], str_len);
                    resp->body.connect_ack.user_permissions.value.ptr[i][str_len] = '\0';
                    offset += str_len;
                }
            }
            break;
        }
        case 0x02: // Success
        {
            resp->tag = RESPONSE_SUCCESS;
            
            uint32_t result_count = read_u32_le(&data[offset]);
            offset += 4;
            
            resp->body.success.result.len = result_count;
            resp->body.success.result.ptr = (ExecutionResult *)malloc(sizeof(ExecutionResult) * result_count);
            
            for (uint32_t i = 0; i < result_count; i++) {
                uint8_t tag = data[offset++];
                
                switch (tag) {
                    case 0: // Ok
                    {
                        resp->body.success.result.ptr[i].tag = EXECUTION_RESULT_OK;
                        
                        // data: Value
                        uint32_t value_len = read_u32_le(&data[offset]);
                        offset += 4;
                        
                        uint64_t consumed = 0;
                        Value *value_data = value_from_bytes(&data[offset], value_len, &consumed);
                        if (!value_data) {
                            // Cleanup and return NULL
                            for (uint32_t j = 0; j < i; j++) {
                                execution_result_free(&resp->body.success.result.ptr[j]);
                            }
                            free(resp->body.success.result.ptr);
                            free(resp);
                            return NULL;
                        }
                        resp->body.success.result.ptr[i].body.ok.data = value_data;
                        offset += consumed;
                        
                        // time: u64
                        resp->body.success.result.ptr[i].body.ok.time = read_u64_le(&data[offset]);
                        offset += 8;
                        
                        // commit_timestamp: Option<u64>
                        resp->body.success.result.ptr[i].body.ok.commit_timestamp.is_some = data[offset++];
                        if (resp->body.success.result.ptr[i].body.ok.commit_timestamp.is_some) {
                            resp->body.success.result.ptr[i].body.ok.commit_timestamp.value = read_u64_le(&data[offset]);
                            offset += 8;
                        }
                        
                        // time_elapsed: Option<u64>
                        resp->body.success.result.ptr[i].body.ok.time_elapsed.is_some = data[offset++];
                        if (resp->body.success.result.ptr[i].body.ok.time_elapsed.is_some) {
                            resp->body.success.result.ptr[i].body.ok.time_elapsed.value = read_u64_le(&data[offset]);
                            offset += 8;
                        }
                        
                        // row_count: Option<u64>
                        resp->body.success.result.ptr[i].body.ok.row_count.is_some = data[offset++];
                        if (resp->body.success.result.ptr[i].body.ok.row_count.is_some) {
                            resp->body.success.result.ptr[i].body.ok.row_count.value = read_u64_le(&data[offset]);
                            offset += 8;
                        }
                        break;
                    }
                    case 1: // Created
                    {
                        resp->body.success.result.ptr[i].tag = EXECUTION_RESULT_CREATED;
                        
                        resp->body.success.result.ptr[i].body.created.time = read_u64_le(&data[offset]);
                        offset += 8;
                        
                        resp->body.success.result.ptr[i].body.created.commit_timestamp = read_u64_le(&data[offset]);
                        offset += 8;
                        break;
                    }
                    case 2: // Modified
                    {
                        resp->body.success.result.ptr[i].tag = EXECUTION_RESULT_MODIFIED;
                        
                        resp->body.success.result.ptr[i].body.modified.time = read_u64_le(&data[offset]);
                        offset += 8;
                        
                        resp->body.success.result.ptr[i].body.modified.commit_timestamp.is_some = data[offset++];
                        if (resp->body.success.result.ptr[i].body.modified.commit_timestamp.is_some) {
                            resp->body.success.result.ptr[i].body.modified.commit_timestamp.value = read_u64_le(&data[offset]);
                            offset += 8;
                        }
                        
                        resp->body.success.result.ptr[i].body.modified.rows_affected.is_some = data[offset++];
                        if (resp->body.success.result.ptr[i].body.modified.rows_affected.is_some) {
                            resp->body.success.result.ptr[i].body.modified.rows_affected.value = read_u64_le(&data[offset]);
                            offset += 8;
                        }
                        break;
                    }
                    default:
                        // Unknown tag, cleanup and fail
                        for (uint32_t j = 0; j < i; j++) {
                            execution_result_free(&resp->body.success.result.ptr[j]);
                        }
                        free(resp->body.success.result.ptr);
                        free(resp);
                        return NULL;
                }
            }
            break;
        }
        case 0x03: // Error
        {
            resp->tag = RESPONSE_ERROR;
            
            // code: u16 (little-endian)
            resp->body.error.code = data[offset] | (data[offset + 1] << 8);
            offset += 2;
            
            // message: String
            uint32_t msg_len = read_u32_le(&data[offset]);
            offset += 4;
            
            resp->body.error.message.len = msg_len;
            resp->body.error.message.ptr = (char *)malloc(msg_len + 1);
            memcpy(resp->body.error.message.ptr, &data[offset], msg_len);
            resp->body.error.message.ptr[msg_len] = '\0';
            break;
        }
        case 0x04: // Stream
        {
            resp->tag = RESPONSE_STREAM;
            break;
        }
        case 0x05: // Ack
        {
            resp->tag = RESPONSE_ACK;
            break;
        }
        default:
            free(resp);
            return NULL;
    }
    
    return resp;
}

void response_free(Response *response)
{
    if (!response) return;
    
    switch (response->tag) {
        case RESPONSE_CONNECT_ACK:
            if (response->body.connect_ack.user_permissions.is_some) {
                for (uint64_t i = 0; i < response->body.connect_ack.user_permissions.value.len; i++) {
                    free(response->body.connect_ack.user_permissions.value.ptr[i]);
                }
                free(response->body.connect_ack.user_permissions.value.ptr);
            }
            break;
        case RESPONSE_SUCCESS:
            for (uint64_t i = 0; i < response->body.success.result.len; i++) {
                execution_result_free(&response->body.success.result.ptr[i]);
            }
            free(response->body.success.result.ptr);
            break;
        case RESPONSE_ERROR:
            free(response->body.error.message.ptr);
            break;
        default:
            break;
    }
    
    free(response);
}

void request_free(Request *request)
{
    if (!request) return;
    
    switch (request->tag) {
        case REQUEST_CONNECT:
            free(request->body.connect.auth_token.ptr);
            break;
        case REQUEST_EXECUTE:
            free(request->body.execute.query.ptr);
            free(request->body.execute.params.ptr);
            if (request->body.execute.user_id.is_some) {
                free(request->body.execute.user_id.value.ptr);
            }
            break;
        case REQUEST_BEGINTX:
            if (request->body.begintx.user_id.is_some) {
                free(request->body.begintx.user_id.value.ptr);
            }
            break;
        default:
            break;
    }
    
    free(request);
}