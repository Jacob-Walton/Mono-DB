#include "value.h"
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

// Little-endian write helpers
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

static inline void write_i32_le(uint8_t *buf, int32_t val) {
    write_u32_le(buf, (uint32_t)val);
}

static inline void write_i64_le(uint8_t *buf, int64_t val) {
    write_u64_le(buf, (uint64_t)val);
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

static inline int32_t read_i32_le(const uint8_t *buf) {
    return (int32_t)read_u32_le(buf);
}

static inline int64_t read_i64_le(const uint8_t *buf) {
    return (int64_t)read_u64_le(buf);
}

void value_free(Value *value) {
    if (!value) return;

    switch (value->tag) {
        case VALUE_STRING:
            free(value->as.string.ptr);
            break;
        case VALUE_BINARY:
            free(value->as.binary.ptr);
            break;
        case VALUE_ARRAY:
            for (uint64_t i = 0; i < value->as.array.len; i++) {
                value_free(&value->as.array.ptr[i]);
            }
            free(value->as.array.ptr);
            break;
        case VALUE_OBJECT:
            for (uint64_t i = 0; i < value->as.object.len; i++) {
                free(value->as.object.ptr[i].key);
                value_free(value->as.object.ptr[i].value);
                free(value->as.object.ptr[i].value);
            }
            free(value->as.object.ptr);
            break;
        case VALUE_SET:
            for (uint64_t i = 0; i < value->as.set.len; i++) {
                free(value->as.set.ptr[i]);
            }
            free(value->as.set.ptr);
            break;
        case VALUE_ROW:
            for (uint64_t i = 0; i < value->as.row.len; i++) {
                free(value->as.row.ptr[i].key);
                value_free(value->as.row.ptr[i].value);
                free(value->as.row.ptr[i].value);
            }
            free(value->as.row.ptr);
            break;
        case VALUE_SORTEDSET:
            for (uint64_t i = 0; i < value->as.sortedset.len; i++) {
                free(value->as.sortedset.ptr[i].member);
            }
            free(value->as.sortedset.ptr);
            break;
        case VALUE_REFERENCE:
            value_free(value->as.reference.id);
            free(value->as.reference.id);
            free(value->as.reference.collection);
            break;
        default:
            break;
    }
}

Value *value_clone(const Value *value) {
    if (!value) return NULL;

    Value *new_value = (Value *)malloc(sizeof(Value));
    if (!new_value) return NULL;

    new_value->tag = value->tag;

    switch (value->tag) {
        case VALUE_STRING:
            new_value->as.string.len = value->as.string.len;
            new_value->as.string.ptr = (char *)malloc(new_value->as.string.len);
            if (new_value->as.string.ptr) {
                memcpy(new_value->as.string.ptr, value->as.string.ptr, new_value->as.string.len);
            }
            break;
        case VALUE_BINARY:
            new_value->as.binary.len = value->as.binary.len;
            new_value->as.binary.ptr = (uint8_t *)malloc(new_value->as.binary.len);
            if (new_value->as.binary.ptr) {
                memcpy(new_value->as.binary.ptr, value->as.binary.ptr, new_value->as.binary.len);
            }
            break;
        case VALUE_ARRAY:
            new_value->as.array.len = value->as.array.len;
            new_value->as.array.ptr = (Value *)malloc(sizeof(Value) * new_value->as.array.len);
            for (uint64_t i = 0; i < new_value->as.array.len; i++) {
                Value *cloned_element = value_clone(&value->as.array.ptr[i]);
                if (cloned_element) {
                    new_value->as.array.ptr[i] = *cloned_element;
                    free(cloned_element);
                }
            }
            break;
        // TODO: Other types
        default:
            memcpy(&new_value->as, &value->as, sizeof(new_value->as));
            break;
    }

    return new_value;
}

// Initializers

Value value_init_null() {
    Value value;
    value.tag = VALUE_NULL;
    
    return value;
}

Value value_new_bool(bool b) {
    Value value;
    value.tag = VALUE_BOOL;
    value.as.boolean = b;
    
    return value;
}

Value value_new_int32(int32_t i) {
    Value value;
    value.tag = VALUE_INT32;
    value.as.i32 = i;

    return value;
}

Value value_new_int64(int64_t i) {
    Value value;
    value.tag = VALUE_INT64;
    value.as.i64 = i;

    return value;
}

Value value_new_float32(float f) {
    Value value;
    value.tag = VALUE_FLOAT32;
    value.as.f32 = f;

    return value;
}

Value value_new_float64(double d) {
    Value value;
    value.tag = VALUE_FLOAT64;
    value.as.f64 = d;

    return value;
}

Value value_new_string(const char *str, uint64_t len) {
    Value value;
    value.tag = VALUE_STRING;
    value.as.string.len = len;
    value.as.string.ptr = (char *)malloc(len);
    if (value.as.string.ptr) {
        memcpy(value.as.string.ptr, str, len);
    }

    return value;
}

Value value_new_binary(const uint8_t *data, uint64_t len) {
    Value value;
    value.tag = VALUE_BINARY;
    value.as.binary.len = len;
    value.as.binary.ptr = (uint8_t *)malloc(len);
    if (value.as.binary.ptr) {
        memcpy(value.as.binary.ptr, data, len);
    }

    return value;
}

Value value_new_date(Date date) {
    Value value;
    value.tag = VALUE_DATE;
    value.as.date = date;

    return value;
}

Value value_new_time(Time time) {
    Value value;
    value.tag = VALUE_TIME;
    value.as.time = time;

    return value;
}

Value value_new_datetime(DateTime datetime) {
    Value value;
    value.tag = VALUE_DATETIME;
    value.as.datetime = datetime;

    return value;
}

Value value_new_uuid(const uint8_t uuid[16]) {
    Value value;
    value.tag = VALUE_UUID;
    memcpy(value.as.uuid, uuid, 16);

    return value;
}

Value value_new_objectid(const uint8_t objectid[12]) {
    Value value;
    value.tag = VALUE_OBJECTID;
    memcpy(value.as.objectid, objectid, 12);

    return value;
}

Value value_new_array(const Value *elements, uint64_t len) {
    Value value;
    value.tag = VALUE_ARRAY;
    value.as.array.len = len;
    value.as.array.ptr = (Value *)malloc(sizeof(Value) * len);
    for (uint64_t i = 0; i < len; i++) {
        value.as.array.ptr[i] = elements[i];
    }

    return value;
}

Value value_new_object(const ObjectEntry *entries, uint64_t len) {
    Value value;
    value.tag = VALUE_OBJECT;
    value.as.object.len = len;
    value.as.object.ptr = (ObjectEntry *)malloc(sizeof(ObjectEntry) * len);
    for (uint64_t i = 0; i < len; i++) {
        value.as.object.ptr[i] = entries[i];
    }

    return value;
}

Value value_new_set(char **elements, uint64_t len) {
    Value value;
    value.tag = VALUE_SET;
    value.as.set.len = len;
    value.as.set.ptr = (char **)malloc(sizeof(char *) * len);
    for (uint64_t i = 0; i < len; i++) {
        value.as.set.ptr[i] = elements[i];
    }

    return value;
}

Value value_new_row(const ObjectEntry *entries, uint64_t len) {
    Value value;
    value.tag = VALUE_ROW;
    value.as.row.len = len;
    value.as.row.ptr = (ObjectEntry *)malloc(sizeof(ObjectEntry) * len);
    for (uint64_t i = 0; i < len; i++) {
        value.as.row.ptr[i] = entries[i];
    }

    return value;
}

Value value_new_sortedset(const SortedEntry *entries, uint64_t len) {
    Value value;
    value.tag = VALUE_SORTEDSET;
    value.as.sortedset.len = len;
    value.as.sortedset.ptr = (SortedEntry *)malloc(sizeof(SortedEntry) * len);
    for (uint64_t i = 0; i < len; i++) {
        value.as.sortedset.ptr[i] = entries[i];
    }

    return value;
}

Value value_new_geopoint(double lat, double lng) {
    Value value;
    value.tag = VALUE_GEOPOINT;
    value.as.geopoint.lat = lat;
    value.as.geopoint.lng = lng;

    return value;
}

Value value_new_reference(const char *collection, const Value *id) {
    Value value;
    value.tag = VALUE_REFERENCE;
    value.as.reference.collection = (char *)malloc(strlen(collection) + 1);
    if (value.as.reference.collection) {
        strcpy(value.as.reference.collection, collection);
    }
    value.as.reference.id = (Value *)malloc(sizeof(Value));
    if (value.as.reference.id) {
        *(value.as.reference.id) = *id;
    }

    return value;
}

bool value_equals(const Value *a, const Value *b) {
    if (a->tag != b->tag) {
        return false;
    }

    switch (a->tag) {
        case VALUE_BOOL:
            return a->as.boolean == b->as.boolean;
        case VALUE_INT32:
            return a->as.i32 == b->as.i32;
        case VALUE_INT64:
            return a->as.i64 == b->as.i64;
        case VALUE_FLOAT32:
            return a->as.f32 == b->as.f32;
        case VALUE_FLOAT64:
            return a->as.f64 == b->as.f64;
        case VALUE_STRING:
            return (a->as.string.len == b->as.string.len) &&
                   (memcmp(a->as.string.ptr, b->as.string.ptr, a->as.string.len) == 0);
        case VALUE_BINARY:
            return (a->as.binary.len == b->as.binary.len) &&
                     (memcmp(a->as.binary.ptr, b->as.binary.ptr, a->as.binary.len) == 0);
        case VALUE_NULL:
            return true;
        default:
            return false; // TODO: Implement other types
    }
}

bool value_is_truthy(const Value *value) {
    switch (value->tag) {
        case VALUE_BOOL:
            return value->as.boolean;
        case VALUE_INT32:
            return value->as.i32 != 0;
        case VALUE_INT64:
            return value->as.i64 != 0;
        case VALUE_FLOAT32:
            return value->as.f32 != 0.0f;
        case VALUE_FLOAT64:
            return value->as.f64 != 0.0;
        case VALUE_STRING:
            return value->as.string.len > 0;
        case VALUE_BINARY:
            return value->as.binary.len > 0;
        case VALUE_ARRAY:
            return value->as.array.len > 0;
        case VALUE_OBJECT:
            return value->as.object.len > 0;
        case VALUE_SET:
            return value->as.set.len > 0;
        case VALUE_ROW:
            return value->as.row.len > 0;
        case VALUE_SORTEDSET:
            return value->as.sortedset.len > 0;
        case VALUE_NULL:
            return false;
        default:
            return true; // Other types are considered truthy
    }
}

uint8_t *value_to_bytes(const Value *value, uint8_t *buffer, uint64_t *length)
{
    (void)buffer; // Mark as intentionally unused
    
    // Create a dynamic buffer to hold the serialized data
    uint8_t *buf = (uint8_t *)malloc(1);
    uint64_t buf_size = 0;

    switch (value->tag) {
        case VALUE_NULL:
            buf = (uint8_t *)realloc(buf, buf_size + 1);
            buf[buf_size] = 0;
            buf_size += 1;
            break;
        case VALUE_BOOL:
            buf = (uint8_t *)realloc(buf, buf_size + 2);
            buf[buf_size] = 1;
            buf[buf_size + 1] = value->as.boolean ? 1 : 0;
            buf_size += 2;
            break;
        case VALUE_INT32:
            buf = (uint8_t *)realloc(buf, buf_size + 5);
            buf[buf_size] = 2;
            write_i32_le(buf + buf_size + 1, value->as.i32);
            buf_size += 5;
            break;
        case VALUE_INT64:
            buf = (uint8_t *)realloc(buf, buf_size + 9);
            buf[buf_size] = 3;
            write_i64_le(buf + buf_size + 1, value->as.i64);
            buf_size += 9;
            break;
        case VALUE_FLOAT32:
            buf = (uint8_t *)realloc(buf, buf_size + 5);
            buf[buf_size] = 4;
            {
                uint32_t bits;
                memcpy(&bits, &value->as.f32, 4);
                write_u32_le(buf + buf_size + 1, bits);
            }
            buf_size += 5;
            break;
        case VALUE_FLOAT64:
            buf = (uint8_t *)realloc(buf, buf_size + 9);
            buf[buf_size] = 5;
            {
                uint64_t bits;
                memcpy(&bits, &value->as.f64, 8);
                write_u64_le(buf + buf_size + 1, bits);
            }
            buf_size += 9;
            break;
        case VALUE_STRING:
            buf = (uint8_t *)realloc(buf, buf_size + 1 + 4 + value->as.string.len);
            buf[buf_size] = 6;
            write_u32_le(buf + buf_size + 1, (uint32_t)value->as.string.len);
            memcpy(buf + buf_size + 1 + 4, value->as.string.ptr, value->as.string.len);
            buf_size += 1 + 4 + value->as.string.len;
            break;
        case VALUE_BINARY:
            buf = (uint8_t *)realloc(buf, buf_size + 1 + 4 + value->as.binary.len);
            buf[buf_size] = 7;
            write_u32_le(buf + buf_size + 1, (uint32_t)value->as.binary.len);
            memcpy(buf + buf_size + 1 + 4, value->as.binary.ptr, value->as.binary.len);
            buf_size += 1 + 4 + value->as.binary.len;
            break;
        case VALUE_DATE:
            buf = (uint8_t *)realloc(buf, buf_size + 1 + 4 + 1 + 1);
            buf[buf_size] = 8;
            write_i32_le(buf + buf_size + 1, value->as.date.year);
            buf[buf_size + 1 + 4] = value->as.date.month;
            buf[buf_size + 1 + 4 + 1] = value->as.date.day;
            buf_size += 1 + 4 + 1 + 1;
            break;
        case VALUE_TIME:
            buf = (uint8_t *)realloc(buf, buf_size + 1 + 1 + 1 + 1 + 4);
            buf[buf_size] = 9;
            buf[buf_size + 1] = value->as.time.hour;
            buf[buf_size + 1 + 1] = value->as.time.minute;
            buf[buf_size + 1 + 1 + 1] = value->as.time.second;
            write_u32_le(buf + buf_size + 1 + 1 + 1 + 1, value->as.time.microsecond);
            buf_size += 1 + 1 + 1 + 1 + 4;
            break;
        case VALUE_DATETIME:
            buf = (uint8_t *)realloc(buf, buf_size + 1 + 8 + 4);
            buf[buf_size] = 10;
            write_i64_le(buf + buf_size + 1, value->as.datetime.timestamp);
            write_i32_le(buf + buf_size + 1 + 8, value->as.datetime.offset_minutes);
            buf_size += 1 + 8 + 4;
            break;
        case VALUE_UUID:
            buf = (uint8_t *)realloc(buf, buf_size + 1 + 16);
            buf[buf_size] = 11;
            memcpy(buf + buf_size + 1, value->as.uuid, 16);
            buf_size += 1 + 16;
            break;
        case VALUE_OBJECTID:
            buf = (uint8_t *)realloc(buf, buf_size + 1 + 12);
            buf[buf_size] = 12;
            memcpy(buf + buf_size + 1, value->as.objectid, 12);
            buf_size += 1 + 12;
            break;
        case VALUE_ARRAY:
            buf = (uint8_t *)realloc(buf, buf_size + 1 + 4);
            buf[buf_size] = 13;
            write_u32_le(buf + buf_size + 1, (uint32_t)value->as.array.len);
            buf_size += 1 + 4;
            for (uint64_t i = 0; i < value->as.array.len; i++) {
                uint64_t elem_length = 0;
                uint8_t *elem_bytes = value_to_bytes(&value->as.array.ptr[i], NULL, &elem_length);
                buf = (uint8_t *)realloc(buf, buf_size + elem_length);
                memcpy(buf + buf_size, elem_bytes, elem_length);
                buf_size += elem_length;
                free(elem_bytes);
            }
            break;
        case VALUE_OBJECT:
            buf = (uint8_t *)realloc(buf, buf_size + 1 + 4);
            buf[buf_size] = 14;
            write_u32_le(buf + buf_size + 1, (uint32_t)value->as.object.len);
            buf_size += 1 + 4;
            for (uint64_t i = 0; i < value->as.object.len; i++) {
                uint32_t key_len = (uint32_t)strlen(value->as.object.ptr[i].key);
                buf = (uint8_t *)realloc(buf, buf_size + 4 + key_len);
                write_u32_le(buf + buf_size, key_len);
                memcpy(buf + buf_size + 4, value->as.object.ptr[i].key, key_len);
                buf_size += 4 + key_len;

                uint64_t val_length = 0;
                uint8_t *val_bytes = value_to_bytes(value->as.object.ptr[i].value, NULL, &val_length);
                buf = (uint8_t *)realloc(buf, buf_size + val_length);
                memcpy(buf + buf_size, val_bytes, val_length);
                buf_size += val_length;
                free(val_bytes);
            }
            break;
        case VALUE_SET:
            buf = (uint8_t *)realloc(buf, buf_size + 1 + 4);
            buf[buf_size] = 15;
            write_u32_le(buf + buf_size + 1, (uint32_t)value->as.set.len);
            buf_size += 1 + 4;
            for (uint64_t i = 0; i < value->as.set.len; i++) {
                uint32_t str_len = (uint32_t)strlen(value->as.set.ptr[i]);
                buf = (uint8_t *)realloc(buf, buf_size + 4 + str_len);
                write_u32_le(buf + buf_size, str_len);
                memcpy(buf + buf_size + 4, value->as.set.ptr[i], str_len);
                buf_size += 4 + str_len;
            }
            break;
        case VALUE_ROW:
            buf = (uint8_t *)realloc(buf, buf_size + 1 + 4);
            buf[buf_size] = 16;
            write_u32_le(buf + buf_size + 1, (uint32_t)value->as.row.len);
            buf_size += 1 + 4;
            for (uint64_t i = 0; i < value->as.row.len; i++) {
                uint32_t key_len = (uint32_t)strlen(value->as.row.ptr[i].key);
                buf = (uint8_t *)realloc(buf, buf_size + 4 + key_len);
                write_u32_le(buf + buf_size, key_len);
                memcpy(buf + buf_size + 4, value->as.row.ptr[i].key, key_len);
                buf_size += 4 + key_len;
                uint64_t val_length = 0;
                uint8_t *val_bytes = value_to_bytes(value->as.row.ptr[i].value, NULL, &val_length);
                buf = (uint8_t *)realloc(buf, buf_size + val_length);
                memcpy(buf + buf_size, val_bytes, val_length);
                buf_size += val_length;
                free(val_bytes);
            }
            break;
        case VALUE_SORTEDSET:
            buf = (uint8_t *)realloc(buf, buf_size + 1 + 4);
            buf[buf_size] = 17;
            write_u32_le(buf + buf_size + 1, (uint32_t)value->as.sortedset.len);
            buf_size += 1 + 4;
            for (uint64_t i = 0; i < value->as.sortedset.len; i++) {
                uint32_t member_len = (uint32_t)strlen(value->as.sortedset.ptr[i].member);
                buf = (uint8_t *)realloc(buf, buf_size + 8 + 4 + member_len);
                uint64_t score_bits;
                memcpy(&score_bits, &value->as.sortedset.ptr[i].score, 8);
                write_u64_le(buf + buf_size, score_bits);
                write_u32_le(buf + buf_size + 8, member_len);
                memcpy(buf + buf_size + 8 + 4, value->as.sortedset.ptr[i].member, member_len);
                buf_size += 8 + 4 + member_len;
            }
            break;
        case VALUE_GEOPOINT:
            buf = (uint8_t *)realloc(buf, buf_size + 1 + 16);
            buf[buf_size] = 18;
            {
                uint64_t lat_bits, lng_bits;
                memcpy(&lat_bits, &value->as.geopoint.lat, 8);
                memcpy(&lng_bits, &value->as.geopoint.lng, 8);
                write_u64_le(buf + buf_size + 1, lat_bits);
                write_u64_le(buf + buf_size + 1 + 8, lng_bits);
            }
            buf_size += 1 + 16;
            break;
        case VALUE_REFERENCE:
            buf = (uint8_t *)realloc(buf, buf_size + 1);
            buf[buf_size] = 19;
            buf_size += 1;
            {
                uint32_t coll_len = (uint32_t)strlen(value->as.reference.collection);
                buf = (uint8_t *)realloc(buf, buf_size + 4 + coll_len);
                write_u32_le(buf + buf_size, coll_len);
                memcpy(buf + buf_size + 4, value->as.reference.collection, coll_len);
                buf_size += 4 + coll_len;
            }
            {
                uint64_t id_length = 0;
                uint8_t *id_bytes = value_to_bytes(value->as.reference.id, NULL, &id_length);
                buf = (uint8_t *)realloc(buf, buf_size + id_length);
                memcpy(buf + buf_size, id_bytes, id_length);
                buf_size += id_length;
                free(id_bytes);
            }
            break;
        default:
            // Unsupported type
            free(buf);
            *length = 0;
            return NULL;
    }

    // Return the serialized buffer
    *length = buf_size;
    return buf;
}

Value *value_from_bytes(const uint8_t *buffer, uint64_t length, uint64_t *consumed_length)
{
    if (!buffer || length == 0) {
        return NULL;
    }

    uint64_t offset = 0;
    uint8_t kind = buffer[offset++];

    Value *value = (Value *)malloc(sizeof(Value));
    if (!value) return NULL;

    #define NEED(n) do { \
        if (offset + (n) > length) { \
            free(value); \
            return NULL; \
        } \
    } while(0)

    switch (kind) {
        case 0: // Null
            value->tag = VALUE_NULL;
            break;

        case 1: // Bool
            NEED(1);
            value->tag = VALUE_BOOL;
            value->as.boolean = buffer[offset++] != 0;
            break;

        case 2: // Int32
            NEED(4);
            value->tag = VALUE_INT32;
            value->as.i32 = read_i32_le(&buffer[offset]);
            offset += 4;
            break;

        case 3: // Int64
            NEED(8);
            value->tag = VALUE_INT64;
            value->as.i64 = read_i64_le(&buffer[offset]);
            offset += 8;
            break;

        case 4: // Float32
            NEED(4);
            value->tag = VALUE_FLOAT32;
            {
                uint32_t bits = read_u32_le(&buffer[offset]);
                memcpy(&value->as.f32, &bits, 4);
            }
            offset += 4;
            break;

        case 5: // Float64
            NEED(8);
            value->tag = VALUE_FLOAT64;
            {
                uint64_t bits = read_u64_le(&buffer[offset]);
                memcpy(&value->as.f64, &bits, 8);
            }
            offset += 8;
            break;

        case 6: // String
        {
            NEED(4);
            uint32_t str_len = read_u32_le(&buffer[offset]);
            offset += 4;
            NEED(str_len);
            value->tag = VALUE_STRING;
            value->as.string.len = str_len;
            value->as.string.ptr = (char *)malloc(str_len);
            if (value->as.string.ptr) {
                memcpy(value->as.string.ptr, &buffer[offset], str_len);
            }
            offset += str_len;
            break;
        }

        case 7: // Binary
        {
            NEED(4);
            uint32_t bin_len = read_u32_le(&buffer[offset]);
            offset += 4;
            NEED(bin_len);
            value->tag = VALUE_BINARY;
            value->as.binary.len = bin_len;
            value->as.binary.ptr = (uint8_t *)malloc(bin_len);
            if (value->as.binary.ptr) {
                memcpy(value->as.binary.ptr, &buffer[offset], bin_len);
            }
            offset += bin_len;
            break;
        }

        case 8: // Date
        {
            NEED(4 + 2);
            value->tag = VALUE_DATE;
            value->as.date.year = read_i32_le(&buffer[offset]);
            offset += 4;
            value->as.date.month = buffer[offset++];
            value->as.date.day = buffer[offset++];
            break;
        }

        case 9: // Time
        {
            NEED(3 + 4);
            value->tag = VALUE_TIME;
            value->as.time.hour = buffer[offset++];
            value->as.time.minute = buffer[offset++];
            value->as.time.second = buffer[offset++];
            value->as.time.microsecond = read_u32_le(&buffer[offset]);
            offset += 4;
            break;
        }

        case 10: // DateTime
        {
            NEED(8 + 4);
            value->tag = VALUE_DATETIME;
            value->as.datetime.timestamp = read_i64_le(&buffer[offset]);
            offset += 8;
            value->as.datetime.offset_minutes = read_i32_le(&buffer[offset]);
            offset += 4;
            break;
        }

        case 11: // UUID
        {
            NEED(16);
            value->tag = VALUE_UUID;
            memcpy(value->as.uuid, &buffer[offset], 16);
            offset += 16;
            break;
        }

        case 12: // ObjectId
        {
            NEED(12);
            value->tag = VALUE_OBJECTID;
            memcpy(value->as.objectid, &buffer[offset], 12);
            offset += 12;
            break;
        }

        case 13: // Array
        {
            NEED(4);
            uint32_t arr_len = read_u32_le(&buffer[offset]);
            offset += 4;
            value->tag = VALUE_ARRAY;
            value->as.array.len = arr_len;
            value->as.array.ptr = (Value *)malloc(sizeof(Value) * arr_len);
            
            for (uint32_t i = 0; i < arr_len; i++) {
                uint64_t elem_consumed = 0;
                Value *elem = value_from_bytes(&buffer[offset], length - offset, &elem_consumed);
                if (!elem) {
                    // Cleanup on failure
                    for (uint32_t j = 0; j < i; j++) {
                        value_free(&value->as.array.ptr[j]);
                    }
                    free(value->as.array.ptr);
                    free(value);
                    return NULL;
                }
                value->as.array.ptr[i] = *elem;
                free(elem);
                offset += elem_consumed;
            }
            break;
        }

        case 14: // Object
        {
            NEED(4);
            uint32_t obj_len = read_u32_le(&buffer[offset]);
            offset += 4;
            value->tag = VALUE_OBJECT;
            value->as.object.len = obj_len;
            value->as.object.ptr = (ObjectEntry *)malloc(sizeof(ObjectEntry) * obj_len);
            
            for (uint32_t i = 0; i < obj_len; i++) {
                NEED(4);
                uint32_t key_len = read_u32_le(&buffer[offset]);
                offset += 4;
                NEED(key_len);
                
                char *key = (char *)malloc(key_len + 1);
                memcpy(key, &buffer[offset], key_len);
                key[key_len] = '\0';
                offset += key_len;
                
                uint64_t val_consumed = 0;
                Value *val = value_from_bytes(&buffer[offset], length - offset, &val_consumed);
                if (!val) {
                    free(key);
                    // Cleanup on failure
                    for (uint32_t j = 0; j < i; j++) {
                        free(value->as.object.ptr[j].key);
                        value_free(value->as.object.ptr[j].value);
                        free(value->as.object.ptr[j].value);
                    }
                    free(value->as.object.ptr);
                    free(value);
                    return NULL;
                }
                
                value->as.object.ptr[i].key = key;
                value->as.object.ptr[i].value = val;
                offset += val_consumed;
            }
            break;
        }

        case 15: // Set
        {
            NEED(4);
            uint32_t set_len = read_u32_le(&buffer[offset]);
            offset += 4;
            value->tag = VALUE_SET;
            value->as.set.len = set_len;
            value->as.set.ptr = (char **)malloc(sizeof(char *) * set_len);
            
            for (uint32_t i = 0; i < set_len; i++) {
                NEED(4);
                uint32_t str_len = read_u32_le(&buffer[offset]);
                offset += 4;
                NEED(str_len);
                
                char *str = (char *)malloc(str_len + 1);
                memcpy(str, &buffer[offset], str_len);
                str[str_len] = '\0';
                value->as.set.ptr[i] = str;
                offset += str_len;
            }
            break;
        }

        case 16: // Row
        {
            NEED(4);
            uint32_t row_len = read_u32_le(&buffer[offset]);
            offset += 4;
            value->tag = VALUE_ROW;
            value->as.row.len = row_len;
            value->as.row.ptr = (ObjectEntry *)malloc(sizeof(ObjectEntry) * row_len);
            
            for (uint32_t i = 0; i < row_len; i++) {
                NEED(4);
                uint32_t key_len = read_u32_le(&buffer[offset]);
                offset += 4;
                NEED(key_len);
                
                char *key = (char *)malloc(key_len + 1);
                memcpy(key, &buffer[offset], key_len);
                key[key_len] = '\0';
                offset += key_len;
                
                uint64_t val_consumed = 0;
                Value *val = value_from_bytes(&buffer[offset], length - offset, &val_consumed);
                if (!val) {
                    free(key);
                    // Cleanup on failure
                    for (uint32_t j = 0; j < i; j++) {
                        free(value->as.row.ptr[j].key);
                        value_free(value->as.row.ptr[j].value);
                        free(value->as.row.ptr[j].value);
                    }
                    free(value->as.row.ptr);
                    free(value);
                    return NULL;
                }
                
                value->as.row.ptr[i].key = key;
                value->as.row.ptr[i].value = val;
                offset += val_consumed;
            }
            break;
        }

        case 17: // SortedSet
        {
            NEED(4);
            uint32_t ss_len = read_u32_le(&buffer[offset]);
            offset += 4;
            value->tag = VALUE_SORTEDSET;
            value->as.sortedset.len = ss_len;
            value->as.sortedset.ptr = (SortedEntry *)malloc(sizeof(SortedEntry) * ss_len);
            
            for (uint32_t i = 0; i < ss_len; i++) {
                NEED(8);
                uint64_t score_bits = read_u64_le(&buffer[offset]);
                memcpy(&value->as.sortedset.ptr[i].score, &score_bits, 8);
                offset += 8;
                
                NEED(4);
                uint32_t member_len = read_u32_le(&buffer[offset]);
                offset += 4;
                NEED(member_len);
                
                char *member = (char *)malloc(member_len + 1);
                memcpy(member, &buffer[offset], member_len);
                member[member_len] = '\0';
                value->as.sortedset.ptr[i].member = member;
                offset += member_len;
            }
            break;
        }

        case 18: // GeoPoint
        {
            NEED(16);
            value->tag = VALUE_GEOPOINT;
            uint64_t lat_bits = read_u64_le(&buffer[offset]);
            offset += 8;
            uint64_t lng_bits = read_u64_le(&buffer[offset]);
            offset += 8;
            memcpy(&value->as.geopoint.lat, &lat_bits, 8);
            memcpy(&value->as.geopoint.lng, &lng_bits, 8);
            break;
        }

        case 19: // Reference
        {
            NEED(4);
            uint32_t coll_len = read_u32_le(&buffer[offset]);
            offset += 4;
            NEED(coll_len);
            
            value->tag = VALUE_REFERENCE;
            value->as.reference.collection = (char *)malloc(coll_len + 1);
            memcpy(value->as.reference.collection, &buffer[offset], coll_len);
            value->as.reference.collection[coll_len] = '\0';
            offset += coll_len;
            
            uint64_t id_consumed = 0;
            Value *id = value_from_bytes(&buffer[offset], length - offset, &id_consumed);
            if (!id) {
                free(value->as.reference.collection);
                free(value);
                return NULL;
            }
            
            value->as.reference.id = id;
            offset += id_consumed;
            break;
        }

        default:
            free(value);
            return NULL;
    }

    #undef NEED

    if (consumed_length) {
        *consumed_length = offset;
    }
    
    return value;
}