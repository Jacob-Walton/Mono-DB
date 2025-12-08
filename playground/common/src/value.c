#include "value.h"
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

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