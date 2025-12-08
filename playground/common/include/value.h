#ifndef VALUE_H
#define VALUE_H

#include <stdint.h>
#include <stdbool.h>

typedef enum
{
    VALUE_NULL,
    VALUE_BOOL,
    VALUE_INT32,
    VALUE_INT64,
    VALUE_FLOAT32,
    VALUE_FLOAT64,
    VALUE_STRING,
    VALUE_BINARY,
    VALUE_DATETIME,
    VALUE_DATE,
    VALUE_TIME,
    VALUE_UUID,
    VALUE_OBJECTID,
    VALUE_ARRAY,
    VALUE_OBJECT,
    VALUE_SET,
    VALUE_ROW,
    VALUE_SORTEDSET,
    VALUE_GEOPOINT,
    VALUE_REFERENCE
} ValueTag;

typedef struct
{
    char *key;
    struct Value *value;
} ObjectEntry;

typedef struct
{
    double score;
    char *member;
} SortedEntry;

typedef struct
{
    int32_t year;
    uint8_t month;
    uint8_t day;
} Date;

typedef struct
{
    uint8_t hour;
    uint8_t minute;
    uint8_t second;
    uint32_t microsecond;
} Time;

typedef struct
{
    int64_t timestamp;
    int32_t offset_minutes;
} DateTime;

typedef struct Value
{
    ValueTag tag;

    union
    {
        bool boolean;
        int32_t i32;
        int64_t i64;
        float f32;
        double f64;

        struct
        {
            char *ptr;
            uint64_t len;
        } string;
        struct
        {
            uint8_t *ptr;
            uint64_t len;
        } binary;

        Date date;
        Time time;
        DateTime datetime;

        uint8_t uuid[16];
        uint8_t objectid[12];

        struct
        {
            struct Value *ptr;
            uint64_t len;
        } array;
        struct
        {
            ObjectEntry *ptr;
            uint64_t len;
        } object;
        struct
        {
            char **ptr;
            uint64_t len;
        } set;

        struct
        {
            ObjectEntry *ptr;
            uint64_t len;
        } row;
        struct
        {
            SortedEntry *ptr;
            uint64_t len;
        } sortedset;

        struct
        {
            double lat;
            double lng;
        } geopoint;

        struct
        {
            char *collection;
            struct Value *id;
        } reference;
    } as;
} Value;

void value_free(Value *value);
Value *value_clone(const Value *value);

// Initialize various Value types
Value value_new_null();
Value value_new_bool(bool b);
Value value_new_int32(int32_t i);
Value value_new_int64(int64_t i);
Value value_new_float32(float f);
Value value_new_float64(double d);
Value value_new_string(const char *str, uint64_t len);
Value value_new_binary(const uint8_t *data, uint64_t len);
Value value_new_date(Date date);
Value value_new_time(Time time);
Value value_new_datetime(DateTime datetime);
Value value_new_uuid(const uint8_t uuid[16]);
Value value_new_objectid(const uint8_t objectid[12]);
Value value_new_array(const Value *elements, uint64_t len);
Value value_new_object(const ObjectEntry *entries, uint64_t len);
Value value_new_set(char **elements, uint64_t len);
Value value_new_row(const ObjectEntry *entries, uint64_t len);
Value value_new_sortedset(const SortedEntry *entries, uint64_t len);
Value value_new_geopoint(double lat, double lng);
Value value_new_reference(const char *collection, const Value *id);
bool value_equals(const Value *a, const Value *b);
bool value_is_truthy(const Value *value);
uint8_t *value_to_bytes(const Value *value, uint8_t *buffer, uint64_t *length);
Value *value_from_bytes(const uint8_t *buffer, uint64_t length, uint64_t *consumed_length);

#endif