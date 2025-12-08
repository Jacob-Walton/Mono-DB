#include "value.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

void main()
{
    // Create a new Value
    const char *str = "Hello, World!";
    Value original = value_new_string(str, strlen(str));

    // Clone the Value
    Value *cloned = value_clone(&original);
    // Free the original Value
    value_free(&original);

    // Print the cloned Value
    if (cloned && cloned->tag == VALUE_STRING)
    {
        printf("Cloned string '%s' with length %d\n", cloned->as.string.ptr, (int)cloned->as.string.len);
    }
    else
    {
        printf("Cloning failed.\n");
    }

    // Free the cloned Value
    value_free(cloned);
    free(cloned);

    // Create a boolean
    Value bool_test = value_new_bool(true);

    printf("Bool value retrieved: %s\n", bool_test.as.boolean ? "true" : "false");

    value_free(&bool_test);
}