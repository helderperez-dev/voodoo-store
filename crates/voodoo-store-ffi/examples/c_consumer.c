#include "voodoo_store.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(void) {
    const char *path = "voodoo-c-consumer.vstore";
    VdsHandle *store = NULL;
    assert(vds_abi_version() == 2);
    assert(vds_open(path, &store) == VDS_OK);
    assert(vds_put(store, (const uint8_t *)"language", 8,
                   (const uint8_t *)"C", 1) == VDS_OK);

    size_t required = 0;
    assert(vds_get(store, (const uint8_t *)"language", 8, NULL, 0,
                   &required) == VDS_BUFFER_TOO_SMALL);
    assert(required == 1);
    uint8_t value[1] = {0};
    assert(vds_get(store, (const uint8_t *)"language", 8, value,
                   sizeof(value), &required) == VDS_OK);
    assert(required == 1 && value[0] == 'C');

    vds_close(store);
    remove(path);
    puts("voodoo-store C consumer passed");
    return 0;
}
