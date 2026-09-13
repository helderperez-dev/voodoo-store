#ifndef VOODOO_STORE_H
#define VOODOO_STORE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct VdsHandle VdsHandle;

enum {
    VDS_OK = 0,
    VDS_NOT_FOUND = 1,
    VDS_BUFFER_TOO_SMALL = 2,
    VDS_INVALID_ARGUMENT = -1,
    VDS_INVALID_UTF8 = -2,
    VDS_ENGINE_ERROR = -3
};

uint32_t vds_abi_version(void);

int32_t vds_open(const char *path, VdsHandle **out_handle);
void vds_close(VdsHandle *handle);

int32_t vds_put(
    VdsHandle *handle,
    const uint8_t *key_ptr,
    size_t key_len,
    const uint8_t *value_ptr,
    size_t value_len
);

int32_t vds_delete(
    VdsHandle *handle,
    const uint8_t *key_ptr,
    size_t key_len
);

int32_t vds_get(
    const VdsHandle *handle,
    const uint8_t *key_ptr,
    size_t key_len,
    uint8_t *out_ptr,
    size_t out_capacity,
    size_t *out_len
);

#ifdef __cplusplus
}
#endif

#endif
