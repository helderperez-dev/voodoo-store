#ifndef VOODOO_STORE_H
#define VOODOO_STORE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct VdsHandle VdsHandle;
typedef struct VdsTransaction VdsTransaction;

enum {
    VDS_OK = 0,
    VDS_NOT_FOUND = 1,
    VDS_BUFFER_TOO_SMALL = 2,
    VDS_INVALID_ARGUMENT = -1,
    VDS_INVALID_UTF8 = -2,
    VDS_ENGINE_ERROR = -3,
    VDS_PANIC = -4
};

uint32_t vds_abi_version(void);

/*
 * Copies the calling thread's last error message as UTF-8 bytes.
 * Messages are not NUL-terminated. Pass NULL/0 to query the required length.
 */
int32_t vds_last_error_message(
    uint8_t *out_ptr,
    size_t out_capacity,
    size_t *out_len
);

/*
 * Handles and transactions require external synchronization when shared across
 * threads. A VdsHandle must outlive all VdsTransaction values created from it.
 */
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

/* Buffered transaction API. Commit and rollback consume the transaction. */
int32_t vds_tx_begin(VdsHandle *handle, VdsTransaction **out_tx);

int32_t vds_tx_put(
    VdsTransaction *tx,
    const uint8_t *key_ptr,
    size_t key_len,
    const uint8_t *value_ptr,
    size_t value_len
);

int32_t vds_tx_delete(
    VdsTransaction *tx,
    const uint8_t *key_ptr,
    size_t key_len
);

int32_t vds_tx_commit(VdsTransaction *tx);
void vds_tx_rollback(VdsTransaction *tx);

#ifdef __cplusplus
}
#endif

#endif
