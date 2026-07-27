/*
 *     Copyright 2026 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Thin C shim over libfabric for the Dragonfly RDMA piece transport.
 *
 * Most of the libfabric data-path API (fi_tsend, fi_trecv, fi_cq_read, ...) consists of
 * static inline functions dispatching through per-object ops tables, which cannot be bound
 * directly from Rust. This shim wraps exactly the subset the transport needs behind a plain
 * C ABI with opaque handles. It contains no policy: retries, timeouts, buffer lifetimes, and
 * completion routing all live on the Rust side (fabric.rs).
 *
 * The endpoint model is a single shared FI_EP_RDM endpoint per fabric handle with two-sided
 * tagged messaging. This is the only verbs surface common to AWS EFA (SRD) and RoCE/
 * InfiniBand (via verbs;ofi_rxm), and it never exposes remote-access memory keys.
 *
 * Thread safety: the Rust wrapper posts operations concurrently with CQ progress, so only
 * providers that grant FI_THREAD_SAFE are accepted.
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_tagged.h>

/* Request the API version of the headers we compile against; libfabric keeps runtime
 * backwards compatibility within a major version. */
#define DFRDMA_API_VERSION FI_VERSION(FI_MAJOR_VERSION, FI_MINOR_VERSION)

#ifndef FI_CONTEXT2
#define FI_CONTEXT2 (0ULL)
#endif

#define DFRDMA_CQ_BATCH_MAX 32

typedef struct dfrdma_completion {
    void *context;
    uint64_t flags;
    size_t len;
    int64_t err;
} dfrdma_completion;

typedef struct dfrdma_fabric {
    struct fi_info *info;
    struct fid_fabric *fabric;
    struct fid_domain *domain;
    struct fid_av *av;
    struct fid_cq *cq;
    struct fid_ep *ep;
    int mr_required;
} dfrdma_fabric;

void dfrdma_close(dfrdma_fabric *f)
{
    if (f == NULL) {
        return;
    }
    if (f->ep != NULL) {
        fi_close(&f->ep->fid);
    }
    if (f->cq != NULL) {
        fi_close(&f->cq->fid);
    }
    if (f->av != NULL) {
        fi_close(&f->av->fid);
    }
    if (f->domain != NULL) {
        fi_close(&f->domain->fid);
    }
    if (f->fabric != NULL) {
        fi_close(&f->fabric->fid);
    }
    if (f->info != NULL) {
        fi_freeinfo(f->info);
    }
    free(f);
}

/*
 * Closes only the endpoint so outstanding receives can no longer reference application
 * buffers. The domain and its memory registrations intentionally remain alive; Rust drops
 * the now-safe pending buffers/MRs before the final dfrdma_close tears down the remaining
 * fabric objects.
 */
int dfrdma_close_endpoint(dfrdma_fabric *f)
{
    int rc;

    if (f == NULL || f->ep == NULL) {
        return 0;
    }
    rc = fi_close(&f->ep->fid);
    if (rc == 0) {
        f->ep = NULL;
    }
    return rc;
}

/*
 * Prefers a non-efa-direct fi_info entry. Raw `fi_info -p efa` often lists efa-direct
 * first; that fabric is MTU-limited and is not a drop-in for Dragonfly's multi-MiB pieces.
 * Tagged-messaging hints usually filter it already; this walk is defense in depth.
 * Returns a duplicated info that the caller owns; frees the original list.
 */
static struct fi_info *dfrdma_select_info(struct fi_info *info)
{
    struct fi_info *it;
    struct fi_info *selected = NULL;

    for (it = info; it != NULL; it = it->next) {
        const char *fabric_name =
            (it->fabric_attr != NULL && it->fabric_attr->name != NULL) ? it->fabric_attr->name
                                                                      : "";
        if (strcmp(fabric_name, "efa-direct") == 0) {
            continue;
        }
        selected = fi_dupinfo(it);
        break;
    }
    if (selected == NULL && info != NULL) {
        selected = fi_dupinfo(info);
    }
    fi_freeinfo(info);
    return selected;
}

/*
 * Opens a reliable-datagram endpoint on the requested provider ("efa", "verbs", "tcp", ...)
 * or on the best provider libfabric can find when prov_name is NULL. Returns 0 on success
 * or a negative fi_errno value.
 */
int dfrdma_open(const char *prov_name, const char *domain_name, dfrdma_fabric **out)
{
    struct fi_info *hints = NULL;
    struct fi_info *info = NULL;
    dfrdma_fabric *f = NULL;
    struct fi_av_attr av_attr;
    struct fi_cq_attr cq_attr;
    int rc;

    *out = NULL;

    f = calloc(1, sizeof(*f));
    if (f == NULL) {
        return -FI_ENOMEM;
    }

    hints = fi_allocinfo();
    if (hints == NULL) {
        free(f);
        return -FI_ENOMEM;
    }
    hints->ep_attr->type = FI_EP_RDM;
    hints->caps = FI_TAGGED | FI_SEND | FI_RECV;
    /* We allocate provider scratch space with every posted operation, so providers that
     * require FI_CONTEXT/FI_CONTEXT2 (for example EFA) are usable. */
    hints->mode = FI_CONTEXT | FI_CONTEXT2;
    hints->domain_attr->mr_mode =
        FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
    hints->domain_attr->threading = FI_THREAD_SAFE;
    if (prov_name != NULL) {
        hints->fabric_attr->prov_name = strdup(prov_name);
    }
    if (domain_name != NULL) {
        hints->domain_attr->name = strdup(domain_name);
    }

    rc = fi_getinfo(DFRDMA_API_VERSION, NULL, NULL, 0, hints, &info);
    fi_freeinfo(hints);
    if (rc != 0) {
        goto fail;
    }

    f->info = dfrdma_select_info(info);
    if (f->info == NULL) {
        rc = -FI_ENODATA;
        goto fail;
    }
    if (f->info->domain_attr == NULL ||
        f->info->domain_attr->threading != FI_THREAD_SAFE) {
        rc = -FI_ENODATA;
        goto fail;
    }

    f->mr_required = (f->info->domain_attr->mr_mode & FI_MR_LOCAL) != 0;

    rc = fi_fabric(f->info->fabric_attr, &f->fabric, NULL);
    if (rc != 0) {
        goto fail;
    }

    rc = fi_domain(f->fabric, f->info, &f->domain, NULL);
    if (rc != 0) {
        goto fail;
    }

    memset(&av_attr, 0, sizeof(av_attr));
    av_attr.type = f->info->domain_attr->av_type;
    rc = fi_av_open(f->domain, &av_attr, &f->av, NULL);
    if (rc != 0) {
        goto fail;
    }

    memset(&cq_attr, 0, sizeof(cq_attr));
    cq_attr.format = FI_CQ_FORMAT_MSG;
    cq_attr.wait_obj = FI_WAIT_NONE;
    rc = fi_cq_open(f->domain, &cq_attr, &f->cq, NULL);
    if (rc != 0) {
        goto fail;
    }

    rc = fi_endpoint(f->domain, f->info, &f->ep, NULL);
    if (rc != 0) {
        goto fail;
    }

    rc = fi_ep_bind(f->ep, &f->av->fid, 0);
    if (rc != 0) {
        goto fail;
    }

    rc = fi_ep_bind(f->ep, &f->cq->fid, FI_TRANSMIT | FI_RECV);
    if (rc != 0) {
        goto fail;
    }

    rc = fi_enable(f->ep);
    if (rc != 0) {
        goto fail;
    }

    *out = f;
    return 0;

fail:
    dfrdma_close(f);
    return rc;
}

const char *dfrdma_provider_name(dfrdma_fabric *f)
{
    return f->info->fabric_attr->prov_name;
}

const char *dfrdma_domain_name(dfrdma_fabric *f)
{
    return f->info->domain_attr->name;
}

size_t dfrdma_max_msg_size(dfrdma_fabric *f)
{
    return f->info->ep_attr->max_msg_size;
}

int dfrdma_mr_required(dfrdma_fabric *f)
{
    return f->mr_required;
}

const char *dfrdma_strerror(int64_t err)
{
    return fi_strerror((int)(err < 0 ? -err : err));
}

/* Returns 0 on success, 1 when buf is too small (with *len updated to the required size),
 * or a negative fi_errno value. */
int dfrdma_getname(dfrdma_fabric *f, uint8_t *buf, size_t *len)
{
    int rc = fi_getname(&f->ep->fid, buf, len);
    if (rc == -FI_ETOOSMALL) {
        return 1;
    }
    return rc;
}

int dfrdma_av_insert(dfrdma_fabric *f, const uint8_t *addr, size_t len, uint64_t *out)
{
    fi_addr_t fi_addr = FI_ADDR_UNSPEC;
    int n;

    /* fi_av_insert reads the provider's native address length from the buffer. Peer
     * endpoint blobs come off the wire, so a short blob must be rejected here or the
     * provider would read out of bounds. */
    if (len < f->info->src_addrlen) {
        return -FI_EINVAL;
    }
    n = fi_av_insert(f->av, addr, 1, &fi_addr, 0, NULL);
    if (n != 1) {
        return n < 0 ? n : -FI_EINVAL;
    }
    *out = (uint64_t)fi_addr;
    return 0;
}

int dfrdma_mr_reg(dfrdma_fabric *f, void *buf, size_t len, void **mr_out, void **desc_out)
{
    struct fid_mr *mr = NULL;
    int rc;

    *mr_out = NULL;
    *desc_out = NULL;
    rc = fi_mr_reg(f->domain, buf, len, FI_SEND | FI_RECV, 0, 0, 0, &mr, NULL);
    if (rc != 0) {
        return rc;
    }
    *mr_out = mr;
    *desc_out = fi_mr_desc(mr);
    return 0;
}

int dfrdma_mr_close(void *mr)
{
    if (mr == NULL) {
        return 0;
    }
    return fi_close(&((struct fid_mr *)mr)->fid);
}

/* Post operations return 0 on success, 1 when the queue is full and the post should be
 * retried, or a negative fi_errno value. FI_EAGAIN is normalized here because fi_errno
 * values alias platform errno values and are not portable constants for Rust. */
int64_t dfrdma_trecv(dfrdma_fabric *f, void *buf, size_t len, void *desc, uint64_t tag,
                     void *context)
{
    if (f == NULL || f->ep == NULL) {
        return -FI_EOPBADSTATE;
    }
    ssize_t rc = fi_trecv(f->ep, buf, len, desc, FI_ADDR_UNSPEC, tag, 0, context);
    if (rc == -FI_EAGAIN) {
        return 1;
    }
    return (int64_t)rc;
}

int64_t dfrdma_tsend(dfrdma_fabric *f, const void *buf, size_t len, void *desc,
                     uint64_t dest, uint64_t tag, void *context)
{
    if (f == NULL || f->ep == NULL) {
        return -FI_EOPBADSTATE;
    }
    ssize_t rc = fi_tsend(f->ep, buf, len, desc, (fi_addr_t)dest, tag, context);
    if (rc == -FI_EAGAIN) {
        return 1;
    }
    return (int64_t)rc;
}

/*
 * Reads up to capacity completions. Returns the number of success/error completions written,
 * 0 when the queue is empty, or a negative fi_errno value on a fatal queue error. Error
 * completions are read one at a time because libfabric exposes them through fi_cq_readerr.
 */
int dfrdma_cq_read_batch(dfrdma_fabric *f, dfrdma_completion *out, size_t capacity)
{
    struct fi_cq_msg_entry entries[DFRDMA_CQ_BATCH_MAX];
    struct fi_cq_err_entry err_entry;
    ssize_t rc;
    size_t i;

    if (out == NULL || capacity == 0 || capacity > DFRDMA_CQ_BATCH_MAX) {
        return -FI_EINVAL;
    }

    rc = fi_cq_read(f->cq, entries, capacity);
    if (rc > 0) {
        for (i = 0; i < (size_t)rc; i++) {
            out[i].context = entries[i].op_context;
            out[i].flags = entries[i].flags;
            out[i].len = entries[i].len;
            out[i].err = 0;
        }
        return (int)rc;
    }
    if (rc == -FI_EAGAIN) {
        return 0;
    }
    if (rc == -FI_EAVAIL) {
        memset(&err_entry, 0, sizeof(err_entry));
        rc = fi_cq_readerr(f->cq, &err_entry, 0);
        if (rc == 1) {
            out[0].context = err_entry.op_context;
            out[0].flags = err_entry.flags;
            out[0].len = err_entry.len;
            out[0].err = err_entry.err != 0 ? (int64_t)err_entry.err : (int64_t)FI_EIO;
            return 1;
        }
        return rc < 0 ? (int)rc : -FI_EIO;
    }
    return (int)rc;
}

int dfrdma_cancel(dfrdma_fabric *f, void *context)
{
    if (f == NULL || f->ep == NULL) {
        return -FI_EOPBADSTATE;
    }
    int rc = (int)fi_cancel(&f->ep->fid, context);
    /* The operation can complete between the Rust pending-map check and fi_cancel. Treat
     * that race as success; the progress thread will reap the already-queued completion. */
    return rc == -FI_ENOENT ? 0 : rc;
}
