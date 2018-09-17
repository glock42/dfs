// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache_rsm.h"
#include <stdio.h>
#include <iostream>
#include <sstream>
#include "rpc.h"
#include "tprintf.h"

#include "rsm_client.h"

static void *releasethread(void *x) {
    lock_client_cache_rsm *cc = (lock_client_cache_rsm *)x;
    cc->releaser();
    return 0;
}

int lock_client_cache_rsm::last_port = 0;

lock_client_cache_rsm::lock_client_cache_rsm(std::string xdst,
                                             class lock_release_user *_lu)
    : lock_client(xdst), lu(_lu) {
    srand(time(NULL) ^ last_port);
    rlock_port = ((rand() % 32000) | (0x1 << 10));
    const char *hname;
    // VERIFY(gethostname(hname, 100) == 0);
    hname = "127.0.0.1";
    std::ostringstream host;
    host << hname << ":" << rlock_port;
    id = host.str();
    last_port = rlock_port;
    rpcs *rlsrpc = new rpcs(rlock_port);
    rlsrpc->reg(rlock_protocol::revoke, this,
                &lock_client_cache_rsm::revoke_handler);
    rlsrpc->reg(rlock_protocol::retry, this,
                &lock_client_cache_rsm::retry_handler);
    xid = 0;
    // You fill this in Step Two, Lab 7
    // - Create rsmc, and use the object to do RPC
    //   calls instead of the rpcc object of lock_client

    // rsmc = new rsm_client(xdst);

    pthread_t th;
    int r = pthread_create(&th, NULL, &releasethread, (void *)this);
    VERIFY(r == 0);
}

void lock_client_cache_rsm::releaser() {
    // This method should be a continuous loop, waiting to be notified of
    // freed locks that have been revoked by the server, so that it can
    // send a release RPC.
    int r;
    while (true) {
        struct release_entry e;
        wait_release.deq(&e);

        tprintf("real releaser: id: %s, release_entry deq, lid: %d, xid: %d \n",
                id.c_str(), int(e.lid), int(e.xid));

        if (lu) lu->dorelease(e.lid);

        auto ret = cl->call(lock_protocol::release, e.lid, id, e.xid, r);

        {
            ScopedLock m(&mtx);
            auto iter = lock_cache.find(e.lid);
            iter->second.status = rlock_protocol::NONE;
            pthread_cond_broadcast(&release_cond);
        }
    }
}

lock_protocol::status lock_client_cache_rsm::acquire(
    lock_protocol::lockid_t lid) {
    int ret = lock_protocol::OK;
    int r = 0;
    bool loop = true;
    pthread_mutex_lock(&mtx);
    tprintf("%s acquire lock %d\n", id.c_str(), int(lid));
    auto iter = lock_cache.find(lid);
    if (iter == lock_cache.end()) {
        lock_cache[lid].status = rlock_protocol::NONE;
    }
    iter = lock_cache.find(lid);
    while (loop) {
        switch (iter->second.status) {
            case rlock_protocol::NONE:
                iter->second.retry = false;
                iter->second.status = rlock_protocol::ACQUIRING;
                iter->second.xid = xid;
                xid++;
                tprintf("%s call server acqure lock %d, xid: %d\n", id.c_str(),
                        int(lid), int(iter->second.xid));
                pthread_mutex_unlock(&mtx);
                ret = cl->call(lock_protocol::acquire, lid, id,
                               iter->second.xid, r);
                pthread_mutex_lock(&mtx);
                if (ret == lock_protocol::OK) {
                    iter->second.status = rlock_protocol::FREE;
                } else if (ret == lock_protocol::RETRY) {
                    if (!iter->second.retry) {
                        pthread_cond_wait(&retry_cond, &mtx);
                    }
                }
                break;
            case rlock_protocol::LOCKED:
                pthread_cond_wait(&normal_cond, &mtx);
                break;
            case rlock_protocol::RELEASING:
                pthread_cond_wait(&release_cond, &mtx);
                break;
            case rlock_protocol::ACQUIRING:
                if (!iter->second.retry) {
                    pthread_cond_wait(&normal_cond, &mtx);
                } else {
                    iter->second.retry = false;
                    iter->second.xid = xid;
                    xid++;

                    tprintf("%s retry, acqure lock %d, xid: %d\n", id.c_str(),
                            int(lid), int(iter->second.xid));

                    pthread_mutex_unlock(&mtx);
                    ret = cl->call(lock_protocol::acquire, lid, id,
                                   iter->second.xid, r);
                    pthread_mutex_lock(&mtx);
                    if (ret == lock_protocol::OK) {
                        iter->second.status = rlock_protocol::FREE;
                    } else if (ret == lock_protocol::RETRY) {
                        if (!iter->second.retry) {
                            pthread_cond_wait(&retry_cond, &mtx);
                        }
                    }
                }
                break;
            case rlock_protocol::FREE:
                iter->second.status = rlock_protocol::LOCKED;
                loop = false;
                tprintf("%s acqure lock %d done\n", id.c_str(), int(lid));
                pthread_mutex_unlock(&mtx);
                break;
        }
    }
    return ret;
}

lock_protocol::status lock_client_cache_rsm::release(
    lock_protocol::lockid_t lid) {
    ScopedLock m(&mtx);
    int ret = lock_protocol::OK;
    int r = 0;
    tprintf("%s release lock %d \n", id.c_str(), int(lid));
    auto iter = lock_cache.find(lid);
    if (iter == lock_cache.end()) {
        ret = lock_protocol::RPCERR;
    } else {
        if (iter->second.status != rlock_protocol::LOCKED) {
            ret = lock_protocol::RPCERR;
        } else {
            if (iter->second.revoke) {
                iter->second.status = rlock_protocol::RELEASING;
                iter->second.revoke = false;

                wait_release.enq(release_entry(lid, iter->second.xid));

                tprintf(
                    "release: id: %s, release_entry enq, lid: %d, xid: %d \n",
                    id.c_str(), int(lid), int(iter->second.xid));
            } else {
                tprintf("%s release lock %d, but revoke flag not set\n",
                        id.c_str(), int(lid));
                iter->second.status = rlock_protocol::FREE;
                pthread_cond_broadcast(&normal_cond);
            }
        }
    }
    tprintf("%s release lock %d done\n", id.c_str(), int(lid));
    return ret;
}

rlock_protocol::status lock_client_cache_rsm::revoke_handler(
    lock_protocol::lockid_t lid, lock_protocol::xid_t xid, int &) {
    ScopedLock m(&mtx);
    int ret = rlock_protocol::OK;
    int r = 0;
    tprintf("%s revoke lock %d, xid: %d \n", id.c_str(), int(lid), int(xid));
    auto iter = lock_cache.find(lid);
    if (iter == lock_cache.end()) {
        ret = lock_protocol::RPCERR;
    } else {
        if (iter->second.xid == xid) {
            if (iter->second.status == rlock_protocol::FREE) {
                iter->second.status = rlock_protocol::RELEASING;

                wait_release.enq(release_entry(lid, iter->second.xid));

                tprintf(
                    "revoke: id: %s, release_entry enq, lid: %d, xid: %d \n",
                    id.c_str(), int(lid), int(iter->second.xid));

                // tprintf("%s set lock %d to NONE\n", id.c_str(), int(lid));
            } else {
                iter->second.revoke = true;
                tprintf("%s set %d lock revoke flag true, xid: %d \n",
                        id.c_str(), int(lid), int(xid));
            }
        }
    }
    tprintf("%s revoke lock %d done, xid: %d, lock_xid: %d\n", id.c_str(),
            int(lid), int(xid), int(iter->second.xid));
    return ret;
}

rlock_protocol::status lock_client_cache_rsm::retry_handler(
    lock_protocol::lockid_t lid, lock_protocol::xid_t xid, int &) {
    ScopedLock m(&mtx);
    tprintf("%s retry lock %d, xid: %d \n", id.c_str(), int(lid), int(xid));
    int ret = rlock_protocol::OK;
    auto iter = lock_cache.find(lid);
    if (iter == lock_cache.end()) {
        ret = rlock_protocol::RPCERR;
    } else {
        if (iter->second.xid == xid) {
            iter->second.retry = true;
            pthread_cond_signal(&retry_cond);
        }
    }
    tprintf("%s retry lock %d done, xid: %d, lock_xid: %d\n", id.c_str(),
            int(lid), int(xid), int(iter->second.xid));
    return ret;
}
