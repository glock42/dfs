// the caching lock server implementation

#include "lock_server_cache_rsm.h"
#include <arpa/inet.h>
#include <stdio.h>
#include <unistd.h>
#include <sstream>
#include "handle.h"
#include "lang/verify.h"
#include "tprintf.h"

static void *revokethread(void *x) {
    lock_server_cache_rsm *sc = (lock_server_cache_rsm *)x;
    sc->revoker();
    return 0;
}

static void *retrythread(void *x) {
    lock_server_cache_rsm *sc = (lock_server_cache_rsm *)x;
    sc->retryer();
    return 0;
}

lock_server_cache_rsm::lock_server_cache_rsm(class rsm *_rsm) : rsm(_rsm) {
    pthread_t th;
    int r = pthread_create(&th, NULL, &revokethread, (void *)this);
    VERIFY(r == 0);
    r = pthread_create(&th, NULL, &retrythread, (void *)this);
    VERIFY(r == 0);
}

void lock_server_cache_rsm::revoker() {
    // This method should be a continuous loop, that sends revoke
    // messages to lock holders whenever another client wants the
    // same lock
    int r = 0;
    while (true) {
        revoke_entry e;
        wait_to_revoke.deq(&e);

        handle h(e.client);
        rpcc *cl = h.safebind();
        tprintf("%s try revoke lock %d, xid: %d \n", e.client.c_str(),
                int(e.lid), int(e.xid));
        cl->call(rlock_protocol::revoke, e.lid, e.xid, r);
        tprintf("%s revoke lock %d done, xid: %d\n", e.client.c_str(),
                int(e.lid), int(e.xid));
    }
}

void lock_server_cache_rsm::retryer() {
    // This method should be a continuous loop, waiting for locks
    // to be released and then sending retry messages to those who
    // are waiting for it.
    int r = 0;
    while (true) {
        retry_entry e;
        wait_to_retry.deq(&e);

        handle h(e.client);
        rpcc *cl = h.safebind();
        tprintf("server call %s retry lock %d \n", e.client.c_str(),
                int(e.lid));
        cl->call(rlock_protocol::retry, e.lid, e.xid, r);
    }
}

int lock_server_cache_rsm::acquire(lock_protocol::lockid_t lid, std::string id,
                                   lock_protocol::xid_t xid, int &) {
    ScopedLock m(&mtx);
    tprintf("%s acquire lock %d, xid: %d \n", id.c_str(), int(lid), int(xid));
    int ret = lock_protocol::OK;
    int r = 0;
    auto iter = lock_map.find(lid);
    if (iter == lock_map.end()) {
        struct lock_content lc;
        lc.owned_client = id;
        lock_map[lid] = lc;
        highest_xid[id][lid] = xid;
        tprintf("%s acquire lock %d done, xid:%d\n", id.c_str(), int(lid),
                int(xid));
    } else {
        auto lid2xid_iter = highest_xid.find(id);
        if (lid2xid_iter == highest_xid.end()) {
            highest_xid[id][lid] = -1;
        }
        auto xid_iter = (highest_xid[id]).find(lid);
        if (xid_iter->second == -1 || xid > xid_iter->second) {
            highest_xid[id][lid] = xid;
            if (release_cache.count(id) != 0) {
                release_cache[id].erase(lid);
            }

            if (iter->second.owned_client == "") {
                iter->second.owned_client = id;
                auto &wait_clients = iter->second.wait_clients;
                auto wait_iter =
                    std::find(wait_clients.begin(), wait_clients.end(), id);
                if (wait_iter != iter->second.wait_clients.end()) {
                    iter->second.wait_clients.erase(wait_iter);
                }
                if (iter->second.wait_clients.size() > 0) {
                    tprintf("%d lock has other waiter, call %s revoke \n",
                            int(lid), id.c_str());
                    // handle h(iter->second.owned_client);
                    wait_to_revoke.enq(
                        revoke_entry(lid, xid, iter->second.owned_client));
                    // h.safebind()->call(rlock_protocol::revoke, lid, r);
                } else {
                    tprintf("%d lock has no other waiter, only %s \n", int(lid),
                            id.c_str());
                }
                tprintf("%s reacquire lock %d done, xid: %d\n", id.c_str(),
                        int(lid), int(xid));
            } else {
                auto &lc = iter->second;
                if (lc.wait_clients.size() == 0) {
                    lc.wait_clients.push_back(id);
                    // handle h(lc.owned_client);
                    // rpcc *cl = h.safebind();
                    // int(lid));
                    // cl->call(rlock_protocol::revoke, lid, r);

                    tprintf(
                        "id:%s, acqure: revoke entry enq, lid: %d, xid: %d\n",
                        id.c_str(), int(lid), int(xid));

                    wait_to_revoke.enq(
                        revoke_entry(lid, highest_xid[iter->second.owned_client][lid], iter->second.owned_client));
                    ret = lock_protocol::RETRY;

                    // tprintf("%s revoke lock %d done\n", id.c_str(),
                    // int(lid));
                } else {
                    lc.wait_clients.push_back(id);
                    ret = lock_protocol::RETRY;
                    tprintf("%s Neet to RETRY lock %d \n", id.c_str(),
                            int(lid));
                }
                acquire_cache[id][lid] = ret;
            }
        } else if (xid < xid_iter->second) {
            tprintf(
                "out-of-date request, xid: %d is lower than the highest_xid: "
                "%d \n",
                int(xid), int(xid_iter->second));
        } else {
            tprintf("duplicate request, xid: %d\n", int(xid));
            ret = acquire_cache[id][lid];
        }
    }
    tprintf("total %s acquire lock %d done, xid: %d\n", id.c_str(), int(lid),
            int(xid));
    return ret;
}

int lock_server_cache_rsm::release(lock_protocol::lockid_t lid, std::string id,
                                   lock_protocol::xid_t xid, int &r) {
    ScopedLock m(&mtx);
    tprintf("%s release lock %d, xid: %d \n", id.c_str(), int(lid), int(xid));
    int ret = lock_protocol::OK;
    auto iter = lock_map.find(lid);
    if (iter == lock_map.end()) {
        ret = lock_protocol::NOENT;
    } else {
        if (iter->second.owned_client != id) {
            ret = lock_protocol::NOENT;
        } else {
            auto lid2xid_iter = highest_xid.find(id);
            if (lid2xid_iter == highest_xid.end()) {
                ret = lock_protocol::NOENT;
            } else {
                auto xid_iter = lid2xid_iter->second.find(lid);
                if (xid_iter != lid2xid_iter->second.end() &&
                    xid_iter->second == xid) {
                    if (release_cache.count(id) != 0 &&
                        release_cache[id].count(lid) != 0) {
                        tprintf("release: has cached, xid: %d\n", int(xid));
                        ret = release_cache[id][lid];
                    } else {
                        iter->second.owned_client = "";
                        if (iter->second.wait_clients.size() > 0) {
                            auto client_id = iter->second.wait_clients.front();
                            iter->second.wait_clients.pop_front();

                            // handle h(client_id);
                            // rpcc *cl = h.safebind();
                            wait_to_retry.enq(retry_entry(
                                lid, highest_xid[client_id][lid], client_id));
                            tprintf(
                                "server call %s retry lock %d, retry_entry "
                                "enq, xid: %d \n",
                                client_id.c_str(), int(lid), int(xid));
                            // ret = cl->call(rlock_protocol::retry, lid, r);
                        }
                        release_cache[id][lid] = ret;
                    }

                } else {
                    tprintf("release: error, incorrect xid: %d \n", int(xid));
                }
            }
        }
    }
    tprintf("%s release lock %d done, xid: %d\n", id.c_str(), int(lid),
            int(xid));
    return ret;
}

std::string lock_server_cache_rsm::marshal_state() {
    std::ostringstream ost;
    std::string r;
    return r;
}

void lock_server_cache_rsm::unmarshal_state(std::string state) {}

lock_protocol::status lock_server_cache_rsm::stat(lock_protocol::lockid_t lid,
                                                  int &r) {
    printf("stat request\n");
    r = nacquire;
    return lock_protocol::OK;
}
