// the caching lock server implementation

#include "lock_server_cache.h"
#include <arpa/inet.h>
#include <stdio.h>
#include <unistd.h>
#include <sstream>
#include "handle.h"
#include "lang/verify.h"
#include "tprintf.h"

lock_server_cache::lock_server_cache() {}

int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id,
                               int &) {
    pthread_mutex_lock(&mtx);
    tprintf("%s acquire lock %d \n", id.c_str(), int(lid));
    int ret = lock_protocol::OK;
    int r = 0;
    auto iter = lock_map.find(lid);
    if(iter == lock_map.end()) {
        struct lock_content lc;
        lc.owned_client = id;
        lock_map[lid] = lc;
        tprintf("%s acquire lock %d done\n", id.c_str(), int(lid));
        pthread_mutex_unlock(&mtx);
    }
    else {
        if(iter->second.owned_client == "") {
            iter->second.owned_client = id;
            auto &wait_clients = iter->second.wait_clients;
            auto wait_iter = std::find(wait_clients.begin(), wait_clients.end(), id);
            if(wait_iter != iter->second.wait_clients.end()) {
                iter->second.wait_clients.erase(wait_iter);
            }
            if(iter->second.wait_clients.size() > 0) {
                tprintf("%d lock has other waiter, call %s revoke \n", int(lid), id.c_str());
                handle h(iter->second.owned_client);
                pthread_mutex_unlock(&mtx);
                h.safebind()->call(rlock_protocol::revoke, lid, r);
            } else {
                pthread_mutex_unlock(&mtx);
            }
            tprintf("%s reacquire lock %d done\n", id.c_str(), int(lid));
        } else {
            auto &lc = iter->second;
            if(lc.wait_clients.size() == 0) {
                lc.wait_clients.push_back(id);
                handle h(lc.owned_client);
                rpcc *cl = h.safebind();
                if(cl) {
                    tprintf("%s try revoke lock %d \n", id.c_str(), int(lid));
                    pthread_mutex_unlock(&mtx);
                    cl->call(rlock_protocol::revoke, lid, r);
                    ret = lock_protocol::RETRY;
                    tprintf("%s revoke lock %d done\n", id.c_str(), int(lid));
                } else {
                    pthread_mutex_unlock(&mtx);
                    ret = lock_protocol::RPCERR;
                }
            }
            else {
                lc.wait_clients.push_back(id);
                ret = lock_protocol::RETRY;
                tprintf("%s Neet to RETRY lock %d \n", id.c_str(), int(lid));
                pthread_mutex_unlock(&mtx);
            }
        }
    }
    tprintf("total %s acquire lock %d done\n", id.c_str(), int(lid));
    return ret;
}

int lock_server_cache::release(lock_protocol::lockid_t lid, std::string id,
                               int &) {
    pthread_mutex_lock(&mtx);
    tprintf("%s release lock %d \n", id.c_str(), int(lid));
    int r = 0;
    int ret = lock_protocol::OK;
    auto iter = lock_map.find(lid);
    if(iter == lock_map.end()) {
        ret = lock_protocol::NOENT;
        pthread_mutex_unlock(&mtx);
    } else {
        if(iter->second.owned_client != id) {
            ret = lock_protocol::NOENT;
            pthread_mutex_unlock(&mtx);
        } else {
            iter->second.owned_client = "";
            if(iter->second.wait_clients.size() > 0) {
                auto client_id = iter->second.wait_clients.front();
                iter->second.wait_clients.pop_front();
                tprintf("server call %s retry lock %d \n", client_id.c_str(), int(lid));
                handle h(client_id);
                rpcc *cl = h.safebind();
                if(cl) {
                    pthread_mutex_unlock(&mtx);
                    ret = cl->call(rlock_protocol::retry, lid, r);
                } else {
                    ret = lock_protocol::RPCERR;
                    pthread_mutex_unlock(&mtx);
                }
            } else {
                pthread_mutex_unlock(&mtx);
            }
        }
    }
    tprintf("%s release lock %d done\n", id.c_str(), int(lid));
    return ret;
}

lock_protocol::status lock_server_cache::stat(lock_protocol::lockid_t lid,
                                              int &r) {
    tprintf("stat request\n");
    r = nacquire;
    return lock_protocol::OK;
}
