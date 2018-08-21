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
    std::cout << id  << " acquire lock " << lid <<std::endl;
    lock_protocol::status ret = lock_protocol::OK;
    auto iter = lock_map.find(lid);
    if(iter == lock_map.end()) {
        struct lock_content lc;
        lc.owned_client = id;
        lock_map[lid] = lc;
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
            pthread_mutex_unlock(&mtx);
        } else {
            auto lc = iter->second;
            handle h(lc.owned_client);
            rpcc *cl = h.safebind();
            if(cl) {
                lc.wait_clients.push_back(id);
                std::cout << id  << " try revoke lock " << lid <<std::endl;
                pthread_mutex_unlock(&mtx);
                cl->call(rlock_protocol::revoke, lid, ret);
                ret = lock_protocol::RETRY;
            } else {
                pthread_mutex_unlock(&mtx);
                ret = lock_protocol::RPCERR;
            }
        }
    }
    std::cout << id  << " acquire lock " << lid << " done" <<std::endl;
    return ret;
}

int lock_server_cache::release(lock_protocol::lockid_t lid, std::string id,
                               int &r) {
    std::cout << id  << " release lock " << lid <<std::endl;
    lock_protocol::status ret = lock_protocol::OK;
    auto iter = lock_map.find(lid);
    if(iter == lock_map.end()) {
        ret = lock_protocol::NOENT;
    } else {
        if(iter->second.owned_client != id) {
            ret = lock_protocol::NOENT;
        } else {
            iter->second.owned_client = "";
            if(iter->second.wait_clients.size() > 0) {
                auto client_id = iter->second.wait_clients.front();
                handle h(client_id);
                rpcc *cl = h.safebind();
                if(cl) {
                    std::cout << "call "<< id  << " rery lock " << lid <<std::endl;
                    cl->call(rlock_protocol::retry, lid, ret);
                    ret = lock_protocol::OK;
                } else {
                    ret = lock_protocol::RPCERR;
                }
            }
        }
    }
    std::cout << id  << " release lock " << lid << " done" <<std::endl;
    return ret;
}

lock_protocol::status lock_server_cache::stat(lock_protocol::lockid_t lid,
                                              int &r) {
    tprintf("stat request\n");
    r = nacquire;
    return lock_protocol::OK;
}
