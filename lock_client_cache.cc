// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include <stdio.h>
#include <iostream>
#include <sstream>
#include "rpc.h"
#include "tprintf.h"

lock_client_cache::lock_client_cache(std::string xdst,
                                     class lock_release_user *_lu)
    : lock_client(xdst), lu(_lu) {
    rpcs *rlsrpc = new rpcs(0);
    rlsrpc->reg(rlock_protocol::revoke, this,
                &lock_client_cache::revoke_handler);
    rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

    const char *hname;
    hname = "127.0.0.1";
    std::ostringstream host;
    host << hname << ":" << rlsrpc->port();
    id = host.str();
}

lock_protocol::status lock_client_cache::acquire(lock_protocol::lockid_t lid) {
    int ret = lock_protocol::OK;
    int r = 0;
    pthread_mutex_lock(&mtx);
    tprintf("%s acquire lock %d\n", id.c_str(), int(lid));
    auto iter = lock_cache.find(lid);
    auto tid = pthread_self();
    if(iter == lock_cache.end() ) {
        tprintf("%s call server acqure lock %d\n", id.c_str(), int(lid));
        pthread_mutex_unlock(&mtx);
        ret = cl->call(lock_protocol::acquire, lid, id, r);
        pthread_mutex_lock(&mtx);
        if(ret == lock_protocol::OK) {
            struct lock_attr la;
            la.status = rlock_protocol::LOCKED;
            la.owned_thread = tid;
            lock_cache[lid] = la;
        } else if(ret == lock_protocol::RETRY) {
            while(lock_cache.find(lid)==lock_cache.end()) {
                pthread_cond_wait(&cond, &mtx);   
            }
            lock_cache[lid].status = rlock_protocol::LOCKED;
            lock_cache[lid].owned_thread = tid;
        }
        tprintf("%s acqure lock %d done\n", id.c_str(), int(lid));
        pthread_mutex_unlock(&mtx);
    } else {

        while(iter->second.status != rlock_protocol::FREE && iter->second.owned_thread == 0){
            //wait for other thread to release;
            pthread_cond_wait(&cond, &mtx);   
        }    
        iter->second.status = rlock_protocol::LOCKED;
        iter->second.owned_thread = tid;
        tprintf("%s acqure lock %d done\n", id.c_str(), int(lid));
        pthread_mutex_unlock(&mtx);
    }
    return ret;
}

lock_protocol::status lock_client_cache::release(lock_protocol::lockid_t lid) {
    int ret = lock_protocol::OK;
    int r = 0;
    pthread_mutex_lock(&mtx);
    tprintf("%s release lock %d \n", id.c_str(), int(lid));
    auto iter = lock_cache.find(lid);
    if(iter == lock_cache.end()){
        ret = lock_protocol::NOENT;
        pthread_mutex_unlock(&mtx);
    } else {
        if(iter->second.status != rlock_protocol::LOCKED) {
            ret = lock_protocol::IOERR;
            pthread_mutex_unlock(&mtx);
        } else {
            if(!iter->second.revoke) {
                tprintf("%s release lock %d, but revoke flag not set\n", id.c_str(), int(lid));
                iter->second.status = rlock_protocol::FREE;
                iter->second.owned_thread = 0;
                pthread_cond_signal(&cond);
                pthread_mutex_unlock(&mtx);
            } else {
                lock_cache.erase(lid);
                tprintf("%s erase lock %d \n", id.c_str(), int(lid));
                pthread_cond_signal(&cond);
                pthread_mutex_unlock(&mtx);
                ret = cl->call(lock_protocol::release, lid, id, r);
            }
        }
    }
    tprintf("%s release lock %d done\n", id.c_str(), int(lid));
    return ret;
}

rlock_protocol::status lock_client_cache::revoke_handler(
    lock_protocol::lockid_t lid, int &) {
    pthread_mutex_lock(&mtx);
    int ret = rlock_protocol::OK;
    int r = 0;
    tprintf("%s revoke lock %d \n", id.c_str(), int(lid));
    auto iter = lock_cache.find(lid);
    if(iter == lock_cache.end()) {
        struct lock_attr la;
        la.status = rlock_protocol::NONE;
        la.owned_thread = 0;
        lock_cache[lid] = la;
    } 
    iter = lock_cache.find(lid);
    if(iter->second.status == rlock_protocol::FREE) {
        lock_cache.erase(lid);
        tprintf("%s erase lock %d \n", id.c_str(), int(lid));
        pthread_mutex_unlock(&mtx);
        ret = cl->call(lock_protocol::release, lid, id, r);
    } else {
        iter->second.revoke = true;
        tprintf("%s set %d lock revoke flag true \n", id.c_str(), int(lid));
        pthread_mutex_unlock(&mtx);
    }
    tprintf("%s revoke lock %d done\n", id.c_str(), int(lid));
    return ret;
}

rlock_protocol::status lock_client_cache::retry_handler(
    lock_protocol::lockid_t lid, int &) {
    pthread_mutex_lock(&mtx);
    tprintf("%s retry lock %d \n", id.c_str(), int(lid));
    int ret = rlock_protocol::OK;
    int r = 0;
    auto iter = lock_cache.find(lid);
    if(iter != lock_cache.end()) {
        pthread_mutex_unlock(&mtx);
        return rlock_protocol::RPCERR;
    } else {
        pthread_mutex_unlock(&mtx);
        ret = cl->call(lock_protocol::acquire, lid, id, r);
        pthread_mutex_lock(&mtx);
        if(ret == lock_protocol::OK) {
            lock_cache[lid].status = rlock_protocol::NONE;
            lock_cache[lid].owned_thread = 0;
            pthread_cond_signal(&cond);
        }
        pthread_mutex_unlock(&mtx);
    }
    tprintf("%s retry lock %d done\n", id.c_str(), int(lid));
    return ret;
}
