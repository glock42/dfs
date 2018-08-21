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
    pthread_mutex_lock(&mtx);
    std::cout << id  << " acqure lock " << lid << std::endl;
    auto iter = lock_cache.find(lid);
    auto tid = pthread_self();
    if(iter == lock_cache.end() ) {
        if(lock_cache.size() > 0) {
            std::cout << lock_cache[1].owned_thread<< "xxxxxxxxxxxxxxxxx" << std::endl;
        }
        std::cout << id  << " call server acqure lock " << lid <<std::endl;
        pthread_mutex_unlock(&mtx);
        cl->call(lock_protocol::acquire, lid, id, ret);
        pthread_mutex_lock(&mtx);
        if(ret == lock_protocol::OK) {
            struct lock_attr la;
            la.status = rlock_protocol::LOCKED;
            la.owned_thread = tid;
            lock_cache[lid] = la;
        }
        std::cout << id  << " acqure lock " << lid << " done "<<std::endl;
        pthread_mutex_unlock(&mtx);
    } else {
        if(iter->second.status == rlock_protocol::FREE && iter->second.owned_thread == 0) {
            iter->second.status = rlock_protocol::LOCKED;
            iter->second.owned_thread = tid;
        }
        while(iter->second.status == rlock_protocol::LOCKED && iter->second.owned_thread != tid){
            //wait for other thread to release;
            pthread_cond_wait(&cond, &mtx);   
        }    
        std::cout << id  << " acqure lock " << lid << " done "<<std::endl;
        pthread_mutex_unlock(&mtx);
    }
    return ret;
}

lock_protocol::status lock_client_cache::release(lock_protocol::lockid_t lid) {
    int ret = lock_protocol::OK;
    pthread_mutex_lock(&mtx);
    std::cout << id  << " release lock " << lid <<std::endl;
    auto iter = lock_cache.find(lid);
    if(iter == lock_cache.end()){
        ret = lock_protocol::NOENT;
    } else {
        if(iter->second.status != rlock_protocol::LOCKED) {
            ret = lock_protocol::IOERR;
        }
        else {
            iter->second.status = rlock_protocol::FREE;
            iter->second.owned_thread = 0;
            pthread_cond_signal(&cond);
        }
    }
    std::cout << id  << " release lock " << lid << " done "<<std::endl;
    pthread_mutex_unlock(&mtx);
    return ret;
}

rlock_protocol::status lock_client_cache::revoke_handler(
    lock_protocol::lockid_t lid, int &) {
    int ret = rlock_protocol::OK;
    pthread_mutex_lock(&mtx);
    std::cout << id  << " revoke lock " << lid <<std::endl;
    auto iter = lock_cache.find(lid);
    if(iter == lock_cache.end()) {
        ret = rlock_protocol::RPCERR;
    } else {
        while(iter->second.status != rlock_protocol::FREE) {
            pthread_cond_wait(&cond, &mtx);   
        }
        lock_cache.erase(lid);
    }
    std::cout << id  << " revoke lock " << lid<< " done " <<std::endl;
    pthread_mutex_unlock(&mtx);
    return ret;
}

rlock_protocol::status lock_client_cache::retry_handler(
    lock_protocol::lockid_t lid, int &) {
    pthread_mutex_lock(&mtx);
    std::cout << id  << " retry lock " << lid <<std::endl;
    int ret = rlock_protocol::OK;
    auto iter = lock_cache.find(lid);
    if(iter != lock_cache.end()) {
        pthread_mutex_unlock(&mtx);
        return rlock_protocol::RPCERR;
    } else {
        pthread_mutex_unlock(&mtx);
        ret = acquire(lid);
    }
    std::cout << id  << " retry lock " << lid<< " done " <<std::endl;
    return ret;
}
