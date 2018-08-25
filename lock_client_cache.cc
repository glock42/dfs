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
                //tprintf("%s call server acqure lock %d\n", id.c_str(),
                //        int(lid));
                iter->second.retry = false;
                iter->second.status = rlock_protocol::ACQUIRING;
                pthread_mutex_unlock(&mtx);
                ret = cl->call(lock_protocol::acquire, lid, id, r);
                pthread_mutex_lock(&mtx);
                if (ret == lock_protocol::OK) {
                    iter->second.status = rlock_protocol::FREE;
                } else if (ret == lock_protocol::RETRY) {
                    if(!iter->second.retry) {
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
                if(!iter->second.retry) {
                    pthread_cond_wait(&normal_cond, &mtx);
                } else {
                    iter->second.retry = false;
                    pthread_mutex_unlock(&mtx);
                    ret = cl->call(lock_protocol::acquire, lid, id, r);
                    pthread_mutex_lock(&mtx);
                    if (ret == lock_protocol::OK) {
                        iter->second.status = rlock_protocol::FREE;
                    } else if (ret == lock_protocol::RETRY) {
                        if(!iter->second.retry) {
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

lock_protocol::status lock_client_cache::release(lock_protocol::lockid_t lid) {
    pthread_mutex_lock(&mtx);
    int ret = lock_protocol::OK;
    int r = 0;
    tprintf("%s release lock %d \n", id.c_str(), int(lid));
    auto iter = lock_cache.find(lid);
    if (iter == lock_cache.end()) {
        ret = lock_protocol::RPCERR;
        pthread_mutex_unlock(&mtx);
    } else {
        if (iter->second.status != rlock_protocol::LOCKED) {
            ret = lock_protocol::RPCERR;
            pthread_mutex_unlock(&mtx);
        } else {

            if (iter->second.revoke) {
                iter->second.status = rlock_protocol::RELEASING;
                iter->second.revoke = false;
                pthread_mutex_unlock(&mtx);
                ret = cl->call(lock_protocol::release, lid, id, r);
                pthread_mutex_lock(&mtx);
                iter->second.status = rlock_protocol::NONE;
                //tprintf("%s set lock %d to NONE\n", id.c_str(), int(lid));
                pthread_cond_signal(&release_cond);
                pthread_mutex_unlock(&mtx);
            } else {
                //tprintf("%s release lock %d, but revoke flag not set\n",
                //        id.c_str(), int(lid));
                iter->second.status = rlock_protocol::FREE;
                pthread_cond_broadcast(&normal_cond);
                pthread_mutex_unlock(&mtx);
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
    //tprintf("%s revoke lock %d \n", id.c_str(), int(lid));
    auto iter = lock_cache.find(lid);
    if (iter == lock_cache.end()) {
        ret = lock_protocol::RPCERR;
        pthread_mutex_unlock(&mtx);
    } else{ 
        if (iter->second.status == rlock_protocol::FREE) {
            iter->second.status = rlock_protocol::RELEASING;
            pthread_mutex_unlock(&mtx);
            ret = cl->call(lock_protocol::release, lid, id, r);
            pthread_mutex_lock(&mtx);
            iter->second.status = rlock_protocol::NONE;
            //tprintf("%s set lock %d to NONE\n", id.c_str(), int(lid));
            pthread_cond_broadcast(&release_cond);
            pthread_mutex_unlock(&mtx);
        } else {
            iter->second.revoke = true;
            //tprintf("%s set %d lock revoke flag true \n", id.c_str(), int(lid));
            pthread_mutex_unlock(&mtx);
        }
    }
    //tprintf("%s revoke lock %d done\n", id.c_str(), int(lid));
    return ret;
}

rlock_protocol::status lock_client_cache::retry_handler(
    lock_protocol::lockid_t lid, int &) {
    pthread_mutex_lock(&mtx);
    //tprintf("%s retry lock %d \n", id.c_str(), int(lid));
    int ret = rlock_protocol::OK;
    auto iter = lock_cache.find(lid);
    if (iter == lock_cache.end()) {
        ret = rlock_protocol::RPCERR;
        pthread_mutex_unlock(&mtx);
    } else {
        iter->second.retry = true;
        pthread_cond_signal(&retry_cond);
        pthread_mutex_unlock(&mtx);
    }
    //tprintf("%s retry lock %d done\n", id.c_str(), int(lid));
    return ret;
}
