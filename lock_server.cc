// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdio.h>

lock_server::lock_server():
  nacquire (0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status 
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r){
    lock_protocol::status ret = lock_protocol::OK;
    r = 0; // r doesn't matter for result;
    printf("release from clt %d, lid %llu\n", clt, lid);
    pthread_mutex_lock(&mtx);
    if(lock_table.find(lid) != lock_table.end()) {
        printf("lid %llu notify\n", lid);
        lock_table.erase(lid);
        pthread_cond_signal(&cond);    
        
    }
    pthread_mutex_unlock(&mtx);
    return ret;
}

lock_protocol::status 
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r){

    lock_protocol::status ret = lock_protocol::OK;
    r = 0; // r doesn't matter for result;
    printf("acuire from clt %d, lid %llu\n", clt, lid);
    pthread_mutex_lock(&mtx);
    while(lock_table.find(lid) != lock_table.end()) {
        printf("lid %llu start wait\n", lid);
        pthread_cond_wait(&cond, &mtx);
    }

    lock_table[lid] = 1;
    printf("lid %llu get lock\n", lid);
    
    pthread_mutex_unlock(&mtx);
    return ret;
}



