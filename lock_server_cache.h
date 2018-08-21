#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>

#include <algorithm>
#include <list>
#include <map>
#include "lock_protocol.h"
#include "lock_server.h"
#include "rpc.h"

class lock_server_cache {
   private:
    struct lock_content {
        std::string owned_client;
        std::list<std::string> wait_clients;
    };
    int nacquire;
    std::map<int, lock_content> lock_map;
    pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

   public:
    lock_server_cache();
    lock_protocol::status stat(lock_protocol::lockid_t, int &);
    int acquire(lock_protocol::lockid_t, std::string id, int &);
    int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
