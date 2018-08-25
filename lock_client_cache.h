// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include "lang/verify.h"
#include "lock_client.h"
#include "lock_protocol.h"
#include "rpc.h"

// Classes that inherit lock_release_user can override dorelease so that
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 5.
class lock_release_user {
   public:
    virtual void dorelease(lock_protocol::lockid_t) = 0;
    virtual ~lock_release_user(){};
};

class lock_client_cache : public lock_client {
   private:
    class lock_release_user *lu;
    struct lock_attr {
        bool revoke = false;
        bool retry = false;
        rlock_protocol::client_status status;
    };
    int rlock_port;
    std::string hostname;
    std::string id;
    std::map<lock_protocol::lockid_t, lock_attr> lock_cache;
    std::map<lock_protocol::lockid_t, rlock_protocol::client_status> client_status;
    pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t release_cond = PTHREAD_COND_INITIALIZER;
    pthread_cond_t retry_cond = PTHREAD_COND_INITIALIZER;
    pthread_cond_t normal_cond= PTHREAD_COND_INITIALIZER;
    std::list<lock_protocol::lockid_t> wait_to_revoke;
    std::list<lock_protocol::lockid_t> wait_to_retry;
   public:
    lock_client_cache(std::string xdst, class lock_release_user *l = 0);
    virtual ~lock_client_cache(){};
    lock_protocol::status acquire(lock_protocol::lockid_t);
    lock_protocol::status release(lock_protocol::lockid_t);
    rlock_protocol::status revoke_handler(lock_protocol::lockid_t, int &);
    rlock_protocol::status retry_handler(lock_protocol::lockid_t, int &);
};

#endif
