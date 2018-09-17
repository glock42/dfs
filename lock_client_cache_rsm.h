// lock client interface.

#ifndef lock_client_cache_rsm_h

#define lock_client_cache_rsm_h

#include <string>
#include "lang/verify.h"
#include "lock_client.h"
#include "lock_protocol.h"
#include "rpc.h"

#include "rsm_client.h"

// Classes that inherit lock_release_user can override dorelease so that
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 5.
class lock_release_user {
   public:
    virtual void dorelease(lock_protocol::lockid_t) = 0;
    virtual ~lock_release_user(){};
};

class lock_client_cache_rsm;

// Clients that caches locks.  The server can revoke locks using
// lock_revoke_server.
class lock_client_cache_rsm : public lock_client {
   private:
    struct lock_attr {
        bool revoke = false;
        bool retry = false;
        lock_protocol::xid_t xid = 0;
        rlock_protocol::client_status status;
    };

    struct release_entry {
        lock_protocol::lockid_t lid;
        lock_protocol::xid_t xid;
        release_entry() = default;
        release_entry(lock_protocol::lockid_t _lid, lock_protocol::xid_t _xid)
            : lid(_lid), xid(_xid) {}
    };

    rsm_client *rsmc;
    class lock_release_user *lu;
    int rlock_port;
    std::string hostname;
    std::string id;
    lock_protocol::xid_t xid;

    fifo<struct release_entry> wait_release;

    std::map<lock_protocol::lockid_t, lock_attr> lock_cache;
    pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t release_cond = PTHREAD_COND_INITIALIZER;
    pthread_cond_t retry_cond = PTHREAD_COND_INITIALIZER;
    pthread_cond_t normal_cond = PTHREAD_COND_INITIALIZER;

   public:
    static int last_port;
    lock_client_cache_rsm(std::string xdst, class lock_release_user *l = 0);
    virtual ~lock_client_cache_rsm(){};
    lock_protocol::status acquire(lock_protocol::lockid_t);
    virtual lock_protocol::status release(lock_protocol::lockid_t);
    void releaser();
    rlock_protocol::status revoke_handler(lock_protocol::lockid_t,
                                          lock_protocol::xid_t, int &);
    rlock_protocol::status retry_handler(lock_protocol::lockid_t,
                                         lock_protocol::xid_t, int &);
};

#endif
