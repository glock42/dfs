#ifndef lock_server_cache_rsm_h
#define lock_server_cache_rsm_h

#include <algorithm>
#include <list>
#include <map>
#include <string>

#include "lock_protocol.h"
#include "rpc.h"
#include "rsm.h"
#include "rsm_state_transfer.h"

class lock_server_cache_rsm : public rsm_state_transfer {
   private:
    int nacquire;
    class rsm *rsm;

    struct lock_content {
        std::string owned_client;
        std::list<std::string> wait_clients;
        lock_content() = default;
        lock_content(std::string _owned_client,
                     std::list<std::string> _wait_clients)
            : owned_client(_owned_client), wait_clients(_wait_clients) {}
    };

    struct revoke_entry {
        lock_protocol::lockid_t lid;
        lock_protocol::xid_t xid;
        std::string client;
        revoke_entry() = default;
        revoke_entry(lock_protocol::lockid_t _lid, lock_protocol::xid_t _xid,
                     std::string _client)
            : lid(_lid), xid(_xid), client(_client) {}
    };

    struct retry_entry {
        lock_protocol::lockid_t lid;
        lock_protocol::xid_t xid;
        std::string client;
        retry_entry() = default;
        retry_entry(lock_protocol::lockid_t _lid, lock_protocol::xid_t _xid,
                    std::string _client)
            : lid(_lid), xid(_xid), client(_client) {}
    };

    std::map<int, lock_content> lock_map;
    std::map<std::string,
             std::map<lock_protocol::lockid_t, lock_protocol::xid_t>>
        highest_xid;
    std::map<std::string,
             std::map<lock_protocol::lockid_t, lock_protocol::xid_t>>
        acquire_cache;
    std::map<std::string,
             std::map<lock_protocol::lockid_t, lock_protocol::xid_t>>
        release_cache;
    fifo<revoke_entry> wait_to_revoke;
    fifo<retry_entry> wait_to_retry;

    pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

   public:
    lock_server_cache_rsm(class rsm *rsm = 0);
    lock_protocol::status stat(lock_protocol::lockid_t, int &);
    void revoker();
    void retryer();
    std::string marshal_state();
    void unmarshal_state(std::string state);
    int acquire(lock_protocol::lockid_t, std::string id, lock_protocol::xid_t,
                int &);
    int release(lock_protocol::lockid_t, std::string id, lock_protocol::xid_t,
                int &);
};

#endif
