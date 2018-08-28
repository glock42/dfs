// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <map>
#include <string>
#include "extent_protocol.h"
#include "rpc.h"

class extent_client {
   private:
    struct extent {
        std::string buf;
        extent_protocol::attr attr;
        bool is_data_dirty;
        int status;
    };

    rpcc *cl;
    std::map<extent_protocol::extentid_t, struct extent> cache;
    pthread_mutex_t m_ = PTHREAD_MUTEX_INITIALIZER;

   public:
    enum cache_status {ALL_CACHE, ATTR_CACHE};
    extent_client(std::string dst);

    extent_protocol::status get(extent_protocol::extentid_t eid,
                                std::string &buf);
    extent_protocol::status getattr(extent_protocol::extentid_t eid,
                                    extent_protocol::attr &a);
    extent_protocol::status put(extent_protocol::extentid_t eid,
                                std::string buf);
    extent_protocol::status remove(extent_protocol::extentid_t eid);
    extent_protocol::status flush(extent_protocol::extentid_t eid);
};

#endif
