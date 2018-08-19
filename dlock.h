#ifndef __DISTRIBUTED_LOCK__
#define __DISTRIBUTED_LOCK__
#include "yfs_client.h"
#include "lock_client.h"
#include "lang/verify.h"
struct Dlock {

    public:
        Dlock(lock_client *lc, yfs_client::inum ino):lc(lc), ino(ino) {
            printf("start acquire  %016llx \n", ino);
            VERIFY(lc->acquire(ino) == lock_protocol::OK);
            printf("acquire successd  %016llx \n", ino);
        }
        ~Dlock(){
            printf("start release  %016llx\n", ino);
            VERIFY(lc->release(ino) == lock_protocol::OK);
            printf("release successd  %016llx\n", ino);
        }

    private:
        lock_client *lc;
        yfs_client::inum ino;
};
#endif  /*__DISTRIBUTED_LOCK__*/

