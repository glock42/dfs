#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include <vector>
#include "extent_client.h"
#include "lock_protocol.h"
#include "lock_client.h"

class yfs_client {
    extent_client *ec;

   public:
    typedef unsigned long long inum;
    enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
    typedef int status;

    struct fileinfo {
        unsigned long long size;
        unsigned long atime;
        unsigned long mtime;
        unsigned long ctime;
    };
    struct dirinfo {
        unsigned long atime;
        unsigned long mtime;
        unsigned long ctime;
    };
    struct dirent {
        std::string name;
        yfs_client::inum inum;
    };

   private:
    static std::string filename(inum);
    static inum n2i(std::string);
    inum random_ino_();

   public:
    yfs_client(std::string, std::string);

    bool isfile(inum);
    bool isdir(inum);

    int getfile(inum, fileinfo &);
    int getdir(inum, dirinfo &);

    int create(inum parent, std::string name, inum &ret_ino);
    int lookup(inum parent, std::string name, inum &ret_ino);
    int readdir(inum dir, std::map<std::string, inum> &files);
    int read(inum file, std::string& buf, size_t size, size_t off);
    int write(inum file, std::string buf, size_t size, size_t off);
    int truncate(inum file, size_t size);
};

#endif
