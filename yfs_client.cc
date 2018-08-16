// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include "extent_client.h"

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst) {
    ec = new extent_client(extent_dst);
}

yfs_client::inum yfs_client::n2i(std::string n) {
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string yfs_client::filename(inum inum) {
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool yfs_client::isfile(inum inum) {
    if (inum & 0x80000000) return true;
    return false;
}

bool yfs_client::isdir(inum inum) { return !isfile(inum); }

int yfs_client::getfile(inum inum, fileinfo &fin) {
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:

    return r;
}

int yfs_client::getdir(inum inum, dirinfo &din) {
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}

int yfs_client::create(inum parent, std::string name, inum& ret_ino) { return OK;}

int yfs_client::lookup(inum parent, std::string name, inum& ret_ino) { 
    std::string buf;
    std::string::size_type pos = 0, last_pos = -1;
    bool is_parent_name = true;
    auto r = ec->get(parent, buf);
    if(r != OK) return IOERR;
    while((pos = buf.find(",")) != std::string::npos) {
        std::string file_entry = buf.substr(last_pos + 1, pos - last_pos - 1);
        auto divide_pos = file_entry.find(":");
        auto file_name = file_entry.substr(last_pos + 1, divide_pos - last_pos - 1);
        auto file_ino = n2i(file_entry.substr(divide_pos));
        last_pos = pos;
        if(is_parent_name) {
            is_parent_name = false;
            continue; 
        }
        if(file_name == name) {
            ret_ino = file_ino;
            return OK;
        }
    }
    return NOENT;
}
