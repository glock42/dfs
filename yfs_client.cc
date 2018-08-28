// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include "dlock.h"
#include "extent_client.h"
#include "lock_client_cache.h"
#include "tprintf.h"

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst) {
    ec = new extent_client(extent_dst);
    lc = new lock_client_cache(lock_dst, new lock_release_extent(ec));
    ec->put(1, "");
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
    // You modify this function for Lab 3
    // - hold and release the file lock
    Dlock d(lc, inum);
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
    // You modify this function for Lab 3
    // - hold and release the directory lock
    Dlock d(lc, inum);

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

int yfs_client::unlink(inum parent, std::string name) {
    inum ino;
    std::string parent_buf;
    if (isfile(parent)) return IOERR;
    if (lookup(parent, name, ino) != OK) return IOERR;
    Dlock d(lc, parent);

    ec->remove(ino);
    ec->get(parent, parent_buf);
    auto begin_pos = parent_buf.find(name);
    auto finish_pos = parent_buf.find(',', begin_pos);
    parent_buf.erase(begin_pos, finish_pos - begin_pos + 1);
    ec->put(parent, parent_buf);
    return OK;
}

int yfs_client::mkdir(inum parent, std::string name, inum &ret_ino) {
    inum ino;
    std::string parent_buf;
    tprintf("lab5: mkdir %s\n", name.c_str());
    if (lookup(parent, name, ino) == OK) return EXIST;
    Dlock d(lc, parent);
    ino = random_ino_(false);
    if (isfile(ino)) return IOERR;
    ec->put(ino, "");

    ec->get(parent, parent_buf);
    parent_buf.append(name + ":" + std::to_string(ino) + ",");
    ec->put(parent, parent_buf);
    ret_ino = ino;
    tprintf("lab5: mkdir %s over \n", name.c_str());
    return OK;
}

int yfs_client::truncate(inum file, size_t size) {
    std::string file_buf;
    Dlock d(lc, file);
    ec->get(file, file_buf);
    if (size < file_buf.size()) {
        file_buf = file_buf.substr(0, size);
    } else {
        file_buf += std::string(size - file_buf.size(), '\0');
    }
    ec->put(file, file_buf);
    return OK;
}

int yfs_client::read(inum file, std::string &buf, size_t size, size_t off) {
    Dlock d(lc, file);
    std::string file_buf;
    extent_protocol::attr attr;
    ec->getattr(file, attr);
    if (off >= attr.size) {
        return OK;
    }
    ec->get(file, file_buf);
    buf = file_buf.substr(off, size);
    return OK;
}
int yfs_client::write(inum file, std::string buf, size_t size, size_t off) {
    std::string file_buf;
    std::string new_write_buf;
    Dlock d(lc, file);
    ec->get(file, file_buf);
    if (off < file_buf.size()) {
        std::string buf1 = file_buf.substr(0, off);
        std::string buf2 = buf.substr(0, size);
        if (size > buf.size()) {
            buf2 += std::string(size - buf.size(), '\0');
        }
        new_write_buf = buf1 + buf2;
        if (off + size < file_buf.size()) {
            new_write_buf += file_buf.substr(off + size);
        }
    } else {
        std::string hole = std::string(off - file_buf.size(), '\0');
        new_write_buf = file_buf + hole + buf.substr(0, size);
        if (size > buf.size()) {
            new_write_buf += std::string(size - buf.size(), '\0');
        }
    }
    ec->put(file, new_write_buf);
    return OK;
}

yfs_client::inum yfs_client::random_ino_(bool is_file) {
    inum ino = (inum)rand() | 0x80000000;
    if (!is_file) {
        ino &= (~(1 << 31));
    }
    return ino;
}

int yfs_client::create(inum parent, std::string name, inum &ret_ino) {
    inum ino;
    std::string parent_buf;
    tprintf("lab5: create, parent: %d, name: %s\n", int(parent), name.c_str());
    if (lookup(parent, name, ino) == OK) {
        return EXIST;
    }
    Dlock d(lc, parent);
    ino = random_ino_(true);
    ec->put(ino, "");

    ec->get(parent, parent_buf);
    parent_buf.append(name + ":" + std::to_string(ino) + ",");
    ec->put(parent, parent_buf);
    ret_ino = ino;
    tprintf("lab5: create, parent: %d, name: %s over\n", int(parent), name.c_str());
    return OK;
}

int yfs_client::lookup(inum parent, std::string name, inum &ret_ino) {
    std::string buf;
    std::string::size_type pos = -1, last_pos = -1;
    Dlock d(lc, parent);
    auto r = ec->get(parent, buf);
    if (r != OK) return IOERR;
    while ((pos = buf.find(",", pos + 1)) != std::string::npos) {
        std::string file_entry = buf.substr(last_pos + 1, pos - last_pos - 1);
        auto divide_pos = file_entry.find(":");
        auto file_name = file_entry.substr(0, divide_pos);
        auto file_ino = n2i(file_entry.substr(divide_pos + 1));
        last_pos = pos;
        if (file_name == name) {
            ret_ino = file_ino;
            return OK;
        }
    }
    return NOENT;
}

int yfs_client::readdir(inum dir, std::map<std::string, inum> &files) {
    std::string buf;
    std::string::size_type pos = -1, last_pos = -1;
    // bool is_parent_name = true;
    Dlock d(lc, dir);
    auto r = ec->get(dir, buf);
    if (r != OK) return IOERR;
    while ((pos = buf.find_first_of(",", pos + 1)) != std::string::npos) {
        std::string file_entry = buf.substr(last_pos + 1, pos - last_pos - 1);
        auto divide_pos = file_entry.find(":");
        auto file_name = file_entry.substr(0, divide_pos);
        auto file_ino = n2i(file_entry.substr(divide_pos + 1));
        last_pos = pos;
        files[file_name] = file_ino;
    }
    return OK;
}
