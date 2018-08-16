// the extent server implementation

#include "extent_server.h"
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <sstream>

extent_server::extent_server() {}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &) {
    // You fill this in for Lab 2.
    ScopedLock m(&m_);
    auto iter = contents_.find(id); 
    
    if(iter != contents_.end()) {
        ext_content content;
        extent_protocol::attr attr;
        content.id = id;
        content.buf = buf;

        attr.size = buf.size();
        attr.ctime = time(0);
        attr.mtime = time(0);
        attr.atime = time(0);

        content.attr = attr;
        contents_[id] = content;
    } else {
       iter->second.buf = buf;
       iter->second.attr.size = buf.size();
       iter->second.attr.mtime = time(0);
       iter->second.attr.atime = time(0);
    }

    return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf) {
    // You fill this in for Lab 2.
    ScopedLock m(&m_);
    auto iter = contents_.find(id); 
    if(iter != contents_.end()) {
        buf = iter->second.buf;
        iter->second.attr.atime = time(0);
        return extent_protocol::OK;
    } else {
        return extent_protocol::IOERR;
    }
}

int extent_server::getattr(extent_protocol::extentid_t id,
                           extent_protocol::attr &a) {
    // You fill this in for Lab 2.
    // You replace this with a real implementation. We send a phony response
    // for now because it's difficult to get FUSE to do anything (including
    // unmount) if getattr fails.
    
    ScopedLock m(&m_);
    auto iter = contents_.find(id); 
    if(iter != contents_.end()) {
        a.size = iter->second.attr.size;
        a.atime = iter->second.attr.atime;
        a.mtime = iter->second.attr.mtime;
        a.ctime = iter->second.attr.ctime;
        return extent_protocol::OK;
    } else {
        return extent_protocol::IOERR;
    }
}

int extent_server::remove(extent_protocol::extentid_t id, int &) {
    // You fill this in for Lab 2.
    ScopedLock m(&m_);
    auto iter = contents_.find(id); 
    if(iter != contents_.end()) {
        contents_.erase(iter);
        return extent_protocol::OK;
    }
    else {
        return extent_protocol::IOERR;
    }
}
