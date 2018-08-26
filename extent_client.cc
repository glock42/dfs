// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <iostream>
#include <sstream>

// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst) {
    sockaddr_in dstsock;
    make_sockaddr(dst.c_str(), &dstsock);
    cl = new rpcc(dstsock);
    if (cl->bind() != 0) {
        printf("extent_client: bind failed\n");
    }
}

extent_protocol::status extent_client::get(extent_protocol::extentid_t eid,
                                           std::string &buf) {
    extent_protocol::status ret = extent_protocol::OK;
    auto iter = cache.find(eid);
    if (iter == cache.end()) {
        struct extent ext;
        ret = cl->call(extent_protocol::get, eid, ext.buf);
        ret = getattr(eid, ext.attr);
        cache[eid] = ext;
        buf = ext.buf;
    } else {
        buf = iter->second.buf;
        iter->second.attr.atime = time(0);
    }
    return ret;
}

extent_protocol::status extent_client::getattr(extent_protocol::extentid_t eid,
                                               extent_protocol::attr &attr) {
    extent_protocol::status ret = extent_protocol::OK;
    auto iter = cache.find(eid);
    if(iter == cache.end()) {
        ret = cl->call(extent_protocol::getattr, eid, attr);
    } else {
        attr = iter->second.attr;
    }
    return ret;
}

extent_protocol::status extent_client::put(extent_protocol::extentid_t eid,
                                           std::string buf) {
    extent_protocol::status ret = extent_protocol::OK;
    //int r;
    //ret = cl->call(extent_protocol::put, eid, buf, r);
    auto &ext = cache[eid];
    ext.buf = buf;
    ext.attr.size = buf.size();
    ext.attr.ctime = time(0);
    ext.attr.mtime = time(0);
    ext.attr.atime = time(0);
    return ret;
}

extent_protocol::status extent_client::remove(extent_protocol::extentid_t eid) {
    extent_protocol::status ret = extent_protocol::OK;
    //int r;
    auto iter = cache.find(eid);
    //ret = cl->call(extent_protocol::remove, eid, r);
    if(iter!= cache.end()) {
        cache.erase(iter);
    }
    return ret;
}
