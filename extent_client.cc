// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include "tprintf.h"

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
    tprintf("lab5: get, eid: %d, buf: %s \n", int(eid), buf.c_str());
    pthread_mutex_lock(&m_);
    auto iter = cache.find(eid);
    if (iter == cache.end()) {
        struct extent ext;
        tprintf("lab5: get: call server, eid: %d, buf: %s \n", int(eid), buf.c_str());
        pthread_mutex_unlock(&m_);
        ret = cl->call(extent_protocol::get, eid, ext.buf);
        tprintf("lab5: get: call server back, eid: %d, buf: %s \n", int(eid), buf.c_str());
        ret = cl->call(extent_protocol::getattr, eid, ext.attr);
        ScopedLock m(&m_);
        ext.is_data_dirty = false;
        ext.status = extent_client::ALL_CACHE;
        cache[eid] = ext;
        buf = ext.buf;
    } else {
        if(iter->second.status == extent_client::ATTR_CACHE) {
            pthread_mutex_unlock(&m_);
            ret = cl->call(extent_protocol::get, eid, iter->second.buf);
            pthread_mutex_lock(&m_);
            iter->second.status = extent_client::ALL_CACHE;
        }
        buf = iter->second.buf;
        iter->second.attr.atime = time(0);
        pthread_mutex_unlock(&m_);
    }
    return ret;
}

extent_protocol::status extent_client::getattr(extent_protocol::extentid_t eid,
                                               extent_protocol::attr &attr) {
    extent_protocol::status ret = extent_protocol::OK;
    tprintf("lab5: getattr, eid: %d \n", int(eid));
    pthread_mutex_lock(&m_);
    auto iter = cache.find(eid);
    if(iter == cache.end()) {
        pthread_mutex_unlock(&m_);
        ret = cl->call(extent_protocol::getattr, eid, attr);
        pthread_mutex_lock(&m_);
        cache[eid].attr = attr;
        cache[eid].status = extent_client::ATTR_CACHE;
        pthread_mutex_unlock(&m_);
    } else {
        attr = iter->second.attr;
        pthread_mutex_unlock(&m_);
    }
    return ret;
}

extent_protocol::status extent_client::put(extent_protocol::extentid_t eid,
                                           std::string buf) {
    extent_protocol::status ret = extent_protocol::OK;
    tprintf("lab5: put, eid: %d, buf: %s \n", int(eid), buf.c_str());
    ScopedLock m(&m_);
    auto &ext = cache[eid];
    ext.buf = buf;
    ext.attr.size = buf.size();
    ext.attr.ctime = time(0);
    ext.attr.mtime = time(0);
    ext.attr.atime = time(0);
    ext.is_data_dirty = true;
    return ret;
}

extent_protocol::status extent_client::remove(extent_protocol::extentid_t eid) {
    extent_protocol::status ret = extent_protocol::OK;
    ScopedLock m(&m_);
    tprintf("lab5: remove, eid: %d \n", int(eid));
    auto iter = cache.find(eid);
    if(iter!= cache.end()) {
        cache.erase(iter);
    }
    return ret;
}

extent_protocol::status extent_client::flush(extent_protocol::extentid_t eid) {
    extent_protocol::status ret = extent_protocol::OK;
    tprintf("lab5: flush, eid: %d \n", int(eid));
    pthread_mutex_lock(&m_);
    auto iter = cache.find(eid);
    int r;
    if(iter == cache.end()) {
        pthread_mutex_unlock(&m_);
        ret = cl->call(extent_protocol::remove, eid, r); 
    } else {
        if(iter->second.is_data_dirty) {
            tprintf("lab5: flush to server, eid: %d, buf: %s \n", int(eid), iter->second.buf.c_str());
            ret = cl->call(extent_protocol::put, eid, iter->second.buf, r);
            iter->second.is_data_dirty = false;
        }
        cache.erase(iter);
        pthread_mutex_unlock(&m_);
    }
    return ret;
}
