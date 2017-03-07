package cn.net.communion.elasticsearch.service;

import cn.net.communion.elasticsearch.entity.SearchDesc;

interface SearchService {

    String search(String index, SearchDesc desc);

    public String search(String type, String keyword, int from, int size);

    String hotSearch(String type, int size);
}
