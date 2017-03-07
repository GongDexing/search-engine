package cn.net.communion.elasticsearch.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import cn.net.communion.elasticsearch.client.Client;
import cn.net.communion.elasticsearch.entity.SearchDesc;

@Service
public class SearchServiceImpl implements SearchService {
    @Autowired
    private Client client;

    @Override
    public String search(String index, SearchDesc desc) {
        return client.search(index, desc);
    }

    public String search(String type, String keyword, int from, int size) {
        return client.search(type, keyword, from, size);
    }

    @Override
    public String hotSearch(String type, int size) {
        return client.hotSearch(type, size);
    }

}
