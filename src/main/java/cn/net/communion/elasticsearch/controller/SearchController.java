package cn.net.communion.elasticsearch.controller;



import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import cn.net.communion.elasticsearch.service.SearchServiceImpl;

@RestController
public class SearchController {
    @Autowired
    private SearchServiceImpl impl;

    // @RequestMapping(path = "/{client}/search/{index}", method = RequestMethod.POST)
    // public String search(@PathVariable String index, @ModelAttribute SearchDesc desc) {
    // return impl.search(index, desc);
    // }

    @RequestMapping(path = "/{client}/search/{type}", method = RequestMethod.POST)
    public String search(@PathVariable String type,
            @RequestParam(defaultValue = "0", required = false) final int from,
            @RequestParam(defaultValue = "10", required = false) final int size,
            @RequestParam String keyword) {
        return impl.search(type, keyword, from, size);
    }

    @RequestMapping(path = "/{client}/{type}/hotsearch")
    public String hotSearch(@PathVariable String type,
            @RequestParam(defaultValue = "10", required = false) final int size) {
        return impl.hotSearch(type, size);
    }
}
