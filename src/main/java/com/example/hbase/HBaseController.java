package com.example.hbase;

import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/hbase")
public class HBaseController {

    private final HBaseService hBaseService;

    public HBaseController(HBaseService hBaseService) {
        this.hBaseService = hBaseService;
    }

    @GetMapping("/get")
    public String getValue(
            @RequestParam String table,
            @RequestParam String rowKey,
            @RequestParam String family,
            @RequestParam String qualifier
    ) throws Exception {
        return hBaseService.getValue(table, rowKey, family, qualifier);
    }
}