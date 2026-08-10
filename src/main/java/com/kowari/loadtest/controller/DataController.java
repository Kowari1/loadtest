package com.kowari.loadtest.controller;

import com.kowari.loadtest.model.DataRequest;
import com.kowari.loadtest.service.DataService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/data")
public class DataController {

    private final DataService dataService;

    public DataController(DataService dataService) {
        this.dataService = dataService;
    }

    @PostMapping
    public ResponseEntity<Void> add(
            @RequestBody DataRequest request) {

        dataService.add(request.id());

        return ResponseEntity.ok().build();
    }

    @GetMapping
    public ResponseEntity<List<String>> get() {

        return ResponseEntity.ok(
                dataService.getNewData()
        );
    }
}
