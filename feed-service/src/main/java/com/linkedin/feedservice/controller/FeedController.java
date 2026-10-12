package com.linkedin.feedservice.controller;

import com.linkedin.feedservice.service.FeedService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/feed")
@Slf4j
@RequiredArgsConstructor
public class FeedController {

    private final FeedService feedService;

    // Get Feed - gets paginated feed response, where latest posts come on top
    @GetMapping("/userId")
    public ResponseEntity<List<String>> getFeed(
            @PathVariable String userId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size
    ) {
        return ResponseEntity.ok(
                feedService.getFeed(userId, page, size)
        );
    }

    // Clear Feed - this for testing purpose
    @DeleteMapping("/{userId}/cache")
    public ResponseEntity<String> clearFeed(
            @PathVariable String userId
    ) {
        feedService.clearFeed(userId);

        return ResponseEntity.ok("Feed cleared");
    }
}
