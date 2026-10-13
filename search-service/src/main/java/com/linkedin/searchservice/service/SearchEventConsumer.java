package com.linkedin.searchservice.service;

import com.linkedin.searchservice.model.PostDocument;
import com.linkedin.searchservice.model.UserDocument;
import com.linkedin.searchservice.repository.PostSearchRepository;
import com.linkedin.searchservice.repository.UserSearchRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class SearchEventConsumer {

    private final UserSearchRepository userSearchRepository;
    private final PostSearchRepository postSearchRepository;

    @KafkaListener(topics = "user.created")
    public void consumeUserCreated(@Payload Map<String, Object> payload) {
        try {
            log.info("Indexing new user: {}", payload.get("userId"));

            UserDocument document = new UserDocument();
            document.setId(payload.get("userId").toString());
            document.setFirstName(payload.get("firstName").toString());
            document.setLastName(payload.get("lastName").toString());
            document.setEmail(payload.get("email").toString());
            document.setHeadline(payload.get("headline").toString());
            document.setLocation(payload.get("location").toString());

            userSearchRepository.save(document);
            log.info("User indexed: {}", payload.get("userId"));
        } catch (Exception e) {
            log.error("Error indexing user: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "user.updated")
    public void consumeUserUpdated(@Payload Map<String, Object> payload) {
        try {
            String userId = payload.get("userId").toString();
            log.info("Updating user: {}", payload.get("userId"));

            userSearchRepository.findById(userId).ifPresent( doc -> {
                        doc.setFirstName(payload.get("firstName").toString());
                        doc.setLastName(payload.get("lastName").toString());
                        doc.setHeadline(payload.get("headline").toString());
                        doc.setLocation(payload.get("location").toString());

                        if (payload.get("skills") != null ) {
                            doc.setSkills((List<String>) payload.get("skills"));
                        }

                        userSearchRepository.save(doc);
                        log.info("User index updated: {}", payload.get("userId"));
                    }
            );
        } catch (Exception e) {
            log.error("Error updating user index: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "post.created")
    public void consumePostCreated(@Payload Map<String, Object> payload) {
        try {
            log.info("Indexing new post: {}", payload.get("postId"));

            PostDocument document = new PostDocument();
            document.setId(payload.get("postId").toString());
            document.setAuthorId(payload.get("authorId").toString());
            document.setContent(payload.get("content").toString());
            document.setImageUrl(payload.get("imageUrl").toString());
            document.setCreatedAt(payload.get("createdAt").toString());

            postSearchRepository.save(document);
            log.info("Post indexed: {}", payload.get("postId"));
        } catch (Exception e) {
            log.error("Error indexing post: {}", e.getMessage());
        }
    }
}
