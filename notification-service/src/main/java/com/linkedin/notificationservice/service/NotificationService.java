package com.linkedin.notificationservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationService {

    @KafkaListener(topics = "user.created")
    public void consumeUserCreatedEvent(
            @Payload Map<String, Object> payload
    ) {
        try {
            String userId = payload.get("userId").toString();
            String firstName = payload.get("firstName").toString();

            sendNotification(userId,
                    "Welcome to LinkedIn!",
                    String.format(
                            "Welcome %s Your account has been created" +
                                    "Start connecting with professionals.",
                            firstName
                    ));
        } catch (Exception e) {
            log.error("Error sending welcome notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "connection.requested")
    public void consumeConnectionRequestedEvent(
            @Payload Map<String, Object> payload
    ) {
        try {
            String receiverId = (String) payload.get("receiverId");
            String requestorId = (String) payload.get("requestorId");

            sendNotification(receiverId, "New Connection Request",
                    String.format(
                            "%s wants to connect with you!", requestorId
                    ));
        } catch (Exception e) {
            log.error("Error sending connection request notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "connection.accepted")
    public void consumeConnectionAcceptedEvent(
            @Payload Map<String, Object> payload
    ) {
        try {
            String receiverId = (String) payload.get("receiverId");
            String requestorId = (String) payload.get("requestorId");

            sendNotification(requestorId, "Connection Accepted!",
                    String.format(
                            "%s accepted your connection request!!", receiverId
                    ));
        } catch (Exception e) {
            log.error("Error sending connection accepted notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "post.liked")
    public void consumePostLikedEvent(
            @Payload Map<String, Object> payload
    ) {
        try {
            String postId = (String) payload.get("postId");
            String userId = (String) payload.get("userId");
            String authorId = (String) payload.get("authorId");

            sendNotification(userId, "Post Liked!",
                    String.format(
                            "%s liked your post %s!!", authorId, postId
                    ));
        } catch (Exception e) {
            log.error("Error sending post liked notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "post.commented")
    public void consumePostCommentedEvent(
            @Payload Map<String, Object> payload
    ) {
        try {
            String postId = (String) payload.get("postId");
            String authorId = (String) payload.get("authorId");
            String commentId = (String) payload.get("commentId");
            String postAuthorId = (String) payload.get("postAuthorId");

            sendNotification(postAuthorId, "Post Commented!",
                    String.format(
                            "%s commented %s on your post %s!!", authorId, commentId, postId
                    ));
        } catch (Exception e) {
            log.error("Error sending post commented notification: {}", e.getMessage());
        }
    }

    private void sendNotification(String userId, String title, String message) {
        log.info("-----------------------------------------------");
        log.info("NOTIFICATION SENT");
        log.info("To User: {}", userId);
        log.info("Title: {}", title);
        log.info("Message: {}", message);
        log.info("-----------------------------------------------");
    }

}
