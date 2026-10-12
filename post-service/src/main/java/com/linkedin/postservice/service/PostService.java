package com.linkedin.postservice.service;

import com.linkedin.postservice.entity.Comment;
import com.linkedin.postservice.entity.Like;
import com.linkedin.postservice.entity.Post;
import com.linkedin.postservice.repository.CommentRepository;
import com.linkedin.postservice.repository.LikeRepository;
import com.linkedin.postservice.repository.PostRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class PostService {

    private final PostRepository postRepository;
    private final LikeRepository likeRepository;
    private final CommentRepository commentRepository;
    private final S3Service s3Service;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String POST_CREATED_TOPIC = "post.created";
    private static final String POST_LIKED_TOPIC = "post.liked";
    private static final String POST_COMMENTED_TOPIC = "post.commented";

    /**
     * Create a post
     * Optionally Upload image to S3
     * Publish post.created event to kakfa
     * Feed service and Search Service will consume this
     * @param authorId
     * @param content
     * @param image
     * @return
     */
    public Post createPost(String authorId, String content, MultipartFile image) {

        log.info("Creating post for user: {}", authorId);

        Post post = new Post();
        post.setAuthorId(authorId);
        post.setContent(content);

        if (image != null && !image.isEmpty()) {
            String imageUrl = s3Service.uploadFile(
                    image, "posts/" + authorId);

            post.setImageUrl(imageUrl);
        }

        Post savedPost = postRepository.save(post);
        log.info("Post created: {}", savedPost.getId());

        // Publish to Kafka

        Map<String, Object> postCreatedEvent = new HashMap<>();
        postCreatedEvent.put("postId", savedPost.getId());
        postCreatedEvent.put("authorId", savedPost.getAuthorId());
        postCreatedEvent.put("content", savedPost.getContent());
        postCreatedEvent.put("imageUrl", savedPost.getImageUrl());
        postCreatedEvent.put("createdAt", savedPost.getCreatedAt().toString());

        kafkaTemplate.send(POST_CREATED_TOPIC, savedPost.getAuthorId(), postCreatedEvent);
        log.info("post.created event published: {}", savedPost.getId());

        return savedPost;
    }


    public Post getPost(String postId) {
        log.info("Getting post: {}", postId);

        return postRepository.findById(postId).orElseThrow(
                () -> new RuntimeException("Post not found: " + postId)
        );
    }

    public List<Post> getUserPosts(String userId) {
        log.info("Getting posts for user: {}", userId);

        return postRepository.findByAuthorIdOrderByCreatedAtDesc(userId);
    }

    /**
     * Like or unlike a post
     * @param postId
     * @param userId
     * @return
     */
    public String likePost(String postId, String userId) {

        Post post = getPost(postId);

        if (likeRepository.existsByPostIdAndUserId(postId, userId)) {
            // Unlike
            likeRepository.findByPostIdAndUserId(postId, userId)
                    .ifPresent(likeRepository::delete);
            post.setLikeCount(post.getLikeCount() - 1);
            postRepository.save(post);
            return "Post unliked";
        }

        // Like
        Like like = new Like();
        like.setPostId(postId);
        like.setUserId(userId);
        likeRepository.save(like);

        post.setLikeCount(post.getLikeCount() + 1);
        postRepository.save(post);

        // Publish post.liked event
        Map<String, Object> postLikedEvent = new HashMap<>();
        postLikedEvent.put("postId", postId);
        postLikedEvent.put("userId", userId);
        postLikedEvent.put("authorId", post.getAuthorId());

        kafkaTemplate.send(POST_LIKED_TOPIC, postId, postLikedEvent);
        return "Post liked";
    }

    public Comment addComment(String postId, String authorId, String content) {
        Post post = getPost(postId);

        Comment comment = new Comment();
        comment.setPostId(postId);
        comment.setAuthorId(authorId);
        comment.setContent(content);

        Comment savedComment = commentRepository.save(comment);

        post.setCommentCount(post.getCommentCount() + 1);
        postRepository.save(post);

        // Publish post.commented event
        Map<String, Object> postCommentedEvent = new HashMap<>();
        postCommentedEvent.put("postId", postId);
        postCommentedEvent.put("authorId", authorId);
        postCommentedEvent.put("commentId", savedComment.getId());
        postCommentedEvent.put("postAuthorId", post.getAuthorId());

        kafkaTemplate.send(POST_COMMENTED_TOPIC, postId, postCommentedEvent);

        return savedComment;
    }

    public List<Comment> getComments(String postId) {
        return commentRepository.findByPostIdOrderByCreatedAtDesc(postId);
    }

    /**
     * Delete post
     * @param postId
     * @param userId
     */
    public void deletePost(String postId, String userId) {
        Post post = getPost(postId);

        if (!post.getAuthorId().equals(userId)) {
            throw new RuntimeException("Not authorized to delete post: " + postId);
        }

        postRepository.delete(post);
        log.info("Post deleted: {}", postId);
    }
}
