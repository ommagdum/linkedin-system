package com.linkedin.userservice.service;

import com.linkedin.userservice.dto.UserResponse;
import com.linkedin.userservice.entity.Connection;
import com.linkedin.userservice.entity.ConnectionStatus;
import com.linkedin.userservice.entity.User;
import com.linkedin.userservice.repository.ConnectionRepository;
import com.linkedin.userservice.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class UserService {

    private final ConnectionRepository connectionRepository;
    private final UserRepository userRepository;
    private final S3Service s3Service;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String CONNECTION_REQUESTED_TOPIC = "connection.requested";
    private static final String CONNECTION_ACCEPTED_TOPIC = "connection.accepted";
    private static final String USER_UPDATED_TOPIC = "user.updated";

    public String sendConnectionRequest(String receiverId, String requestorId) {

        if(connectionRepository.existsByRequestorIdAndReceiverId(requestorId, receiverId)) {
            throw new RuntimeException(
                    "Connection request already sent"
            );
        }

        Connection connection = new Connection();
        connection.setRequestorId(requestorId);
        connection.setReceiverId(receiverId);
        connection.setStatus(ConnectionStatus.PENDING);

        connectionRepository.save(connection);

        // Publish connection.requested event

        Map<String, Object> connectionRequestEvent = new HashMap<>();
        connectionRequestEvent.put("requestorId", requestorId);
        connectionRequestEvent.put("receiverId", receiverId);

        kafkaTemplate.send(CONNECTION_REQUESTED_TOPIC, requestorId, connectionRequestEvent);

        log.info("Connection request sent: {} -> {}", requestorId, receiverId);
        return "Connection request sent";
    }

    public String acceptConnectionRequest(String connectionId) {

        Connection connection = connectionRepository.findById(connectionId).orElseThrow(
                        () -> new RuntimeException("Connection not found: " + connectionId)
        );
        connection.setStatus(ConnectionStatus.CONNECTED);
        connectionRepository.save(connection);

        //Publish connection.accepted event
        Map<String, Object> connectionAcceptedEvent = new HashMap<>();
        connectionAcceptedEvent.put("requestorId", connection.getRequestorId());
        connectionAcceptedEvent.put("receiverId", connection.getReceiverId());

        kafkaTemplate.send(CONNECTION_ACCEPTED_TOPIC, connection.getRequestorId(), connectionAcceptedEvent);

        log.info("Connection accepted sent: {}", connectionId);

        return "Connection accepted";
    }

    public List<UserResponse> getConnections(String userId) {

        List<Connection> connections = connectionRepository.findByRequestorIdAndStatus(userId, ConnectionStatus.CONNECTED);

        return connections.stream()
                .map(c -> getUserProfile(c.getReceiverId()))
                .collect(Collectors.toList());
    }


    public UserResponse getUserProfile(String userId) {

        User user = userRepository.findById(userId).orElseThrow(
                () -> new RuntimeException("User not found: " + userId)
        );

        return mapToResponse(user);
    }

    private UserResponse mapToResponse(User user) {
        UserResponse response = new UserResponse();
        response.setId(user.getId());
        response.setEmail(user.getEmail());
        response.setFirstName(user.getFirstName());
        response.setLastName(user.getLastName());
        response.setHeadLine(user.getHeadLine());
        response.setAbout(user.getAbout());
        response.setLocation(user.getLocation());
        response.setProfilePhotoUrl(user.getProfilePhotoUrl());
        response.setCoverPhotoUrl(user.getCoverPhotoUrl());
        response.setRole(user.getRole());
        response.setSkills(user.getSkills());
        response.setCreatedAt(user.getCreatedAt());

        return response;
    }

    public UserResponse updateProfile(String userId, UserResponse userResponse) {

        User user = userRepository.findById(userId).orElseThrow(
                () -> new RuntimeException("User not found: " + userId)
        );

        user.setHeadLine(userResponse.getHeadLine());
        user.setAbout(userResponse.getAbout());
        user.setLocation(userResponse.getLocation());
        user.setSkills(userResponse.getSkills());
        user.setProfilePhotoUrl(userResponse.getProfilePhotoUrl());
        user.setCoverPhotoUrl(userResponse.getCoverPhotoUrl());
        user.setRole(userResponse.getRole());

        User savedUser = userRepository.save(user);

        // Publish user.updated event
        Map<String, Object> userUpdateEvent = new HashMap<>();
        userUpdateEvent.put("userId", savedUser.getId());
        userUpdateEvent.put("firstName", user.getFirstName());
        userUpdateEvent.put("lastName", user.getLastName());
        userUpdateEvent.put("headLine", user.getHeadLine());
        userUpdateEvent.put("about", user.getAbout());
        userUpdateEvent.put("location", user.getLocation());
        userUpdateEvent.put("profilePhotoUrl", user.getProfilePhotoUrl());
        userUpdateEvent.put("coverPhotoUrl", user.getCoverPhotoUrl());
        userUpdateEvent.put("skills", user.getSkills());

        kafkaTemplate.send(USER_UPDATED_TOPIC, savedUser.getId(), userUpdateEvent);

        log.info("User updated event published: {}", savedUser.getId());

        return mapToResponse(savedUser);
    }

    public List<UserResponse> getPendingConnections(String userId) {
        List<Connection> connections = connectionRepository.findByRequestorIdAndStatus(userId, ConnectionStatus.PENDING);

        return connections.stream()
                .map(c -> getUserProfile(c.getReceiverId()))
                .collect(Collectors.toList());
    }

    public UserResponse uploadProfilePhoto(String userId, MultipartFile file) {

        User user = userRepository.findById(userId).orElseThrow(
                () -> new RuntimeException("User not found: " + userId)
        );

        String photoUrl = s3Service.uploadFile(file, "profiles/" + userId + "/avatar");
        user.setProfilePhotoUrl(photoUrl);
        User savedUser = userRepository.save(user);

        log.info("User uploaded profile photo: {}", savedUser.getId());
        return mapToResponse(savedUser);
    }
}
