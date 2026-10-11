package com.linkedin.userservice.repository;

import com.linkedin.userservice.entity.Connection;
import com.linkedin.userservice.entity.ConnectionStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface ConnectionRepository extends JpaRepository<Connection, String> {
    boolean existsByRequestorIdAndReceiverId(String requestorId, String receiverId);

    List<Connection> findByRequestorIdAndStatus(String userId, ConnectionStatus connectionStatus);
}
