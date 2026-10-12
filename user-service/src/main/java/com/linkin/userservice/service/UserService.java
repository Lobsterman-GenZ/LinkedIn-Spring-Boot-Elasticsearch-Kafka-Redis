package com.linkin.userservice.service;

import com.linkin.userservice.Repository.ConnectionRepository;
import com.linkin.userservice.Repository.UserRepository;
import com.linkin.userservice.dto.UserResponse;
import com.linkin.userservice.entity.Connection;
import com.linkin.userservice.entity.ConnectionStatus;
import com.linkin.userservice.entity.User;
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
    private final UserRepository userRepository;
    private final ConnectionRepository connectionRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final String CONNECTION_REQUESTED_TOPIC = "connection.requested";
    private static final String CONNECTION_ACCEPTED_TOPIC = "connection.accepted";
    private static final String USER_UPDATED_TOPIC = "user.updated";
    private final S3Service s3Service;


    public UserResponse getUserProfile(String userId) {
        User user = userRepository.findById(userId).orElseThrow(
                ()-> new RuntimeException("User not found: " + userId)
        );
        return mapToResponse(user);
    }

    public UserResponse updateProfile(String userId, UserResponse request) {
        User user = userRepository.findById(userId).orElseThrow(
                ()-> new RuntimeException("User not found: " + userId)
        );
        user.setHeadline(request.getHeadline());
        user.setAbout(request.getAbout());
        user.setLocation(request.getLocation());
        user.setSkills(request.getSkills());

        User savedUser = userRepository.save(user);

        Map<String, Object> userUpdatedEvent = new HashMap<>();
        userUpdatedEvent.put("userId", savedUser.getId());
        userUpdatedEvent.put("firstName", savedUser.getFirstName());
        userUpdatedEvent.put("lastName", savedUser.getLastName());
        userUpdatedEvent.put("headline", savedUser.getHeadline());
        userUpdatedEvent.put("location", savedUser.getLocation());
        userUpdatedEvent.put("skills", savedUser.getSkills());

        kafkaTemplate.send(USER_UPDATED_TOPIC, savedUser.getId(), userUpdatedEvent);
        log.info("user.updated event published: {}", savedUser.getId());

        return mapToResponse(savedUser);
    }

    public UserResponse uploadProfilePhoto(String userId, MultipartFile file) {
        User user = userRepository.findById(userId).orElseThrow(
                ()-> new RuntimeException("User not found: " + userId)
        );
        String photoUrl = s3Service.uploadFile(
                file,
                "profiles/" + userId + "/avatar"
        );
        user.setProfilePhotoUrl(photoUrl);
        User savedUser = userRepository.save(user);
        log.info("Profile photo uploaded for user: {}", userId);
        return mapToResponse(savedUser);
    }

    public String sendConnectionRequest(String receiverId, String requesterId) {
        if (connectionRepository.existsByRequesterIdAndReceiverId(
                requesterId,
                receiverId
        )){
            throw new RuntimeException("Connection request already send");
        }
        Connection connection = new Connection();
        connection.setRequesterId(requesterId);
        connection.setReceiverId(receiverId);
        connection.setStatus(ConnectionStatus.PENDING);
        connectionRepository.save(connection);

        Map<String, Object> connectionRequestedEvent = new HashMap<>();
        connectionRequestedEvent.put("requesterId", requesterId);
        connectionRequestedEvent.put("receiverId", receiverId);
        kafkaTemplate.send(CONNECTION_REQUESTED_TOPIC, requesterId, connectionRequestedEvent);
        log.info("Connection request send: {} -> {}", requesterId, receiverId);

        return "Connection request send";
    }


    public String acceptConnectionRequest(String connectionId, String requestingUserId) {
        Connection connection = connectionRepository.findById(connectionId)
                .orElseThrow(()-> new RuntimeException("Connection not found: " + connectionId));

        connection.setStatus(ConnectionStatus.CONNECTED);
        connectionRepository.save(connection);
        Map<String, Object> connectionAcceptedEvent = new HashMap<>();
        connectionAcceptedEvent.put("requesterId", connection.getRequesterId());
        connectionAcceptedEvent.put("receiverId", connection.getReceiverId());
        kafkaTemplate.send(CONNECTION_ACCEPTED_TOPIC, connection.getRequesterId(), connectionAcceptedEvent);
        log.info("Connection accepted: {}", connection.getRequesterId());
        return "Connection accepted";
    }

    public List<UserResponse> getConnections(String userId) {
        List<Connection> connections = connectionRepository.findByRequesterIdAndStatus(userId, ConnectionStatus.CONNECTED);

        return connections.stream()
                .map(c-> getUserProfile(c.getReceiverId()))
                .collect(Collectors.toList());
    }

    private UserResponse mapToResponse(User user) {
        UserResponse userResponse = new UserResponse();

        userResponse.setId(user.getId());
        userResponse.setEmail(user.getEmail());
        userResponse.setFirstName(user.getFirstName());
        userResponse.setLastName(user.getLastName());

        userResponse.setHeadline(user.getHeadline());
        userResponse.setAbout(user.getAbout());
        userResponse.setLocation(user.getLocation());

        userResponse.setProfilePhotoUrl(user.getProfilePhotoUrl());
        userResponse.setCoverPhotoUrl(user.getCoverPhotoUrl());

        userResponse.setRole(user.getRole());
        userResponse.setSkills(user.getSkills());
        userResponse.setCreatedAt(user.getCreatedAt());

        return userResponse;
    }

    public List<Connection> getPendingConnections(String userId) {
        return connectionRepository.findByReceiverIdAndStatus(userId, ConnectionStatus.PENDING);
    }
}
