package com.linkin.userservice.controller;

import com.linkin.userservice.dto.UserResponse;
import com.linkin.userservice.entity.Connection;
import com.linkin.userservice.service.UserService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@RestController
@RequestMapping("/api/v1/users")
@Slf4j
@RequiredArgsConstructor
public class UserController {
    private final UserService userService;

    /**
     * Get user profile
     * X-user-Id = requesting user (from gateway)
     * userId in path = target user to fetch
     * @return
     */
    @GetMapping("/{userId}")
    public ResponseEntity<UserResponse> getUserProfile(
        @PathVariable String userId,
        @RequestHeader("X-user-Id") String requestingUserId
    ){
        log.info("Get profile: {} requesting by: {}", userId, requestingUserId);
        return ResponseEntity.ok(
                userService.getUserProfile(userId)
        );
    }

    /**
     * Update own profile
     * user can only update their own profle
     * @param userId
     * @param requestingUserId
     * @return
     */
    @PutMapping("/{userId}/profile")
    public ResponseEntity<UserResponse> updateProfile(
        @PathVariable String userId,
        @RequestBody UserResponse request,
        @RequestHeader("X-user-Id") String requestingUserId
    ){
        if(!userId.equals(requestingUserId)){
            return ResponseEntity.status(403).build();
        }
        return ResponseEntity.ok(
                userService.updateProfile(userId, request)
        );
    }

    @PostMapping("/{userId}/profile-photo")
    public ResponseEntity<UserResponse> uploadProfilePhoto(
            @PathVariable String userId,
            @RequestHeader("X-user-Id") String requestingUserId,
            @RequestParam("file")MultipartFile file
            ){
        if(!userId.equals(requestingUserId)){
            return ResponseEntity.status(403).build();
        }
        return ResponseEntity.ok(
                userService.uploadProfilePhoto(userId, file)
        );
    }

    /**
     * Send connection request
     * Requester Id come from X-user-Id header - already validated
     * @param targetUserId
     * @param requestingUserId
     * @return
     */
    @PostMapping("/{targetUserId}/connect")
    public ResponseEntity<String> sendConnectionRequest(
            @PathVariable String targetUserId,
            @RequestHeader("X-user-Id") String requestingUserId
    ){
        return ResponseEntity.ok(
                userService.sendConnectionRequest(targetUserId, requestingUserId)
        );
    }

    @GetMapping("/{userId}/connections/pending")
    public ResponseEntity<List<Connection>> getPendingConnection(
            @PathVariable String userId
    ){
        return ResponseEntity.ok(userService.getPendingConnections(userId));
    }

    @PostMapping("/{connectionId}/accept")
    public ResponseEntity<String> acceptConnection(
        @PathVariable String connectionId,
        @RequestHeader("X-user-Id") String requestingUserId
    ){
        return ResponseEntity.ok(
                userService.acceptConnectionRequest(connectionId, requestingUserId)
        );
    }

    @GetMapping("/{userId}/connections")
    public ResponseEntity<List<UserResponse>> getConnections(
            @PathVariable String userId
    ){
        return ResponseEntity.ok(
                userService.getConnections(userId)
        );
    }
}
