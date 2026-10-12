package com.linkin.postservice.repository;

import com.linkin.postservice.entity.Like;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface LikeRepository extends JpaRepository<Like, String> {
    boolean existsByPostIdAndUserId(String postId, String userId);
    Optional<Like> findByPostIdAndUserId(String postId, String userId);
}
