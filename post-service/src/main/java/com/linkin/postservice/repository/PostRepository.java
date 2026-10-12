package com.linkin.postservice.repository;

import com.linkin.postservice.entity.Post;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface PostRepository extends JpaRepository<Post, String> {
    List<Post> findByAuthorIdOrderByCreatedAtDesc(String userId);
}
