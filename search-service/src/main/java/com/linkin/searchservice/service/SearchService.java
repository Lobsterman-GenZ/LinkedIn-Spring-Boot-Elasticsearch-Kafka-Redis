package com.linkin.searchservice.service;

import com.linkin.searchservice.model.PostDocument;
import com.linkin.searchservice.model.UserDocument;
import com.linkin.searchservice.repository.PostSearchRepository;
import com.linkin.searchservice.repository.UserSearchRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class SearchService {

    private final UserSearchRepository userSearchRepository;
    private final PostSearchRepository postSearchRepository;

    public List<UserDocument> searchUsers(String query) {
        log.info("Search users: {}", query);
        return userSearchRepository.searchUsers(query);
    }

    public List<UserDocument> searchBySkill(String skill) {
        log.info("Search users by skill: {}", skill);
        return userSearchRepository.findBySkillsContaining(skill);
    }

    public List<PostDocument> searchPosts(String query) {
        log.info("Search posts: {}", query);
        return postSearchRepository.searchPosts(query);
    }
}
