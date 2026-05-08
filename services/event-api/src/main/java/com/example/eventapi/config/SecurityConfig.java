package com.example.eventapi.config;

import com.example.eventapi.security.JwtAccessDeniedHandler;
import com.example.eventapi.security.JwtAuthenticationEntryPoint;
import com.example.eventapi.security.JwtAuthenticationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import java.util.List;

@Configuration
@EnableConfigurationProperties(AppProperties.class)
public class SecurityConfig {

    private static final Logger log = LoggerFactory.getLogger(SecurityConfig.class);

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http,
                                                   JwtAuthenticationFilter jwtFilter,
                                                   JwtAuthenticationEntryPoint entryPoint,
                                                   JwtAccessDeniedHandler deniedHandler) throws Exception {
        return http
                .csrf(csrf -> csrf.disable())
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        // Open endpoints
                        .requestMatchers(HttpMethod.POST, "/events").permitAll()
                        .requestMatchers(HttpMethod.POST, "/auth/login").permitAll()
                        // Role-protected
                        .requestMatchers(HttpMethod.DELETE, "/events/**").hasRole("ADMIN")
                        .requestMatchers(HttpMethod.GET, "/events", "/events/**").hasAnyRole("USER", "ADMIN")
                        .requestMatchers(HttpMethod.GET, "/stats", "/stats/**").hasAnyRole("USER", "ADMIN")
                        // Default deny
                        .anyRequest().authenticated())
                .exceptionHandling(eh -> eh
                        .authenticationEntryPoint(entryPoint)
                        .accessDeniedHandler(deniedHandler))
                .addFilterBefore(jwtFilter, UsernamePasswordAuthenticationFilter.class)
                .build();
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    /**
     * Seeds an in-memory user store from {@link AppProperties}. Passwords from configuration
     * are hashed with BCrypt at startup; the plaintext value never leaves this method.
     */
    @Bean
    public UserDetailsService userDetailsService(AppProperties properties, PasswordEncoder encoder) {
        List<AppProperties.User> users = properties.users();
        if (users == null || users.isEmpty()) {
            throw new IllegalStateException("app.users must contain at least one user");
        }
        InMemoryUserDetailsManager manager = new InMemoryUserDetailsManager();
        for (AppProperties.User u : users) {
            manager.createUser(User.withUsername(u.username())
                    .password(encoder.encode(u.password()))
                    .roles(u.role())
                    .build());
            log.info("Seeded user '{}' with role {}", u.username(), u.role());
        }
        return manager;
    }

    @Bean
    public AuthenticationManager authenticationManager(UserDetailsService uds, PasswordEncoder encoder) {
        DaoAuthenticationProvider provider = new DaoAuthenticationProvider();
        provider.setUserDetailsService(uds);
        provider.setPasswordEncoder(encoder);
        return new ProviderManager(provider);
    }
}
