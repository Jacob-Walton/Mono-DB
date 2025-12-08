#include "value.h"
#include "protocol.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <time.h>

int main()
{
    // Open a socket to 127.0.0.1:7899
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(7899);
    server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    if (connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect");
        close(sock);
        return 1;
    }

    // Create a Request object
    Request *req = request_new_connect(1, NULL);
    if (!req) {
        fprintf(stderr, "Failed to create request\n");
        close(sock);
        return 1;
    }

    // Serialize the Request object
    uint64_t length = 0;
    uint8_t *encoded = request_encode(req, &length);
    if (!encoded) {
        fprintf(stderr, "Failed to encode request\n");
        free(req);
        close(sock);
        return 1;
    }

    // Print hex dump of sent data for debugging
    printf("Sending %lu bytes (hex): ", length);
    for (uint64_t i = 0; i < length && i < 64; i++) {
        printf("%02x ", encoded[i]);
    }
    printf("\n");

    // Send the serialized data over the socket
    ssize_t sent = send(sock, encoded, length, 0);
    if (sent < 0) {
        perror("send");
        free(encoded);
        free(req);
        close(sock);
        return 1;
    }
    printf("Sent %zd bytes\n", sent);

    // Add timeout to avoid hanging forever
    struct timeval tv;
    tv.tv_sec = 5;  // 5 second timeout
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);

    // Wait for a response
    printf("Waiting for response...\n");
    uint8_t recv_buffer[1024];
    
    // Try reading the frame length first
    ssize_t received = recv(sock, recv_buffer, 4, MSG_WAITALL);
    if (received < 0) {
        perror("recv frame length");
        free(encoded);
        request_free(req);
        close(sock);
        return 1;
    }
    if (received == 0) {
        fprintf(stderr, "Connection closed by server (no frame length)\n");
        free(encoded);
        request_free(req);
        close(sock);
        return 1;
    }
    printf("Received frame length: %zd bytes\n", received);
    
    // Read frame length
    uint32_t response_len = recv_buffer[0] | (recv_buffer[1] << 8) | (recv_buffer[2] << 16) | (recv_buffer[3] << 24);
    printf("Response frame length: %u bytes\n", response_len);
    
    // Now read the rest
    received = recv(sock, recv_buffer + 4, response_len, MSG_WAITALL);
    if (received < 0) {
        perror("recv frame body");
        free(encoded);
        request_free(req);
        close(sock);
        return 1;
    }
    if (received == 0) {
        fprintf(stderr, "Connection closed by server (no frame body)\n");
        free(encoded);
        request_free(req);
        close(sock);
        return 1;
    }
    printf("Received frame body: %zd bytes\n", received);
    
    ssize_t total_received = 4 + received;
    printf("Total received %zd bytes\n", total_received);

    // Print hex dump of received data for debugging
    printf("Received data (hex): ");
    for (ssize_t i = 0; i < total_received && i < 64; i++) {
        printf("%02x ", recv_buffer[i]);
    }
    printf("\n");

    // Deserialize the response
    Response *response = response_decode(recv_buffer, total_received);
    if (!response) {
        fprintf(stderr, "Failed to decode response\n");
        free(encoded);
        request_free(req);
        close(sock);
        return 1;
    }
    printf("Decoded response with tag: %d\n", response->tag);

    if (response->tag == RESPONSE_CONNECT_ACK) {
        printf("Connection acknowledged by server.\n");
        
        printf("Protocol Version: %u\n", response->body.connect_ack.protocol_version);
        if (response->body.connect_ack.server_timestamp.is_some) {
            // Convert timestamp to human-readable format
            uint64_t ts = response->body.connect_ack.server_timestamp.value;
            time_t t = (time_t)ts;
            struct tm *tm_info = localtime(&t);
            char buffer[26];
            strftime(buffer, 26, "%Y-%m-%d %H:%M:%S", tm_info);
            printf("Server Timestamp: %s\n", buffer);
        } else {
            printf("Server Timestamp: None\n");
        }

        if (response->body.connect_ack.user_permissions.is_some) {
            printf("User Permissions (%lu):\n", response->body.connect_ack.user_permissions.value.len);
            for (uint64_t i = 0; i < response->body.connect_ack.user_permissions.value.len; i++) {
                char *perm = response->body.connect_ack.user_permissions.value.ptr[i];
                printf("  - %s\n", perm);
            }
        } else {
            printf("User Permissions: None\n");
        }
    } else {
        printf("Received unexpected response tag: %d\n", response->tag);
    }

    // Clean up
    free(encoded);
    request_free(req);
    response_free(response);

    // Close the socket
    close(sock);
    
    return 0;
}