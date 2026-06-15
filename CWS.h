#ifndef CWS_H
#define CWS_H

#include <stdint.h>
#include <sys/types.h>

struct CWS_Server;

struct CWS_Server * cws_server_start(const uint16_t port);
int8_t cws_server_close(struct CWS_Server * server);
int8_t cws_server_loop(struct CWS_Server * server);
int8_t cws_server_add_valid_path(struct CWS_Server * server, const char * path,
    void(*onConnect)(const uint64_t connection_id),
    void(*onDisconnect)(const uint64_t connection_id),
    size_t (*onMessage)(const uint64_t connection_id, const char * message, const size_t message_size, char * reply));

size_t cws_send_broadcast(char * message);
size_t cws_send_message_to(const uint64_t receiver_id, char * message);

#endif // CWS_H

#ifdef CWS_IMPL
#endif
