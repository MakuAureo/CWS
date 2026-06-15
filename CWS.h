#ifndef CWS_H
#define CWS_H

#include <stdint.h>
#include <sys/types.h>

typedef struct CWS_Server CWS_Server_t;

CWS_Server_t * cws_server_start(const uint16_t port);
int8_t cws_server_close(CWS_Server_t * server);
int8_t cws_server_loop(CWS_Server_t * server);
int8_t cws_server_add_valid_path(CWS_Server_t * server, const char * path,
    void(*onConnect)(const uint64_t connection_id),
    void(*onDisconnect)(const uint64_t connection_id),
    size_t (*onMessage)(const uint64_t connection_id, const char * message, const size_t message_size, char * reply));

size_t cws_send_broadcast(char * message);
size_t cws_send_message_to(const uint64_t receiver_id, char * message);

#endif // CWS_H

#ifdef CWS_IMPL

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>

#define CWS_TEXT_BLUE   "\x1b[38;5;39m"
#define CWS_TEXT_YELLOW "\x1b[38;5;220m"
#define CWS_TEXT_RED    "\x1b[38;5;196m"
#define CWS_TEXT_RESET  "\x1b[39m"

#ifdef CWS_LOCAL
#define CWS_ADDR htonl(INADDR_LOOPBACK)
#else
#define CWS_ADDR INADDR_ANY
#endif

#ifndef CWS_SERVER_MAX_EVENTS_PER_LOOP
#define CWS_SERVER_MAX_EVENTS_PER_LOOP 32
#endif

#ifndef CWS_WORKER_MAX
#define CWS_WORKER_MAX sysconf(_SC_NPROCESSORS_ONLN)
#endif

struct CWS_Socket {
  uint32_t fd;
  uint32_t opts;
  struct sockaddr_in address;
};

struct CWS_EventPoll {
  uint32_t fd;
  uint32_t opts;
};

struct CWS_Callback {
  void(*onConnect)(const uint64_t connection_id);
  void(*onDisconnect)(const uint64_t connection_id);
  size_t (*onMessage)(const uint64_t connection_id, const char * message, const size_t message_size, char * reply);
};

struct CWS_Worker {
  enum CWS_WorkerState {
    CWS_WORKER_IDLE,
    CWS_WORKER_RUNING,
    CWS_WORKER_STATES
  } state;
  pthread_t thread_id;
  struct CWS_EventPoll epoll;
};

struct CWS_Connection {
  enum CWS_ConnectionState {
    CWS_CONNECTION_CLOSED,
    CWS_CONNECTION_ESTABLISHED,
    CWS_CONNECTION_STATES
  } state;
  uint64_t id;
  struct CWS_Socket peer_socket;
  struct CWS_Callback * callbacks;
};

struct CWS_Server {
  enum CWS_ServerState {
    CWS_SERVER_CLOSED,
    CWS_SERVER_STARTED,
    CWS_SERVER_RUNNING,
    CWS_SERVER_CLOSING,
    CWS_SERVER_STATES
  } state;
  struct CWS_Socket socket;
  struct CWS_EventPoll epoll;
  struct CWS_Connection * connections;
};

CWS_Server_t * cws_server_start(const uint16_t port) {
  CWS_Server_t * server = (CWS_Server_t *)calloc(1, sizeof(CWS_Server_t));
  if (server == NULL) {
    printf("%sError allocating server memory: %s%s\n", CWS_TEXT_RED, strerror(errno), CWS_TEXT_RESET);
    goto fail_alloc;
  }
  
  int sock = 0;
  if ((sock = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0)) == -1) {
    printf("%sError creating socket endpoint for server: %s%s\n", CWS_TEXT_RED, strerror(errno), CWS_TEXT_RESET);
    goto fail_sock;
  }
  server->socket.fd = sock;

  int sockopts = 0;
  if (setsockopt(server->socket.fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &sockopts, sizeof(int)) == -1) {
    printf("%sError configuring socket: %s%s\n", CWS_TEXT_RED, strerror(errno), CWS_TEXT_RESET);
    goto fail_sockopts;
  }
  server->socket.opts = sockopts;

  int epoll;
  if ((epoll = epoll_create1(0)) == -1) {
    printf("%sError creating socket event poll: %s%s\n", CWS_TEXT_RED, strerror(errno), CWS_TEXT_RESET);
    goto fail_epoll;
  }
  server->epoll.fd = epoll;

  struct epoll_event events = { .data.fd = sock, .events = EPOLLIN };
  if (epoll_ctl(epoll, EPOLL_CTL_ADD, sock, &events) == -1) {
    printf("%sError configuring socket event poll: %s%s\n", CWS_TEXT_RED, strerror(errno), CWS_TEXT_RESET);
    goto fail_epollctl;
  }

  socklen_t addrLen = sizeof(struct sockaddr_in);
  server->socket.address.sin_family = AF_INET;
  server->socket.address.sin_addr.s_addr = CWS_ADDR;
  server->socket.address.sin_port = htons(port);
  if (bind(sock, (struct sockaddr *)&(server->socket.address), addrLen) == -1) {
    printf("%sError binding socket: %s%s\n", CWS_TEXT_RED, strerror(errno), CWS_TEXT_RESET);
    goto fail_bind;
  }

  if (listen(sock, CWS_SERVER_MAX_EVENTS_PER_LOOP) == -1) {
    printf("%sError making socket a listener: %s%s\n", CWS_TEXT_RED, strerror(errno), CWS_TEXT_RESET);
    goto fail_listen;
  }

  printf("%sServer correctly setup%s\n", CWS_TEXT_BLUE, CWS_TEXT_RESET);
  server->state = CWS_SERVER_STARTED;
  return server;

fail_listen:
fail_bind:
fail_epollctl:
  close(epoll);
fail_epoll:
fail_sockopts:
  shutdown(sock, SHUT_RDWR);
  close(sock);
fail_sock:
  free(server);
  server = NULL;
fail_alloc:
  return NULL;
}

static struct CWS_Connection * handle_new_connection(CWS_Server_t * server);

int8_t cws_server_loop(CWS_Server_t * server) {
  struct epoll_event event_vec[CWS_SERVER_MAX_EVENTS_PER_LOOP];
  while (server->state == CWS_SERVER_RUNNING) {
    int32_t events = epoll_wait(server->epoll.fd, event_vec, CWS_SERVER_MAX_EVENTS_PER_LOOP, -1);
    if (events == -1) {
      printf("%sError while waiting for new connecitons: %s%s\n", CWS_TEXT_RED, strerror(errno), CWS_TEXT_RESET);
      goto fail_epoll_wait;
    }
    for (int32_t i = 0; i < events; i++) {
      struct CWS_Connection * new_conn = handle_new_connection(server);
      if (new_conn == NULL) continue;
      new_conn->callbacks->onConnect(new_conn->id);
    }
  }

  return 0;

fail_epoll_wait:
  return -1;
}

#endif
