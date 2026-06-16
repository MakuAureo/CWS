#ifndef CWS_H
#define CWS_H

#define _GNU_SOURCE
#include <stdint.h>
#include <sys/types.h>

typedef struct CWS_Server CWS_Server_t;
typedef enum CWS_ServerResult CWS_ServerResult_t;

enum CWS_ServerResult {
  CWS_SERVER_NOT_READY,
  CWS_SERVER_CLEAN_INTERRUPTED,
  CWS_SERVER_ERROR
};

int8_t cws_server_init(CWS_Server_t * server, const uint16_t port);
int8_t cws_server_close(CWS_Server_t * server);
CWS_ServerResult_t cws_server_run(CWS_Server_t * server);
int8_t cws_server_add_valid_path(CWS_Server_t * server, const char * path,
    void(*onConnect)(const uint64_t connection_id),
    void(*onDisconnect)(const uint64_t connection_id),
    size_t (*onMessage)(const uint64_t connection_id, const char * message, const size_t message_size, char * reply));

size_t cws_send_broadcast(char * message);
size_t cws_send_message_to(const uint64_t receiver_id, char * message);

#endif // CWS_H

#define CWS_IMPL
#ifdef CWS_IMPL

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>      

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
  uint32_t load;
};

struct CWS_Connection {
  enum CWS_ConnectionState {
    CWS_CONNECTION_CLOSED,
    CWS_CONNECTION_ACCEPTED,
    CWS_CONNECTION_ESTABLISHED,
    CWS_CONNECTION_STATES
  } state;
  uint64_t id;
  struct CWS_Socket peer_socket;
  struct CWS_Callback * callbacks;
};

struct CWS_ConnectionNode {
  struct CWS_Connection connection;
  struct CWS_ConnectionNode * prev;
  struct CWS_ConnectionNode * next;
};

struct CWS_ConnectionList {
  struct CWS_ConnectionNode * root;
  struct CWS_ConnectionNode * last;
  uint64_t size;
};

struct CWS_Server {
  enum CWS_ServerState {
    CWS_SERVER_CLOSED,
    CWS_SERVER_STARTED,
    CWS_SERVER_RUNNING,
    CWS_SERVER_STOPED,
    CWS_SERVER_STATES
  } state;
  struct CWS_Socket socket;
  struct CWS_EventPoll epoll;
  struct CWS_Worker * threads;
  struct CWS_ConnectionList connections;
};

int8_t cws_server_init(CWS_Server_t * server, const uint16_t port)
{
  if (server == NULL) {
    printf(CWS_TEXT_RED "Server pointer is null" CWS_TEXT_RESET "\n");
    goto fail_alloc;
  }
  
  int sock = sock = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
  if (sock == -1) {
    printf(CWS_TEXT_RED "Error creating socket endpoint for server: %s" CWS_TEXT_RESET "\n", strerror(errno));
    goto fail_sock;
  }
  server->socket.fd = sock;

  int sockopts = setsockopt(server->socket.fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &sockopts, sizeof(int));
  if (sockopts == -1) {
    printf(CWS_TEXT_RED "Error configuring socket: %s" CWS_TEXT_RESET "\n", strerror(errno));
    goto fail_sockopts;
  }
  server->socket.opts = sockopts;

  int epoll = epoll_create1(0);
  if (epoll == -1) {
    printf(CWS_TEXT_RED "Error creating socket event poll: %s" CWS_TEXT_RESET "\n", strerror(errno));
    goto fail_epoll;
  }
  server->epoll.fd = epoll;

  struct epoll_event events = { .data.fd = sock, .events = EPOLLIN };
  if (epoll_ctl(epoll, EPOLL_CTL_ADD, sock, &events) == -1) {
    printf(CWS_TEXT_RED "Error configuring socket event poll: %s" CWS_TEXT_RESET "\n", strerror(errno));
    goto fail_epollctl;
  }

  socklen_t addrLen = sizeof(struct sockaddr_in);
  server->socket.address.sin_family = AF_INET;
  server->socket.address.sin_addr.s_addr = CWS_ADDR;
  server->socket.address.sin_port = htons(port);
  if (bind(sock, (struct sockaddr *)&(server->socket.address), addrLen) == -1) {
    printf(CWS_TEXT_RED "Error binding socket: %s" CWS_TEXT_RESET "\n", strerror(errno));
    goto fail_bind;
  }

  if (listen(sock, CWS_SERVER_MAX_EVENTS_PER_LOOP) == -1) {
    printf(CWS_TEXT_RED "Error making socket a listener: %s" CWS_TEXT_RESET "\n", strerror(errno));
    goto fail_listen;
  }

  server->connections.root = calloc(1, sizeof(struct CWS_ConnectionNode));
  if (server->connections.root == NULL) {
    printf(CWS_TEXT_RED "Error allocating root of connection list" CWS_TEXT_RESET "\n");
    goto fail_root_alloc;
  }

  server->connections.last = calloc(1, sizeof(struct CWS_ConnectionNode));
  if (server->connections.last == NULL) {
    printf(CWS_TEXT_RED "Error allocating last of connection list" CWS_TEXT_RESET "\n");
    goto fail_last_alloc;
  }

  server->connections.root->prev = NULL;
  server->connections.root->next = server->connections.last;
  server->connections.last->prev = server->connections.root;
  server->connections.last->next = NULL;

  printf(CWS_TEXT_BLUE "Server correctly initialized" CWS_TEXT_RESET "\n");
  server->state = CWS_SERVER_STARTED;
  return 0;

fail_last_alloc:
  free(server->connections.root);
fail_root_alloc:
fail_listen:
fail_bind:
fail_epollctl:
  close(epoll);
fail_epoll:
fail_sockopts:
  shutdown(sock, SHUT_RDWR);
  close(sock);
fail_sock:
fail_alloc:
  return -1;
}

static volatile sig_atomic_t g_sigint = 0;
static void handle_sigint(int sig) {
  if (g_sigint == 0) {
    const char SIGINTWARN[] = CWS_TEXT_YELLOW "SIGINT detected, trying to stop the server cleanly, try signaling SIGINT again if this hangs" CWS_TEXT_RESET "\n";
    write(STDIN_FILENO, SIGINTWARN, sizeof(SIGINTWARN));
    g_sigint = 1;
  } else {
    const char SIGINTFORCE[] = CWS_TEXT_RED "Forcefully shuting down the server" CWS_TEXT_RESET "\n";
    write(STDIN_FILENO, SIGINTFORCE, sizeof(SIGINTFORCE));
    exit(1);
  }
}

static void *sigint_listener_job(void* args) {
  pause();
  CWS_Server_t * server = args;
  write(server->epoll.fd, "", 1);
  return NULL;
}

static void *worker_job(void* args);

static inline int8_t spawn_worker_threads(CWS_Server_t * server)
{
  struct CWS_Worker * threads = calloc(CWS_WORKER_MAX, sizeof(struct CWS_Worker));
  if (threads == NULL) {
    printf(CWS_TEXT_BLUE "Error allocating threads memory: %s" CWS_TEXT_RESET "\n", strerror(errno));
    goto fail_alloc;
  }

  for (size_t i = 0; i < CWS_WORKER_MAX; i++) {
    int epoll = epoll_create1(0);
    if (epoll == -1) {
      for (size_t j = 0; j < i; j++)
        close(threads[j].epoll.fd);
      printf(CWS_TEXT_BLUE "Error creating event poll for thread #%zu: %s" CWS_TEXT_RESET "\n", i, strerror(errno));
      goto fail_epoll;
    }
    threads[i].epoll.fd = epoll;
  }

  for (size_t i = 0; i < CWS_WORKER_MAX; i++) {
    pthread_t id;
    if (pthread_create(&id, NULL, worker_job, threads + i) == -1) {
      for (size_t j = 0; j < i; j++)
        pthread_cancel(threads[j].thread_id);
      printf(CWS_TEXT_BLUE "Error spawning thread #%zu: %s" CWS_TEXT_RESET "\n", i, strerror(errno));
      goto fail_thread;
    }
    threads[i].thread_id = id;
    threads[i].state = CWS_WORKER_RUNING;
  }

  server->threads = threads;
  return 0;

fail_thread:
  for (size_t j = 0; j < CWS_WORKER_MAX; j++)
    close(threads[j].epoll.fd);
fail_epoll:
  free(threads);
  threads = NULL;
fail_alloc:
  return -1;
}

static inline void stop_worker_threads_clean(CWS_Server_t * server);
static inline void stop_worker_threads_force(CWS_Server_t * server);

static inline size_t find_least_loaded_worker(CWS_Server_t * server)
{
  uint32_t min_load = UINT32_MAX;
  size_t min_loaded = 0;
  for (size_t i = 0; i < CWS_WORKER_MAX; i++) {
    if (server->threads[i].load < min_load) {
      min_load = server->threads[i].load;
      min_loaded = i;
    }
  }

  return min_loaded;
}

static inline int8_t handle_new_connection(CWS_Server_t * server, struct CWS_ConnectionNode * connection)
{
  socklen_t addrLen = sizeof(struct sockaddr_in);
  struct sockaddr_in addr;
  int conn = accept4(server->socket.fd, &addr, &addrLen, SOCK_NONBLOCK);
  if (conn == -1) {
    switch (errno) {
      case EAGAIN:
      case ENETDOWN:
      case EPROTO:
      case ENOPROTOOPT:
      case EHOSTDOWN:
      case ENONET:
      case EHOSTUNREACH:
      case EOPNOTSUPP:
      case ENETUNREACH:
        return -2;
    }
    printf(CWS_TEXT_RED "Unexpected error accepting new connection: %s" CWS_TEXT_RESET, strerror(errno));
    goto fail_accept;
  }

  struct epoll_event event = {
    .data.ptr = connection,
    .events = EPOLLET
  };
  size_t next_thread = find_least_loaded_worker(server);
  if (epoll_ctl(server->threads[next_thread].epoll.fd, EPOLL_CTL_ADD, conn, &event) == -1) {
    printf(CWS_TEXT_RED "Error adding new connection to thread#%zu's event poll: %s" CWS_TEXT_RESET "\n", next_thread, strerror(errno));
    goto fail_epoll_add;
  }

  connection->next = server->connections.last;
  connection->prev = server->connections.last->prev;
  server->connections.last->prev->next = connection;
  server->connections.last->prev = connection;

  server->threads[next_thread].load++;
  server->connections.size++;

  connection->connection.peer_socket.fd = conn;
  connection->connection.peer_socket.address = addr;
  connection->connection.id = (uint64_t)connection;

  char addr_str[INET_ADDRSTRLEN];
  inet_ntop(AF_INET, &(addr.sin_addr), addr_str, INET_ADDRSTRLEN);
  printf(CWS_TEXT_BLUE "New connection from %s accepted to thread #%zu" CWS_TEXT_RESET "\n", addr_str, next_thread);
  
  connection->connection.state = CWS_CONNECTION_ACCEPTED;
  return 0;

fail_epoll_add:
fail_accept:
  return -1;
}

CWS_ServerResult_t cws_server_run(CWS_Server_t * server)
{
  if (server->state != CWS_SERVER_STARTED)
    return CWS_SERVER_NOT_READY;

  struct sigaction sigint_action = {
    .sa_handler = handle_sigint
  };
  struct sigaction sigint_old;
  sigaction(SIGINT, &sigint_action, &sigint_old);
  pthread_t sigint_listener;
  pthread_create(&sigint_listener, NULL, sigint_listener_job, server);

  sigset_t sigset;
  sigemptyset(&sigset);
  sigaddset(&sigset, SIGINT);
  pthread_sigmask(SIG_BLOCK, &sigset, NULL);
  if (spawn_worker_threads(server) == -1)
    goto fail_spawn_threads;

  server->state = CWS_SERVER_RUNNING;
  struct epoll_event event_vec[CWS_SERVER_MAX_EVENTS_PER_LOOP];
  while (server->state == CWS_SERVER_RUNNING) {
    int32_t events = epoll_wait(server->epoll.fd, event_vec, CWS_SERVER_MAX_EVENTS_PER_LOOP, -1);
    if (g_sigint == 1)
      goto stop;
    if (events == -1) {
      printf(CWS_TEXT_BLUE "Error while waiting for new connecitons: %s" CWS_TEXT_RESET "\n", strerror(errno));
      goto fail_epoll_wait;
    }
    for (int32_t i = 0; i < events; i++) {
      struct CWS_ConnectionNode * new_conn = calloc(1, sizeof(struct CWS_ConnectionNode));
      if (new_conn == NULL) {
        printf(CWS_TEXT_BLUE "Error allocating memory for new connection: %s" CWS_TEXT_RESET "\n", strerror(errno));
        goto fail_connection_alloc;
      }
      if (handle_new_connection(server, new_conn) < 0) {
        free(new_conn);
        continue;
      }
    }
  }

stop:
  stop_worker_threads_clean(server);
  sigaction(SIGINT, &sigint_old, NULL);
  server->state = CWS_SERVER_STOPED;
  return CWS_SERVER_CLEAN_INTERRUPTED;

fail_connection_alloc:
fail_epoll_wait:
  stop_worker_threads_force(server);
fail_spawn_threads:
  pthread_sigmask(SIG_UNBLOCK, &sigset, NULL);
  sigaction(SIGINT, &sigint_old, NULL);
  server->state = CWS_SERVER_STOPED;
  return CWS_SERVER_ERROR;
}

#endif
