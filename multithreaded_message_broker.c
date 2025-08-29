#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <time.h>
#include <errno.h>
#include <ctype.h>
#include <alloca.h>
#include <limits.h>

// ------------------------------------------------------------
// Utilities: time, sleep, random
// ------------------------------------------------------------
static long long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

static void msleep(long ms){
    struct timespec ts; ts.tv_sec = ms/1000; ts.tv_nsec = (ms%1000)*1000000L;
    while (nanosleep(&ts, &ts) == -1 && errno == EINTR) { /* retry on interrupt */ }
}

static int rnd(int lo, int hi){ // inclusive range
    return lo + rand() % (hi - lo + 1);
}

// ------------------------------------------------------------
// Configuration flags (runtime via argv)
// ------------------------------------------------------------
typedef struct Config {
    int seconds;            // runtime seconds
    int producers;          // number of producer threads
    int consumers;          // number of consumer threads
    int topics;             // number of dynamic topics
    int bounded_enabled;    // --bounded
    int capacity;           // --capacity N (per-topic & per-subscriber queues)
    int retention_enabled;  // --retention
    long ttl_ms;            // --ttl ms (per message, default applied by producer; 0 = infinite)
    long gc_interval_ms;    // --gc-interval ms
    int fanout_enabled;     // --fanout (enables pub/sub at dispatcher stage)
} Config;

static void default_config(Config *c){
    c->seconds = 5;
    c->producers = 5;
    c->consumers = 5;
    c->topics = 3;
    c->bounded_enabled = 0;
    c->capacity = 0; // 0 => unbounded
    c->retention_enabled = 0;
    c->ttl_ms = 0; // 0 => no TTL by default
    c->gc_interval_ms = 800;
    c->fanout_enabled = 0;
}

static int parse_int(const char *s, int *out){
    char *end; long v = strtol(s, &end, 10);
    if (*s == '\0' || *end != '\0') return -1; *out = (int)v; return 0;
}
static int parse_long(const char *s, long *out){
    char *end; long v = strtol(s, &end, 10);
    if (*s == '\0' || *end != '\0') return -1; *out = v; return 0;
}

static void parse_args(Config *cfg, int argc, char **argv){
    for (int i=1;i<argc;i++){
        if (!strcmp(argv[i], "--bounded")) cfg->bounded_enabled = 1;
        else if (!strcmp(argv[i], "--retention")) cfg->retention_enabled = 1;
        else if (!strcmp(argv[i], "--fanout")) cfg->fanout_enabled = 1;
        else if (!strncmp(argv[i], "--seconds", 9)){
            const char *p = strchr(argv[i], '='); if (p) parse_int(p+1, &cfg->seconds);
            else if (i+1<argc) parse_int(argv[++i], &cfg->seconds);
        } else if (!strncmp(argv[i], "--producers", 11)){
            const char *p = strchr(argv[i], '='); if (p) parse_int(p+1, &cfg->producers);
            else if (i+1<argc) parse_int(argv[++i], &cfg->producers);
        } else if (!strncmp(argv[i], "--consumers", 11)){
            const char *p = strchr(argv[i], '='); if (p) parse_int(p+1, &cfg->consumers);
            else if (i+1<argc) parse_int(argv[++i], &cfg->consumers);
        } else if (!strncmp(argv[i], "--topics", 8)){
            const char *p = strchr(argv[i], '='); if (p) parse_int(p+1, &cfg->topics);
            else if (i+1<argc) parse_int(argv[++i], &cfg->topics);
        } else if (!strncmp(argv[i], "--capacity", 10)){
            const char *p = strchr(argv[i], '='); if (p) parse_int(p+1, &cfg->capacity);
            else if (i+1<argc) parse_int(argv[++i], &cfg->capacity);
        } else if (!strncmp(argv[i], "--ttl", 5)){
            const char *p = strchr(argv[i], '='); if (p) parse_long(p+1, &cfg->ttl_ms);
            else if (i+1<argc) parse_long(argv[++i], &cfg->ttl_ms);
        } else if (!strncmp(argv[i], "--gc-interval", 13)){
            const char *p = strchr(argv[i], '='); if (p) parse_long(p+1, &cfg->gc_interval_ms);
            else if (i+1<argc) parse_long(argv[++i], &cfg->gc_interval_ms);
        } else {
            fprintf(stderr, "Unknown arg: %s\n", argv[i]);
        }
    }
}

// ------------------------------------------------------------
// Message + Queue (linked list)
// ------------------------------------------------------------
typedef struct Message {
    int id;                 // unique id
    int priority;           // 0=high, 1=med, 2=low
    long est_ms;            // estimated processing time
    long long deadline_ms;  // absolute timestamp in ms; 0 => no deadline
    long ttl_ms;            // relative TTL in ms; 0 => infinite
    long long created_ms;   // fill at enqueue time
    long long assigned_ms;  // when dispatcher assigns to consumer
    char topic[32];         // source topic name for logging
    char payload[128];      // demonstration payload
} Message;

typedef struct Node { Message msg; struct Node *next; } Node;

// CHANGED: Priority queue with separate queues for each priority level
typedef struct Queue {
    Node *head[3];          // head for each priority (0=high, 1=med, 2=low)
    Node *tail[3];          // tail for each priority
    int size[3];            // size for each priority
    int total_size;         // total messages in queue
    int capacity;           // capacity: 0 => unbounded
} Queue;

static void q_init(Queue *q, int capacity){ 
    memset(q, 0, sizeof(*q)); 
    q->capacity = capacity; 
}

static void q_push_back_nolock(Queue *q, Message m){
    Node *nd = (Node*)malloc(sizeof(Node));
    nd->msg = m; 
    nd->next = NULL;
    
    int prio = m.priority % 3;
    
    if (!q->tail[prio]) {
        q->head[prio] = q->tail[prio] = nd;
    } else {
        q->tail[prio]->next = nd;
        q->tail[prio] = nd;
    }
    q->size[prio]++;
    q->total_size++;
}

static int q_pop_front_nolock(Queue *q, Message *out){
    // Check priorities in order: high (0), medium (1), low (2)
    for (int prio = 0; prio < 3; prio++) {
        if (q->head[prio] != NULL) {
            Node *nd = q->head[prio]; 
            *out = nd->msg; 
            q->head[prio] = nd->next; 
            if (!q->head[prio]) q->tail[prio] = NULL; 
            q->size[prio]--;
            q->total_size--;
            free(nd); 
            return 1;
        }
    }
    return 0;
}

static int q_remove_expired_nolock(Queue *q, long long now){
    int removed = 0;
    
    for (int prio = 0; prio < 3; prio++) {
        Node **pp = &q->head[prio];
        while (*pp){
            Node *cur = *pp;
            if (cur->msg.ttl_ms > 0 && cur->msg.created_ms + cur->msg.ttl_ms <= now){
                *pp = cur->next; 
                if (q->tail[prio] == cur) q->tail[prio] = NULL; 
                free(cur); 
                q->size[prio]--;
                q->total_size--;
                removed++;
            } else {
                pp = &cur->next;
            }
        }
        // Fix tail if necessary
        if (!q->head[prio]) {
            q->tail[prio] = NULL;
        } else if (!q->tail[prio]) {
            Node *t = q->head[prio]; 
            while (t->next) t = t->next; 
            q->tail[prio] = t;
        }
    }
    return removed;
}

// ------------------------------------------------------------
// Forward decls
// ------------------------------------------------------------
struct Consumer; struct Topic; struct Broker;

typedef struct Consumer Consumer;

typedef struct Topic {
    char name[32];
    Queue q;                    // main queue (producer -> topic)
    pthread_mutex_t mtx;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
    int fanout;                 // when 1, dispatcher fans out to all subs
    Consumer **subs; int sub_count; int sub_cap;
    int *producer_ids; int producer_count; int producer_cap;
    struct Broker *broker;      // back-pointer to broker
} Topic;

struct Consumer {
    int id; char name[32];
    Topic **topics; int topic_count;      // topics this consumer is registered for
    Queue personal_q;                      // dispatcher -> consumer (now priority queue)
    pthread_mutex_t c_mtx;
    pthread_cond_t c_not_empty;
    pthread_cond_t c_not_full;
    long long load_ms;                    // est work queued
    long long load_by_prio[3];            // CHANGED: load separated by priority
    atomic_long done_count;               // processed count
};

typedef struct Broker {
    Config cfg;
    Topic *topics;              // array of dynamic topics
    int topic_count;
    Consumer *consumers;        // array of consumers

    pthread_t *producer_threads;  // [producers]
    pthread_t *consumer_threads;  // [consumers]
    pthread_t *dispatcher_threads; // [topics] one per topic
    pthread_t gc_thread;
    pthread_t dashboard_thread;

    atomic_int stop;

    atomic_long prio_count[3];   // processed counters by priority
    atomic_long prio_total_ms[3];
    atomic_long prio_total_latency[3]; // total end-to-end latency by priority

    atomic_int rejected_count;   // total rejected
    atomic_int *rejected_by_topic_prio; // length = topic_count * 3

    atomic_int next_msg_id;      // id generator
    atomic_int next_topic_id;    // topic id generator
} Broker;

// ------------------------------------------------------------
// Topic helpers
// ------------------------------------------------------------
static void topic_init(Topic *t, const char *name, int capacity, int fanout, Broker *b){
    memset(t, 0, sizeof(*t));
    strncpy(t->name, name, sizeof(t->name)-1);
    q_init(&t->q, capacity);
    pthread_mutex_init(&t->mtx, NULL);
    pthread_cond_init(&t->not_empty, NULL);
    pthread_cond_init(&t->not_full, NULL);
    t->fanout = fanout;
    t->subs = NULL; t->sub_count = 0; t->sub_cap = 0;
    t->producer_ids = NULL; t->producer_count = 0; t->producer_cap = 0;
    t->broker = b;
}

static void topic_add_subscriber(Topic *t, Consumer *c){
    pthread_mutex_lock(&t->mtx);
    if (t->sub_count == t->sub_cap){
        int nc = t->sub_cap ? t->sub_cap*2 : 4;
        t->subs = (Consumer**)realloc(t->subs, nc * sizeof(Consumer*));
        t->sub_cap = nc;
    }
    t->subs[t->sub_count++] = c;
    pthread_mutex_unlock(&t->mtx);
}

static void topic_add_producer(Topic *t, int producer_id){
    pthread_mutex_lock(&t->mtx);
    if (t->producer_count == t->producer_cap){
        int nc = t->producer_cap ? t->producer_cap*2 : 4;
        t->producer_ids = (int*)realloc(t->producer_ids, nc * sizeof(int));
        t->producer_cap = nc;
    }
    t->producer_ids[t->producer_count++] = producer_id;
    pthread_mutex_unlock(&t->mtx);
}

static int topic_get_producer_count(Topic *t){
    pthread_mutex_lock(&t->mtx);
    int count = t->producer_count;
    pthread_mutex_unlock(&t->mtx);
    return count;
}

static int topic_get_subscriber_count(Topic *t){
    pthread_mutex_lock(&t->mtx);
    int count = t->sub_count;
    pthread_mutex_unlock(&t->mtx);
    return count;
}

static void topic_enqueue_workqueue(Topic *t, Message m){
    pthread_mutex_lock(&t->mtx);
    if (t->broker->cfg.bounded_enabled && t->q.capacity > 0){
        while (t->q.total_size >= t->q.capacity && !atomic_load(&t->broker->stop)) {
            pthread_cond_wait(&t->not_full, &t->mtx);
        }
    }
    if (atomic_load(&t->broker->stop)){
        pthread_mutex_unlock(&t->mtx); return;
    }
    m.created_ms = now_ms();
    q_push_back_nolock(&t->q, m);
    pthread_cond_signal(&t->not_empty);
    pthread_mutex_unlock(&t->mtx);
}

static void topic_publish(Topic *t, Message m){
    topic_enqueue_workqueue(t, m);
}

static int topic_pop(Topic *t, Message *out){
    pthread_mutex_lock(&t->mtx);
    while (t->q.total_size == 0 && !atomic_load(&t->broker->stop)){
        pthread_cond_wait(&t->not_empty, &t->mtx);
    }
    if (t->q.total_size == 0 && atomic_load(&t->broker->stop)){
        pthread_mutex_unlock(&t->mtx); return 0;
    }
    int ok = q_pop_front_nolock(&t->q, out);
    if (ok && t->broker->cfg.bounded_enabled && t->q.capacity > 0){
        pthread_cond_signal(&t->not_full);
    }
    pthread_mutex_unlock(&t->mtx);
    return ok;
}

static int consumer_pop_personal(Consumer *c, Message *out){
    pthread_mutex_lock(&c->c_mtx);
    while (c->personal_q.total_size == 0 && !atomic_load(&c->topics[0]->broker->stop)){
        pthread_cond_wait(&c->c_not_empty, &c->c_mtx);
    }
    if (c->personal_q.total_size == 0 && atomic_load(&c->topics[0]->broker->stop)){
        pthread_mutex_unlock(&c->c_mtx); return 0;
    }
    int ok = q_pop_front_nolock(&c->personal_q, out);
    if (ok && c->topics[0]->broker->cfg.bounded_enabled && c->personal_q.capacity > 0){
        pthread_cond_signal(&c->c_not_full);
    }
    pthread_mutex_unlock(&c->c_mtx);
    return ok;
}

// ------------------------------------------------------------
// Consumer thread
// ------------------------------------------------------------
typedef struct ConsumerArg { Broker *b; Consumer *c; } ConsumerArg;

static void *consumer_thread(void *arg){
    ConsumerArg *ca = (ConsumerArg*)arg; Broker *b = ca->b; Consumer *c = ca->c; free(ca);
    while (!atomic_load(&b->stop)){
        Message m;
        int ok = consumer_pop_personal(c, &m);
        if (ok) {
            long long start = now_ms();
            msleep((int)m.est_ms);
            long long end = now_ms();

            long actual = (long)(end - start);
            long long total_latency = end - m.created_ms;
            long long queue_latency = start - m.assigned_ms;
            long long system_latency = m.assigned_ms - m.created_ms;

            atomic_fetch_add(&b->prio_count[m.priority % 3], 1);
            atomic_fetch_add(&b->prio_total_ms[m.priority % 3], actual);
            atomic_fetch_add(&b->prio_total_latency[m.priority % 3], total_latency);

            atomic_fetch_add(&c->done_count, 1);
            
            // CHANGED: Update priority-specific load
            if (c->load_by_prio[m.priority] >= m.est_ms) 
                c->load_by_prio[m.priority] -= m.est_ms; 
            else 
                c->load_by_prio[m.priority] = 0;
                
            if (c->load_ms >= m.est_ms) 
                c->load_ms -= m.est_ms; 
            else 
                c->load_ms = 0;
                
            printf("[Consumer %s] done msg=%d prio=%d est=%ldms actual=%ldms total_latency=%lldms from topic=%s\n", 
                   c->name, m.id, m.priority, m.est_ms, actual, total_latency, m.topic);
        }
    }
    return NULL;
}

// ------------------------------------------------------------
// Producer thread
// ------------------------------------------------------------
typedef struct ProducerArg { Broker *b; int id; Topic **topics; int topic_count; } ProducerArg;

static void *producer_thread(void *arg){
    ProducerArg *pa = (ProducerArg*)arg; Broker *b = pa->b; int pid = pa->id; 
    Topic **my_topics = pa->topics; int my_topic_count = pa->topic_count;
    free(pa);

    while (!atomic_load(&b->stop)){
        Message m; memset(&m, 0, sizeof(m));
        m.id = atomic_fetch_add(&b->next_msg_id, 1);
        m.priority = rnd(0, 2);
        if (m.priority == 0) m.est_ms = rnd(100, 200);
        else if (m.priority == 1) m.est_ms = rnd(200, 300);
        else m.est_ms = rnd(300, 400);
        if (rnd(0,3) == 0) m.deadline_ms = now_ms() + rnd(200, 1200);
        m.ttl_ms = b->cfg.ttl_ms;
        snprintf(m.payload, sizeof(m.payload), "%s:%d says hi!", "producer", pid);

        Topic *chosen_topic = my_topics[0];
        strncpy(m.topic, chosen_topic->name, sizeof(m.topic)-1);
        topic_publish(chosen_topic, m);

        printf("[Producer %d] produced TASK id=%d prio=%d est=%ldms deadline=%lld ttl=%ld to topic=%s\n",
               pid, m.id, m.priority, m.est_ms, (long long)m.deadline_ms, m.ttl_ms, chosen_topic->name);

        msleep(rnd(40, 140));
    }
    return NULL;
}

// ------------------------------------------------------------
// Dispatcher thread (one per topic)
// ------------------------------------------------------------
typedef struct DispatcherArg { Broker *b; int topic_index; } DispatcherArg;

static void *dispatcher_thread(void *arg){
    DispatcherArg *da = (DispatcherArg*)arg; Broker *b = da->b; int t_idx = da->topic_index; free(da);
    Topic *topic = &b->topics[t_idx];

    while (!atomic_load(&b->stop)){
        Message m;
        int ok = topic_pop(topic, &m);
        if (!ok) continue;

        long long now = now_ms();
        m.assigned_ms = now;

        if (!topic->fanout){
            // Work-queue: pick the best consumer
            int best = -1; 
            long long bestScore = LLONG_MAX;
            
            for (int i=0; i < b->cfg.consumers; i++){
                Consumer *c = &b->consumers[i];
                int registered = 0;
                for (int j=0; j < c->topic_count; j++){
                    if (c->topics[j] == topic){ registered = 1; break; }
                }
                if (!registered) continue;
                
                // CHANGED: Calculate priority-aware load for prediction
                long long relevant_load = 0;
                for (int p = 0; p <= m.priority; p++) {
                    relevant_load += c->load_by_prio[p];
                }
                
                long long predicted_finish = now + relevant_load + m.est_ms;
                long long time_until_deadline = (m.deadline_ms > 0) ? (m.deadline_ms - now) : LLONG_MAX;
                
                // Score considers both load and deadline urgency
                long long score = relevant_load;
                if (m.deadline_ms > 0) {
                    score = score * 1000 / (time_until_deadline + 1); // Deadline urgency factor
                }
                
                if (score < bestScore){
                    best = i; 
                    bestScore = score;
                }
            }
            
            if (best == -1) {
                printf("[Dispatcher topic=%s] WARNING: No consumer registered, msg=%d lost\n",
                       topic->name, m.id);
                continue;
            }

            Consumer *chosen = &b->consumers[best];
            
            // CHANGED: Calculate priority-aware load for deadline checking
            long long relevant_load = 0;
            for (int p = 0; p <= m.priority; p++) {
                relevant_load += chosen->load_by_prio[p];
            }
            long long predicted_finish = now + relevant_load + m.est_ms;
            
            if (m.deadline_ms > 0 && predicted_finish > m.deadline_ms){
                atomic_fetch_add(&b->rejected_count, 1);
                atomic_fetch_add(&b->rejected_by_topic_prio[t_idx*3 + (m.priority%3)], 1);
                printf("[Dispatcher topic=%s] REJECT msg=%d (predicted_finish=%lld > deadline=%lld)\n",
                       topic->name, m.id, predicted_finish, (long long)m.deadline_ms);
                continue;
            }

            pthread_mutex_lock(&chosen->c_mtx);
            if (b->cfg.bounded_enabled && chosen->personal_q.capacity > 0){
                while (chosen->personal_q.total_size >= chosen->personal_q.capacity && !atomic_load(&b->stop)){
                    pthread_cond_wait(&chosen->c_not_full, &chosen->c_mtx);
                }
            }
            if (atomic_load(&b->stop)) { pthread_mutex_unlock(&chosen->c_mtx); continue; }
            
            q_push_back_nolock(&chosen->personal_q, m);
            pthread_cond_signal(&chosen->c_not_empty);
            pthread_mutex_unlock(&chosen->c_mtx);
            
            // CHANGED: Update both total and priority-specific load
            chosen->load_ms += m.est_ms;
            chosen->load_by_prio[m.priority] += m.est_ms;

            printf("[Dispatcher topic=%s] assigned msg=%d -> consumer %s (load=%lld, prio_load=[%lld,%lld,%lld])\n",
                   topic->name, m.id, chosen->name, chosen->load_ms,
                   chosen->load_by_prio[0], chosen->load_by_prio[1], chosen->load_by_prio[2]);
                   
        } else {
            // Fanout: push to ALL subscribers
            pthread_mutex_lock(&topic->mtx);
            int n = topic->sub_count;
            Consumer **subs = (Consumer**)alloca(sizeof(Consumer*) * (n > 0 ? n : 1));
            for (int i=0;i<n;i++) subs[i] = topic->subs[i];
            pthread_mutex_unlock(&topic->mtx);

            for (int i=0;i<n;i++){
                Consumer *c = subs[i];
                
                // CHANGED: Priority-aware load for fanout deadline checking
                long long relevant_load = 0;
                for (int p = 0; p <= m.priority; p++) {
                    relevant_load += c->load_by_prio[p];
                }
                long long predicted_finish = now + relevant_load + m.est_ms;
                
                if (m.deadline_ms > 0 && predicted_finish > m.deadline_ms){
                    atomic_fetch_add(&b->rejected_count, 1);
                    atomic_fetch_add(&b->rejected_by_topic_prio[t_idx*3 + (m.priority%3)], 1);
                    printf("[Dispatcher topic=%s] REJECT (fanout) msg=%d for consumer %s (pred=%lld > dl=%lld)\n",
                           topic->name, m.id, c->name, predicted_finish, (long long)m.deadline_ms);
                    continue;
                }

                pthread_mutex_lock(&c->c_mtx);
                if (b->cfg.bounded_enabled && c->personal_q.capacity > 0){
                    while (c->personal_q.total_size >= c->personal_q.capacity && !atomic_load(&b->stop)){
                        pthread_cond_wait(&c->c_not_full, &c->c_mtx);
                    }
                }
                if (atomic_load(&b->stop)) { pthread_mutex_unlock(&c->c_mtx); break; }
                
                q_push_back_nolock(&c->personal_q, m);
                pthread_cond_signal(&c->c_not_empty);
                pthread_mutex_unlock(&c->c_mtx);

                c->load_ms += m.est_ms;
                c->load_by_prio[m.priority] += m.est_ms;
                
                printf("[Dispatcher topic=%s] fanout msg=%d -> consumer %s (load=%lld)\n",
                       topic->name, m.id, c->name, c->load_ms);
            }
        }
    }
    return NULL;
}

// ------------------------------------------------------------
// Retention GC thread
// ------------------------------------------------------------
typedef struct GCArg { Broker *b; } GCArg;

static void purge_topic_queues(Topic *t){
    long long n = now_ms();
    int removed = 0;

    pthread_mutex_lock(&t->mtx);
    removed += q_remove_expired_nolock(&t->q, n);
    if (removed && t->broker->cfg.bounded_enabled && t->q.capacity > 0)
        pthread_cond_broadcast(&t->not_full);
    pthread_mutex_unlock(&t->mtx);

    if (removed>0){
        printf("[GC] topic=%s removed %d expired\n", t->name, removed);
    }
}

static void *gc_thread(void *arg){
    GCArg *ga = (GCArg*)arg; Broker *b = ga->b; free(ga);
    while (!atomic_load(&b->stop)){
        if (b->cfg.retention_enabled && b->cfg.ttl_ms > 0){
            for (int i=0;i<b->topic_count;i++) purge_topic_queues(&b->topics[i]);
        }
        msleep((int)b->cfg.gc_interval_ms);
    }
    return NULL;
}

// ------------------------------------------------------------
// Dashboard thread
// ------------------------------------------------------------
typedef struct DashboardArg { Broker *b; } DashboardArg;

static void *dashboard_thread(void *arg){
    DashboardArg *da = (DashboardArg*)arg; Broker *b = da->b; free(da);
    while (!atomic_load(&b->stop)){
        printf("\n===== DASHBOARD =====\n");

        // Topics
        for (int i=0;i<b->topic_count;i++){
            Topic *t = &b->topics[i];
            int qsize = 0, producers = 0, consumers = 0;
            pthread_mutex_lock(&t->mtx);
            qsize = t->q.total_size;
            producers = t->producer_count;
            consumers = t->sub_count;
            pthread_mutex_unlock(&t->mtx);

            printf("Topic[%d] %s (fanout=%d): qsize=%d producers=%d consumers=%d\n",
                   i, t->name, t->fanout, qsize, producers, consumers);
        }

        // Consumers
        for (int i=0;i<b->cfg.consumers;i++){
            Consumer *c = &b->consumers[i];
            int cqsize = 0;
            pthread_mutex_lock(&c->c_mtx);
            cqsize = c->personal_q.total_size;
            pthread_mutex_unlock(&c->c_mtx);

            printf("Consumer[%d] %s: qsize=%d load=%lld prio_load=[%lld,%lld,%lld] done=%ld topics=",
                   i, c->name, cqsize, c->load_ms, 
                   c->load_by_prio[0], c->load_by_prio[1], c->load_by_prio[2],
                   atomic_load(&c->done_count));
            for (int j=0;j<c->topic_count;j++){
                printf("%s ", c->topics[j]->name);
            }
            printf("\n");
        }

        long cntH = atomic_load(&b->prio_count[0]); 
        long totH = atomic_load(&b->prio_total_ms[0]);
        long latH = atomic_load(&b->prio_total_latency[0]);

        long cntM = atomic_load(&b->prio_count[1]); 
        long totM = atomic_load(&b->prio_total_ms[1]);
        long latM = atomic_load(&b->prio_total_latency[1]);

        long cntL = atomic_load(&b->prio_count[2]); 
        long totL = atomic_load(&b->prio_total_ms[2]);
        long latL = atomic_load(&b->prio_total_latency[2]);

        // Calculate averages
        char bufH[32], bufM[32], bufL[32];
        char latBufH[32], latBufM[32], latBufL[32];
        
        if (cntH) {
            snprintf(bufH, sizeof(bufH), "%.1f ms", (double)totH/(double)cntH);
            snprintf(latBufH, sizeof(latBufH), "%.1f ms", (double)latH/(double)cntH);
        } else {
            snprintf(bufH, sizeof(bufH), "-");
            snprintf(latBufH, sizeof(latBufH), "-");
        }
        
        if (cntM) {
            snprintf(bufM, sizeof(bufM), "%.1f ms", (double)totM/(double)cntM);
            snprintf(latBufM, sizeof(latBufM), "%.1f ms", (double)latM/(double)cntM);
        } else {
            snprintf(bufM, sizeof(bufM), "-");
            snprintf(latBufM, sizeof(latBufM), "-");
        }
        
        if (cntL) {
            snprintf(bufL, sizeof(bufL), "%.1f ms", (double)totL/(double)cntL);
            snprintf(latBufL, sizeof(latBufL), "%.1f ms", (double)latL/(double)cntL);
        } else {
            snprintf(bufL, sizeof(bufL), "-");
            snprintf(latBufL, sizeof(latBufL), "-");
        }

        printf("Rejected(total)=%d\n", atomic_load(&b->rejected_count));
        printf("Avg processing: H=%s  M=%s  L=%s\n", bufH, bufM, bufL);
        printf("Avg end-to-end: H=%s  M=%s  L=%s\n", latBufH, latBufM, latBufL);

        // per-topic, per-priority rejection breakdown
        for (int i=0; i<b->topic_count; i++){
            int rH = atomic_load(&b->rejected_by_topic_prio[i*3 + 0]);
            int rM = atomic_load(&b->rejected_by_topic_prio[i*3 + 1]);
            int rL = atomic_load(&b->rejected_by_topic_prio[i*3 + 2]);
            printf("  Rejected by Topic[%d] %s: H=%d M=%d L=%d\n", i, b->topics[i].name, rH, rM, rL);
        }

        // if (cntH) printf("  * High:   %.1f ms\n", (double)totH/(double)cntH);
        // if (cntM) printf("  * Medium: %.1f ms\n", (double)totM/(double)cntM);
        // if (cntL) printf("  * Low:    %.1f ms\n", (double)totL/(double)cntL);
        printf("=====================\n\n");

        msleep(800);
    }
    return NULL;
}

// ------------------------------------------------------------
// Setup / Teardown
// ------------------------------------------------------------
static void consumer_init(Consumer *c, int id, const char *name, int personal_capacity){
    memset(c, 0, sizeof(*c));
    c->id = id; strncpy(c->name, name, sizeof(c->name)-1); 
    c->load_ms = 0; 
    memset(c->load_by_prio, 0, sizeof(c->load_by_prio));
    atomic_store(&c->done_count, 0);
    c->topics = NULL; c->topic_count = 0;
    q_init(&c->personal_q, personal_capacity);
    pthread_mutex_init(&c->c_mtx, NULL);
    pthread_cond_init(&c->c_not_empty, NULL);
    pthread_cond_init(&c->c_not_full, NULL);
}

static void consumer_add_topic(Consumer *c, Topic *topic){
    c->topics = (Topic**)realloc(c->topics, (c->topic_count + 1) * sizeof(Topic*));
    c->topics[c->topic_count++] = topic;
}

static void register_to_topics(Broker *b, int entity_id, int is_producer) {
    int min_count = INT_MAX;
    int selected_topic = -1;
    for (int j = 0; j < b->topic_count; j++) {
        int count = is_producer ? topic_get_producer_count(&b->topics[j]) : topic_get_subscriber_count(&b->topics[j]);
        if (count < min_count) {
            min_count = count;
            selected_topic = j;
        }
    }
    if (selected_topic == -1) {
        fprintf(stderr, "No topic found for registration\n");
        return;
    }
    if (is_producer) {
        topic_add_producer(&b->topics[selected_topic], entity_id);
    } else {
        topic_add_subscriber(&b->topics[selected_topic], &b->consumers[entity_id]);
        consumer_add_topic(&b->consumers[entity_id], &b->topics[selected_topic]);
    }
}

static void broker_init(Broker *b, Config cfg){
    memset(b, 0, sizeof(*b)); b->cfg = cfg; 
    atomic_store(&b->stop, 0); atomic_store(&b->rejected_count, 0); 
    atomic_store(&b->next_msg_id, 1); atomic_store(&b->next_topic_id, 0);
    for (int i=0;i<3;i++){ 
        atomic_store(&b->prio_count[i], 0); 
        atomic_store(&b->prio_total_ms[i], 0); 
        atomic_store(&b->prio_total_latency[i], 0);
    }

    // topics
    b->topic_count = cfg.topics;
    b->topics = (Topic*)calloc(b->topic_count, sizeof(Topic));
    for (int i=0; i < b->topic_count; i++){
        char tname[32]; snprintf(tname, sizeof(tname), "Topic%d", i);
        topic_init(&b->topics[i], tname, cfg.bounded_enabled ? cfg.capacity : 0, cfg.fanout_enabled, b);
    }

    // validation
    if (cfg.producers < b->topic_count || cfg.consumers < b->topic_count) {
        fprintf(stderr, "Must have at least as many producers and consumers as topics\n");
        exit(1);
    }
    int max_multiple = 2;
    if (cfg.producers > max_multiple * b->topic_count || cfg.consumers > max_multiple * b->topic_count) {
        fprintf(stderr, "Cannot have more than %d times producers or consumers than topics\n", max_multiple);
        exit(1);
    }

    // consumers
    b->consumers = (Consumer*)calloc(cfg.consumers, sizeof(Consumer));
    for (int i=0;i<cfg.consumers;i++){
        char cname[32]; snprintf(cname, sizeof(cname), "C%d", i);
        consumer_init(&b->consumers[i], i, cname, cfg.bounded_enabled ? cfg.capacity : 0);
    }

    // per-topic, per-priority rejected counters
    b->rejected_by_topic_prio = (atomic_int*)malloc(sizeof(atomic_int) * b->topic_count * 3);
    for (int i=0;i<b->topic_count*3;i++) atomic_init(&b->rejected_by_topic_prio[i], 0);
}

static void broker_start_threads(Broker *b){
    // attach consumers and producers
    for (int i=0;i<b->cfg.consumers;i++) register_to_topics(b, i, 0);

    b->producer_threads = (pthread_t*)calloc(b->cfg.producers, sizeof(pthread_t));
    for (int i=0;i<b->cfg.producers;i++){
        register_to_topics(b, i, 1);
        ProducerArg *pa = (ProducerArg*)malloc(sizeof(ProducerArg)); 
        pa->b = b; pa->id = i;
        int topic_count = 0;
        Topic **producer_topics = NULL;
        for (int j=0;j<b->topic_count;j++){
            for (int k=0;k<b->topics[j].producer_count;k++){
                if (b->topics[j].producer_ids[k] == i){
                    producer_topics = (Topic**)realloc(producer_topics, (topic_count + 1) * sizeof(Topic*));
                    producer_topics[topic_count++] = &b->topics[j];
                    break;
                }
            }
        }
        pa->topics = producer_topics;
        pa->topic_count = topic_count;
        pthread_create(&b->producer_threads[i], NULL, producer_thread, pa);
    }

    b->consumer_threads = (pthread_t*)calloc(b->cfg.consumers, sizeof(pthread_t));
    for (int i=0;i<b->cfg.consumers;i++){
        ConsumerArg *ca = (ConsumerArg*)malloc(sizeof(ConsumerArg)); ca->b = b; ca->c = &b->consumers[i];
        pthread_create(&b->consumer_threads[i], NULL, consumer_thread, ca);
    }

    // one dispatcher thread per topic
    b->dispatcher_threads = (pthread_t*)calloc(b->topic_count, sizeof(pthread_t));
    for (int t=0; t<b->topic_count; t++){
        DispatcherArg *da = (DispatcherArg*)malloc(sizeof(DispatcherArg)); da->b = b; da->topic_index = t;
        pthread_create(&b->dispatcher_threads[t], NULL, dispatcher_thread, da);
    }

    if (b->cfg.retention_enabled){
        GCArg *ga = (GCArg*)malloc(sizeof(GCArg)); ga->b = b;
        pthread_create(&b->gc_thread, NULL, gc_thread, ga);
    }

    DashboardArg *dba = (DashboardArg*)malloc(sizeof(DashboardArg)); dba->b = b;
    pthread_create(&b->dashboard_thread, NULL, dashboard_thread, dba);
}

static void broker_stop(Broker *b){
    atomic_store(&b->stop, 1);
    for (int i=0;i<b->topic_count;i++){
        pthread_mutex_lock(&b->topics[i].mtx); 
        pthread_cond_broadcast(&b->topics[i].not_empty); 
        pthread_cond_broadcast(&b->topics[i].not_full); 
        pthread_mutex_unlock(&b->topics[i].mtx);
    }
    for (int i=0;i<b->cfg.consumers;i++){
        pthread_mutex_lock(&b->consumers[i].c_mtx);
        pthread_cond_broadcast(&b->consumers[i].c_not_empty);
        pthread_cond_broadcast(&b->consumers[i].c_not_full);
        pthread_mutex_unlock(&b->consumers[i].c_mtx);
    }
}

static void broker_join(Broker *b){
    for (int i=0;i<b->cfg.producers;i++) pthread_join(b->producer_threads[i], NULL);
    for (int i=0;i<b->cfg.consumers;i++) pthread_join(b->consumer_threads[i], NULL);
    for (int t=0; t<b->topic_count; t++) pthread_join(b->dispatcher_threads[t], NULL);
    if (b->cfg.retention_enabled) pthread_join(b->gc_thread, NULL);
    pthread_join(b->dashboard_thread, NULL);
}

static void broker_destroy(Broker *b){
    free(b->producer_threads);
    free(b->consumer_threads);
    free(b->dispatcher_threads);

    for (int i=0;i<b->topic_count;i++){
        // Free all messages in topic queue
        Message tmp;
        while (q_pop_front_nolock(&b->topics[i].q, &tmp)){}
        pthread_mutex_destroy(&b->topics[i].mtx); 
        pthread_cond_destroy(&b->topics[i].not_empty); 
        pthread_cond_destroy(&b->topics[i].not_full);
        free(b->topics[i].subs);
        free(b->topics[i].producer_ids);
    }
    free(b->topics);

    for (int i=0;i<b->cfg.consumers;i++){
        // Free all messages in consumer personal queue
        Message tmp;
        while (q_pop_front_nolock(&b->consumers[i].personal_q, &tmp)){}
        pthread_mutex_destroy(&b->consumers[i].c_mtx); 
        pthread_cond_destroy(&b->consumers[i].c_not_empty); 
        pthread_cond_destroy(&b->consumers[i].c_not_full);
        free(b->consumers[i].topics);
    }
    free(b->consumers);

    free(b->rejected_by_topic_prio);
    free(b);
}

// ------------------------------------------------------------
// Main
// ------------------------------------------------------------
int main(int argc, char **argv){
    srand((unsigned)time(NULL));
    Config cfg; default_config(&cfg); parse_args(&cfg, argc, argv);

    printf("Config: seconds=%d producers=%d consumers=%d topics=%d bounded=%d cap=%d retention=%d ttl=%ld gc=%ld fanout=%d\n",
           cfg.seconds, cfg.producers, cfg.consumers, cfg.topics, cfg.bounded_enabled, cfg.capacity, 
           cfg.retention_enabled, cfg.ttl_ms, cfg.gc_interval_ms, cfg.fanout_enabled);

    Broker *b = (Broker*)malloc(sizeof(Broker));
    broker_init(b, cfg);
    broker_start_threads(b);
    msleep(cfg.seconds * 1000);
    broker_stop(b);
    broker_join(b);

    // print totals BEFORE destroy/free
    printf("Done. Rejected=%d\n", atomic_load(&b->rejected_count));
    printf("Processing totals: H=%ld M=%ld L=%ld\n",
           atomic_load(&b->prio_count[0]),
           atomic_load(&b->prio_count[1]),
           atomic_load(&b->prio_count[2]));
    printf("End-to-end latency totals: H=%ldms M=%ldms L=%ldms\n",
           atomic_load(&b->prio_total_latency[0]),
           atomic_load(&b->prio_total_latency[1]),
           atomic_load(&b->prio_total_latency[2]));

    broker_destroy(b);
    return 0;
}