#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "mapreduce.h"

typedef struct {
    char *key;
    unsigned long hash;
    char **values;
    size_t value_count;
    size_t value_capacity;
    size_t next_value;
} KeyEntry;

typedef struct {
    KeyEntry *keys;
    size_t key_count;
    size_t key_capacity;
    size_t *index;          /* open-addressing table: key slot + 1, 0 = empty */
    size_t index_capacity;  /* always a power of two (or 0) */
    size_t current_key;
    pthread_mutex_t lock;
} Partition;

typedef struct {
    int argc;
    char **argv;
    Mapper map;
    int num_mappers;
    Reducer reduce;
    int num_partitions;
    Partitioner partition;
    Partition *partitions;
} Job;

typedef struct {
    Job *job;
    int thread_index;
} ThreadArgs;

static Job *active_job = NULL;
static pthread_mutex_t active_lock = PTHREAD_MUTEX_INITIALIZER;

static unsigned long djb2(const char *s)
{
    unsigned long hash = 5381;
    int c;

    while ((c = *s++) != '\0')
        hash = hash * 33 + c;
    return hash;
}

static char *copy_string(const char *s)
{
    size_t len = strlen(s) + 1;
    char *copy = malloc(len);

    if (copy != NULL)
        memcpy(copy, s, len);
    return copy;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
    if (key == NULL || num_partitions <= 0)
        return 0;
    return djb2(key) % num_partitions;
}

/* Rebuild the hash index with the given capacity. Caller holds the lock. */
static int rebuild_index(Partition *p, size_t capacity)
{
    size_t *index = calloc(capacity, sizeof(size_t));
    size_t i;

    if (index == NULL)
        return -1;
    for (i = 0; i < p->key_count; i++) {
        size_t slot = p->keys[i].hash & (capacity - 1);
        while (index[slot] != 0)
            slot = (slot + 1) & (capacity - 1);
        index[slot] = i + 1;
    }
    free(p->index);
    p->index = index;
    p->index_capacity = capacity;
    return 0;
}

/* Insert a value under a key. Caller holds the lock. Takes ownership of
 * *key_copy and value_copy on success; sets *key_copy to NULL if the key copy
 * was consumed by a new entry. Returns 0 on success, -1 on allocation failure
 * (nothing is modified and nothing is taken over). */
static int partition_add(Partition *p, char **key_copy, unsigned long hash,
                         char *value_copy)
{
    KeyEntry *entry = NULL;
    size_t slot = 0;

    if ((p->key_count + 1) * 2 > p->index_capacity) {
        size_t cap = p->index_capacity ? p->index_capacity * 2 : 64;
        if (rebuild_index(p, cap) != 0)
            return -1;
    }

    slot = hash & (p->index_capacity - 1);
    while (p->index[slot] != 0) {
        KeyEntry *e = &p->keys[p->index[slot] - 1];
        if (e->hash == hash && strcmp(e->key, *key_copy) == 0) {
            entry = e;
            break;
        }
        slot = (slot + 1) & (p->index_capacity - 1);
    }

    if (entry == NULL) {
        char **values = malloc(4 * sizeof(char *));
        if (values == NULL)
            return -1;
        if (p->key_count == p->key_capacity) {
            size_t cap = p->key_capacity ? p->key_capacity * 2 : 32;
            KeyEntry *keys = realloc(p->keys, cap * sizeof(KeyEntry));
            if (keys == NULL) {
                free(values);
                return -1;
            }
            p->keys = keys;
            p->key_capacity = cap;
        }
        entry = &p->keys[p->key_count];
        entry->key = *key_copy;
        entry->hash = hash;
        entry->values = values;
        entry->value_count = 0;
        entry->value_capacity = 4;
        entry->next_value = 0;
        p->index[slot] = ++p->key_count;
        *key_copy = NULL;
    } else if (entry->value_count == entry->value_capacity) {
        size_t cap = entry->value_capacity * 2;
        char **values = realloc(entry->values, cap * sizeof(char *));
        if (values == NULL)
            return -1;
        entry->values = values;
        entry->value_capacity = cap;
    }

    entry->values[entry->value_count++] = value_copy;
    return 0;
}

void MR_Emit(char *key, char *value)
{
    Job *job;
    Partition *p;
    char *key_copy, *value_copy;
    unsigned long hash, pnum;
    int rc;

    if (key == NULL || value == NULL)
        return;

    pthread_mutex_lock(&active_lock);
    job = active_job;
    pthread_mutex_unlock(&active_lock);
    if (job == NULL)
        return;

    pnum = job->partition(key, job->num_partitions) % job->num_partitions;
    hash = djb2(key);
    key_copy = copy_string(key);
    value_copy = copy_string(value);
    if (key_copy == NULL || value_copy == NULL) {
        free(key_copy);
        free(value_copy);
        return;
    }

    p = &job->partitions[pnum];
    pthread_mutex_lock(&p->lock);
    rc = partition_add(p, &key_copy, hash, value_copy);
    pthread_mutex_unlock(&p->lock);

    /* key_copy is NULL if a new entry took ownership of it. */
    free(key_copy);
    if (rc != 0)
        free(value_copy);
}

static void *mapper_thread(void *arg)
{
    ThreadArgs *a = arg;
    Job *job = a->job;
    int i;

    for (i = 1 + a->thread_index; i < job->argc; i += job->num_mappers)
        job->map(job->argv[i]);
    return NULL;
}

static int compare_keys(const void *a, const void *b)
{
    return strcmp(((const KeyEntry *)a)->key, ((const KeyEntry *)b)->key);
}

static char *get_next(char *key, int partition_number)
{
    Job *job;
    Partition *p;
    KeyEntry *entry = NULL;

    pthread_mutex_lock(&active_lock);
    job = active_job;
    pthread_mutex_unlock(&active_lock);
    if (job == NULL || key == NULL || partition_number < 0 ||
        partition_number >= job->num_partitions)
        return NULL;

    p = &job->partitions[partition_number];
    if (p->current_key < p->key_count &&
        strcmp(p->keys[p->current_key].key, key) == 0) {
        entry = &p->keys[p->current_key];
    } else {
        KeyEntry probe;
        probe.key = key;
        entry = bsearch(&probe, p->keys, p->key_count, sizeof(KeyEntry),
                        compare_keys);
        if (entry == NULL)
            return NULL;
    }

    if (entry->next_value >= entry->value_count)
        return NULL;
    return entry->values[entry->next_value++];
}

static void *reducer_thread(void *arg)
{
    ThreadArgs *a = arg;
    Job *job = a->job;
    Partition *p = &job->partitions[a->thread_index];
    size_t i;

    for (i = 0; i < p->key_count; i++) {
        p->current_key = i;
        job->reduce(p->keys[i].key, get_next, a->thread_index);
    }
    return NULL;
}

static void free_partition(Partition *p)
{
    size_t i, j;

    for (i = 0; i < p->key_count; i++) {
        for (j = 0; j < p->keys[i].value_count; j++)
            free(p->keys[i].values[j]);
        free(p->keys[i].values);
        free(p->keys[i].key);
    }
    free(p->keys);
    free(p->index);
    pthread_mutex_destroy(&p->lock);
}

/* Start `count` threads running `fn`; if a thread cannot be created, run its
 * work on the calling thread instead. Then join every created thread. */
static void run_threads(Job *job, int count, void *(*fn)(void *))
{
    ThreadArgs *args = malloc(count * sizeof(ThreadArgs));
    pthread_t *threads = malloc(count * sizeof(pthread_t));
    char *created = calloc(count, 1);
    int i;

    if (args == NULL || threads == NULL || created == NULL) {
        ThreadArgs a;
        free(args);
        free(threads);
        free(created);
        a.job = job;
        for (i = 0; i < count; i++) {
            a.thread_index = i;
            fn(&a);
        }
        return;
    }

    for (i = 0; i < count; i++) {
        args[i].job = job;
        args[i].thread_index = i;
        if (pthread_create(&threads[i], NULL, fn, &args[i]) == 0)
            created[i] = 1;
        else
            fn(&args[i]);
    }
    for (i = 0; i < count; i++)
        if (created[i])
            pthread_join(threads[i], NULL);

    free(args);
    free(threads);
    free(created);
}

void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partition)
{
    Job job;
    int i;

    if (argv == NULL || map == NULL || reduce == NULL)
        return;
    if (argc < 1)
        argc = 1;
    if (num_mappers <= 0)
        num_mappers = 1;
    if (num_reducers <= 0)
        num_reducers = 1;
    if (partition == NULL)
        partition = MR_DefaultHashPartition;

    job.argc = argc;
    job.argv = argv;
    job.map = map;
    job.num_mappers = num_mappers;
    job.reduce = reduce;
    job.num_partitions = num_reducers;
    job.partition = partition;
    job.partitions = calloc(num_reducers, sizeof(Partition));
    if (job.partitions == NULL)
        return;
    for (i = 0; i < num_reducers; i++)
        pthread_mutex_init(&job.partitions[i].lock, NULL);

    pthread_mutex_lock(&active_lock);
    active_job = &job;
    pthread_mutex_unlock(&active_lock);

    run_threads(&job, num_mappers, mapper_thread);

    for (i = 0; i < num_reducers; i++) {
        Partition *p = &job.partitions[i];
        free(p->index);
        p->index = NULL;
        p->index_capacity = 0;
        if (p->key_count > 1)
            qsort(p->keys, p->key_count, sizeof(KeyEntry), compare_keys);
    }

    run_threads(&job, num_reducers, reducer_thread);

    pthread_mutex_lock(&active_lock);
    active_job = NULL;
    pthread_mutex_unlock(&active_lock);

    for (i = 0; i < num_reducers; i++)
        free_partition(&job.partitions[i]);
    free(job.partitions);
}
