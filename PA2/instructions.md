# MapReduce Implementation Instructions

## Goal

Implement the MapReduce library in `mapreduce.c`. The public API is defined in `mapreduce.h`. Do not change `mapreduce.h` unless a compile error or test requirement shows that it is necessary.

The implementation must:

- Run mapper functions concurrently.
- Emit and store intermediate key/value pairs safely.
- Partition and sort all emitted keys.
- Run reducer functions concurrently.
- Copy and eventually free every dynamically allocated string and data structure.
- Compile with `-Wall -Werror -pthread -O`.

## API to implement

Use the function types in `mapreduce.h`:

```c
typedef char *(*Getter)(char *key, int partition_number);
typedef void (*Mapper)(char *file_name);
typedef void (*Reducer)(char *key, Getter get_func, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);
```

Implement:

```c
void MR_Emit(char *key, char *value);
unsigned long MR_DefaultHashPartition(char *key, int num_partitions);
void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partition);
```

## Thread lifecycle

`MR_Run()` should control the complete job:

1. Store the function arguments and initialize shared state.
2. Create `num_mappers` mapper threads.
3. Give input files to the mapper threads.
4. Join every mapper thread before sorting or reducing.
5. Sort each partition.
6. Create `num_reducers` reducer threads.
7. Join every reducer thread.
8. Free all intermediate and thread-related state.

`MR_Run()` should not return until every mapper and reducer has finished.

## Input-file assignment

The files passed to `MR_Run()` are `argv[1]` through `argv[argc - 1]`.

Each input file must be mapped exactly once. A simple and correct approach is to let thread `i` process files `i`, `i + num_mappers`, `i + 2 * num_mappers`, and so on.

Before testing performance, prioritize correctness. If necessary, start with round-robin assignment rather than a more complex work queue.

## Shared state design

The implementation needs:

- A partition for each reducer.
- A lock for each partition or a lock for each shared structure used to add keys.
- A mapping from a key to a list of copied values.
- Storage for the partitioning function and reducer count.
- A way for `MR_Emit()` to access the current job's shared state from a mapper thread.

The likely structure is:

```text
Global job state
  Partition 0
    Key "apple"
      Value "1"
      Value "1"
  Partition 1
    Key "banana"
      Value "1"
```

Avoid storing raw `char *key` or `char *value` pointers supplied to `MR_Emit()`. Those strings are owned by the caller and may be changed or freed after `MR_Emit()` returns.

## MR_Emit implementation

The expected behavior is:

1. Check that key and value are non-NULL.
2. Use the configured partition function to determine the partition:

   ```c
   partition(key, num_partitions)
   ```

3. Copy both strings with `malloc` and `strcpy` or `strdup`, if available.
4. Lock the selected partition.
5. Find an existing key with `strcmp`.
6. Append the copied value to that key's value list, or create a new key entry and value list.
7. Unlock the partition.

One locking approach is to use one `pthread_mutex_t` per partition. Only one mapper can update a partition at a time, while mappers that use different partitions can proceed concurrently.

Do not hold a partition lock while copying string memory, hashing, or calling unrelated functions. Prepare copies and the partition number first; only lock immediately around the shared data update.

## MR_DefaultHashPartition

Implement the DJB2-style hash exactly as specified:

```c
unsigned long hash = 5381;
int c;

while ((c = *key++) != '\0')
    hash = hash * 33 + c;

return hash % num_partitions;
```

Handle `key == NULL` and `num_partitions <= 0` defensively. The normal tests should pass valid arguments, but the implementation should avoid division by zero or a crash for invalid input.

## Sorting

After every mapper has returned:

- Sort each partition's keys in ascending string order.
- Use `strcmp`, not memory-address or hash comparisons.
- Do not sort values.

The reducer's iteration order for a key is not specified beyond the key being unique. The values may be retained in emission order or in any order, as long as the reducer receives every value exactly once.

Sort with `qsort` or an appropriate keyed insertion sort. `qsort` can be efficient for typical tests.

## Getter implementation

Implement a `Getter` function for the reducer API. It must:

1. Receive a key and partition number.
2. Track which value of that key has been returned by the current reducer.
3. Return the next copied value.
4. Return `NULL` after all values have been consumed.

A simple implementation can use a static or thread-local index for every call context. However, because multiple reducer threads execute concurrently, do not use one global index variable.

For each reducer invocation, provide a getter closure or state that begins with index 0 for that key. The public `Getter` type has no context parameter, so pass this state through a design that is safe under concurrent reducer execution.

Possible approaches include:

- Store a key-specific current index and enforce each partition is handled by only one reducer.
- Maintain one getter state per reducer thread and update it before calling `Reduce`.
- Assign one partition to each reducer and keep iteration state local to that reducer.

The implementation must ensure that separate reducer threads do not consume or overwrite each other's iteration state.

## Reduction phase

Each reducer should operate on a consistent set or one assigned partition. A simple implementation is to give partition `i` to reducer thread `i`, assuming `num_reducers` equals the number of partitions.

For every sorted key in that partition:

```c
Reduce(key_string, getter, partition_number);
```

The getter must:

- Return values only for the specified key.
- Return `NULL` once that key's values are exhausted.
- Start over for the next key.

Do not call the user's `Reduce` function while holding the partition lock unless the tests require the library to serialize all reducers. Reducers only need to read the already-sorted and fully-populated data.

## Concurrency guidelines

Use `pthread_create`, `pthread_join`, and `pthread_mutex_t`.

Use a separate partition lock while:

- Searching for a key.
- Appending a value.
- Creating a key entry.
- Creating or adding any linked list or array node.

Do not update shared state outside a lock.

There is no need to use condition variables in the basic implementation because the library waits for all mappers with `pthread_join` before starting reducers. Condition variables are only useful if you choose a producer/consumer queue and otherwise do not replace join synchronization.

## Thread and argument safety

Do not pass stack variables that are reused by later loop iterations to `pthread_create` by pointer.

For example, pass each thread's index or a heap-allocated thread argument that remains valid until joined.

A concise approach is:

```c
typedef struct {
    int thread_index;
} MapperArgs;

MapperArgs args[num_mappers];
```

This works only if `num_mappers` is known and `pthread_join` is called in the same scope.

For each mapper thread, calculate the thread index or file offset independently. All `MapperArgs` structures must remain valid until that thread exits.

## Memory ownership

After copying in `MR_Emit()`, every key and value string belongs to the MapReduce library.

Before `MR_Emit()` returns, the library must own copies of both strings. If allocation fails, fail cleanly rather than storing dangling pointers or crashing. Allocation policy for out-of-memory behavior is up to you, but do not leak partially created data.

Free all of the following after all reducer threads finish:

- Every copied key
- Every copied value
- Every key/value list node
- Every partition
- Every partition lock
- Every thread argument if heap-allocated

Potentially, the implementation can leave a sorted data structure intact until cleanup. Freeing it only after joining reducers is safest.

## Edge cases

Add defensive handling for at least:

- `argc <= 1`, where there are no input files.
- `num_mappers <= 0`.
- `num_reducers <= 0`.
- `map == NULL`.
- `reduce == NULL`.
- `partition == NULL`.
- `MR_Emit()` called outside `MR_Run()` or after the shared state is destroyed.
- Duplicate and empty keys.
- Empty files.
- Repeated values for the same key.
- Many mappers updating the same partition.
- Keys containing the same string in separate threads.
- Special characters and nonalphabetic tokens.
- Lines and files larger than fixed-size buffers.
- Allocation failure.

Do not assume valid input unless the test suite reasonably does. Avoid dereferencing invalid pointers or dividing by zero.

## Implementing with a data structure

One possible data structure is:

```c
typedef struct {
    char *value;
} ValueNode;

typedef struct {
    char *key;
    char **values;
    size_t value_count;
    size_t value_capacity;
} KeyEntry;

typedef struct {
    KeyEntry *keys;
    size_t key_count;
    size_t key_capacity;
    pthread_mutex_t lock;
} Partition;
```

Another option is linked lists. Arrays provide simpler sorting and cleanup in many implementations.

Whichever structure you choose:

- Keep partition and key lookup synchronization consistent.
- Sort only after mapping has ended.
- Keep key and value pointers stable during sorting and reduction.
- Separate `capacity` from `count` and check both before appending.

## Step-by-step implementation plan

1. Include `stdio.h`, `stdlib.h`, `string.h`, `pthread.h`, and `mapreduce.h`.
2. Define internal key, value, partition, thread argument, and job-state structures.
3. Add a global pointer to the active job state so `MR_Emit()` can access it.
4. Protect access to the active global pointer if thread-safe job management is required.
5. Implement allocation helpers for copied strings and data structures.
6. Implement `MR_DefaultHashPartition()`.
7. Implement `MR_Emit()`.
8. Implement mapper thread functions.
9. Implement the getter used by a reducer.
10. Implement reducer thread functions.
11. Implement cleanup functions.
12. Implement `MR_Run()`:
    - Validate arguments.
    - Allocate partitions and synchronization objects.
    - Set global state.
    - Create mapper threads.
    - Join mapper threads.
    - Sort every partition.
    - Create reducer threads.
    - Join reducer threads.
    - Clear global state.
    - Free all memory.

## Debugging strategy

1. Start with one input file and one mapper.
2. Test an application that emits a small set of known key/value pairs.
3. Add one custom print or reduce test.
4. Test the default hash partitioner independently.
5. Test one partition with multiple keys.
6. Add one mapper with many emitted keys.
7. Add multiple mappers that emit the same key.
8. Add multiple partitions and reducers.
9. Test the output ordering for keys.
10. Test cleanup and error cases under Valgrind.

Useful diagnostic output can show:

- Input file assignment
- Emitted key, value, and partition
- Keys found in each partition
- Sort order
- Reducer and partition pairs
- Cleanup state

Remove or disable debug output before final testing unless the test harness expects no extra output.

## Validation

First compile your implementation against the assignment header:

```bash
gcc -Wall -Werror -pthread -O -c mapreduce.c
```

Then build a small test application that:

- Implements `Map`
- Implements `Reduce`
- Calls `MR_Run`
- Counts words or emits and prints known values

For example, test word count with `num_mappers = 4` and `num_reducers = 4`, then compare the output against a known answer.

Test repeated runs of the same executable, since concurrency bugs and stale global state may only appear intermittently.

Run the compiled program under Valgrind:

```bash
valgrind --leak-check=full --error-exitcode=1 ./your_test_app file1.txt file2.txt
```

Expected final properties:

- Every input file is processed exactly once.
- Every emitted value reaches exactly one partition.
- `Reduce` is called once for each unique key in each partition.
- Keys are in ascending `strcmp` order within each partition.
- The getter returns every value for the key and then `NULL`.
- The process is race-free.
- No heap memory is leaked or invalidly accessed.

## Common mistakes

- Storing the original key or value pointer instead of copying it.
- Forgetting to protect partition/key updates.
- Sorting after reducers start instead of before.
- Calling `Reduce` twice for one key or never calling it.
- Returning a partition number outside `0 ... num_partitions - 1`.
- Using the same getter index for every reducer thread.
- Using the same loop-variable address for every thread argument.
- Forgetting to join all threads.
- Freeing data before reducers have finished.
- Destroying a mutex after a mapper or reducer still uses it.
- Using `realloc` or `malloc` without updating counts and capacities safely.
- Printing extra data in a test application or library.
- Comparing `qsort` keys with an incorrect comparator.
