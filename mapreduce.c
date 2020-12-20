#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include "mapreduce.h"

#include <unistd.h>

int NUM_HASHMAPS;
// structure declarations
struct file_node {
    char *filename;
    long length;
    struct file_node *next;
};

struct BST_node {
    char *key;
    struct link_node *list;
    struct BST_node *left;
    struct BST_node *right;
};

struct hash_bucket {
    struct BST_node **trees;
};

struct link_node {
    char *value;
    struct link_node *next;
};

// Lock for the list of list of filenames
pthread_mutex_t filelist_lock;

pthread_t *reducerIds;
pthread_t *mapperIds;

struct hash_bucket **COMBINE_DATA;    // intermediate data storage between mapping and combining
                                      //      there is 1 list per mapper thread
pthread_cond_t *IM_AVAILABLE;         // list of condition variables for intermediate data being available
                                      //      for the respective reducer
pthread_mutex_t **IM_LOCK;            // a list of reducer hashmap locks
                                      //      being sent to the respective reducer
int **IM_LOCK_STATUS;
int *IM_C;                            // variable that goes along with the condition variable
struct hash_bucket **REDUCE_IM;       // intermediate data storage for the respective reducer
struct hash_bucket **REDUCE_DATA;     // final data storage between combining and reducing
                                      //      (or mapping and reducing, depending); there is 1 list
                                      //      per reducer thread

struct file_node *HEAD;
int serving_file;
char **FILES;

Reducer REDUCE_FUNCTION;
Combiner COMBINE_FUNCTION;
Partitioner PARTITION_FUNCTION;

int total_reducers;
int MAPPERS_STOPPED;

// function declarations
char * next_combine_value(char *);
char * next_reducer_value(char *, int);
char * final_reducer_value(char *, int);
char * get_reduce_state(char *, int);
void * run_mapper(void *);
void * run_reducer(void *);
char * pop_next_file();
int findMyself(int);

void HASH_insert(struct hash_bucket *, char *, char *, int);
char * HASH_remove(struct hash_bucket *, char *, int);
char * HASH_peak_next_key(struct hash_bucket *);
char * HASH_peak_next_free_key(struct hash_bucket *, int);

struct link_node * LINK_insert(struct link_node *, char *);
struct link_node * LINK_remove(struct link_node *, char **);

struct BST_node * BST_insert(struct BST_node *, char *, char *);
struct BST_node * BST_remove(struct BST_node *, char *, char **);
struct BST_node * BST_min_node(struct BST_node *);
struct BST_node * BST_new_node(char *, char *);
void BST_print(struct BST_node *);

char * HASH_peak_next_free_key(struct hash_bucket *bucket, int partition) {
    for(int i = 0; i < NUM_HASHMAPS; i++)
        if(bucket->trees[i] != NULL && IM_LOCK_STATUS[partition] == 0)
            return strdup(bucket->trees[i]->key);

    return NULL;
}

char * HASH_peak_next_key(struct hash_bucket *bucket) {
    for(int i = 0; i < NUM_HASHMAPS; i++)
        if(bucket->trees[i] != NULL)
            return strdup(bucket->trees[i]->key);

    return NULL;
}

unsigned long HASH_function(char *key){
    unsigned long hash = NUM_HASHMAPS;
    int c;
    while((c = *key++) != '\0')
        hash = hash * 37 + c;
    return hash % NUM_HASHMAPS;
}

int HASH_key(char *key) {
    if(key == NULL) printf("KEY IS NULL\n");
    return HASH_function(key);
}

void HASH_insert(struct hash_bucket *bucket, char *key, char *value, int key_part) {
    bucket->trees[key_part] = BST_insert(bucket->trees[key_part], key, value);
}

char * HASH_remove(struct hash_bucket *bucket, char *key, int key_part) {
    char *ret = NULL;
    bucket->trees[key_part] = BST_remove(bucket->trees[key_part], key, &ret);
    return ret;
}

struct BST_node * BST_new_node(char *key, char *value) {
    struct BST_node *temp = malloc(sizeof(struct BST_node));
    temp->key = strdup(key);
    struct link_node *link = malloc(sizeof(struct link_node));
    link->next = NULL;
    link->value = strdup(value);
    temp->list = link;
    temp->left = temp->right = NULL;
    return temp;
}

struct BST_node * BST_min_node(struct BST_node *node) {
    struct BST_node *curr = node;
    while(curr && curr->left != NULL)
        curr = curr->left;

    return curr;
}

struct BST_node * BST_insert(struct BST_node *node, char *key, char *value) {
    if(node == NULL) return BST_new_node(key, value);
    if(strcmp(key, node->key) < 0)
        node->left = BST_insert(node->left, key, value);
    else if(strcmp(key, node->key) > 0)
        node->right = BST_insert(node->right, key, value);
    else
        node->list = LINK_insert(node->list, value);

    return node;
}

struct link_node * LINK_insert(struct link_node *node, char *value) {
    struct link_node *new = malloc(sizeof(struct link_node));
    new->value = strdup(value);
    new->next = node;
    return new;
}

struct link_node * LINK_remove(struct link_node *node, char **value_out) {
    if(node == NULL) return NULL;
    if(value_out != NULL) *value_out = strdup(node->value);
    struct link_node *toRet = node->next;
    // free(node->value);
    free(node);
    return toRet;
}

struct BST_node * BST_remove(struct BST_node *root, char *key, char **value_out) {
    if(root == NULL) return root;

    if(strcmp(key, root->key) < 0)
        root->left = BST_remove(root->left, key, value_out);
    else if(strcmp(key, root->key) > 0)
        root->right = BST_remove(root->right, key, value_out);
    else {
        if(root->left == NULL) {
            root->list = LINK_remove(root->list, value_out);

            if(root->list == NULL) {
                struct BST_node *temp = root->right;
                free(root->key);
                free(root);
                return temp;
            } else {
                return root;
            }
        } else if(root->right == NULL) {
            root->list = LINK_remove(root->list, value_out);

            if(root->list == NULL) {
                struct BST_node *temp = root->left;
                free(root->key);
                free(root);
                return temp;
            } else {
                return root;
            }
        }

        root->list = LINK_remove(root->list, value_out);
        if(root->list == NULL) {
            struct BST_node *temp = BST_min_node(root->right);
            free(root->key);
            root->key = strdup(temp->key);
            root->list = temp->list;
            temp->list = NULL;
            root->right = BST_remove(root->right, temp->key, NULL);
        }

    }
    return root;
}

int _part[1000];
void MR_EmitToCombiner(char *key, char *value) {
    // first, find my ID
    int _me = findMyself(0);
    // we're the only ones using our combiner storage so no need to lock it
    int key_part = HASH_key(key);
    HASH_insert(COMBINE_DATA[_me], key, value, key_part);

_part[key_part]++;
}
 
char * next_combine_value(char *key) {
    int _me = findMyself(0);

    char *ret = HASH_remove(COMBINE_DATA[_me], key, HASH_key(key));
    return ret;
}

// the respective lock MUST be required before entering this function
char * next_reducer_value(char *key, int partition) {
    int key_part = HASH_key(key);
    pthread_mutex_lock(&IM_LOCK[partition][key_part]);
    IM_LOCK_STATUS[partition][key_part] = 1;
    char *ret = HASH_remove(REDUCE_IM[partition], key, key_part);
    pthread_mutex_unlock(&IM_LOCK[partition][key_part]);
    IM_LOCK_STATUS[partition][key_part] = 0;
    
    // update the condition variable if needed
    char *check = HASH_peak_next_key(REDUCE_IM[partition]);
    if(check == NULL)
        IM_C[partition] = 0;
    free(check);

    return ret;
}

// this gets called when we're done processing everything for the given partition,
// so we return NULL to let the user know that we're done processing that key
char * final_reducer_value(char *key, int partition) {
    return NULL;
}

void MR_EmitToReducer(char *key, char *value) {
    int partition = PARTITION_FUNCTION(key, total_reducers);

    int key_part = HASH_key(key);
    pthread_mutex_lock(&IM_LOCK[partition][key_part]);
    IM_LOCK_STATUS[partition][key_part] = 1;
    HASH_insert(REDUCE_IM[partition], key, value, key_part);
    pthread_mutex_unlock(&IM_LOCK[partition][key_part]);
    IM_LOCK_STATUS[partition][key_part] = 0;

    if(IM_C[partition] == 0) {
        IM_C[partition] = 1;
    }
    pthread_cond_signal(&IM_AVAILABLE[partition]);
}

void MR_EmitReducerState(char *key, char *value, int partition) {
    // this will only ever get called by the reducer of the given partition number
    HASH_insert(REDUCE_DATA[partition], key, value, HASH_key(key));
}

char * get_reduce_state(char* key, int partition) {
    // this will only ever get called by the reducer of the given partition number
    char *ret = HASH_remove(REDUCE_DATA[partition], key, HASH_key(key));

    return ret;
}

/*
 * argc/argv: command line arguments; all argv[1:n-1] are file names
 * map: mapper function
 * num_mappers: number of mapper threads
 * reduce: reducer function
 * num_reducers: number of reducer threads
 * combine: combiner function
 * partition: partition function
 */
void MR_Run(int argc, char *argv[], Mapper map, 
            int num_mappers, Reducer reduce, int num_reducers, 
            Combiner combine, Partitioner partition) {

    NUM_HASHMAPS = 1000;
    serving_file = argc-2;
    FILES = malloc(sizeof(char *) * (argc - 1));
    for(int i = 0; i < argc - 1; i++) {
        FILES[i] = strdup(argv[i+1]);
    }

    if(combine != NULL) { // we only need to allocate this data if we're actually combining
        COMBINE_DATA = malloc(sizeof(struct hash_bucket *) * num_mappers);

        for(int i = 0; i < num_mappers; i++) {
            COMBINE_DATA[i] = malloc(sizeof(struct hash_bucket));
            COMBINE_DATA[i]->trees = malloc(sizeof(struct BST_node *) * NUM_HASHMAPS);
            for(int j = 0; j < NUM_HASHMAPS; j++) {
                COMBINE_DATA[i]->trees[j] = NULL;
                // COMBINE_DATA[i]->trees[j]->list = NULL;
            }
        }
    } else {
        COMBINE_DATA = NULL;
    }
    REDUCE_IM = malloc(sizeof(struct hash_bucket *) * num_reducers);
    for(int i = 0; i < num_reducers; i++) {
        REDUCE_IM[i] = malloc(sizeof(struct hash_bucket));
        REDUCE_IM[i]->trees = malloc(sizeof(struct BST_node *) * NUM_HASHMAPS);
        for(int j = 0; j < NUM_HASHMAPS; j++) {
            REDUCE_IM[i]->trees[j] = NULL;
            // REDUCE_IM[i]->trees[j]->list = NULL;
        }
    }
    IM_AVAILABLE = malloc(sizeof(pthread_cond_t) * num_reducers);
    for(int i = 0; i < num_reducers; i++) {
        pthread_cond_init(&IM_AVAILABLE[i], NULL);
    }

    REDUCE_DATA = malloc(sizeof(struct hash_bucket *) * num_reducers);
    for(int i = 0; i < num_reducers; i++) {
        REDUCE_DATA[i] = malloc(sizeof(struct hash_bucket));
        REDUCE_DATA[i]->trees = malloc(sizeof(struct BST_node *) * NUM_HASHMAPS);
        for(int j = 0; j < NUM_HASHMAPS; j++) {
            REDUCE_DATA[i]->trees[j] = NULL;
            // REDUCE_DATA[i]->trees[j]->list = NULL;
        }
    }

    IM_C = malloc(sizeof(int) * num_reducers);
    for(int i = 0; i < num_reducers; i++)
        IM_C[i] = 0;

    REDUCE_FUNCTION = reduce;
    COMBINE_FUNCTION = combine;
    PARTITION_FUNCTION = (partition == NULL) ? MR_DefaultHashPartition : partition;
    total_reducers = num_reducers;
    MAPPERS_STOPPED = 0;
    

	/* 
	 * ORDER OF THINGS:
	 * 1. Organize files into an efficient order
	 * 2. Launch mappers & reducers (mappers are also combiners)
	 * 3. Mappers do their work and submit it to combiner (if it exists) as they go
	 *    As work comes in, reducers reduce what they're given
	 */

    // First we organize the files into shortest file first
    pthread_mutex_init(&filelist_lock, NULL);
    
    // the files are now sorted from shorter to longest
    // mappers will take files in a first come, first serve manner.

    IM_LOCK = malloc(sizeof(pthread_mutex_t *) * num_reducers);
    IM_LOCK_STATUS = malloc(sizeof(int *) * num_reducers);
    for(int i = 0; i < num_reducers; i++) {
        IM_LOCK[i] = malloc(sizeof(pthread_mutex_t) * NUM_HASHMAPS);
        IM_LOCK_STATUS[i] = malloc(sizeof(int) * NUM_HASHMAPS);
        for(int j = 0; j < NUM_HASHMAPS; j++) {
            pthread_mutex_init(&IM_LOCK[i][j], NULL);
            IM_LOCK_STATUS[i][j] = 0;
        }
    }

    mapperIds = malloc(sizeof(pthread_t) * num_mappers);
    for(int i = 0; i < num_mappers; i++) {
        pthread_create(&mapperIds[i], NULL, &run_mapper, map);
    }

    reducerIds = malloc(sizeof(pthread_t) * num_reducers);
    for(int i = 0; i < num_reducers; i++) {
        pthread_create(&reducerIds[i], NULL, &run_reducer, reduce);
    }

    for(int i = 0; i < num_mappers; i++)
        pthread_join(mapperIds[i], NULL);

    MAPPERS_STOPPED = 1;
    for(int i = 0; i < num_reducers; i++)
        pthread_cond_signal(&IM_AVAILABLE[i]);

    // freeing for once all the mappers are done
    pthread_mutex_lock(&filelist_lock);
    if(HEAD != NULL)
        free(HEAD);
    HEAD = NULL;
    pthread_mutex_unlock(&filelist_lock);



    for(int i = 0; i < num_reducers; i++)
        pthread_join(reducerIds[i], NULL);

    // freeing for once all the reducers are done


}

// Driver function for reducers
void * run_reducer(void *reducer_function) {
    int _me = findMyself(1);

    char *key = NULL;
    while(1) {
        pthread_mutex_lock(&IM_LOCK[_me][0]);
        
        while(IM_C[_me] == 0 && !MAPPERS_STOPPED) {
            pthread_cond_wait(&IM_AVAILABLE[_me], &IM_LOCK[_me][0]);
        }

        pthread_mutex_unlock(&IM_LOCK[_me][0]);

        if(IM_C[_me] == 0 && MAPPERS_STOPPED) {
            break;
        }

        key = HASH_peak_next_key(REDUCE_IM[_me]);
        if(key == NULL)
            continue;

        REDUCE_FUNCTION(key, &get_reduce_state, &next_reducer_value, _me);

        free(key);
    }

    while((key = HASH_peak_next_key(REDUCE_DATA[_me])) != NULL) {
        REDUCE_FUNCTION(key, &get_reduce_state, &final_reducer_value, _me);
        free(key);
    }

    return NULL;
}

// Driver function for mappers
void * run_mapper(void *mapper_function) {
    int _me = findMyself(0);
    while(1) {
        pthread_mutex_lock(&filelist_lock);
        char *file = pop_next_file();
        pthread_mutex_unlock(&filelist_lock);

        if(file == NULL) {
            break;
        }

        ((Mapper)mapper_function)(file);

        if(COMBINE_FUNCTION != NULL) {
            char *key = NULL;
            while((key = HASH_peak_next_key(COMBINE_DATA[_me])) != NULL) {
                COMBINE_FUNCTION(key, &next_combine_value);
                free(key);
            }
        }

        free(file);
    }
    return NULL;
}

// filelist_lock MUST be acquired going into this
char * pop_next_file() {
    if(serving_file == -1)
        return NULL;

    char *toRet = FILES[serving_file];
    serving_file--;
    return toRet;
}

// type == 0 means mapper,
// type == 1 means reducer
int findMyself(int type) {
    pthread_t tId = pthread_self();

    pthread_t *arr = (type == 0) ? mapperIds : reducerIds;

    int _me = 0;
    while(arr[_me] != tId)
        _me++;

    return _me;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions){
    unsigned long hash = 5381;
    int c;
    while((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}
