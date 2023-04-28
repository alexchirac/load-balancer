/* Copyright 2023 <> */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// #include "server.h"
#include "load_balancer.h"

struct server {
	int id;
	unsigned int hash;
	server_memory *memory;
};

struct load_balancer {
	server **servers;
	int max_servers;
	int nr_servers;
};

unsigned int hash_function_servers(void *a) {
	unsigned int uint_a = *((unsigned int *)a);

	uint_a = ((uint_a >> 16u) ^ uint_a) * 0x45d9f3b;
	uint_a = ((uint_a >> 16u) ^ uint_a) * 0x45d9f3b;
	uint_a = (uint_a >> 16u) ^ uint_a;
	return uint_a;
}

unsigned int hash_function_key(void *a) {
	unsigned char *puchar_a = (unsigned char *)a;
	unsigned int hash = 5381;
	int c;

	while ((c = *puchar_a++))
		hash = ((hash << 5u) + hash) + c;

	return hash;
}

load_balancer *init_load_balancer() {
	load_balancer *lb = malloc(sizeof(load_balancer));
	lb->max_servers = MAXSERVERS;
	lb->nr_servers = 0;
	lb->servers = malloc(MAXSERVERS * sizeof(server *));
	return lb;
}

int get_insert_poz(load_balancer *main, unsigned int hash, int id)
{
	unsigned int prev_hash = 0;
	if (main->servers[main->nr_servers - 1]->hash < hash)
		return -1;
	if (main->servers[main->nr_servers - 1]->hash == hash) {
		if (main->servers[main->nr_servers - 1]->id < id)
			return -1;
		else
			return main->nr_servers - 1;
	}

	for (int i = 0; i < main->nr_servers; i++) {
		if (main->servers[i]->hash > hash && prev_hash < hash)
			return i;
		if (main->servers[i]->hash == hash) {
			if (main->servers[i]->id < id)
				return i;
			else
				return i + 1;
		}
		prev_hash = main->servers[i]->hash;
	}
	return -1;
}

char **get_all_keys(server *s)
{
	char **key_list;
	key_list = malloc(s->memory->size * sizeof(char *));
	server_memory *memory = s->memory;
	int k = 0;
	for (int i = 0; i < HMAX; i++) {
		ll_node_t *cur = memory->buckets[i]->head;
		while (cur != NULL) {
			key_list[k] = ((info *)cur->data)->key;
			k++;
			cur = cur->next;
		}
	}
	return key_list;
}

void redistribute_keys(char **key_list, server *new_s, server *next_s, int poz)
{
	int nr_keys = next_s->memory->size;
	for (int i = 0; i < nr_keys; i++) {
		unsigned int hash = hash_function_key(key_list[i]);
		int ok = 0;
		if (poz == 0 && (hash > next_s->hash || hash <= new_s->hash))
			ok = 1;
		if (poz != 0 && poz != -1 && hash <= new_s->hash)
			ok = 1;
		if (poz == -1 && hash > next_s->hash && hash <= new_s->hash)
			ok = 1;
		if (ok) {
			char *value = server_retrieve(next_s->memory, key_list[i]);
			server_store(new_s->memory, key_list[i], value);
			server_remove(next_s->memory, key_list[i]);
		}
	}
}

void insert_server(load_balancer *main, server *s)
{
	char **key_list;
	int poz = get_insert_poz(main, s->hash, s->id);
	if (poz == -1) {
		main->servers[main->nr_servers] = s;
		key_list = get_all_keys(main->servers[0]);
		redistribute_keys(key_list, s, main->servers[0], poz);
	} else {
		for (int i = main->nr_servers - 1; i >= poz; i--)
			main->servers[i + 1] = main->servers[i];
		main->servers[poz] = s;
		key_list = get_all_keys(main->servers[poz + 1]);
		redistribute_keys(key_list, s, main->servers[poz + 1], poz);
	}
	main->nr_servers++;
	free(key_list);
}

void loader_add_server(load_balancer *main, int server_id) {
	server *s, *copy1, *copy2;
	s = malloc(sizeof(server));
	s->id = server_id;
	s->hash = hash_function_servers(&server_id);
	// printf("%u\n", s->hash);
	s->memory = init_server_memory();

	copy1 = malloc(sizeof(server));
	copy1->id = server_id + COPY;
	copy1->hash = hash_function_servers(&copy1->id);
	// printf("%u\n", copy1->hash);
	copy1->memory = init_server_memory();

	copy2 = malloc(sizeof(server));
	copy2->id = server_id + 2 * COPY;
	copy2->hash = hash_function_servers(&copy2->id);
	// printf("%u\n", copy2->hash);
	copy2->memory = init_server_memory();

	if (main->nr_servers == 0) {
		main->servers[0] = s;
		main->nr_servers++;
		insert_server(main, copy1);
		insert_server(main, copy2);
	} else {
		if (main->max_servers <= main->nr_servers + 3) {
			main->max_servers *= INCR;
			main->servers = realloc(main->servers, main->max_servers * sizeof(server *));
		}
		insert_server(main, s);
		insert_server(main, copy1);
		insert_server(main, copy2);
	}
	// for(int i = 0; i < main->nr_servers; i++)
	// 	printf("%d ", main->servers[i]->id);
	// printf("\n");
	// for(int i = 0; i < main->nr_servers; i++)
	// 	printf("%u ", main->servers[i]->hash);
	// printf("\n");
}

void loader_remove_server(load_balancer *main, int server_id) {
	for (int i = 0; i < main->nr_servers; i++) {
		if (main->servers[i]->id % COPY == server_id) {
			char **key_list = get_all_keys(main->servers[i]);
			server *next_s;
			if (i == main->nr_servers - 1)
				next_s = main->servers[0];
			else
				next_s = main->servers[i + 1];
			server_memory *del_memory = main->servers[i]->memory;
			for (int j = 0; j < main->servers[i]->memory->size; j++) {
				char *value = server_retrieve(del_memory, key_list[j]);
				server_store(next_s->memory, key_list[j], value);
			}
			free_server_memory(main->servers[i]->memory);
			free(main->servers[i]);
			for (int j = i; j < main->nr_servers - 1; j++)
				main->servers[j] = main->servers[j + 1];
			main->nr_servers--;
			i--;
			free(key_list);
		}
	}
	if (main->nr_servers < main->max_servers / 2) {
		main->max_servers = main->max_servers / INCR;
		main->servers = realloc(main->servers, main->max_servers * sizeof(server *));
	}
}

void loader_store(load_balancer *main, char *key, char *value, int *server_id) {
	unsigned int hash = hash_function_key(key);
	if (hash > main->servers[main->nr_servers - 1]->hash) {
		int id = main->servers[0]->id % COPY;
		*server_id = id;
		server_store(main->servers[0]->memory, key, value);
		return;
	}
	for (int i = 0; i < main->nr_servers; i++) {
		if (hash < main->servers[i]->hash) {
			int id = main->servers[i]->id % COPY;
			*server_id = id;
			server_store(main->servers[i]->memory, key, value);
			return;
		}
	}
}

char *loader_retrieve(load_balancer *main, char *key, int *server_id)
{
	unsigned int hash = hash_function_key(key);
	// for(int i = 0; i < main->nr_servers; i++)
	// 	printf("%d ", main->servers[i]->id);
	// printf("\n");
	// for(int i = 0; i < main->nr_servers; i++)
	// 	printf("%u ", main->servers[i]->hash);
	// printf("\n");
	// printf("%u\n", hash);
	if (hash > main->servers[main->nr_servers - 1]->hash) {
		int id = main->servers[0]->id % COPY;
		*server_id = id;
		return server_retrieve(main->servers[0]->memory, key);
	} else {
		for (int i = 0; i < main->nr_servers; i++) {
			if (hash < main->servers[i]->hash) {
				int id = main->servers[i]->id % COPY;
				*server_id = id;
				return server_retrieve(main->servers[i]->memory, key);
			}
		}
	}
	return NULL;
}

void free_load_balancer(load_balancer *main) {
	for (int i = 0; i < main->nr_servers; i++) {
		free_server_memory(main->servers[i]->memory);
		free(main->servers[i]);
	}
	free(main->servers);
	free(main);
}
