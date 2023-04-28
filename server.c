/* Copyright 2023 <> */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "server.h"

// #define HMAX 20

linked_list_t *ll_create(unsigned int data_size)
{
	linked_list_t* ll;

	ll = malloc(sizeof(*ll));

	ll->head = NULL;
	ll->data_size = data_size;
	ll->size = 0;

	return ll;
}

/*
 * Pe baza datelor trimise prin pointerul new_data, se creeaza un nou nod care e
 * adaugat pe pozitia n a listei reprezentata de pointerul list. Pozitiile din
 * lista sunt indexate incepand cu 0 (i.e. primul nod din lista se afla pe
 * pozitia n=0). Daca n >= nr_noduri, noul nod se adauga la finalul listei. Daca
 * n < 0, eroare.
 */
void ll_add_nth_node(linked_list_t* list, unsigned int n, const void* new_data)
{
	ll_node_t *prev, *curr;
	ll_node_t* new_node;

	if (!list) {
		return;
	}

	/* n >= list->size inseamna adaugarea unui nou nod la finalul listei. */
	if (n > list->size) {
		n = list->size;
	}

	curr = list->head;
	prev = NULL;
	while (n > 0) {
		prev = curr;
		curr = curr->next;
		--n;
	}

	new_node = malloc(sizeof(*new_node));
	new_node->data = malloc(list->data_size);
	memcpy(new_node->data, new_data, list->data_size);

	new_node->next = curr;
	if (prev == NULL) {
		/* Adica n == 0. */
		list->head = new_node;
	} else {
		prev->next = new_node;
	}

	list->size++;
}

/*
 * Elimina nodul de pe pozitia n din lista al carei pointer este trimis ca
 * parametru. Pozitiile din lista se indexeaza de la 0 (i.e. primul nod din
 * lista se afla pe pozitia n=0). Daca n >= nr_noduri - 1, se elimina nodul de
 * la finalul listei. Daca n < 0, eroare. Functia intoarce un pointer spre acest
 * nod proaspat eliminat din lista. Este responsabilitatea apelantului sa
 * elibereze memoria acestui nod.
 */
ll_node_t *ll_remove_nth_node(linked_list_t* list, unsigned int n)
{
	ll_node_t *prev, *curr;

	if (!list || !list->head) {
		return NULL;
	}

	/* n >= list->size - 1 inseamna eliminarea nodului de la finalul listei. */
	if (n > list->size - 1) {
		n = list->size - 1;
	}

	curr = list->head;
	prev = NULL;
	while (n > 0) {
		prev = curr;
		curr = curr->next;
		--n;
	}

	if (prev == NULL) {
		/* Adica n == 0. */
		list->head = curr->next;
	} else {
		prev->next = curr->next;
	}

	list->size--;

	return curr;
}

/*
 * Functia intoarce numarul de noduri din lista al carei pointer este trimis ca
 * parametru.
 */
unsigned int ll_get_size(linked_list_t* list)
{
	 if (!list) {
		return -1;
	}

	return list->size;
}

/*
 * Procedura elibereaza memoria folosita de toate nodurile din lista, iar la
 * sfarsit, elibereaza memoria folosita de structura lista si actualizeaza la
 * NULL valoarea pointerului la care pointeaza argumentul (argumentul este un
 * pointer la un pointer).
 */
void ll_free(linked_list_t** pp_list)
{
	ll_node_t* currNode;

	if (!pp_list || !*pp_list) {
		return;
	}

	while (ll_get_size(*pp_list) > 0) {
		currNode = ll_remove_nth_node(*pp_list, 0);
		free(currNode->data);
		currNode->data = NULL;
		free(currNode);
		currNode = NULL;
	}

	free(*pp_list);
	*pp_list = NULL;
}

unsigned int hash_function_str(void *a)
{
	unsigned char *puchar_a = (unsigned char *)a;
	unsigned int hash = 5381;
	int c;

	while ((c = *puchar_a++))
		hash = ((hash << 5u) + hash) + c;

	return hash;
}

/*
 * Functie apelata pentru a elibera memoria ocupata de cheia si valoarea unei
 * perechi din hashtable. Daca cheia sau valoarea contin tipuri de date complexe
 * aveti grija sa eliberati memoria luand in considerare acest aspect.
 */
void key_val_free_function(void *data) {
	free(((info *)data)->key);
	free(((info *)data)->value);
	free(data);
}

server_memory *init_server_memory()
{
	server_memory *server;
	server = malloc(sizeof(server_memory));
	server->hmax = HMAX;
	server->size = 0;
	server->hash_function = hash_function_str;
	server->key_val_free_function = key_val_free_function;
	server->buckets = malloc(HMAX * sizeof(linked_list_t *));
	for(int i = 0; i < HMAX; i++)
	{
		server->buckets[i] = ll_create(sizeof(info));
	}
	return server;
}

void server_store(server_memory *server, char *key, char *value) {
	unsigned int hash = server->hash_function(key);
	int indx = hash % server->hmax;
	info *new_info;
	new_info = malloc(sizeof(info));
	new_info->key = malloc(KEY_LENGTH);
	new_info->value = malloc(VALUE_LENGTH);
	memcpy(new_info->key, key, KEY_LENGTH);
	memcpy(new_info->value, value, VALUE_LENGTH);
	// new_info->key = key;
	// new_info->value = value;
	linked_list_t *my_list = server->buckets[indx];
	ll_add_nth_node(my_list, my_list->size, new_info);
	free(new_info);
	server->size++;
}

char *server_retrieve(server_memory *server, char *key) {
	unsigned int hash = server->hash_function(key);
	int indx = hash % server->hmax;
	ll_node_t *cur = server->buckets[indx]->head;
	if (cur != NULL)
		while (strcmp(((info *)cur->data)->key, key) != 0) {
			cur = cur->next;
			if (cur == NULL)
				break;
		}
	if (cur == NULL)
		return NULL;
	else
		return ((info *)cur->data)->value;
}

void server_remove(server_memory *server, char *key) {
	unsigned int hash = server->hash_function(key);
	int indx = hash % server->hmax;
	ll_node_t *cur = server->buckets[indx]->head;
	ll_node_t *removed;
	int i = 0;
	if (cur != NULL)
		while (strcmp(((info *)cur->data)->key, key) != 0) {
			cur = cur->next;
			i++;
			if (cur == NULL)
				break;
		}
	if (cur == NULL) {
		printf("Nu exista cheia in server\n");
		return;
	} else {
		removed = ll_remove_nth_node(server->buckets[indx], i);
		server->key_val_free_function(removed->data);
		free(removed);
		server->size--;
	}
}

void free_server_memory(server_memory *server) {
	for (int i = 0; i < server->hmax; i++) {
		linked_list_t *my_list = server->buckets[i];
		while (my_list->head != NULL) {
			char *key = ((info *)my_list->head->data)->key;
			server_remove(server, key);
		}
		free(my_list);
	}
	free(server->buckets);
	free(server);
}
