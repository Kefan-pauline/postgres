
typedef struct dict_entry_s {

	char *key;
	int value;

} dict_entry;

typedef struct dict_mcv {

	int len;
	int limit_entry;
	dict_entry *entry;
	
} dict_mcv, *dict_t;

struct histogram {

	int steps;
	int low_index;
	int max_index;
	int *values;

};


int dict_find_index(dict_t dict, const char *key) {

	for(int i = 0; i < dict->len; i++) {
		if(!strcmp(dict->entry[i].key, key)) {
			return i;
		}
	}
	
	return -1;
}



void dict_add(dict_t dict, const char *key) {

	
	int index = dict_find_index(dict, key);
	
	if(dict->len == dict->limit_entry) {
		dict->limit_entry *= 2;
		dict->entry = realloc(dict->entry, dict->limit_entry * sizeof(dict_entry));
	}
	
	if(index != -1) {
		dict->entry[index].value += 1;
		return;
	}
	
	dict->entry[dict->len].key = strdup(key);
	dict->entry[dict->len].value = 1;
	dict->len++;
	
}

dict_t dict_new(void) {

	int limit;
	limit = 20;

	dict_mcv dict = {0, limit, malloc(limit * sizeof(dict_entry)) };
	dict_t res = malloc(sizeof(dict_mcv));
	*res = dict;
	return res;
	
}


void dict_free(dict_t dict) {


	for(int i = 0; i < dict->len; i++) {
		free(dict->entry[i].key);
	}
	
	free(dict->entry);
	free(dict);

}


void display_dict(dict_t dict, const char *key) {
	
	int index;

	index = dict_find_index(dict, key);
	
	if(index != -1) {
		printf("Key %s : %d\n", key, dict->entry[index].value);
		return;
	}
	
	else {
		printf("Erreur Key not found !\n");
	}

}


