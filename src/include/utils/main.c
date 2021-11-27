
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "utils.h"


int main(int argc, char *argv[]) {

	dict_t dict_mcv;
	
	
	dict_mcv = dict_new();
	dict_add(dict_mcv, "ok");
	display_dict(dict_mcv, "ok");
	dict_free(dict_mcv);
	display_dict(dict_mcv, "ok");
	

}
