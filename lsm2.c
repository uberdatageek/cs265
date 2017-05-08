#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
typedef int bool;
#define true 1
#define false 0

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>


#include "bloom.h"

#define MAXC 256

#define array_size 100000
#define threshold 50000
#define factor 6
#define FPR 0
#define maintain_dir 0 //change to 0 (default setting) to wipe out all directories and files before next dataset is loaded

//time for put queries
clock_t p_start, p_end;

//time for get queries
clock_t g_start, g_end;


//time for range queries
clock_t r_start, r_end;


//time for delete queries
clock_t d_start, d_end;

//global variable
int total_array_size;

int level_1_threshold = threshold * factor; //REMOVE/////////////

int counter = 0; 

char absolute_path[70]="/Users/eaofxr/desktop/c/daslab/generator/";

//for testing lookup_lsm........
int l_test = 0;
int theFirst, theSecond, originalA, originalB;
///////////////////////////////


typedef struct {
    int run_merge;
    int run0;
    int level_count; 
    int count_A[7];
    int count_B[7];
    int level[7];
    int level_merge[7];
    int bloom_hit[6];
    int bloom_miss[6];
} lsm_meta;

typedef struct  {
    int keys;
    int values;
    int delete_me;
} lsm;

off_t fsize(const char *filename) {
    struct stat st;

    if (stat(filename, &st) == 0)
        return st.st_size;

    return -1;
}


//for qsort
int cmp (const void * a, const void * b)
{
   return ( *(int*)a - *(int*)b );
}

void merge_arrays(lsm *ptr2, lsm *ptr3, int destTree,lsm_meta *lm, bloom_t current_bloom_filter,bloom_t next_bloom_filter) {

    //int run;
    int dest, dest2;
    int final_count;
    int next_level;
    int level_threshold;

    FILE* lsmTree;
    FILE* lsmTree2;
    FILE* lsmTree3;

    char dest1_base[20];
    char dest2_base[20];

    char lsmTree3_base[22];
    //char lsmTree3_file[10];

    int i;


    sprintf(dest1_base, "lsm_tree/%d/a.dat", destTree);
    sprintf(dest2_base, "lsm_tree/%d/b.dat", destTree);

    //destTree = level (should be populated in lsm_flush)
    dest = lm[0].count_A[destTree];
    dest2 = lm[0].count_B[destTree];

    /*printf("DEST %d\n",dest);
    printf("DEST 2 %d\n",dest2);
    printf("THE DEST TREE IS: %d\n",destTree);*/


    lsmTree = fopen(dest1_base,"rb");
    lsmTree2 = fopen(dest2_base,"rb");

    if(fread(ptr2,sizeof(lsm),dest,lsmTree) != dest)
    {
        printf("\n fread() [a] failed\n");//chg
        exit(1);
    }

    fclose(lsmTree);

    if(fread(ptr3,sizeof(lsm),dest2,lsmTree2) != dest2)
    {
        printf("\n fread() [b] failed\n"); //chg
        exit(1);
    }

    fclose(lsmTree2);


    //get total size of both files combined
    total_array_size = fsize(dest1_base) + fsize(dest2_base);

    /*printf("ARRAY1 SIZE: %d\n",fsize(dest1_base));
    printf("ARRAY2 SIZE: %d\n",fsize(dest2_base));*/
    printf("TOTAL ARRAY SIZE %d\n",total_array_size);

    //since level counts start at zero 2 => level c1, 3 => level c2 etc.
    //checking for next level

    //current level destTree
    next_level = destTree + 1;

    //calculate level_threshold here
    //level_threshold = new_factor * threshold;


    int x = factor; //factor
    int z;
    for(z=1; z<destTree; z++) {
        x = x * factor;   
    }
    level_threshold = x * threshold;

    printf("LEVEL THRESHOLD: (level)%d (size)%d\n",destTree,level_threshold);

    int move_to = 0;

    //////////////////////////////
    int next_level_file_count;

    next_level_file_count = lm[0].level[next_level];

    //merge array2 with array1
    for(i=0; i<dest2; i++) {

        ptr2[dest].keys = ptr3[i].keys;
        ptr2[dest].values = ptr3[i].values;
        ptr2[dest].delete_me = ptr3[i].delete_me;
    }

    final_count = dest + dest2;

        /*printf("TOTAL ARRAY SIZE: %d\n",total_array_size);
        printf("LEVEL THRESHOLD: %d\n",level_threshold);
        printf("NEXT LEVEL FILE COUNT %d\n",next_level_file_count);*/


        if((next_level_file_count < 2) & (total_array_size > level_threshold)) { 

            char d[70]; //"/Users/eaofxr/desktop/c/daslab/generator/";
            strcpy(d,absolute_path);

            sprintf(d,"lsm_tree/%d/",next_level);

            struct stat s = {0};

            if (stat(d, &s)) {

                //create directory
                mkdir(d, 0777);
                //increment level_count
                lm[0].level_count = lm[0].level_count + 1;

            }  


            move_to = 1;

            if(next_level_file_count == 1){
                lm[0].count_B[next_level] = final_count;
                lm[0].level[next_level] = 2; //file count in the next level -> 2
            } else {
                lm[0].count_A[next_level] = final_count;
                lm[0].level[next_level] = 1; //file count in the next level -> 1
            }
            ////////////////////////////////////////////////

            if(next_level_file_count == 1) {
                //rename file to be flushed to b.dat
                sprintf(lsmTree3_base, "lsm_tree/%d/b.dat", next_level);

            } else { //no files exist in level
                sprintf(lsmTree3_base, "lsm_tree/%d/a.dat", next_level);

            }

            //current level file count should be set to zero
            lm[0].level[destTree] = 0;

            lm[0].count_A[destTree] = 0;
            lm[0].count_B[destTree] = 0;

            //increment level count because next level is added
            //lm[0].level_count = lm[0].level_count + 1;


        } else { //stay in same level

            lm[0].level_merge[destTree] = 1;
            lm[0].count_A[destTree] = final_count;     

            //strcpy(lsmTree3_base,"lsm_tree/");
            sprintf(lsmTree3_base, "lsm_tree/%d/c.dat", destTree);

            lm[0].level[destTree] = 1;
            lm[0].count_B[destTree] = 0;

        }

        lsmTree3 = fopen(lsmTree3_base,"wb");

    
        //rebuild bloom filter
        for(i=0; i<final_count; i++) {
            switch(move_to){
                case 0:
                    bloom_add(current_bloom_filter,&ptr2[i].keys);
                    break;
                case 1:
                    bloom_add(next_bloom_filter,&ptr2[i].keys);
                    break;
            }
        }

        //sort new purged array
        qsort(ptr2, final_count, sizeof(*ptr2), cmp);

        //Write data to disk
        if (fwrite(ptr2, sizeof(lsm), final_count, lsmTree3) != final_count) //counter is the count of the lsm tree being flushed (run_count)
        {
            printf("\n fwrite() failed\n"); //chg
            exit(1);
        }

        fclose(lsmTree3);


        if(move_to == 0){ //means that merged occur in level one and array didn't exceed threshold


            char fullpath3[20];
            sprintf(fullpath3,"lsm_tree/%d/b.dat",destTree);
            remove(fullpath3);

            char oldName[20];
            sprintf(oldName,"lsm_tree/%d/c.dat",destTree);

            char newName[20];
            sprintf(newName,"lsm_tree/%d/a.dat",destTree);

            //rename c1c to c1a....
            if(rename(oldName, newName) != 0)
                {
                    printf("\n rename() failed\n"); //chg
                }

        }


        if(move_to == 1){  //delete both files in original destination level because flushed to next level

            char fullpath4[50];
            sprintf(fullpath4,"lsm_tree/%d/a.dat",destTree);
            remove(fullpath4); 

            char fullpath5[20];
            sprintf(fullpath5,"lsm_tree/%d/b.dat",destTree);
            remove(fullpath5); 

            //wipe out previous bloomfilter. This is UBER important
            //memset(&current_bloom_filter, 0, sizeof(current_bloom_filter));
        }


        //unlock level

        if(move_to == 1){

            lm[0].level_merge[next_level] = 0;
            //lm[0].level[next_level] = next_level;

        } else {

            lm[0].level_merge[destTree] = 0;

        }

    if(lm[0].run0 == 1){ //wipe out run
        lm[0].run0 = 0;
        lm[0].count_B[0] = 0;
    }



}


void lsm_flush(lsm *ptr, lsm *run, lsm *lsm_purge,lsm_meta *lm, bloom_t bloom1){ //pointer to first bloom filter -> c1 level


    //Move data to RUN array -> before FLUSHING to disk (c1)/////////
    memcpy(&run,&ptr,sizeof(ptr));

    //update run_count
    lm[0].count_B[0] = lm[0].count_A[0];

    ///////extra/////////////////////
    lm[0].count_B[0] = lm[0].count_A[0]; //comes from put   

    /////////////////////////////////

    lm[0].run0 = 1;

    //set c0_count to 0
    lm[0].count_A[0] = 0;

    /////////////////////////////////////////


    //change status of run0 to equal 1 -> exist
    if(lm[0].run0 == 0) {
        lm[0].run0 = 1;
    }

    //reset counter
    counter = 0;

    int move_to = 1;
    int z;
    int purge_count = 0;

    FILE* lsm_tree;

    //ensure that level is noted in lsm_meta
    if(lm[0].level_count == 1){ //only increment if one level is accounted for in lsm_meta
        //set to two to account for the addition of level C1
        lm[0].level_count = 2;
    }

    //create c1 directory////////////////////////////////////////////
    //char d[] = "/Users/eaofxr/desktop/c/daslab/generator/lsm_tree/1";

    char d[70]; // = "/Users/eaofxr/desktop/c/daslab/generator/";

    strcpy(d,absolute_path);

    sprintf(d,"lsm_tree/%d/",1);

    struct stat s = {0};

    if (stat(d, &s)) {
        //create directory
        mkdir(d, 0777);
    }  
    //////////////////////////////////////////////////////////////////


    char tree1[20];
    char tree2[20];
    int c1_file_count_hack = 0;

    char meta_test[20];
    char meta_test1[20];
    strcpy(meta_test, "lsm_tree/1/b.dat"); //chg
    strcpy(meta_test1, "lsm_tree/1/a.dat");


    if(fsize(meta_test1) != -1){ //file a exists
        c1_file_count_hack = c1_file_count_hack + 1;
    }

    if(fsize(meta_test) != -1){ //file b exists
        c1_file_count_hack = c1_file_count_hack + 1;
    }


    lm[0].level[1] = c1_file_count_hack;

    if(lm[0].level[1] < 2) {
        //lock c0 and run
        lm[0].level_merge[0] = 1;
        ///////////////////////////////////////

        //purge deleted elements/////////////////////////
        for(z=0; z<lm[0].count_B[0]; z++) {

            if(run[z].delete_me != 1) {
                lsm_purge[z].keys = run[z].keys;
                lsm_purge[z].values = run[z].values;
                lsm_purge[z].delete_me = run[z].delete_me; //or could just be set to equal zero
                //build new bloom filter for after purge
                bloom_add(bloom1, &lsm_purge[z].keys); //chg
                purge_count++;
            }

        }
        /////////////////////////////////////////////////

        if(lm[0].level[1] == 1){
            move_to = 2;
        }


        if (move_to == 1) {

            sprintf(tree1,"lsm_tree/%d/a.dat",1);
            lsm_tree = fopen(tree1, "wb");
            lm[0].level[1] = lm[0].level[1] + 1;
            lm[0].count_A[1] = purge_count;

        } else { //array was flushed to level c1 as b.dat

            sprintf(tree2,"lsm_tree/%d/b.dat",1);
            lsm_tree = fopen(tree2, "wb");
            lm[0].level[1] = lm[0].level[1] + 1;
            lm[0].count_B[1] = purge_count;
        }        

        //sort new purged array
        qsort(lsm_purge, purge_count, sizeof(*lsm_purge), cmp);

        if (fwrite(lsm_purge, sizeof(lsm), purge_count, lsm_tree) != purge_count) //counter is the count of the lsm tree being flushed (run_count, which equals to c0_count at the point of flushing)
        {
            printf("\n fwrite() failed\n"); //chg
            exit(1);
        }

        fclose(lsm_tree);

        //Update file count in level c0 (files_in[0]) and in level c1 (files_in[1])
        lm[0].count_B[0] = 0;
        //////////////////////////////////

        //wipe out run
        //memset(&run,0,sizeof(run));

        //unlock c0 and run
        lm[0].level_merge[0] = 0;
        //////////////////////////////////

        lm[0].run0 = 0;

    }

}



//NEED to rebuild fence pointer to handle possibility of two files a. get num of files in level b. get the count for each - return a number to reflect the correct file -> 1 = a.dat and 2 = b.dat
int fence_pointer(lsm *ptr,lsm_meta *lm, int key, int level,int t_array){
    //int results = 0;
    int the_count;
    int min,max,max_index;
    int results = 0;


    //get 1st key of array
    min = ptr[0].keys;

    if(t_array == 1) {

        the_count = lm[0].count_A[level];

    } else { //must be 2

        the_count = lm[0].count_B[level];

    }



    //sort -> should maybe move outside
    qsort(ptr,the_count,sizeof(*ptr),cmp);

    /*int xy;
    for(xy=0; xy<the_count; xy++){
        printf("THE DAMM KEY: %d\n",ptr[xy].keys);
    }*/

    max_index = the_count - 1;
    max = ptr[max_index].keys;

    int is_there = 0;

    if(key>=min){
        is_there = is_there + 1;;
        //printf("[1]xxxxxxxxxxx IS_THERE IS: %d\n",is_there);
    }
    if(key<=max){
        is_there = is_there + 1;
        //printf("[2]xxxxxxxxxxx IS_THERE IS: %d\n",is_there);
    }


    if(is_there == 2){
        results = 1;
    }  

    return results;
}


//Weak hashing function
unsigned int hash_func (const void *key) {
    const int hash = (unsigned int)key % array_size;
    return hash;
}


//modified LSM_META functions///////////////////////

//replaces files_in[].... and 
int get_file_count(lsm_meta *lm, int level) { //get the file count for each level

    int the_count;

    if((level > 0) & (level < 7)){
        the_count = lm[0].level[level];
    } else {
        the_count = 0;
    }

    return the_count;

}

//update file count in levels
int update_file_count(lsm_meta *lm, int level, int action) {

    int results;

    if((level > 0) & (level < 7)){

        switch(action){
            case 1:
                lm[0].level[level] = lm[0].level[level] + 1;
                results = 1;
                break;
            case 2:
                lm[0].level[level] = lm[0].level[level] - 1;
                results = 2;  
                break;
            default:
                results = 0;                                      
        }
        
    } else {
        results = 0;
    }      

    return results;

}


//get element count -- replace count_A[]....
int get_element_count(int file, lsm_meta *lm, int level) {

    int element_count;

    if(level < 7){

        if(file == 1) {
            element_count = lm[0].count_A[level];
        } else if (file == 2){
            element_count = lm[0].count_B[level];
        } else {
            element_count = 0;
        } 

    } else {
        element_count = 0;
    }

    return element_count;

}
////////////////////////////////////////////////////

void lsm_put (lsm *ptr,lsm *lsm_purge, lsm_meta *lm,lsm *run,bloom_t bloom1,int key, int value) {
    int index_size = counter;
    int current_c0_size;
    current_c0_size = sizeof(*ptr) * counter;

    //////TEMP -> test///////////////////////////

    l_test++;


    if(l_test == 1){
        theFirst = key;
        originalA = value;
        printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n");
    } 

    if (l_test == 58){
        theSecond = key;
        originalB = value;
        printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n");
    } 

    /////////////////////////////////////

    if(current_c0_size >= threshold){

        //Flush c0
        lsm_flush(ptr,run,lsm_purge,lm,bloom1);

    } else { //insertions performed IF flush isn't done. Otherwise a key/value pair would be lost each time a flush occurs

        ptr[index_size].keys = key;
        ptr[index_size].values = value;

        counter++;

        //update_meta(lm,3); //increment c0 count
        lm[0].count_A[0] = lm[0].count_A[0] + 1;

    }


}


int lsm_delete (lsm *ptr, int key) {
    int index_size = counter; 
    int index_start = index_size - 1;
    int i;
    int x = 0;

    //for(i=0; i<index_size; i++){
    for(i=index_start; i>0; i--){
         if (ptr[i].keys == key) {
            x = ptr[i].delete_me = true;
            break;
         }   
    }

    //insert marker to inform lookups and puts
    ptr[index_size].keys = key;
    ptr[index_size].values = -1;
    ptr[index_size].delete_me = true;
    counter++;

    return x;

}


void lsm_read(lsm *fptr, int count, FILE* lsm_tree){

        if (fread(fptr, sizeof(lsm), count, lsm_tree) != count) //counter should be the size of the c1 tree (run_count + c1_count)
        {
            printf("\n fread() [lsm_read function] failed\n"); //chg
            exit(1);
        }

    fclose(lsm_tree);

}


void lsm_write(lsm *ptr, int count, FILE* lsm_tree){//add location argument -> int lsmTree

        if (fwrite(ptr, sizeof(lsm), count, lsm_tree) != count) //counter is the count of the lsm tree being flushed (run_count, which equals to c0_count at the point of flushing)
        {
            printf("\n fwrite() failed\n"); //chg
            exit(1);
        }

    fclose(lsm_tree);
}


int lsm_traverse(lsm *ptr, int key, int index_size) {
    int i;
    int x = 0;

    for (i=0; i<index_size; i++) {

        if((ptr[i].keys == key) && (ptr[i].delete_me != 1)) { //chg
            x = ptr[i].values;
            break;
        }
    }

    return x;

}

//one pointer with large allocation (*ptr1 -> holds on disk arrays) -> *ptr => *c0 and *run => *run
int lsm_lookup (lsm *ptr,lsm *run,lsm *ptr1,lsm *ptr2, lsm_meta *lm, bloom_t bloom1, bloom_t bloom2, bloom_t bloom3, bloom_t bloom4, bloom_t bloom5, bloom_t bloom6, int key) {

    int index_size;
    FILE* lsm_tree;
    FILE* lsm_tree2;
    char tree1[20];
    char tree2[20];
    int x,i;
    int test_results = 0;

    int file_count,file_a,file_b;
    char the_tree[20];
    int the_file = 0;


    if(lm[0].level_merge[0] == 0) { //If c0 isn't flushing to run etc. -> locks all arrays in memory with value of 1

        //Put sort algorithm here/////////////////////

        //search c0
        index_size = lm[0].count_A[0];
        x = lsm_traverse(ptr, key, index_size);

        if(x == 0) {
            if(lm[0].run0 == 1){ //run exists
                //search run
                index_size = lm[0].count_B[0];
                x = lsm_traverse(run, key, index_size);
            }
        }

    }


    if(x == 0){
        
        for(i=1; i<lm[0].level_count; i++) {

            sprintf(tree1,"lsm_tree/%d/a.dat",i);
            sprintf(tree2,"lsm_tree/%d/b.dat",i);
             
            //switch statement for all bloom filters 6 max

            switch(i) {
                case 1:
                    test_results = bloom_test(bloom1,&key);
                    break;
                case 2:
                    test_results = bloom_test(bloom2,&key);
                    break;
                case 3:
                    test_results = bloom_test(bloom3,&key);
                    break;
                case 4:
                    test_results = bloom_test(bloom4,&key);
                    break;
                case 5:
                    test_results = bloom_test(bloom5,&key);
                    break;
                case 6:
                    test_results = bloom_test(bloom6,&key);
                    break;
                default:
                    test_results = 0;
            }


                //inside for loop
                char exist_test[20];
                char exist_test1[20];
                sprintf(exist_test, "lsm_tree/%d/a.dat",i);
                sprintf(exist_test1, "lsm_tree/%d/b.dat",i);


                if(fsize(exist_test) != -1){ //file a exists
                    
                    lsm_tree = fopen(tree1,"rb");

                    //read file
                    lsm_read(ptr1,lm[0].count_A[i], lsm_tree);

                    file_a = fence_pointer(ptr1,lm,key,i,1);

                } else {
                    file_a = 0;
                }

                if(fsize(exist_test1) != -1){ //file b exists
                    
                    lsm_tree2 = fopen(tree2,"rb");

                    //read file
                    lsm_read(ptr2,lm[0].count_B[i],lsm_tree2);                   

                    file_b = fence_pointer(ptr2,lm,key,i,2);
                } else {
                    file_b = 0;
                }



                if((test_results == 1) && (x == 0)){ //registered true (bloom filter)

                    file_count = get_file_count(lm,i);

                    switch(file_count){

                        case 1: //one file (a)
                            //printf("FENCE POINTER FOR FILE A ON LEVEL %d RESULT IS: %d\n",i,file_a);
                            if(file_a == 1){
                                //run get function on file a
                                sprintf(the_tree,"lsm_tree/%d/a.dat",i);
                                the_file = 1;
                                x = 1;
                                lm[0].bloom_hit[i] = lm[0].bloom_hit[i] + 1;
                            } else {
                                x = 0;
                                //INCREMENT FPR because bloom said yes but fence pointer said nope
                                lm[0].bloom_miss[i] = lm[0].bloom_miss[i] + 1;
                            }
                            break;
                        case 2: //two files (a & b) -> only one of the two files should have the data
                            //printf("[2] FENCE POINTER FOR FILE A ON LEVEL %d RESULT IS: %d\n",i,file_a);
                            if(file_a == 1){
                                //run get function on file a
                                sprintf(the_tree,"lsm_tree/%d/a.dat",i);
                                the_file = 1;
                                x = 1;
                                lm[0].bloom_hit[i] = lm[0].bloom_hit[i] + 1;
                            } else if (file_b == 1) {
                                //run get function on file b
                                sprintf(the_tree,"lsm_tree/%d/b.dat",i);
                                the_file = 2;
                                x = 1;
                                lm[0].bloom_hit[i] = lm[0].bloom_hit[i] + 1;
                            } else {
                                x = 0;
                                //increment FPR
                                lm[0].bloom_miss[i] = lm[0].bloom_miss[i] + 1;
                            }
                            break;
                        default:
                            x = 0;

                    }



                    if(x == 1){


                        index_size = get_element_count(the_file,lm,i);

                        lsm_tree = fopen(the_tree,"rb");
                                             
                        //search data for key/value pair
                        switch(the_file){
                            case 1:
                                lsm_read(ptr1, index_size, lsm_tree);
                                x = lsm_traverse(ptr1, key, index_size);
                                break;
                            case 2:
                                lsm_read(ptr2, index_size, lsm_tree2);
                                x = lsm_traverse(ptr2, key, index_size);
                                break;
                        }


                    }


                } //end test_results == 


        }

    }
  

    return x;

}

int lsm_range (lsm *ptr,lsm *run,lsm *ptr1,lsm *ptr2,lsm_meta *lm, bloom_t bloom1, bloom_t bloom2, bloom_t bloom3, bloom_t bloom4, bloom_t bloom5, bloom_t bloom6, int minkey, int maxkey) {

    int y;
    int searchKey;
    int theVal;

    for (y=minkey; y<=maxkey; y++) {
        searchKey = y;
        theVal = lsm_lookup(ptr,run,ptr1,ptr2,lm,bloom1,bloom2,bloom3,bloom4,bloom5,bloom6,searchKey); 
        return theVal; 
    } 

}





int main(int argc, char **argv) 
{

    srand ( time(NULL) );

    double p_time_used; 
    double g_time_used;
    double r_time_used;
    double d_time_used;

    lsm *c0;
    c0=malloc(array_size*sizeof(lsm));

    lsm_meta *lm;
    lm=malloc(10*sizeof(lsm_meta));

    lsm *run; //passed to function for saving flushed data from level0 to level 1
    run=malloc(array_size*sizeof(lsm));

    lsm *lsm_purge; //passed to function for saving flushed data from level1 to level 2
    lsm_purge=malloc(array_size*sizeof(lsm));

    lsm *array1; //for files
    array1=malloc(5*array_size*sizeof(lsm));   

    lsm *array2; //for files
    array2=malloc(5*array_size*sizeof(lsm)); 

    lsm *ptr1; //for files
    ptr1=malloc(5*array_size*sizeof(lsm)); 

    lsm *ptr2; //for files
    ptr2=malloc(5*array_size*sizeof(lsm)); 

    bloom_t bloom1 = bloom_create(array_size);
    bloom_add_hash(bloom1, hash_func);

    bloom_t bloom2 = bloom_create(array_size);
    bloom_add_hash(bloom2, hash_func);

    bloom_t bloom3 = bloom_create(array_size);
    bloom_add_hash(bloom3, hash_func);

    bloom_t bloom4 = bloom_create(array_size);
    bloom_add_hash(bloom4, hash_func);

    bloom_t bloom5 = bloom_create(array_size);
    bloom_add_hash(bloom5, hash_func);

    bloom_t bloom6 = bloom_create(array_size);
    bloom_add_hash(bloom6, hash_func);


    qsort(c0, counter, sizeof(*c0), cmp);


    if(lm[0].level_count == 0){ 
        //Set value to one to account for the creation of level c0
        lm[0].level_count = 1;
    }


    //create necessary directory (lsm_tree)
    char tree_d[70];

    strcpy(tree_d,absolute_path);

    strcat(tree_d,"lsm_tree");

    struct stat s = {0};

    if (stat(tree_d, &s)) {

        mkdir(tree_d, 0777);

    }
    ////////////////////////////////////////


    ///////////parsing///////////////////////////////

    //NOTE: Put has been commented out/////////////////////////////////
    char buf[MAXC] = "";

    while (fgets (buf, MAXC, stdin))    // read each line into buf
    {
        char c;
        int num1, num2, rtn;     // vars for values and sscanf return 

        rtn = sscanf (buf, "%c %d %d", &c, &num1, &num2); 

        if (rtn == 0) { // no successful conversions took place
            fprintf (stderr, "error: no values parsed from line.\n");
            continue;
        }

        if (!*buf || *buf == '\n') {    // check if buf was empty line 
            fprintf (stderr, "error: line is empty or contians only newline.\n");
            continue;
        }

        if (*buf < 'a' || 'z' < *buf) {  // check first char not a-z 
            fprintf (stderr, "error: no lowercase char beginning line.\n");
            continue;
        }

        switch (rtn) {  // switch on number of successful conversions 

            case 3:     // three successful conversions 
                //printf ("all values: '%c'  %d  %d\n", c, num1, num2);
                switch(c){
                    case 'p': //put functions runs here
                        p_start = clock();
                        //printf("PUT -> THE KEY: %d\n",num1);
                        //printf("PUT -> THE VALUE: %d\n",num2);

                        lsm_put(c0,lsm_purge,lm,run,bloom1,num1,num2);

                        p_end = clock();
                        p_time_used += ((double)(p_end - p_start)) / CLOCKS_PER_SEC;

                        //test merge///////////////////////
                        p_start = clock();
                        int y;

                        for(y=1; y<lm[0].level_count; y++){

                            switch(y){
                                case 1:
                                    
                                    if(lm[0].level[y] == 2){
                                        printf("ENTERING LEVEL 1\n");
                                        lm[0].level_merge[y] = 1;
                                        merge_arrays(array1,array2,y,lm,bloom1,bloom2);
                                        lm[0].level_merge[y] = 0;
                                    } 
                                    break;   
                                case 2:
                                    
                                    if(lm[0].level[y] == 2){
                                        printf("ENTERING LEVEL 2\n");
                                        lm[0].level_merge[y] = 1;
                                        merge_arrays(array1,array2,y,lm,bloom2,bloom3);
                                        lm[0].level_merge[y] = 0;
                                    }                                
                                    break;  
                                case 3:
                                    
                                    if(lm[0].level[y] == 2){
                                        printf("ENTERING LEVEL 3\n");
                                        lm[0].level_merge[y] = 1;
                                        merge_arrays(array1,array2,y,lm,bloom3,bloom4);
                                        lm[0].level_merge[y] = 0;
                                    }   
                                    break;
                                case 4: 
                                    
                                    if(lm[0].level[y] == 2){
                                        printf("ENTERING LEVEL 4\n");
                                        lm[0].level_merge[y] = 1;
                                        merge_arrays(array1,array2,y,lm,bloom4,bloom5);
                                        lm[0].level_merge[y] = 0;
                                    }                                   
                                    break;
                                case 5:
                                    
                                    if(lm[0].level[y] == 2){
                                        printf("ENTERING LEVEL 5\n");
                                        lm[0].level_merge[y] = 1;
                                        merge_arrays(array1,array2,y,lm,bloom5,bloom6);
                                        lm[0].level_merge[y] = 0;
                                    } 
                                    break; 
                                case 6:
                                    
                                    if(lm[0].level[y] == 2){
                                        printf("ENTERING LEVEL 6\n");
                                        lm[0].level_merge[y] = 1;
                                        merge_arrays(array1,array2,y,lm,bloom6,bloom6);
                                        lm[0].level_merge[y] = 0;
                                    } 
                                    break;                                                                                                                      
                            }
    
                        }

                        p_end = clock();
                        p_time_used += ((double)(p_end - p_start)) / CLOCKS_PER_SEC;

                        ////////////////////////////////////
                    case 'l': //put functions runs here
                        p_start = clock();
                        //printf("PUT -> THE KEY: %d\n",num1);
                        //printf("PUT -> THE VALUE: %d\n",num2);
                        p_end = clock();
                        p_time_used += ((double)(p_end - p_start)) / CLOCKS_PER_SEC;

                        break;
                    case 'r': //range function runs here
                        r_start = clock();
                        //printf("RANGE -> MIN: %d\n",num1);
                        //printf("RANGE -> MAX: %d\n",num2);

                        lsm_range(c0,run,ptr1,ptr2,lm,bloom1,bloom2,bloom3,bloom4,bloom5,bloom6,num1,num2);

                        r_end = clock();
                        r_time_used += ((double)(r_end - r_start)) / CLOCKS_PER_SEC;
                        
                        break;                      
                }
                break;

            case 2:     // two successful conversions 
                //printf ("two values: '%c'  %d\n", c, num1);
                switch(c){
                    case 'g': //get function runs here
                        g_start = clock();
                        //printf("GET -> THE KEY: %d\n",num1);
                        lsm_lookup(c0,run,ptr1,ptr2,lm,bloom1,bloom2,bloom3,bloom4,bloom5,bloom6,num1); 
                        g_end = clock();
                        g_time_used += ((double)(g_end - g_start)) / CLOCKS_PER_SEC;

                        break;
                    case 'd': //d function runs here
                        d_start = clock();
                        //printf("D -> THE KEY: %d\n",num1);
                        lsm_delete (c0,num1);
                        d_end = clock();
                        d_time_used += ((double)(d_end - d_start)) / CLOCKS_PER_SEC;
                        
                        break;
                }
                break;

            default:    // one or less (need at least two 
                fprintf (stderr, "error: no character and value on line.\n");
        }



    }
    ////////////////////////////////////////////////////////////////

    //DUMMY TEST
    /*int y;
    int yx;
    int r;

    int theFirst, theSecond, originalA, originalB;

    for(y=0; y<48000; y++) {//

        r = rand();
        yx = rand();

        if(y == 2) {
            theFirst = r;
            originalA = yx;
        }

        if(y == 3) {
            theSecond = r;
            originalB = yx;
        }

        c0[y].keys = r;
        c0[y].values = yx;

        lsm_put(c0,lsm_purge,lm,run,bloom1,r,yx);

        //printf("c0 Count: %d\n", lm[0].c0_count);
    
    }*/

    /*printf("VALUE 1: %d\n",originalA);
    printf("KEY 1: %d\n",theFirst);

    printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX LOOKUP 1: %d\n",lsm_lookup (c0,run,ptr1,ptr2,lm,bloom1,bloom2,bloom3,bloom4,bloom5,bloom6,theFirst));

    printf("VALUE 2: %d\n",originalB);
    printf("KEY 2: %d\n",theSecond);

    printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX LOOKUP 2: %d\n",lsm_lookup (c0,run,ptr1,ptr2,lm,bloom1,bloom2,bloom3,bloom4,bloom5,bloom6,theSecond));*/





     //Wipe out all files in directories
    if(maintain_dir == 0){ //delete all directories and files for next run. This should be the DEFAULT setting
        int x;
        for(x=1; x<lm[0].level_count; x++){
            char fullpath3[20];
            char fullpath2[20];
            sprintf(fullpath3,"lsm_tree/%d/b.dat",x); 
            sprintf(fullpath2,"lsm_tree/%d/a.dat",x);
            remove(fullpath3);
            remove(fullpath2);
            
            //wipe out directories
            if(x != 0) {
                char d_path[70]; // = "/Users/eaofxr/desktop/c/daslab/generator/lsm_tree/";
                strcpy(d_path,absolute_path);
                sprintf(d_path,"lsm_tree/%d",x);
                rmdir(d_path);
            }
       
        } 

        //wipe out the tree directory
        char the_tree[70]; // = "/Users/eaofxr/desktop/c/daslab/generator/lsm_tree/";
        strcpy(the_tree,absolute_path);
        strcat(the_tree,"lsm_tree");
        rmdir(the_tree);        
    }


    printf("\n");
    printf("PUT CALLS TOOK %f SECONDS\n", p_time_used);
    printf("GET CALLS TOOK %f SECONDS\n", g_time_used);
    printf("RANGE CALLS TOOK %f SECONDS\n", r_time_used);
    printf("DELETE CALLS TOOK %f SECONDS\n", d_time_used);
    printf("\n");

    printf("TOTAL LEVEL COUNT: %d\n",lm[0].level_count);

    int actual_dir = lm[0].level_count - 1;
    printf("Actual disk levels created %d\n",actual_dir); //sans the c0 level

    printf("\n");
    printf("\n");


    free(lsm_purge);
    free(ptr1);
    free(ptr2);
    free(array1);
    free(array2);       
    free(c0);
    free(lm);
    free(run);



    return 0;   


}



