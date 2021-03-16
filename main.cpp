#include <iostream>
#include <random>
#include <vector>
#include <mutex>
#include <atomic>
#include <unordered_set>
#include <algorithm>

#include "generator.h"
#include "assert_msg.h"
#include "LockFreeCuckoo.h"
#include "timer.h"
#include "ycsb_loader.h"

#define LOCAL 1

#ifdef LOCAL
string load_filepath = "/mnt/c/CLionProjects/Research/Cuckoo_improve/improve_test/load-a.dat";
string run_filepath = "/mnt/c/CLionProjects/Research/Cuckoo_improve/improve_test/run-a.dat";
#else
string load_filepath = "/mnt/nvme/czl/YCSB/load-c-200m-8B.dat";
    string run_filepath = "/mnt/nvme/czl/YCSB/run-c-200m-8B.dat";
#endif


typedef lockFreeCuckoo LFCuckooHash;

LFCuckooHash * store;


ThreadBarrier *tb;

static const int op_type_num = 4;
enum Op_type {
    Find = 0,
    Set = 1, // Insert_or_Assign
//    Update = 3,
    Erase = 2,
    Insert = 3,
    Rand = 4
};


static const uint16_t default_key_len = 8;
static const uint16_t default_value_len = 8;

typedef struct Request {
    Op_type optype;
    char *key;
    uint16_t key_len;
    char *value;
    uint16_t value_len;
};

Request *requests;
//Request *loads;
std::vector<YCSB_request *> ycsb_loads;
std::vector<YCSB_request *> ycsb_requests;


bool YCSB;
int thread_num = 1;
int insert_thread_num = 1;
size_t init_size1 = 1;
size_t init_size2 = 1;
size_t key_range = 1;
size_t total_count = 1;
int timer_range = 0;
int distribution = 0; // 0 unif; 1 zipf
Op_type op_chose = Rand;

static size_t find_success, find_failure;
static size_t insert_success, insert_failure;
static size_t set_insert, set_assign;
static size_t update_success, update_failure;
static size_t erase_success, erase_failure;
static size_t read_add_g;
static size_t kick_num,
        depth0, // ready to kick, then find empty slot
kick_lock_failure_data_check,
        kick_lock_failure_haza_check,
        kick_lock_failure_other_lock,
        kick_lock_failure_haza_check_after,
        kick_lock_failure_data_check_after,
        key_duplicated_after_kick;

size_t kick_path_length_log[6];

thread_local static size_t find_success_l, find_failure_l;
thread_local static size_t insert_success_l, insert_failure_l;
thread_local static size_t set_insert_l, set_assign_l;
thread_local static size_t update_success_l, update_failure_l;
thread_local static size_t erase_success_l, erase_failure_l;
thread_local static size_t read_add_l;

uint64_t *runtimelist;
uint64_t op_num;

std::atomic<int> stopMeasure(0);

enum cuckoo_status {
    ok,
    failure,
    failure_key_not_found,
    failure_key_duplicated,
    failure_table_full,
    failure_under_expansion,
    need_kick
};
struct table_position {
    size_t index;
    size_t slot;
    cuckoo_status status;
};


template<typename R>
class RandomGenerator {
public:
    static inline void generate(R *array, size_t range, size_t count, double skew = 0.0) {
        struct stat buffer;
        if (skew < zipf_distribution<R>::epsilon) {
            std::default_random_engine engine(
                    static_cast<R>(chrono::steady_clock::now().time_since_epoch().count()));
            std::uniform_int_distribution<size_t> dis(0, range + FUZZY_BOUND);
            for (size_t i = 0; i < count; i++) {
                array[i] = static_cast<R>(dis(engine));
            }
        } else {
            zipf_distribution<R> engine(range, skew);
            mt19937 mt;
            for (size_t i = 0; i < count; i++) {
                array[i] = engine(mt);
            }
        }
    }
};

inline void merge_log() {
    __sync_fetch_and_add(&find_success, find_success_l);
    __sync_fetch_and_add(&find_failure, find_failure_l);
    __sync_fetch_and_add(&insert_success, insert_success_l);
    __sync_fetch_and_add(&insert_failure, insert_failure_l);
    __sync_fetch_and_add(&set_insert, set_insert_l);
    __sync_fetch_and_add(&set_assign, set_assign_l);
    __sync_fetch_and_add(&update_success, update_success_l);
    __sync_fetch_and_add(&update_failure, update_failure_l);
    __sync_fetch_and_add(&erase_success, erase_success_l);
    __sync_fetch_and_add(&erase_failure, erase_failure_l);
    __sync_fetch_and_add(&read_add_g, read_add_l);


}


void op_func(const Request &req) {

    Op_type switch_option = op_chose == Rand ? static_cast<Op_type>(rand() % 3) : op_chose;

    switch (switch_option) {

        //switch(Find){
        case Find : {
            auto p = store->Search(req.key, req.key_len);
            if ( p!= nullptr){
                read_add_l += *(uint64_t * )p ;
                find_success_l++;
            }
            else
                find_failure_l++;
        }
            break;
        case Insert : {
            if (store->Insert(req.key, req.key_len, req.value, req.value_len)) {
                insert_success_l++;
            } else {
                insert_failure_l++;
            }
        }
            break;
        case Set : {
            if (store->Insert(req.key, req.key_len, req.value, req.value_len)) {
                set_insert_l++;
            } else {
                set_assign_l++;
            }
        }
            break;
//        case Update :{
//                partial_t partial = (partial_t) req.key;
//                table_position pos = insert_func(req);
//                if(pos.status == failure_key_duplicated){
//                    buckets_[pos.index].mapped(pos.slot) = req.value;
//                    update_success_l++;
//                }else{
//                    update_failure_l++;
//                }
//            }
//            break;
        case Erase : {
            if (store->Delete(req.key, req.key_len)) {
                erase_success_l++;
            } else {
                erase_failure_l++;
            }
        }
            break;

    }

}

void ycsb_op_func(YCSB_request * req){
    switch (req->getOp()) {
        //switch(Find){
        case lookup : {
            auto p = store->Search(req->getKey(), req->keyLength());
            if ( p!= nullptr){
                read_add_l += *(uint64_t * )p ;
                find_success_l++;
            }
            else
                find_failure_l++;
        }
            break;
        case insert : {
            if (store->Insert(req->getKey(), req->keyLength(), req->getVal(), req->valLength())) {
                insert_success_l++;
            } else {
                insert_failure_l++;
            }
        }
            break;
        case update : {
            if (store->Insert(req->getKey(), req->keyLength(), req->getVal(), req->valLength())) {
                set_insert_l++;
            } else {
                set_assign_l++;
            }
        }
            break;
        default:
            ASSERT(false,"optype error");

    }
}



void insert_worker(int tid){

    //Prevent tail debris
    size_t step =  total_count / insert_thread_num;
    size_t num = tid == insert_thread_num -1 ?  step + total_count % insert_thread_num : step;
    size_t base = tid * step;

    for (size_t i = 0; i < num ; i++) {
        if(!YCSB){
            auto &req = requests[base + i];
            if (store->Insert(req.key, req.key_len, req.value, req.value_len)) {
                insert_success_l++;
            } else {
                insert_failure_l++;
            }
        }else{
            auto &req = ycsb_loads[base + i];
            if (store->Insert(req->getKey(), req->keyLength(), req->getVal(), req->valLength())) {
                insert_success_l++;
            } else {
                insert_failure_l++;
            }
        }

    }


//    __sync_fetch_and_add(&kick_num, kick_num_l);
//    __sync_fetch_and_add(&depth0,depth0_l);
//    __sync_fetch_and_add(&kick_lock_failure_data_check,kick_lock_failure_data_check_l);
//    __sync_fetch_and_add(&kick_lock_failure_haza_check,kick_lock_failure_haza_check_l);
//    __sync_fetch_and_add(&kick_lock_failure_other_lock,kick_lock_failure_other_lock_l);
//    __sync_fetch_and_add(&kick_lock_failure_haza_check_after,kick_lock_failure_haza_check_after_l);
//    __sync_fetch_and_add(&kick_lock_failure_data_check_after,kick_lock_failure_data_check_after_l);
//    __sync_fetch_and_add(&key_duplicated_after_kick,key_duplicated_after_kick_l);

//    for(int i = 0; i < 6 ;i++){
//        __sync_fetch_and_add(&kick_path_length_log[i],kick_path_length_log_l[i]);
//    }

    __sync_fetch_and_add(&insert_success, insert_success_l);
    __sync_fetch_and_add(&insert_failure, insert_failure_l);

    store->merge_info();
}

void worker(int tid) {
//    cuckoo_thread_id = tid;
//    store.brown_init_thread(tid);

    size_t step =  total_count / thread_num;
    size_t num = tid == thread_num -1 ?  step + total_count % thread_num : step;
    size_t base = tid * step;

    tb->threadWait();

    Tracer t;
    t.startTime();

    while (stopMeasure.load(std::memory_order_relaxed) == 0) {

        for (size_t i = 0; i < num; i++) {
            if(!YCSB){
                op_func(requests[base + i]);
            }else{
                ycsb_op_func(ycsb_requests[base + i]);
            }

        }

        __sync_fetch_and_add(&op_num, num);

        uint64_t tmptruntime = t.fetchTime();
        if (tmptruntime / 1000000 >= timer_range) {
            stopMeasure.store(1, memory_order_relaxed);
        }
    }
    merge_log();
    runtimelist[tid] = t.getRunTime();
}

void worker_insert_rm(int tid) {

    size_t step =  total_count / thread_num;

    size_t num = tid == thread_num -1 ?  step + total_count % thread_num : step;
    size_t base = tid * step;

    tb->threadWait();

    bool insert_round = true;
    size_t insert_round_count = 0;
    size_t rm_round_count = 0;

    Tracer t;
    t.startTime();

    while (stopMeasure.load(std::memory_order_relaxed) == 0) {
        if(insert_round)
            insert_round_count++;
        else
            rm_round_count++;

        for (size_t i = 0; i < total_count ; i++) {
            Request & req = requests[i];
            if(insert_round){
                if (store->Insert(req.key, req.key_len, req.value, req.value_len)) {
                    insert_success_l++;
                } else {
                    insert_failure_l++;
                }
            }else{
                if (store->Delete(req.key, req.key_len)) {
                    erase_success_l++;
                } else {
                    erase_failure_l++;
                }
            }
        }

        insert_round = !insert_round;

        __sync_fetch_and_add(&op_num, total_count);

        uint64_t tmptruntime = t.fetchTime();
        if (tmptruntime / 1000000 >= timer_range) {
            stopMeasure.store(1, memory_order_relaxed);
        }
    }

    merge_log();
    runtimelist[tid] = t.getRunTime();

    cout <<" insert round "<< insert_round_count<<endl;
    cout <<" rm round "<< rm_round_count<<endl;
}

void prepare(){

    if(!YCSB){
        double skew = distribution == 0? 0.0:0.99;

        uint64_t *loads = new uint64_t[total_count]();
        RandomGenerator<uint64_t>::generate(loads,key_range,total_count,skew);

        srand((unsigned) time(NULL));
        //init_req
        static_assert(op_type_num == 4);
        requests = new Request[total_count];
        for (size_t i = 0; i < total_count; i++) {
            requests[i].optype = Find;//rand() % 2 == 0? Find : Set;

            requests[i].key = (char *) calloc(1, 8 * sizeof(char));
            requests[i].key_len = default_key_len;
            *((size_t *) requests[i].key) = loads[i];

            requests[i].value = (char *) calloc(1, 8 * sizeof(char));
            requests[i].value_len = default_value_len;
            *((size_t *) requests[i].value) = loads[i];

        }

        delete[] loads;
    }else{
        YCSBLoader loader(load_filepath.c_str());
        ycsb_loads=loader.load();
        total_count = ycsb_loads.size();

        YCSBLoader loader1(run_filepath.c_str());
        ycsb_requests=loader1.load();
        ASSERT(total_count == ycsb_requests.size(),"total count error");
        std::cout<<"total_count: "<<total_count<<std::endl;
    }


}

bool check_unique();
void show_info_insert();
void show_info_before();
void show_info_after();
void prepare();


//void kick_test(){
//    uint64_t k1 = 172046561ll;
//    uint64_t k2 = 2653590574ll;
//    uint64_t k3 = 6497310948ll;
//    uint64_t k4 = 6174611380ll;
//
//    Request req1,req2,req3,req4;
//
//    req1.key = (char * )calloc(1,8);
//    *(uint64_t *)req1.key = k1;
//    req1.key_len = 8;
//    req1.value = (char * )calloc(1,8);
//    *(uint64_t *)req1.value = k1;
//    req1.value_len = 8;
//
//    req2.key = (char * )calloc(1,8);
//    *(uint64_t *)req2.key = k2;
//    req2.key_len = 8;
//    req2.value = (char * )calloc(1,8);
//    *(uint64_t *)req2.value = k2;
//    req2.value_len = 8;
//
//    req3.key = (char * )calloc(1,8);
//    *(uint64_t *)req3.key = k3;
//    req3.key_len = 8;
//    req3.value = (char * )calloc(1,8);
//    *(uint64_t *)req3.value = k3;
//    req3.value_len = 8;
//
//    req4.key = (char * )calloc(1,8);
//    *(uint64_t *)req4.key = k4;
//    req4.key_len = 8;
//    req4.value = (char * )calloc(1,8);
//    *(uint64_t *)req4.value = k4;
//    req4.value_len = 8;
//
//    store->Insert(req1.key,req1.key_len,req1.value,req1.value_len);
//    store->Insert(req2.key,req2.key_len,req2.value,req2.value_len);
//    store->Insert(req3.key,req3.key_len,req3.value,req3.value_len);
//    store->Insert(req4.key,req4.key_len,req4.value,req4.value_len);
//
//}



int main(int argc, char **argv) {
    if (argc == 10) {
        insert_thread_num = std::atol(argv[1]);
        thread_num = std::atol(argv[2]);
        init_size1 = std::atol(argv[3]);
        init_size2 = std::atol(argv[4]);
        op_chose = static_cast<Op_type>(std::atol(argv[5]));
        key_range = std::atol(argv[6]);
        total_count = std::atol(argv[7]);
        distribution = std::atol(argv[8]);
        timer_range = std::atol(argv[9]);
        YCSB = false;
    } else if(argc == 6){
        insert_thread_num = std::atol(argv[1]);
        thread_num = std::atol(argv[2]);
        init_size1 = std::atol(argv[3]);
        init_size2 = std::atol(argv[3]);
        timer_range = std::atol(argv[4]);
        YCSB = true;
    }else{
        cout << "micro_benchmark:"<<endl;
        cout << "./a.out <insert_thread_num> <thread_num> <size1> <size2> <op_chose> <key_range>"
                "<total_count> <distribution> <timer_range>" << endl;
        cout << "ycsb:"<<endl;
        cout << "./a.out <insert_thread_num> <thread_num> <init_hashpower> <timer_range>" << endl;
        cout << "op_chose    :0-Find,1-Set,2-Erase,3-Insert,4-Rand " << endl;
        cout << "distribution:0-unif,1-zipf" << endl;

        exit(-1);
    }

    show_info_before();

    store = new LFCuckooHash(init_size1, init_size2);

    tb = new ThreadBarrier(thread_num);

//    kick_test();
//    return 0;

    prepare();

//    std::vector<std::thread> insert_threads;
//    for (int i = 0; i < insert_thread_num; i++) insert_threads.emplace_back(std::thread(insert_worker, i));
//    for (int i = 0; i < insert_thread_num; i++) insert_threads[i].join();
//
//    show_info_insert();

//    if(!YCSB) std::random_shuffle(requests, requests + total_count);

//    ASSERT(store.check_unique(),"key not unique!");
//    ASSERT(store.check_nolock(),"there are still locks in map!");

    runtimelist = new uint64_t[thread_num]();

    std::vector<std::thread> threads;
    for (int i = 0; i < thread_num -1 ; i++) threads.emplace_back(std::thread(worker, i));
    threads.emplace_back(std::thread(worker_insert_rm, thread_num -1));
    for (int i = 0; i < thread_num; i++) threads[i].join();

//    ASSERT(store.check_unique(),"key not unique!");
//    ASSERT(store.check_nolock(),"there are still locks in map!");

    store->cal_info_and_show();

    show_info_after();

}

void show_info_insert(){

    cout << ">>>>>pre insert finish" <<"\tinsert_success: "<<insert_success<<"\tkick_num: "<<kick_num<< endl;
//    cout<<"depth0 "<<depth0<<endl;
//    cout<<"kick_lock_failure_data_check "<<kick_lock_failure_data_check<<endl;
//    cout<<"kick_lock_failure_haza_check "<<kick_lock_failure_haza_check<<endl;
//    cout<<"kick_lock_failure_other_lock "<<kick_lock_failure_other_lock<<endl;
//    cout<<"kick_lock_failure_haza_check_after "<<kick_lock_failure_haza_check_after<<endl;
//    cout<<"kick_lock_failure_data_check_after "<<kick_lock_failure_data_check_after<<endl;
//    cout<<"key_duplicated_after_kick "<<key_duplicated_after_kick<<endl;
//
//    cout<<"path length log:  ";
//    for(int i = 0; i < 6;i++ ) {
//        cout<<" "<<i<<":"<<kick_path_length_log[i]<<" ";
//    }
//    cout<<endl;
    cout<< "   ------------  "<<endl;
}

void show_info_before() {
    if(YCSB){
        std::cout << " thread_num " << thread_num
                  << " init_size1 " << init_size1
                  << " init_size2 " << init_size2
                  << " timer_range " << timer_range << std::endl;
        std::cout << "---YCSB--- "<<std::endl;
        std::cout << "loadpath:\t"<<load_filepath<<std::endl;
        std::cout << "runpath:\t"<<run_filepath<<std::endl;
        std::cout << "loading file... "<<std::endl;
    }else{
        string op_chose_str;
        switch (op_chose) {
            case Find:
                op_chose_str = "Find";
                break;
            case Set:
                op_chose_str = "Set";
                break;
            case Erase:
                op_chose_str = "Erase";
                break;
            case Insert:
                op_chose_str = "Insert";
                break;
            case Rand:
                op_chose_str = "Rand";
                break;
            default:
                ASSERT(false, "op_chose not defined");
        }

        ASSERT(distribution == 0 || distribution == 1, "distribution not defined");
        string distribution_str = distribution == 0 ? "uinf" : "zipf";

        std::cout << " thread_num " << thread_num
                  << " init_size1 " << init_size1
                  << " init_size2 " << init_size2
                  << " op_chose " << op_chose_str
                  << " key_range " << key_range
                  << " total_count " << total_count
                  << " distribution " << distribution_str
                  << " timer_range " << timer_range << std::endl;

        uint64_t total_slot_num = init_size1 + init_size2;
        std::cout << "total_slot_num " << total_slot_num << std::endl;
    }

}

void show_info_after() {

    std::cout << " find_success " << find_success << "\tfind_failure " << find_failure << std::endl;
    std::cout << " insert_success " << insert_success << "\tinsert_failure " << insert_failure << std::endl;
    std::cout << " set_insert " << set_insert << "\tset_assign " << set_assign << std::endl;
    std::cout << " update_success " << update_success << "\tupdate_failure " << update_failure << std::endl;
    std::cout << " erase_success " << erase_success << "\terase_failure " << erase_failure << std::endl;



    //uint64_t item_num = store.get_item_num();

//    std::cout << "items in table " << item_num << std::endl;
//    std::cout << "position ratio "  << key_position[0] << " : "
//              <<key_position[1] << " : "
//              <<key_position[2] << " : "
//              <<key_position[3] <<std::endl;
//    std::cout<< "occupancy "<< item_num * 1.0 / store.slot_num() <<std::endl;

    std::cout << endl << " op_num " << op_num << std::endl;

    uint64_t runtime = 0;
    for (int i = 0; i < thread_num; i++) {
        runtime += runtimelist[i];
    }
    runtime /= thread_num;
    std::cout << " runtime " << runtime << std::endl;

    double throughput = op_num * 1.0 / runtime;
    std::cout << "***throughput " << throughput << std::endl;


//    ASSERT(op_num == find_success + find_failure
//                     + set_insert + set_assign
//                     + erase_success + erase_failure
//                     + insert_success + insert_failure - total_count, "op_num not correct");
//
//    ASSERT(insert_success + set_insert - erase_success == item_num, "item != inert - erase");


}

