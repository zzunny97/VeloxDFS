#include "task_manager.hh"
#include <zookeeper/zookeeper.h>
#include <iostream>
#include <algorithm>
#include <sys/time.h>

using namespace std;
using namespace eclipse;
using namespace eclipse::messages;
using namespace eclipse::network;

namespace eclipse {
	
TaskManager::TaskManager (ClientHandler* net) : Node() {
	network = net;
  	network_size = context.settings.get<vec_str>("network.nodes").size();

}

TaskManager::~TaskManager(){}


// Implemented by using zookeeper 
DistLockStatus get_Dlock(zhandle_t * zh, string znode, bool isPrimary){
	string znode_contents = isPrimary ? "1" : "0"; 
	char buffer[128] = {0};
	int zoo_get_buf_len = 128;
	int rc = zoo_create(zh, znode.c_str(), znode_contents.c_str(), 1, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, buffer, 128);

	if(rc){  // Failed to get a dist lock
		if(!zoo_get(zh, znode.c_str(), 0, buffer, &zoo_get_buf_len, NULL)){
			if(!strncmp(buffer, "1", zoo_get_buf_len)){  // conflict with primary node
				return END_OF_FILE;		
			} else { 										 // conflict with replica nodes
				return isPrimary ? END_OF_FILE : TRY_NEXT_DIST_LOCK;
			}
		}	
	}
	return GET_DIST_LOCK;
}
 
DistLockStatus get_dist_lock(zhandle_t * zh, string target_node, string znode, bool isPrimary, bool* Stealing){
	char buffer[128] = {0};
	int rc;
	int zoo_get_buf_len = 128;
	Stat zoo_stat;

	if(!(*Stealing)){
		rc = zoo_exists(zh, target_node.c_str(), 0 , &zoo_stat);
		if(rc != ZOK){
			return GET_DIST_LOCK;
		} else {
			DistLockStatus lock_rc = get_Dlock(zh, znode, true);
			zoo_set(zh, target_node.c_str(), "Done", strlen("Done"), -1);
			*Stealing = true;
			return lock_rc;
		}
	} else {
		while(true){
			zoo_get(zh, target_node.c_str(), 0, buffer, &zoo_get_buf_len, NULL);
			
			if(!strcmp(buffer, "Done")){
				break;
			} 		
		}

		return get_Dlock(zh, znode, false);
	}

	return GET_DIST_LOCK;
}

//bool produce(ifstream& ifs, messages::BlockInfo& md, struct shm_buf** cur_chunk, int idx){
bool produce(ifstream& ifs, struct semaphore* semap, messages::BlockInfo& md, struct shm_buf** shm){
	
	cout << "Produce : " << md.seq << " " << md.size << endl;
	pthread_mutex_lock(&semap->lock);

	if((semap->tail - semap->head) < semap->queue_size){
		pthread_cond_signal(&semap->nonzero);
	} else {
		pthread_cond_wait(&semap->nonzero, &semap->lock);
	}

	ifs.read(&(shm[semap->tail % semap->queue_size]->buf[0]), (long)md.size);
	shm[semap->tail % semap->queue_size]->chunk_size = md.size;
	shm[semap->tail % semap->queue_size]->chunk_index = md.seq;
	
	semap->tail++;
	pthread_mutex_unlock(&semap->lock);
	
}

bool produce2(string disk_path, struct semaphore* semap, messages::BlockInfo& md, struct shm_buf** shm){
	
	ifstream ifs(disk_path + md.name, ios::binary | ios::in);
	pthread_mutex_lock(&semap->lock);

	if((semap->tail - semap->head) < semap->queue_size){
		pthread_cond_signal(&semap->nonzero);
	} else {
		pthread_cond_wait(&semap->nonzero, &semap->lock);
	}


	ifs.read(&(shm[semap->tail % semap->queue_size]->buf[0]), (long)md.size);
	shm[semap->tail % semap->queue_size]->chunk_size = md.size;
	shm[semap->tail % semap->queue_size]->chunk_index = md.seq;
	
	semap->tail++;
	pthread_mutex_unlock(&semap->lock);
	ifs.close();
	
}

void task_worker(std::string file, struct logical_block_metadata& lblock_metadata, string _job_id, int _task_id){
	string job_id = _job_id;

	/* For Shared Memory */
	uint64_t BLOCK_SIZE = context.settings.get<int>("filesystem.block");
	int shm_buf_depth = context.settings.get<int>("addons.shm_buf_depth");
	int shm_buf_width = context.settings.get<int>("addons.shm_buf_width");
	uint64_t buf_pool_size = sizeof(bool) + (sizeof(struct shm_buf) + BLOCK_SIZE) * shm_buf_depth * shm_buf_width;
	string disk_path = context.settings.get<string>("path.scratch") + "/";
	
	/* For Zookeepr */	
	string zk_server_addr = GET_STR("addons.zk.addr");
  	int zk_server_port = GET_INT("addons.zk.port");
  	string addr_port = zk_server_addr + ":" + to_string(zk_server_port);	
	
	zoo_set_debug_level((ZooLogLevel)0);
 	zhandle_t* zh = zookeeper_init(addr_port.c_str(), NULL , 40000, 0, 0, 0);
	if(!zh){
		cout << "[ " << job_id << " ] : " << "Zookeeper Connection Error" << endl;
	}
	string zk_prefix = "/chunks/" + job_id + "/";

	/* For FD*/
	ifstream ifs;
	ifs.open(disk_path + file, ios::in | ios::binary);
	ifs.seekg(ios::beg);

	int input_block_num = lblock_metadata.primary_chunk_num;
	string my = lblock_metadata.host_name;
	auto& md = lblock_metadata.physical_blocks;
	int task_id = _task_id;	
	cout << "Task_id : " << task_id << endl;

	int shmid;
	void* shared_memory; 
	uint64_t shm_status_addr;
	uint64_t shm_base_addr;
	uint64_t shm_chunk_base_addr;
	
	/* Get Shared Memory Pool */
	struct shm_buf** chunk_index = new struct shm_buf*[shm_buf_width * shm_buf_depth]; 
	shmid = shmget((key_t)(DEFAULT_KEY + task_id), buf_pool_size, 0666|IPC_CREAT);

	if(shmid == -1){
		cout << "shmget failed" << endl;
		exit(1);
	}

	shared_memory = shmat(shmid, NULL, 0);
	if(shared_memory == (void*)-1){
		cout << "shmat failed" << endl;
		exit(1);
	}

	/* For User level semaphore */
	struct semaphore *sema;
	string sema_path = "/tmp/semaphore" + to_string(task_id);

	sema = semaphore_create(sema_path.c_str(), (shm_buf_depth * shm_buf_width) );

	memset(shared_memory, 0, buf_pool_size);
	shm_status_addr = (uint64_t)shared_memory;
	shm_base_addr = (uint64_t)shared_memory + sizeof(bool);
	shm_chunk_base_addr = shm_base_addr + sizeof(struct shm_buf) * shm_buf_width * shm_buf_depth;
	int shm_buf_num = shm_buf_width * shm_buf_depth;

	for(int i = 0; i < shm_buf_num; i++){
		chunk_index[i] = (struct shm_buf*)(shm_base_addr + sizeof(struct shm_buf) * i);
		chunk_index[i]->buf = (char*)(shm_chunk_base_addr + BLOCK_SIZE * i);
	}
	
	int md_index = 0;
	uint64_t input_file_offset = 0, read_bytes = 0;
	bool isPrimary = true;
	bool Stealing = false;
	
	int processed_file_cnt = 0, r_idx = 0, replica_num = 3, shm_idx = 0;
	int process_chunk = 0;
 	//string zk_prefix = "/chunks/" + job_id + "/";
	string zk_eof = zk_prefix + my;

	while( processed_file_cnt < replica_num ) {

		while(md_index < input_block_num){

			string zk_path = zk_prefix + to_string(md[md_index].seq);

			DistLockStatus rc = get_dist_lock(zh, zk_eof, zk_path, isPrimary, &Stealing);
			if(rc == GET_DIST_LOCK){

				if(!isPrimary) {
					ifs.seekg(md[md_index].offset);
				}	
				
				produce(ifs, sema, md[md_index], chunk_index);

				process_chunk++;

			} else if(rc == END_OF_FILE){
				md_index = input_block_num;
				break;
			} else if(rc == TRY_NEXT_DIST_LOCK){
			} else {
				exit(static_cast<int>(ZOO_CREATE_ERROR));
			}
			md_index++;
		}

		ifs.close();	
		processed_file_cnt++;
		if(md.size() == md_index) break;
		ifs.open(disk_path + md[md_index].primary_file, ios::in | ios::binary | ios::ate);
		char buffer[128] = {0};
		int rc;
		
		if(isPrimary){
			rc = zoo_create(zh, zk_eof.c_str(), "None", strlen("None"), &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, buffer, 128);
			zoo_create(zh, (zk_prefix + to_string(md[md_index-1].seq)).c_str(), "1", 1, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, buffer, 128);
			zoo_set(zh, zk_eof.c_str(), "Done", strlen("Done"), -1);
		}

		Stealing = true;
		isPrimary = false;
		zk_eof = zk_prefix + md[md_index].node;
		rc = zoo_create(zh, zk_eof.c_str(), "None", strlen("None"), &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, buffer, 128);
		input_block_num += lblock_metadata.replica_chunk_num[r_idx++];
		input_file_offset = 0;
	}
	
	pthread_mutex_lock(&sema->lock);
	*(bool*)shm_status_addr = true;	
	pthread_cond_broadcast(&sema->nonzero);
	pthread_mutex_unlock(&sema->lock);


//  	zookeeper_close(zh);
	/* Close Task */
	struct shmid_ds stat;

	while(true){
		shmctl(shmid, IPC_STAT, &stat);
		cout << "N attach : " << stat.shm_nattch << endl;
		if(stat.shm_nattch == 1)
			break;
		usleep(100000);
	}

	sleep(200);
	delete[] chunk_index;
	shmdt(shared_memory);
	shmctl(shmid, IPC_RMID, 0);

	return;
}

void static_worker(std::string file, struct logical_block_metadata& lblock_metadata, string _job_id, int _task_id){
	string job_id = _job_id;
	/* For Shared Memory */
	uint64_t BLOCK_SIZE = context.settings.get<int>("filesystem.block");
	int shm_buf_depth = context.settings.get<int>("addons.shm_buf_depth");
	int shm_buf_width = context.settings.get<int>("addons.shm_buf_width");
	uint64_t buf_pool_size = sizeof(bool) + (sizeof(struct shm_buf) + BLOCK_SIZE) * shm_buf_depth * shm_buf_width;
	string disk_path = context.settings.get<string>("path.scratch") + "/";
	
	/* For FD*/
	ifstream ifs;
	ifs.open(disk_path + file, ios::in | ios::binary);
	ifs.seekg(ios::beg);

	int input_block_num = lblock_metadata.primary_chunk_num;
	auto& md = lblock_metadata.physical_blocks;
	int task_id = _task_id;	

	int shmid;
	void* shared_memory; 
	uint64_t shm_status_addr;
	uint64_t shm_base_addr;
	uint64_t shm_chunk_base_addr;
	
	/* Get Shared Memory Pool */
	struct shm_buf** chunk_index = new struct shm_buf*[shm_buf_width * shm_buf_depth]; 
	shmid = shmget((key_t)(DEFAULT_KEY + task_id), buf_pool_size, 0666|IPC_CREAT);

	if(shmid == -1){
		cout << "shmget failed" << endl;
		exit(1);
	}

	shared_memory = shmat(shmid, NULL, 0);
	if(shared_memory == (void*)-1){
		cout << "shmat failed" << endl;
		exit(1);
	}

	struct semaphore *sema;
	string sema_path = "/tmp/semaphore" + to_string(task_id);

	sema = semaphore_create(sema_path.c_str(), (shm_buf_depth * shm_buf_width) );

	memset(shared_memory, 0, buf_pool_size);
	shm_status_addr = (uint64_t)shared_memory;
	shm_base_addr = (uint64_t)shared_memory + sizeof(bool);
	shm_chunk_base_addr = shm_base_addr + sizeof(struct shm_buf) * shm_buf_width * shm_buf_depth;
	int shm_buf_num = shm_buf_width * shm_buf_depth;

	/* Init Mutex Locks for each shm_buf && store chunk addr*/
	for(int i = 0; i < shm_buf_num; i++){
		chunk_index[i] = (struct shm_buf*)(shm_base_addr + sizeof(struct shm_buf) * i);
		chunk_index[i]->buf = (char*)(shm_chunk_base_addr + BLOCK_SIZE * i);
	}
	
	int md_index = 0;
	uint64_t input_file_offset = 0, read_bytes = 0;
	bool isPrimary = true;

	int processed_file_cnt = 0, r_idx = 0, replica_num = 1, shm_idx = 0;

	while(md_index < input_block_num){
		produce(ifs, sema, md[md_index], chunk_index);
		md_index++;
	}

	ifs.close();	

	pthread_mutex_lock(&sema->lock);
	*(bool*)shm_status_addr = true;	
	pthread_cond_broadcast(&sema->nonzero);
	pthread_mutex_unlock(&sema->lock);


	/* Close Task */
	delete[] chunk_index;
	shmdt(shared_memory);
	shmctl(shmid, IPC_RMID, 0);
		
	return;
}

/* For Expriment */
void static_worker_by_idv(std::string file, struct logical_block_metadata& lblock_metadata, string _job_id, int _task_id){
	string job_id = _job_id;

	/* For Shared Memory */
	uint64_t BLOCK_SIZE = context.settings.get<int>("filesystem.block");
	int shm_buf_depth = context.settings.get<int>("addons.shm_buf_depth");
	int shm_buf_width = context.settings.get<int>("addons.shm_buf_width");
	uint64_t buf_pool_size = sizeof(bool) + (sizeof(struct shm_buf) + BLOCK_SIZE) * shm_buf_depth * shm_buf_width;
	string disk_path = context.settings.get<string>("path.scratch") + "/";

	int input_block_num = lblock_metadata.primary_chunk_num;
	auto& md = lblock_metadata.physical_blocks;
	int task_id = _task_id;	

	int shmid;
	void* shared_memory; 
	uint64_t shm_status_addr;
	uint64_t shm_base_addr;
	uint64_t shm_chunk_base_addr;
	
	/* Get Shared Memory Pool */
	struct shm_buf** chunk_index = new struct shm_buf*[shm_buf_width * shm_buf_depth]; 
	shmid = shmget((key_t)(DEFAULT_KEY + task_id), buf_pool_size, 0666|IPC_CREAT);

	if(shmid == -1){
		cout << "shmget failed" << endl;
		exit(1);
	}

	shared_memory = shmat(shmid, NULL, 0);
	if(shared_memory == (void*)-1){
		cout << "shmat failed" << endl;
		exit(1);
	}
	
	struct semaphore *sema;
	string sema_path = "/tmp/semaphore" + to_string(task_id);

	sema = semaphore_create(sema_path.c_str(), (shm_buf_depth * shm_buf_width) );

	memset(shared_memory, 0, buf_pool_size);
	shm_status_addr = (uint64_t)shared_memory;
	shm_base_addr = (uint64_t)shared_memory + sizeof(bool);
	shm_chunk_base_addr = shm_base_addr + sizeof(struct shm_buf) * shm_buf_width * shm_buf_depth;
	int shm_buf_num = shm_buf_width * shm_buf_depth;

	/* Init Mutex Locks for each shm_buf && store chunk addr*/
	for(int i = 0; i < shm_buf_num; i++){
		chunk_index[i] = (struct shm_buf*)(shm_base_addr + sizeof(struct shm_buf) * i);
		chunk_index[i]->buf = (char*)(shm_chunk_base_addr + BLOCK_SIZE * i);
	}
	
	int md_index = 0;
	uint64_t input_file_offset = 0, read_bytes = 0;
	bool isPrimary = true;

	int processed_file_cnt = 0, r_idx = 0, replica_num = 1, shm_idx = 0;
	
	int chunk_cnt = 0;
	while(md_index < input_block_num){
		produce2(disk_path, sema, md[md_index], chunk_index);
		chunk_cnt++;
		md_index++;
	}

	//ifs.close();	

	pthread_mutex_lock(&sema->lock);
	*(bool*)shm_status_addr = true;	
	pthread_cond_broadcast(&sema->nonzero);
	pthread_mutex_unlock(&sema->lock);

	/* Close Task */
	delete[] chunk_index;
	shmdt(shared_memory);
	shmctl(shmid, IPC_RMID, 0);
		

	return;
}

void TaskManager::task_init(std::string file, struct logical_block_metadata& metadata, string job_id, int _task_id){
	string policy = GET_STR("addons.job_policy");
	cout << "Policy : " << policy << endl;
	if(policy == "static"){
		std::thread worker = thread(&static_worker, file, std::ref(metadata), job_id, _task_id);
		worker.detach();
	} else if(policy == "steal"){
		std::thread worker = thread(&task_worker, file, std::ref(metadata), job_id, _task_id);
		worker.detach();
	} else if(policy == "static_by_idv"){
		std::thread worker = thread(&static_worker_by_idv, file, std::ref(metadata), job_id, _task_id);
		worker.detach();
	} else {
		cout << policy << " is not existed " << endl;
		exit(1);
	}
}

//bool TaskManager::destroy_TaskManager(){

//}

}
