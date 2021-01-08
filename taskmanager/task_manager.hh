#pragma once

#include "../nodes/node.hh"
#include "../messages/task_operation.hh"
#include "../common/shared_memory.hh"
#include "../common/context_singleton.hh"
#include <string>
#include <fstream>
#include <vector>
#include <map>
#include <memory>

enum DistLockStatus {
		GET_DIST_LOCK,
		END_OF_FILE,
		TRY_NEXT_DIST_LOCK,
		ZOO_CREATE_ERROR
	};

namespace eclipse {

using vec_str = std::vector<std::string>;

class TaskManager : public Node {

	

protected :
	int network_size;
	std::map <int, shared_ptr<int> > to_process_file;
public :
	uint64_t BLOCK_SIZE;
	int shm_buf_depth;
	int shm_buf_width;
	int replica_num;
	uint64_t buf_pool_size;
	std::string disk_path;
	
	/* For Zookeepr */	
	std::string zk_server_addr = GET_STR("addons.zk.addr");
  	int zk_server_port = GET_INT("addons.zk.port");

	TaskManager(network::ClientHandler*); 
	
	//bool read_from_disk_to_shm(ifstream& ifs, messages::BlockInfo& md, struct shm_buf** cur_chunk, int idx);
	void task_init(std::string file, struct logical_block_metadata& assigned_chunks, std::string job_id, int task_id);
	
	~TaskManager();
};

}
