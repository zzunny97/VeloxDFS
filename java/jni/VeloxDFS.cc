#include <jni.h>
#include "../../client/vdfs.hh"
#include "../../client/model/metadata.hh"
#include "../../client/model/block_metadata.hh"
#include <cstring>
#include <cstdio>

using namespace std;

#ifdef __cplusplus
extern "C" {
#endif

//std::string write_block_cache;
/*JNIEXPORT jlong JNICALL Java_com_dicl_velox_VeloxDFS_write__JJ_3BJJJ
  (JNIEnv* env, jobject obj, jlong fid, jlong pos, jbyteArray buf, jlong off, jlong len, 
  jlong block_size);
*/


JNIEXPORT jboolean JNICALL Java_com_dicl_velox_VeloxDFS_write__JJ_3BJJJ
  (JNIEnv* env, jobject obj, jstring fid, jbyteArray buf, jlong block_size);
  

jobject convert_jmetadata(JNIEnv* env, jobject& obj, velox::model::metadata& md);

velox::vdfs* get_vdfs(JNIEnv* env, jobject& obj) {
  jclass vdfs_c = env->GetObjectClass(obj);
  jmethodID get_vdfs = env->GetMethodID(vdfs_c, "getVeloxDFS", "()J");
  if(get_vdfs == nullptr) return nullptr;
  
  jlong vdfs_ptr = env->CallLongMethod(obj, get_vdfs);
  return (vdfs_ptr == 0) ? nullptr : reinterpret_cast<velox::vdfs*>(vdfs_ptr);
}

/*
 * Class:     com_dicl_velox_VeloxDFS
 * Method:    constructVeloxDFS
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_com_dicl_velox_VeloxDFS_constructVeloxDFS
  (JNIEnv* env, jobject obj, jstring mr_job_id, jlong lid, jboolean initializer) {
  velox::vdfs* vdfs = get_vdfs(env, obj);
  const char* job_id = env->GetStringUTFChars(mr_job_id, 0);

  jlong ret;

  if(vdfs == nullptr)
  	ret = reinterpret_cast<jlong>(new velox::vdfs(std::string(job_id), (long)lid, (bool)initializer)); 
  else 
	ret = reinterpret_cast<jlong>(vdfs);
	
  env->ReleaseStringUTFChars(mr_job_id, job_id);

  return ret;
  //return reinterpret_cast<jlong>(((vdfs == nullptr) ? new velox::vdfs(std::string(job_id), (long)lid) : vdfs));

}

/*
 * Class:     com_dicl_velox_VeloxDFS
 * Method:    destructVeloxDFS
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_dicl_velox_VeloxDFS_destructVeloxDFS
  (JNIEnv* env, jobject obj) {
  velox::vdfs* vdfs = get_vdfs(env, obj);
  if(vdfs != nullptr) delete vdfs;
}

/*
 * Class:     com_dicl_velox_VeloxDFS
 * Method:    open
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_dicl_velox_VeloxDFS_open
  (JNIEnv* env, jobject obj, jstring str) {
  velox::vdfs* vdfs = get_vdfs(env, obj);
  const char* file_name = env->GetStringUTFChars(str, 0);
  //const char* file_name = "50G.dat";
  //fprintf(stderr, "File Name : %s\n", file_name);
  
  jlong fid = (jlong)vdfs->open_file(std::string(file_name));
  env->ReleaseStringUTFChars(str, file_name);

  return fid;
}

/*
 * Class:     com_dicl_velox_VeloxDFS
 * Method:    close
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_dicl_velox_VeloxDFS_close
  (JNIEnv* env, jobject obj, jlong fid) {
  velox::vdfs* vdfs = get_vdfs(env, obj);
  return (jboolean)vdfs->close(fid);
}

/*
 * Class:     com_dicl_velox_VeloxDFS
 * Method:    isOpen
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_dicl_velox_VeloxDFS_isOpen
  (JNIEnv* env, jobject obj, jlong fid) {
  velox::vdfs* vdfs = get_vdfs(env, obj);
  return (jboolean)vdfs->is_open(fid);
}

/*
 * Class:     com_dicl_velox_VeloxDFS
 * Method:    write
 * Signature: (JJ[BJJ)J
JNIEXPORT jlong JNICALL Java_com_dicl_velox_VeloxDFS_write__JJ_3BJJ
  (JNIEnv* env, jobject obj, jlong fid, jlong pos, jbyteArray buf, jlong off, jlong len) {
  return Java_com_dicl_velox_VeloxDFS_write__JJ_3BJJJ(env, obj, fid, pos, buf, off, len, 0);
}
 */
//JNIEXPORT jboolean JNICALL Java_com_dicl_velox_VeloxDFS_write//__JJ_3BJJ
//  (JNIEnv* env, jobject obj, jstring file, jbyteArray buf) {
//  return Java_com_dicl_velox_VeloxDFS_write__JJ_3BJJJ(env, obj, file, buf, 0);
//}

/*
 * Class:     com_dicl_velox_VeloxDFS
 * Method:    write
 * Signature: (JJ[BJJJ)J
 */
//JNIEXPORT jlong JNICALL Java_com_dicl_velox_VeloxDFS_write__JJ_3BJJJ
//  (JNIEnv* env, jobject obj, jlong fid, jlong pos, jbyteArray buf, jlong off, jlong len, 
//  jlong block_size) {
JNIEXPORT jboolean JNICALL Java_com_dicl_velox_VeloxDFS_write//__JJ_3BJJJ
  (JNIEnv* env, jobject obj, jstring file, jbyteArray buf, jlong block_size, jboolean commit) {
  
  printf("Jni Write Start" );
  velox::vdfs* vdfs = get_vdfs(env, obj);

  jsize num_bytes = env->GetArrayLength(buf);
  char* c_buf = new char[num_bytes+1];
  jbyte* jnibuf = env->GetByteArrayElements(buf, NULL); 
  memcpy(c_buf, jnibuf, num_bytes);
  c_buf[num_bytes] = 0;

  const char* file_name = env->GetStringUTFChars(file, 0);
  //env->GetByteArrayRegion(buf, 0, block_size, reinterpret_cast<jbyte*>(c_buf));
  std::string parm(c_buf);

  jboolean ret = vdfs->write(std::string(file_name), parm, block_size, (bool)commit);
  //env->ReleaseByteArrayElements(env, buf, nativeBytes, JNI_ABORT);

  return ret;

  /*
  const char* file_name = env->GetStringUTFChars(file, 0);
  char* buffer = new char[(uint64_t)block_size + 1];
  
  env->GetByteArrayRegion(buf, 0, (jsize)block_size, reinterpret_cast<jbyte*>(buffer));
  
  printf("Jni Write : %s %s\n", file_name, buffer );
  // TODO: Copy buffer data from given buf, it might be slow.
  // If you don't want to copy data, set the second parameter to JNI_FALSE
  //jboolean* isCopy = JNI_FALSE;
  //jbyte* buffer = env->GetByteArrayElements(buf, null);

  //jlong ret = vdfs->write((long)fid, buffer, (uint64_t)pos, (uint64_t)len, block_size);
  std::string str(buffer);
  printf("Check buffer string : %s\n", str.c_str() );
  jboolean ret = vdfs->write(std::string(file_name), str, block_size);

  env->ReleaseStringUTFChars(file, file_name);
  delete[] buffer;
  //env->ReleaseByteArrayElements(buf, buffer, JNI_ABORT);

  return ret;
  */
}

/*JNIEXPORT jboolean JNICALL Java_com_dicl_velox_VeloxDFS_write_commit//__JJ_3BJJJ
  (JNIEnv* env, jobject obj, jstring file) {
  velox::vdfs* vdfs = get_vdfs(env, obj);
  const char* file_name = env->GetStringUTFChars(file, 0);

  printf("Start vdfs write commit function\n");
  jboolean ret = vdfs->write_commit(std::string(file_name));
  printf("end of vdfs write commit function\n");
  //env->ReleaseStringUTFChars(file, file_name);

  return ret;
}
*/

/*
 * Class:     com_dicl_velox_VeloxDFS
 * Method:    read
 * Signature: (JJ[BJJ)J
 * fid : File descriptor
 * pos : position to read in a file
 * buf : buffer to store read data
 * off : offset to read in the buffer
 * len : length to read
 */
JNIEXPORT jlong JNICALL Java_com_dicl_velox_VeloxDFS_read
  (JNIEnv* env, jobject obj, jlong fid, jlong pos, jbyteArray buf, jlong off, jlong len) {
  velox::vdfs* vdfs = get_vdfs(env, obj);

  char* c_buf = new char [len+1];

  //bzero(c_buf, len+1);
  memset(c_buf, 0, len+1);

  int32_t ret = vdfs->read((long)fid, c_buf, (uint64_t)pos, (uint64_t)len);
  int32_t read_bytes = ret;

  if (ret < 0)
    read_bytes = 0;

  env->SetByteArrayRegion(buf, off, read_bytes, (jbyte*)c_buf);

  delete[] c_buf;

  return ret;
}

/*
 * Class:     com_dicl_velox_VeloxDFS
 * Method:    getMetadata
 * Signature: (J)Lcom/dicl/velox/model/Metadata;
 */
JNIEXPORT jobject JNICALL Java_com_dicl_velox_VeloxDFS_getMetadata
  (JNIEnv* env, jobject obj, jlong fid, jbyte type) {
  velox::vdfs* vdfs = get_vdfs(env, obj);

  velox::model::metadata md(vdfs->get_metadata((long)fid, type));
  return convert_jmetadata(env, obj, md);
}

/*
 * Class:     com_dicl_velox_VeloxDFS
 * Method:    remove
 * Signature: (Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_dicl_velox_VeloxDFS_remove
  (JNIEnv* env, jobject obj, jstring fname) {
  velox::vdfs* vdfs = get_vdfs(env, obj);
  const char* file_name = env->GetStringUTFChars(fname, 0);
  jboolean ret = vdfs->rm(file_name);
  env->ReleaseStringUTFChars(fname, file_name);
  return ret;
}

/*
 * Class:     com_dicl_velox_VeloxDFS
 * Method:    exists
 * Signature: (Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_dicl_velox_VeloxDFS_exists
  (JNIEnv* env, jobject obj, jstring fname) {
  velox::vdfs* vdfs = get_vdfs(env, obj);
  const char* file_name = env->GetStringUTFChars(fname, 0);
  jboolean ret = vdfs->exists(file_name);
  env->ReleaseStringUTFChars(fname, file_name);
  return ret;
}

/*
 * Class:     com_dicl_velox_VeloxDFS
 * Method:    list
 * Signature: (ZLjava/lang/String;)[Lcom/dicl/velox/model/Metadata;
 */
JNIEXPORT jobjectArray JNICALL Java_com_dicl_velox_VeloxDFS_list
  (JNIEnv* env, jobject obj, jboolean all, jstring name) {
  velox::vdfs* vdfs = get_vdfs(env, obj);

  const char* n = env->GetStringUTFChars(name, 0);
  std::vector<velox::model::metadata> metadatas = vdfs->list(all, n);
  env->ReleaseStringUTFChars(name, n);

  jclass MetadataClass = env->FindClass("Lcom/dicl/velox/model/Metadata;");
//jmethodID MetadataClassInit = env->GetMethodID(MetadataClass, "<init>", "(Ljava/lang/String;JJIII[Lcom/dicl/velox/model/BlockMetadata;)V");
  jobjectArray list = (jobjectArray)env->NewObjectArray(metadatas.size(), MetadataClass, NULL);

  for(uint32_t i=0; i<metadatas.size(); i++) 
    env->SetObjectArrayElement(list, (jsize)i, convert_jmetadata(env, obj, metadatas[i]));

  return list;
}

jobject convert_jmetadata(JNIEnv* env, jobject& obj, velox::model::metadata& md) {
  jstring file_name = env->NewStringUTF(md.name.c_str());
  jobjectArray block_data = nullptr;

  if(md.has_block_data) {
    jclass velox_model_BlockMetadata = env->FindClass("Lcom/dicl/velox/model/BlockMetadata;");
    block_data = (jobjectArray)env->NewObjectArray(md.num_block, velox_model_BlockMetadata, NULL);

    jmethodID velox_model_BlockMetadata_init = env->GetMethodID(velox_model_BlockMetadata, "<init>", "()V");

    // file_name
    jfieldID fn_field_id = env->GetFieldID(velox_model_BlockMetadata, "fileName", "Ljava/lang/String;");

    for(uint32_t i=0; i<md.num_block; i++) {
      velox::model::block_metadata& bdata = md.block_data[i];

      jobject data = env->NewObject(velox_model_BlockMetadata, velox_model_BlockMetadata_init);

      // name
      jfieldID name_field_id = env->GetFieldID(velox_model_BlockMetadata, "name", "Ljava/lang/String;");
      jstring name = env->NewStringUTF(bdata.name.c_str());
      env->SetObjectField(data, name_field_id, name);
      env->DeleteLocalRef(name);

      // host
      jfieldID host_field_id = env->GetFieldID(velox_model_BlockMetadata, "host", "Ljava/lang/String;");
      jstring host = env->NewStringUTF(bdata.host.c_str());
      env->SetObjectField(data, host_field_id, host);
      env->DeleteLocalRef(host);

      // file_name
      env->SetObjectField(data, fn_field_id, file_name);

      // index
      jfieldID index_field_id = env->GetFieldID(velox_model_BlockMetadata, "index", "I");
      env->SetIntField(data, index_field_id, (jint)i);

      // size
      jfieldID size_field_id = env->GetFieldID(velox_model_BlockMetadata, "size", "J");
      env->SetLongField(data, size_field_id, (jlong)bdata.size);

      uint32_t num_chunks = bdata.chunks.size();

      // size
      jfieldID chunk_size_field_id = env->GetFieldID(velox_model_BlockMetadata, "numChunks", "J");
      env->SetLongField(data, chunk_size_field_id, (jlong)num_chunks);
			//By Ash
		/*	
      jfieldID primary_file_field_id = env->GetFieldID(velox_model_BlockMetadata, "primary_file", "Ljava/lang/String");
      jstring primary_file = env->NewStringUTF(bdata.primary_file.c_str());
      env->SetObjectField(data, primary_file_field_id, primary_file);
      env->DeleteLocalRef(primary_file);

      jfieldID offset_field_id = env->GetFieldID(velox_model_BlockMetadata, "offset", "J");
      env->SetLongField(data, offset_field_id, (jlong)bdata.offset);

      jfieldID foffset_field_id = env->GetFieldID(velox_model_BlockMetadata, "foffset", "J");
      env->SetLongField(data, foffset_field_id, (jlong)bdata.foffset);

      jfieldID primary_seq_field_id = env->GetFieldID(velox_model_BlockMetadata, "primary_seq", "I");
      env->SetIntField(data, primary_seq_field_id, (jint)bdata.primary_seq);
*/
      jobjectArray chunk_array = (jobjectArray)env->NewObjectArray(num_chunks, velox_model_BlockMetadata, NULL);

      for (uint32_t j = 0; j < num_chunks; j++) {
        jobject jchunk = env->NewObject(velox_model_BlockMetadata, velox_model_BlockMetadata_init);
        velox::model::block_metadata& chunk = bdata.chunks[j];

        // name
        jfieldID name_field_id = env->GetFieldID(velox_model_BlockMetadata, "name", "Ljava/lang/String;");
        jstring name = env->NewStringUTF(chunk.name.c_str());
        env->SetObjectField(jchunk, name_field_id, name);
        env->DeleteLocalRef(name);

        // host
        jfieldID host_field_id = env->GetFieldID(velox_model_BlockMetadata, "host", "Ljava/lang/String;");
        jstring host = env->NewStringUTF(chunk.host.c_str());
        env->SetObjectField(jchunk, host_field_id, host);
        env->DeleteLocalRef(host);

        // index
        jfieldID index_field_id = env->GetFieldID(velox_model_BlockMetadata, "index", "I");
        env->SetIntField(jchunk, index_field_id, (jint)chunk.index);

        // size
        jfieldID size_field_id = env->GetFieldID(velox_model_BlockMetadata, "size", "J");
        env->SetLongField(jchunk, size_field_id, (jlong)chunk.size);
				
				//By Ash
        jfieldID primary_file_field_id = env->GetFieldID(velox_model_BlockMetadata, "primary_file", "Ljava/lang/String;");
        jstring primary_file = env->NewStringUTF(chunk.primary_file.c_str());
        env->SetObjectField(jchunk, primary_file_field_id, primary_file);
        env->DeleteLocalRef(primary_file);


        jfieldID offset_field_id = env->GetFieldID(velox_model_BlockMetadata, "offset", "J");
        env->SetLongField(jchunk, offset_field_id, (jlong)chunk.offset);

        jfieldID foffset_field_id = env->GetFieldID(velox_model_BlockMetadata, "foffset", "J");
        env->SetLongField(jchunk, foffset_field_id, (jlong)chunk.foffset);

        jfieldID primary_seq_field_id = env->GetFieldID(velox_model_BlockMetadata, "primary_seq", "I");
        env->SetIntField(jchunk, primary_seq_field_id, (jint)chunk.primary_seq);

        env->SetObjectArrayElement(chunk_array, (jsize)j, jchunk);
        env->DeleteLocalRef(jchunk);
      }


      jfieldID chunks_field_id = env->GetFieldID(velox_model_BlockMetadata, "chunks", "[Lcom/dicl/velox/model/BlockMetadata;");

      env->SetObjectField(data, chunks_field_id, chunk_array);
      env->DeleteLocalRef(chunk_array);

      env->SetObjectArrayElement(block_data, (jsize)i, data);
      env->DeleteLocalRef(data);
    }
  }

  jclass MetadataClass = env->FindClass("Lcom/dicl/velox/model/Metadata;");
  jmethodID init = env->GetMethodID(MetadataClass, "<init>", "(Ljava/lang/String;JJIIIII[Lcom/dicl/velox/model/BlockMetadata;)V");
  jobject ret = env->NewObject(MetadataClass, init,
    file_name, md.hash_key, md.size, md.num_block, md.type, md.replica, md.num_chunks, md.num_static_blocks, block_data
  );
  
  env->DeleteLocalRef(file_name);
  if(block_data != nullptr)
    env->DeleteLocalRef(block_data);

  return std::move(ret);
}

/*
 * Class:     com_dicl_velox_VeloxDFS
 * Method:    rename
 * Signature: (Ljava/lang/String;Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_dicl_velox_VeloxDFS_rename
  (JNIEnv *env, jobject obj, jstring src, jstring dst) {
  velox::vdfs* vdfs = get_vdfs(env, obj);
  const char* src_name = env->GetStringUTFChars(src, 0);
  const char* dst_name = env->GetStringUTFChars(dst, 0);
  jboolean ret = vdfs->rename(src_name, dst_name);
  env->ReleaseStringUTFChars(src, src_name);
  env->ReleaseStringUTFChars(dst, dst_name);
  return ret;
}

/*
 * Class:     com_dicl_velox_VeloxDFS
 * Method:    readChunk
 * Signature: (Ljava/lang/String;Ljava/lang/String;[BJJJ)J
 * fid : File descriptor
 * pos : position to read in a file
 * buf : buffer to store read data
 * off : offset to read in the buffer
 * len : length to read
 */
//JNIEXPORT jlong JNICALL Java_com_dicl_velox_VeloxDFS_readChunk
JNIEXPORT jint JNICALL Java_com_dicl_velox_VeloxDFS_readChunk
  (JNIEnv* env, jobject obj, jbyteArray buf, jint boff) {
  velox::vdfs* vdfs = get_vdfs(env, obj);

  //const char* chunk_name_ = env->GetStringUTFChars(chunk_name, 0);
  //const char* host_ = env->GetStringUTFChars(host, 0);
	
  //int len = 8388608;
  int len = context.settings.get<int>("filesystem.block");
  FILE* fp = fopen("/home/velox/log.txt", "a");
  fprintf(fp, "File Block Len : %d\n", len);
  char* c_buf = new char[len+1];
  //bzero(c_buf, len+1);
  memset(c_buf, 0, len+1);
  int32_t readBytes = vdfs->read_chunk( c_buf, (uint32_t)boff);
  fprintf(fp, "readBytes : %d\n", readBytes);
  env->SetByteArrayRegion(buf, 0, readBytes, (jbyte*)c_buf);
  fprintf(fp, "SetByteArrayRegion\n");
  fclose(fp);
  //delete[] c_buf;
  return readBytes;
  //return 0;
}

JNIEXPORT void JNICALL Java_com_dicl_velox_VeloxDFS_write_file
  (JNIEnv *env, jobject obj, jstring file_name, jstring buf, jlong len) {
  velox::vdfs* vdfs = get_vdfs(env, obj);
  const char* file = env->GetStringUTFChars(file_name, 0);
	/* buf has to be encoded to UTF ? */
	const char* c_buf = env->GetStringUTFChars(buf, 0);
  vdfs->write_file(file, c_buf, (uint64_t)len);
  env->ReleaseStringUTFChars(file_name, file);
  env->ReleaseStringUTFChars(buf, c_buf);
}
#ifdef __cplusplus
}
#endif
