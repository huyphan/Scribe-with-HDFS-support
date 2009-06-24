#ifdef USE_SCRIBE_HDFS_SYNC

class HdfsSync : public FileStore {
 public:
  HdfsSync(const std::string& category, bool multi_category, bool is_buffer_file);
  void configure(pStoreConf configuration);
  bool openInternal(bool incrementFilename, struct tm* current_time);
  void periodicCheck();
  
  boost::shared_ptr<Store> copy(const std::string &category);
  /*
  bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
  bool isOpen();
  void configure(pStoreConf configuration);
  void close();
  void flush();
  */
  virtual ~HdfsSync(); 
   
 protected: 
 
  // Configuration
  static time_t lastSyncTime;  
  unsigned long periodLength;
  std::string hdfsDir;
  
  // Internal configuration
  std::string hdfsHost;
  unsigned hdfsPort;
  std::string hdfsPath;
};

#else

class HdfsSync : public FileStore {
 public:
  HdfsSync(const std::string& category, bool multi_category,
                     bool is_buffer_file) : FileInterface(category, false, false) {
    LOG_OPER("[hdfs] ERROR: HDFS Sync is not supported.  file: %s", name.c_str());
    LOG_OPER("[hdfs] If you want HDFS Sync Support, please recompile scribe with HDFS Sync support");
  }
};

#endif // USE_SCRIBE_HDFS_SYNC
