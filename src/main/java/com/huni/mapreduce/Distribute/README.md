防止reduce工作量过大，导致数据倾斜，这里是都在map阶段进行的
  job.addCacheFile(new URI("file:/g:/h.txt"));