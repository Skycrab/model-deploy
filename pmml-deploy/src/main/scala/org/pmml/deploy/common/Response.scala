package org.pmml.deploy.common

/**
  * Created by yihaibo on 19/2/25.
  */
class Response[T](val errorCode: Int, val errorMsg: String, val data: T)