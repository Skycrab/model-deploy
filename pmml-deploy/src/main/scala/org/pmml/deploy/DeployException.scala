package org.pmml.deploy

/**
  * Created by yihaibo on 19/3/5.
  */
class DeployException(message: String, cause: Throwable)
  extends Exception(message, cause) {

  def this(message: String) = this(message, null)

}
