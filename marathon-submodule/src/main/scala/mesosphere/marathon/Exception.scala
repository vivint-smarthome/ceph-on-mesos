package mesosphere.marathon

import com.wix.accord.Failure

/**
  * Is thrown if an object validation is not successful.
  * @param obj object which is not valid
  * @param failure validation information kept in a Failure object
  */
case class ValidationFailedException(obj: Any, failure: Failure) extends Exception("Validation failed")
