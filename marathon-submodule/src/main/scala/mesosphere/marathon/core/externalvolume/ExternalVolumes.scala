package mesosphere.marathon.core.externalvolume

import com.wix.accord.Validator
import mesosphere.marathon.state.ExternalVolume
trait ExternalVolumes

object ExternalVolumes {
  def validExternalVolume: Validator[ExternalVolume] = ???
}
