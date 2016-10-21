package cephui.css

import scalacss.ScalaCssReact._
import scalacss.internal.mutable.GlobalRegistry
import scalacss.Defaults._

object AppCSS {
  object Style extends StyleSheet.Inline {
    import scalacss.Defaults._
    import dsl._

    val lightGrey = Color("#DDD")

    val hiddenTableRow = style("hiddentablerow")(
      padding.`0`.important,
      backgroundColor(lightGrey))
  }

  def load = {

    GlobalRegistry.register(Style)

    GlobalRegistry.onRegistration(_.addToDocument())
  }
}
