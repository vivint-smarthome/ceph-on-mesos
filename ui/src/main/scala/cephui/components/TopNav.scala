package cephui
package components

import japgolly.scalajs.react._
import japgolly.scalajs.react.extra.Reusability
import japgolly.scalajs.react.extra.router.RouterCtl
import japgolly.scalajs.react.vdom.prefix_<^._

import scala.scalajs.js
import scalacss.Defaults._
import scalacss.ScalaCssReact._
import cephui.models.Menu
import cephui.routes.AppRouter.AppPage


object TopNav {

  object Style extends StyleSheet.Inline {

    import dsl._

    val navMenu = style(display.flex,
      alignItems.center,
      backgroundColor(c"#F2706D"),
      margin.`0`,
      listStyle := "none")

    val menuItem = styleF.bool(selected => styleS(
      padding(20.px),
      fontSize(1.5.em),
      cursor.pointer,
      color(c"rgb(244, 233, 233)"),
      mixinIfElse(selected)(
        backgroundColor(c"#E8433F"),
        fontWeight._500)
        (&.hover(
              backgroundColor(c"#B6413E")))
    ))

  }

  case class Props(menus: Vector[Menu], selectedPage: AppPage, ctrl: RouterCtl[AppPage])

  implicit val currentPageReuse = Reusability.by_==[AppPage]
  implicit val propsReuse = Reusability.by((_:Props).selectedPage)

  val component = ReactComponentB[Props]("TopNav")
    .render_P { P =>
      <.header(
        elements.Nav(
          bsStyle = "pills",
          activeKey = P.menus.find(_.route == P.selectedPage).map(_.name).getOrElse(js.undefined)
        )(
          P.menus.map { item =>
            elements.NavItem(
              key = item.name,
              eventKey = item.name,
              // onClick = hi
              onSelect = P.ctrl.set(item.route).toScalaFn
            )(item.name)

            // <.li(^.key := item.name,
            //   Style.menuItem(item.route.getClass == P.selectedPage.getClass),
            //   item.name,
            //   P.ctrl setOnClick item.route)
          }
        )
      )
    }
    .configure(Reusability.shouldComponentUpdate)
    .build

  def apply(props: Props, ref: js.UndefOr[String] = "", key: js.Any = {}) = component.set(key, ref)(props)

}
