package cephui
package routes

import japgolly.scalajs.react.extra.router.{Resolution, RouterConfigDsl, RouterCtl, _}
import japgolly.scalajs.react.vdom.prefix_<^._

import cephui.components.{TopNav, Footer}
import cephui.models.Menu
import cephui.pages.{HomePage,ConfigPage,DanglingReservationsPage}

object AppRouter {

  sealed trait AppPage

  case object Home extends AppPage
  case object Config extends AppPage
  case object DanglingReservations extends AppPage

  val config = RouterConfigDsl[AppPage].buildConfig { dsl =>
    import dsl._
    (trimSlashes
      | staticRoute(root, Home) ~> render(HomePage())
      | staticRoute("#config", Config) ~> render(ConfigPage())
      | staticRoute("#dangling-reservations", DanglingReservations) ~> render(DanglingReservationsPage())
      ).notFound(redirectToPage(Home)(Redirect.Replace))
      .renderWith(layout)
  }


  val mainMenu = Vector(
   Menu("Home",Home),
   Menu("Config",Config),
   Menu("Dangling Reservations",DanglingReservations)
  )

  def layout(c: RouterCtl[AppPage], r: Resolution[AppPage]) = {
    <.div(
      <.div(
        ^.cls := "container-fluid",
        TopNav(TopNav.Props(mainMenu,r.page,c))),
      <.div(
        r.render(),
        Footer()))
  }

  val baseUrl = BaseUrl.fromWindowOrigin // / "scalajs-react-template/"

  val router = Router(baseUrl, config)
}
