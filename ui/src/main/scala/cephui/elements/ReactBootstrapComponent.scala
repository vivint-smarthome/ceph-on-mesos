package cephui.elements

import com.payalabs.scalajs.react.bridge.{ReactBridgeComponent, ComponentNamespace}
import scala.scalajs.js
import japgolly.scalajs.react._

/**
 * Common class for all [ReactBootstrap](http://react-bootstrap.github.io/)'s components
 */
@ComponentNamespace("ReactBootstrap")
abstract class ReactBootstrapComponent extends ReactBridgeComponent


/**
 * Bridge to [ReactBootstrap](http://react-bootstrap.github.io/)'s Button component
 */
case class Button(
  id: js.UndefOr[String]  = js.undefined, className: js.UndefOr[String] = js.undefined,
  ref: js.UndefOr[String] = js.undefined, key: js.UndefOr[Any] = js.undefined,
  bsStyle: js.UndefOr[String] = js.undefined, // one of: "success", "warning", "danger", "info", "default", "primary", "link"
  bsSize: js.UndefOr[String] = js.undefined, // one of: "lg", "large", "sm", "small", "xs", "xsmall"
  active: js.UndefOr[Boolean] = js.undefined,
  block: js.UndefOr[Boolean] = js.undefined,
  componentClass: js.UndefOr[String] = js.undefined, // You can use a custom element type for this component.
  disabled: js.UndefOr[Boolean] = js.undefined,
  href: js.UndefOr[String] = js.undefined,
  onClick: js.UndefOr[() => Unit] = js.undefined,
  `type`: js.UndefOr[String] = js.undefined // one of: 'button', 'reset', 'submit'
) extends ReactBootstrapComponent

case class Nav(
  id: js.UndefOr[String]  = js.undefined,
  className: js.UndefOr[String] = js.undefined,
  ref: js.UndefOr[String] = js.undefined, key: js.UndefOr[Any] = js.undefined,
  activeKey: js.UndefOr[Any] = js.undefined,
  bsStyle: js.UndefOr[String] = js.undefined)
    extends ReactBootstrapComponent

case class NavItem(
  id: js.UndefOr[String]  = js.undefined,
  className: js.UndefOr[String] = js.undefined,
  ref: js.UndefOr[String] = js.undefined,
  key: js.UndefOr[Any] = js.undefined,
  eventKey: js.UndefOr[Any] = js.undefined,
  href: js.UndefOr[String] = js.undefined,
  title: js.UndefOr[String] = js.undefined,
  onClick: js.UndefOr[() => Unit] = js.undefined,
  onSelect: js.UndefOr[() => Unit] = js.undefined
)
    extends ReactBootstrapComponent

case class Grid(
  id: js.UndefOr[String]  = js.undefined,
  className: js.UndefOr[String] = js.undefined,
  ref: js.UndefOr[String] = js.undefined,
  key: js.UndefOr[Any] = js.undefined,
  bsClass: js.UndefOr[String] = js.undefined,
  componentClass: js.UndefOr[String] = js.undefined,
  fluid: js.UndefOr[Boolean] = js.undefined)
    extends ReactBootstrapComponent

case class Row(
  id: js.UndefOr[String]  = js.undefined,
  className: js.UndefOr[String] = js.undefined,
  ref: js.UndefOr[String] = js.undefined,
  key: js.UndefOr[Any] = js.undefined,
  bsClass: js.UndefOr[String] = js.undefined)
    extends ReactBootstrapComponent


case class Col(
  id: js.UndefOr[String]  = js.undefined,
  className: js.UndefOr[String] = js.undefined,
  ref: js.UndefOr[String] = js.undefined, key: js.UndefOr[Any] = js.undefined,
  // 'col' Base CSS class and prefix for the component. Generally one should only change bsClass to provide new, non-Bootstrap, CSS styles for a component.
  bsClass: js.UndefOr[String] = js.undefined,

  //'div' You can use a custom element type for this component.
  componentClass: js.UndefOr[String] = js.undefined,

  // The number of columns you wish to span for Large devices Desktops (≥1200px) class-prefix col-lg-
  lg: js.UndefOr[Int] = js.undefined,

  /*Hide column on Large devices Desktops adds class hidden-lg*/
  lgHidden: js.UndefOr[Boolean] = js.undefined,

  /*Move columns to the right for Large devices Desktops class-prefix col-lg-offset-*/
  lgOffset: js.UndefOr[Int] = js.undefined,

  /*Change the order of grid columns to the left for Large devices Desktops class-prefix col-lg-pull-*/
  lgPull: js.UndefOr[Int] = js.undefined,

  /* Change the order of grid columns to the right for Large devices Desktops class-prefix col-lg-push- */
  lgPush: js.UndefOr[Int] = js.undefined,

  /*The number of columns you wish to span for Medium devices Desktops (≥992px) class-prefix col-md-*/
  md: js.UndefOr[Int] = js.undefined,

  /*Hide column on Medium devices Desktops adds class hidden-md*/
  mdHidden: js.UndefOr[Boolean] = js.undefined,

  /*Move columns to the right for Medium devices Desktops class-prefix col-md-offset-*/
  mdOffset: js.UndefOr[Int] = js.undefined,

  /*Change the order of grid columns to the left for Medium devices Desktops class-prefix col-md-pull-*/
  mdPull: js.UndefOr[Int] = js.undefined,

  /*Change the order of grid columns to the right for Medium devices Desktops class-prefix col-md-push-*/
  mdPush: js.UndefOr[Int] = js.undefined,

  /*The number of columns you wish to span for Small devices Tablets (≥768px) class-prefix col-sm-*/
  sm: js.UndefOr[Int] = js.undefined,

  /*Hide column on Small devices Tablets adds class hidden-sm*/
  smHidden: js.UndefOr[Boolean] = js.undefined,

  /*Move columns to the right for Small devices Tablets class-prefix col-sm-offset-*/
  smOffset: js.UndefOr[Int] = js.undefined,

  /*Change the order of grid columns to the left for Small devices Tablets class-prefix col-sm-pull-*/
  smPull: js.UndefOr[Int] = js.undefined,

  /* Change the order of grid columns to the right for Small devices Tablets class-prefix col-sm-push-*/
  smPush: js.UndefOr[Int] = js.undefined,

  /*The number of columns you wish to span for Extra small devices Phones (<768px) class-prefix col-xs-*/
  xs: js.UndefOr[Int] = js.undefined,

  /*Hide column on Extra small devices Phones adds class hidden-xs*/
  xsHidden: js.UndefOr[Boolean] = js.undefined,

  /*Move columns to the right for Extra small devices Phones class-prefix col-xs-offset- */
  xsOffset: js.UndefOr[Int] = js.undefined,

  // Change the order of grid columns to the left for Extra small devices Phones class-prefix col-xs-pull-
  xsPull: js.UndefOr[Int] = js.undefined,

  // Change the order of grid columns to the right for Extra small devices Phones class-prefix col-xs-push-
  xsPush: js.UndefOr[Int] = js.undefined)
    extends ReactBootstrapComponent


case class Clearfix(
  id: js.UndefOr[String]  = js.undefined,
  className: js.UndefOr[String] = js.undefined,
  ref: js.UndefOr[String] = js.undefined, key: js.UndefOr[Any] = js.undefined,
  /*'clearfix'Base CSS class and prefix for the component. Generally one should only change bsClass to provide new, non-Bootstrap, CSS styles for a component. */
  bsClass: js.UndefOr[ String] = js.undefined,
  /*'div'You can use a custom element type for this component.*/
  componentClass: js.UndefOr[String] = js.undefined,
  /* Apply clearfix on Large devices Desktops adds class visible-lg-block */
  visibleLgBlock: js.UndefOr[Boolean] = js.undefined,
  /* Apply clearfix on Medium devices Desktops adds class visible-md-block */
  visibleMdBlock: js.UndefOr[Boolean] = js.undefined,
  /* Apply clearfix on Small devices Tablets adds class visible-sm-block */
  visibleSmBlock: js.UndefOr[Boolean] = js.undefined,
  /* Apply clearfix on Extra small devices Phones adds class visible-xs-block */
  visibleXsBlock: js.UndefOr[Boolean] = js.undefined)
    extends ReactBootstrapComponent

case class Panel(
  id: js.UndefOr[String]  = js.undefined,
  className: js.UndefOr[String] = js.undefined,
  ref: js.UndefOr[String] = js.undefined, key: js.UndefOr[Any] = js.undefined,
  bsClass: js.UndefOr[String] = "panel",
  /*'panel' Base CSS class and prefix for the component. Generally one should only change bsClass to provide new, non-Bootstrap, CSS styles for a component.*/

  bsStyle: js.UndefOr[String] = "default",
  /* one of: "success", "warning", "danger", "info", "default", "primary" 'default' - Component visual or contextual style variants.*/

  collapsible: js.UndefOr[Boolean] = js.undefined,
  defaultExpanded: js.UndefOr[Boolean] = false,
  eventKey: js.UndefOr[Any] = js.undefined,
  expanded: js.UndefOr[Boolean] = js.undefined,
  footer: js.UndefOr[Any] = js.undefined, // Node
  header: js.UndefOr[js.|[ReactNode, String]] = js.undefined, // Node
  headerRole: js.UndefOr[String] = js.undefined,
  onEnter: js.UndefOr[() => Unit] = js.undefined,
  onEntered: js.UndefOr[() => Unit] = js.undefined,
  onEntering: js.UndefOr[() => Unit] = js.undefined,
  onExit: js.UndefOr[() => Unit] = js.undefined,
  onExited: js.UndefOr[() => Unit] = js.undefined,
  onExiting: js.UndefOr[() => Unit] = js.undefined,
  onSelect: js.UndefOr[() => Unit] = js.undefined,
  panelRole: js.UndefOr[String] = js.undefined)
extends ReactBootstrapComponent

case class Accordion(
  id: js.UndefOr[String]  = js.undefined,
  className: js.UndefOr[String] = js.undefined,
  ref: js.UndefOr[String] = js.undefined, key: js.UndefOr[Any] = js.undefined,
  bsClass: js.UndefOr[String] = "panel",
  /*'panel' Base CSS class and prefix for the component. Generally one should only change bsClass to provide new, non-Bootstrap, CSS styles for a component.*/

  bsStyle: js.UndefOr[String] = "default",
  /* one of: "success", "warning", "danger", "info", "default", "primary" 'default' - Component visual or contextual style variants.*/

  collapsible: js.UndefOr[Boolean] = js.undefined,
  eventKey: js.UndefOr[Any] = js.undefined,
  expanded: js.UndefOr[Boolean] = js.undefined,
  footer: js.UndefOr[Any] = js.undefined, // Node
  header: js.UndefOr[Any] = js.undefined, // Node
  headerRole: js.UndefOr[String] = js.undefined,
  onEnter: js.UndefOr[() => Unit] = js.undefined,
  onEntered: js.UndefOr[() => Unit] = js.undefined,
  onEntering: js.UndefOr[() => Unit] = js.undefined,
  onExit: js.UndefOr[() => Unit] = js.undefined,
  onExited: js.UndefOr[() => Unit] = js.undefined,
  onExiting: js.UndefOr[() => Unit] = js.undefined,
  onSelect: js.UndefOr[() => Unit] = js.undefined,
  panelRole: js.UndefOr[String] = js.undefined)
    extends ReactBootstrapComponent

case class OverlayTrigger(
  id: js.UndefOr[String]  = js.undefined,
  className: js.UndefOr[String] = js.undefined,
  ref: js.UndefOr[String] = js.undefined, key: js.UndefOr[Any] = js.undefined,
  placement: js.UndefOr[String] = "left",
  overlay: ReactNode)
    extends ReactBootstrapComponent

case class Tooltip(
  id: js.UndefOr[String], // An html id attribute, necessary for accessibility
  className: js.UndefOr[String] = js.undefined,
  ref: js.UndefOr[String] = js.undefined, key: js.UndefOr[Any] = js.undefined,

  arrowOffsetLeft: js.UndefOr[js.|[Int, String]] = js.undefined, // The "left" position value for the Tooltip arrow.
  arrowOffsetTop: js.UndefOr[js.|[Int, String]] = js.undefined, // The "top" position value for the Tooltip arrow.
  bsClass: String = "tooltip", // Base CSS class and prefix for the component. Generally one should only change bsClass to provide new, non-Bootstrap, CSS styles for a component.

  placement: String = "right", //  one of: 'top', 'right', 'bottom', 'left'; Sets the direction the Tooltip is positioned towards.

  positionLeft: js.UndefOr[js.|[Int, String]] = js.undefined, // The "left" position value for the Tooltip.
  positionTop: js.UndefOr[js.|[Int, String]] = js.undefined // The "top" position value for the Tooltip.
) extends ReactBootstrapComponent

case class Table(
  id: js.UndefOr[String] = js.undefined, // An html id attribute, necessary for accessibility
  className: js.UndefOr[String] = js.undefined,
  ref: js.UndefOr[String] = js.undefined, key: js.UndefOr[Any] = js.undefined,
  bordered: Boolean = false,
  bsClass: String = "table", // Base CSS class and prefix for the component. Generally one should only change bsClass to provide new, non-Bootstrap, CSS styles for a component.
  condensed: Boolean = false,
  hover: Boolean = false,
  responsive: Boolean = false,
  striped: Boolean = false
) extends ReactBootstrapComponent

case class Collapse(
  id: js.UndefOr[String] = js.undefined, // An html id attribute, necessary for accessibility
  className: js.UndefOr[String] = js.undefined,
  ref: js.UndefOr[String] = js.undefined, key: js.UndefOr[Any] = js.undefined,

  dimension: String = "width",

  getDimensionValue: js.UndefOr[() => Unit] = js.undefined,

  in: Boolean = false, // Show the component; triggers the fade in or fade out animation

  onEnter: js.UndefOr[() => Unit] = js.undefined, // Callback fired before the component fades in

  onEntered: js.UndefOr[() => Unit] = js.undefined, // Callback fired after the has component faded in

  onEntering: js.UndefOr[() => Unit] = js.undefined, // Callback fired after the component starts to fade in

  onExit: js.UndefOr[() => Unit] = js.undefined, // Callback fired before the component fades out

  onExited: js.UndefOr[() => Unit] = js.undefined, // Callback fired after the component has faded out

  onExiting: js.UndefOr[() => Unit] = js.undefined, // Callback fired after the component starts to fade out

  timeout: Int = 300, // Duration of the fade animation in milliseconds, to ensure that finishing callbacks are fired even if the original browser transition end events are canceled

  transitionAppear: Boolean = false, // Run the fade in animation when the component mounts, if it is initially shown

  unmountOnExit: Boolean = false	// Unmount the component (remove it from the DOM) when it is faded out
) extends ReactBootstrapComponent


case class Alert(
  id: js.UndefOr[String]  = js.undefined,
  className: js.UndefOr[String] = js.undefined,
  ref: js.UndefOr[String] = js.undefined, key: js.UndefOr[Any] = js.undefined,

  /** Base CSS class and prefix for the component. Generally one should only change bsClass to provide new,
    * non-Bootstrap, CSS styles for a component.*/
  bsClass: js.UndefOr[String] = "alert",

  /**one of: "success", "warning", "danger", "info"; Component visual or contextual style variants. */
  bsStyle: js.UndefOr[String] = "info",

  /**'Close alert' */
  closeLabel: js.UndefOr[String] = js.undefined,
  onDismiss: js.UndefOr[() => Unit] = js.undefined
) extends ReactBootstrapComponent
