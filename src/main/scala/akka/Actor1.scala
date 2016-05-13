package akka

import akka.actor.{Props, ActorSystem, Actor}
import akka.event.Logging

/**
  * Created by winie on 2016/4/6.
  */
class Actor1 extends Actor {
  val log = Logging(context.system, this)

  def receive = {
    case "test" => log.info("received test")
    case _ => log.info("received unknown message")
  }
}

object Demo1 extends App {
  val system = ActorSystem("Demo1")
  val actor1 = system.actorOf(Props[Actor1], name = "Actor1")
  actor1 ! "test"
  actor1 ! "other"
}