package akka

import akka.actor.{Props, ActorSystem, Actor}
import akka.util.Timeout

import scala.concurrent.Future


/**
  * Created by winie on 2016/4/6.
  */
class ReplyActor extends Actor{
   def receive={
     case "hi"=> sender!"hello"
   }
}

class FutureActor extends Actor{
  def receive={
    case m=>print(m)
  }
}

object ReplyDemo extends App{
  val system=ActorSystem("ReplyDemo")
  val actor1=system.actorOf(Props[ReplyActor],name="ReplyActor")
  val actor2=system.actorOf(Props[FutureActor],name="FutureActor")
  implicit val timeout=Timeout(1)
  val future1:Future[String]=ask(actor1,"hi").mapTo[String]
  future1 pipeTo actor2
}