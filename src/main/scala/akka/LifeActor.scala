package akka

import akka.actor.Actor
import akka.event.Logging
import akka.io.Tcp.Message

/**
  * Created by winie on 2016/4/6.
  */
class LifeActor  extends  Actor{
  val log = Logging(context.system, this)
  override  def preStart(){}
  override  def preRestart(reason:Throwable,message: Option[Any]): Unit ={
    super.preRestart(reason,message)
  }

  def receive={
    case  _=>
  }
}
