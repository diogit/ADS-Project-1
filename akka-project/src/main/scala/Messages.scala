import akka.actor.ActorRef

// CHORD

case class Create()

case class Join(node:Node)

case class Find_Successor(n:Node)
case class Find_Successor_Response(successor:Node)

case class Fix_Fingers()
case class FF_Find_Successor(n:Node, start:Int)
case class FF_Find_Successor_Response(successor:Node)

case class Stabilize()
case class Ask_For_Predecessor(n:Node)
case class Predecessor_Response(predecessor:Node)
case class Notify(pretender:Node)

case class Trigger_Heart_Beat_Predecessor()
case class Heart_Beat_Predecessor(node:Node)
case class Heart_Beat_ACK_Predecessor()

case class Trigger_Heart_Beat_Successor()
case class Heart_Beat_Successor(node:Node)
case class Heart_Beat_ACK_Successor(successorOfSuccessor:Node)

case class Route(msg:Message, topicId: Int)
case class Send(msg:Message, node:Node)
case class Ask_To_Deliver(msg:Message)
case class Ask_To_Forward(msg:Message)
case class Print_Fingers_Table()


// SCRIBE

case class Forward(msg:Message)
case class Deliver(msg:Message)
case class CreateTopic(topic:String)
case class Subscribe(topic:String)
case class Publish(topic:String, content:String)
case class Unsubscribe(topi:String)
case class Trigger_Subscriptions_Renewal()
case class Print_Subscriptions()
case class Print_Topics()

// TEST
case class Test_Create()
case class Test_Subscribe()
case class Ask_To_Subscribe()
case class Test_Publish()
case class Ask_To_Publish()
case class Test_Print()
case class Created(topic:String)
case class Subscribed(topic:String)
case class Published(topic:String)
case class Received(msg:Message)
case class Log(topic:String, logType:String, count:Int)