import akka.actor.Actor
import akka.actor.ActorRef
import scala.util.Random
import akka.actor.ActorSelection


class Scribe(myHostname:String, myPort:String) extends Actor {
    var topics:Map[String, Topic] = Map() // Map of known topics
    var thisNode:Node = new Node(-1, myHostname, myPort) // this very node
    val m:Int = 8
    val subscriptionExpirationLimit:Int = 5

    def receive = {
        case Forward(msg:Message) =>
            if (msg.msgType == "SUBSCRIBE") {
                // Got a Subscribe message to forward
                if (!(topics contains msg.topic)){
                    topics = topics + (msg.topic -> new Topic(msg.topic))
                    var newMsg = new Message(msg.msgType, thisNode.ip, thisNode.port, msg.topic, msg.content, msg.topicOwnerIp, msg.topicOwnerPort)
                    getChordActorRefFromAddress(thisNode.ip, thisNode.port) ! Route(newMsg, hash(msg.topic))
                }
                // Topic exists
                topics(msg.topic).children = topics(msg.topic).children + (new Node(-1, msg.ip, msg.port))
            } else {
                // Keep forwarding it you dumb shit
                getChordActorRefFromAddress(thisNode.ip, thisNode.port) ! Route(msg, hash(msg.topic))
            }
        case Deliver(msg:Message) =>
            msg.msgType match {
                case "CREATE" =>
                    if (topics contains msg.topic){
                        println("[SCRIBE] Tried to create already existing topic: "+msg.topic)
                    } else {
                        topics = topics + (msg.topic -> new Topic(msg.topic))
                        println("[SCRIBE] Created topic "+msg.topic)
                        getTestActorRefFromAddress(thisNode.ip, thisNode.port) ! Created(msg.topic)
                    }
                case "SUBSCRIBE" =>
                    if (topics contains msg.topic) {
                        if(!((msg.ip == thisNode.ip) && (msg.port == thisNode.port))) {
                            // This isn't me subscribing
                            topics(msg.topic).children = topics(msg.topic).children + (new Node(-1, msg.ip, msg.port))
                            println("[SCRIBE] New subscriber to \""+msg.topic+"\" topic:  "+msg.ip+":"+msg.port)
                        } else {
                            // To prevent entering an infinite loop where it keeps sending itself the message
                            //println("[SCRIBE] Trying to subscribe to myself. Not exactly an error, I'm already subscribed")
                        }
                    } else {
                        println("[SCRIBE] Someone tried to subscribe to an unknown topic. Topic:"+msg.topic+"|Source:"+msg.ip+":"+msg.port)
                        topics = topics + (msg.topic -> new Topic(msg.topic))
                        println("[SCRIBE] Recreated topic "+msg.topic)
                    }
                case "PUBLISH" =>
                    if (topics contains msg.topic){
                        topics(msg.topic).parent = new Node(-1, msg.ip, msg.port)
                        var newMsg = new Message(msg.msgType, thisNode.ip, thisNode.port, msg.topic, msg.content, msg.topicOwnerIp, msg.topicOwnerPort)
                        if (msg.topicOwnerIp == null && msg.topicOwnerPort == null){
                            // Reached the topic owner, replace topic owner in message so we don't get caught in a loop
                            // If this works I apologise for ruining this algorith with spaghetti
                            newMsg = new Message(msg.msgType, thisNode.ip, thisNode.port, msg.topic, msg.content, thisNode.ip, thisNode.port)
                        }
                        for (child <- topics(msg.topic).children){
                            // Send message to all interest children
                            if (!((child.ip == thisNode.ip) && (child.port == thisNode.port)) && !((child.ip == newMsg.topicOwnerIp) && (child.port == newMsg.topicOwnerPort))){
                                println("[SCRIBE] Sending \""+msg.content+"\" to "+child+" for topic: "+msg.topic+" ("+hash(msg.topic)+")")
                                getChordActorRefFromAddress(thisNode.ip, thisNode.port) ! Send(newMsg, child)
                            }
                        }
                        if (topics(msg.topic).subscribed == true){
                            // Trigger the upper layer event
                            println("[SCRIBE] Got the message \""+msg.content+"\" for topic: "+msg.topic+" ("+hash(msg.topic)+")")
                            getTestActorRefFromAddress(thisNode.ip, thisNode.port) ! Received(msg)
                        } else {
                            if (topics(msg.topic).children.size == 0){
                                // I'm not subscribed and I got no one to send the message to
                                if (topics(msg.topic).parent != null && topics(msg.topic).parent.ip != null && topics(msg.topic).parent.port != null){
                                    // I'm not interested, don't have children and am not the topic owner
                                    var newNewMsg = new Message("UNSUBSCRIBE", thisNode.ip, thisNode.port, msg.topic, "UNSUBSCRIBE FROM TOPIC: "+msg.topic, msg.topicOwnerIp, msg.topicOwnerPort)
                                    getChordActorRefFromAddress(thisNode.ip, thisNode.port) ! Send(newNewMsg, topics(msg.topic).parent)

                                    //topics = topics - msg.topic // Remove from list of topics
                                }
                            }
                        }
                    } else {
                        println("[SCRIBE] Someone tried to publish to an unknown topic. Topic:"+msg.topic+"|Source:"+msg.ip+":"+msg.port)
                    }
                case "UNSUBSCRIBE" =>
                    if (topics contains msg.topic) {
                        // The topic exists locally
                        topics(msg.topic).children = topics(msg.topic).children - (new Node(-1, msg.ip, msg.port))
                        if ((topics(msg.topic).children.size == 0) && (topics(msg.topic).subscribed != true) && (topics(msg.topic).parent != null) && (topics(msg.topic).parent.ip != null) && (topics(msg.topic).parent.port != null)){
                            var newMsg = new Message(msg.msgType, thisNode.ip, thisNode.port, msg.topic, msg.content, msg.topicOwnerIp, msg.topicOwnerPort)
                            getChordActorRefFromAddress(thisNode.ip, thisNode.port) ! Send(newMsg, topics(msg.topic).parent)

                            //topics = topics - msg.topic // Remove from list of topics
                        }
                        println("[SCRIBE] Unsubscribed from "+msg.topic)
                    } else {
                        // I don't know this topic, can't unsubscribe
                        println("[SCRIBE] Someone tried to unsubscribe from an unknown topic. Topic:"+msg.topic+"|Source:"+msg.ip+":"+msg.port)
                    }
                case _ =>
                    println("[SCRIBE] Got an unknown scribe message")
            }
        case CreateTopic(topic:String) =>
            var msg = new Message("CREATE", thisNode.ip, thisNode.port, topic, "CREATE TOPIC: "+topic, null, null)
            getChordActorRefFromAddress(thisNode.ip, thisNode.port) ! Route(msg, hash(topic))
        case Subscribe(topic:String) =>
            if (!(topics contains topic)){
                // Didn't know the topic, but I do now
                topics = topics + (topic -> new Topic(topic))
            }
            topics(topic).subscribed = true
            topics(topic).renewalCounter = 0
            getTestActorRefFromAddress(thisNode.ip, thisNode.port) ! Subscribed(topic)
            
            var msg = new Message("SUBSCRIBE", thisNode.ip, thisNode.port, topic, "SUBSCRIBE TO TOPIC: "+topic, null, null)
            getChordActorRefFromAddress(thisNode.ip, thisNode.port) ! Route(msg, hash(topic))
        case Publish(topic:String, content:String) =>
            var msg = new Message("PUBLISH", null, null, topic, content, null, null)
            getChordActorRefFromAddress(thisNode.ip, thisNode.port) ! Route(msg, hash(topic))
            getTestActorRefFromAddress(thisNode.ip, thisNode.port) ! Published(topic)
        case Unsubscribe(topic:String) =>
            if (!(topics contains topic)){
                println("[SCRIBE] Tried to unsubscribe from a topic I'm not subscribed to")
            } else {
                topics(topic).subscribed = false
                if ((topics(topic).children.size == 0) && (topics(topic).parent != null) && (topics(topic).parent.ip != null) && (topics(topic).parent.port != null)){
                    var msg = new Message("UNSUBSCRIBE", thisNode.ip, thisNode.port, topic, "UNSUBSCRIBE FROM TOPIC: "+topic, null, null)
                    getChordActorRefFromAddress(thisNode.ip, thisNode.port) ! Send(msg, topics(topic).parent)

                    //topics = topics - topic // Remove from list of topics
                }
            }
        case Trigger_Subscriptions_Renewal() =>
            for (topic <- topics){
                if (topic._2.subscribed == true){
                    // This is a topic that I am still subscribed
                    if (topic._2.renewalCounter > subscriptionExpirationLimit){
                        // Subscription time expired
                        var msg = new Message("SUBSCRIBE", thisNode.ip, thisNode.port, topic._1, "SUBSCRIBE TO TOPIC: "+topic._1, null, null)
                        getChordActorRefFromAddress(thisNode.ip, thisNode.port) ! Route(msg, hash(topic._1))

                        topic._2.renewalCounter = 0 // Reset time
                    } else {
                        // The limit has not passed
                        topic._2.renewalCounter = topic._2.renewalCounter + 1 // Increment time
                    }
                }
            }
        //FOR DEBUG ONLY, NOT ACTUALLY PART OF THE ALGORITHM
        case Print_Subscriptions() =>
            println("[SCRIBE] Topics this node is subscribed to:")
            for (topic <- topics){
                if (topic._2.subscribed){
                    println(topic._1)
                }
            }
        //FOR DEBUG ONLY, NOT ACTUALLY PART OF THE ALGORITHM
        case Print_Topics() =>
            println("[SCRIBE] Topics table:")
            for (topic <- topics){
                println(topic._2.title+" ("+hash(topic._2.title)+") > parent: "+topic._2.parent+" | #children: "+topic._2.children.size+" | subscribed: "+topic._2.subscribed+" | renewalCounter: "+topic._2.renewalCounter)
            }
        case _ =>
            println("[SCRIBE] Default case. Unknown message was received.")
    }

    //Get Chord actor's reference so we can send them a message.
    def getChordActorRefFromAddress(contactIp:String, contactPort:String) : ActorSelection = {
        return context.actorSelection("akka.tcp://ASDProject@"+contactIp+":"+contactPort+"/user/chord") //akka.<protocol>://<actor system name>@<hostname>:<port>/<actor path>
    }

    //Get Test actor's reference so we can send them a message.
    def getTestActorRefFromAddress(actorIp:String, actorPort:String) : ActorSelection = {
        return context.actorSelection("akka.tcp://ASDProject@"+actorIp+":"+actorPort+"/user/test")
    }

    // Calculate the topic hash
    def hash(topic:String) : Int = {
        return Math.abs((topic.hashCode() % Math.pow(2, m)).toInt)
    }
}