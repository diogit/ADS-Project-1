import akka.actor.Actor
import akka.actor.ActorRef
import scala.util.Random
import akka.actor.ActorSelection


class Test(myHostname:String, myPort:String, testerIp:String, testerPort:String) extends Actor {
    var log:Map[String, TopicLog] = Map( ("t1" -> new TopicLog("t1")), ("t2" -> new TopicLog("t2")), ("t3" -> new TopicLog("t3")), ("t4" -> new TopicLog("t4")), ("t5" -> new TopicLog("t5")), ("t6" -> new TopicLog("t6")), ("t7" -> new TopicLog("t7")), ("t8" -> new TopicLog("t8")), ("t9" -> new TopicLog("t9")), ("t10" -> new TopicLog("t10")), ("t11" -> new TopicLog("t11")), ("t12" -> new TopicLog("t12")), ("t13" -> new TopicLog("t13")), ("t14" -> new TopicLog("t14")), ("t15" -> new TopicLog("t15")), ("t16" -> new TopicLog("t16")), ("t17" -> new TopicLog("t17")), ("t18" -> new TopicLog("t18")), ("t19" -> new TopicLog("t19")), ("t20" -> new TopicLog("t20")), ("t21" -> new TopicLog("t21")), ("t22" -> new TopicLog("t22")), ("t23" -> new TopicLog("t23")), ("t24" -> new TopicLog("t24")), ("t25" -> new TopicLog("t25")), ("t26" -> new TopicLog("t26")), ("t27" -> new TopicLog("t27")), ("t28" -> new TopicLog("t28")), ("t29" -> new TopicLog("t29")), ("t30" -> new TopicLog("t30")), ("t31" -> new TopicLog("t31")), ("t32" -> new TopicLog("t32")), ("t33" -> new TopicLog("t33")), ("t34" -> new TopicLog("t34")), ("t35" -> new TopicLog("t35")), ("t36" -> new TopicLog("t36")), ("t37" -> new TopicLog("t37")), ("t38" -> new TopicLog("t38")), ("t39" -> new TopicLog("t39")), ("t40" -> new TopicLog("t40")), ("t41" -> new TopicLog("t41")), ("t42" -> new TopicLog("t42")), ("t43" -> new TopicLog("t43")), ("t44" -> new TopicLog("t44")), ("t45" -> new TopicLog("t45")), ("t46" -> new TopicLog("t46")), ("t47" -> new TopicLog("t47")), ("t48" -> new TopicLog("t48")), ("t49" -> new TopicLog("t49")), ("t50" -> new TopicLog("t50")))

    def receive = {
        case Test_Create() =>
            println("[TEST] Creating topics")
            for (i <- 1 until 51){
                getScribeActorRefFromAddress(myHostname, myPort) ! CreateTopic("t"+i)
            }
        case Test_Subscribe() =>
            println("[TEST] Asking all nodes to subscribe to random topics")
            for (i <- 0 until 20){
                var port:Int = testerPort.toInt + i
                getTestActorRefFromAddress(testerIp, port.toString) ! Ask_To_Subscribe()
            }
        case Ask_To_Subscribe() =>
            println("[TEST] Was asked to subscribe to random topics")
            var rng = new Random()
            var subs:Set[Int] = Set()
            while (subs.size < 5){
                subs = subs + ((rng.nextInt(50))+1) // choose a new topic to subscribe
            }
            for (sub <- subs){
                getScribeActorRefFromAddress(myHostname, myPort) ! Subscribe("t"+sub)
                //getTestActorRefFromAddress(testerIp, testerPort) ! Log("t"+sub, "SUBSCRIBED", 1) // log it
            }
        case Test_Publish() =>
            println("[TEST] Asking all nodes to publish to random topics")
            for (i <- 0 until 20){
                var port:Int = testerPort.toInt + i
                getTestActorRefFromAddress(testerIp, port.toString) ! Ask_To_Publish()
            }
        case Ask_To_Publish() =>
            println("[TEST] Was asked to publish to random topics")
            var rng = new Random()
            var pubs:Set[Int] = Set()
            while (pubs.size < 5){
                pubs = pubs + ((rng.nextInt(50))+1) // choose a new topic to publish to
            }
            for (pub <- pubs){
                getScribeActorRefFromAddress(myHostname, myPort) ! Publish("t"+pub, "[Ask_To_Publish > Hello from the other side! I'm "+myHostname+":"+myPort+"]")
                //getTestActorRefFromAddress(testerIp, testerPort) ! Log("t"+pub, "PUBLISHED", 1) // log it
            }
        case Test_Print() =>
            var createdTotal:Int = 0
            var subsTotal:Int = 0
            var sentTotal:Int = 0
            var receivedTotal:Int = 0
            var expectedTotal:Int = 0
            println("[TEST] Test Log Table:")
            for (topicLog <- log){
                var expected = topicLog._2.created * topicLog._2.subscribers * topicLog._2.sent
                var percent:Int = 100
                if (expected != 0){
                    percent = ((topicLog._2.received * 100) / expected)
                }
                createdTotal = createdTotal + topicLog._2.created
                subsTotal = subsTotal + topicLog._2.subscribers
                sentTotal = sentTotal + topicLog._2.sent
                receivedTotal = receivedTotal + topicLog._2.received
                expectedTotal = expectedTotal + expected
                println(topicLog._2.title+"\t> Created: "+topicLog._2.created+"\tSubscribers: "+topicLog._2.subscribers+"\tSent: "+topicLog._2.sent+"\tExpected: "+expected+"\tReceived: "+topicLog._2.received+"\t("+percent+"%)")
            }
            var percentTotal:Int = 100
            if (expectedTotal != 0){
                percentTotal = ((receivedTotal * 100) / expectedTotal)
            }
            println("TOTAL\t> Created: "+createdTotal+"\tSubscribers: "+subsTotal+"\tSent: "+sentTotal+"\tExpected: "+expectedTotal+"\tReceived: "+receivedTotal+"\t("+percentTotal+"%)")
        case Created(topic:String) =>
            getTestActorRefFromAddress(testerIp, testerPort) ! Log(topic, "CREATED", 1) // log it
        case Subscribed(topic:String) =>
            getTestActorRefFromAddress(testerIp, testerPort) ! Log(topic, "SUBSCRIBED", 1) // log it
        case Published(topic:String) =>
            getTestActorRefFromAddress(testerIp, testerPort) ! Log(topic, "PUBLISHED", 1) // log it
        case Received(msg:Message) =>
            println("[TEST] Received a "+msg.msgType+" message on \""+msg.topic+"\": "+msg.content)
            getTestActorRefFromAddress(testerIp, testerPort) ! Log(msg.topic, "RECEIVED", 1) // log it
        case Log(topic:String, logType:String, count:Int) =>
            if (log contains topic){
                println("[TEST] Logging "+count+" "+logType+" event(s) on topic \""+topic+"\"")
                logType match {
                    case "CREATED" =>
                        log(topic).created = log(topic).created + count
                    case "PUBLISHED" =>
                        log(topic).sent = log(topic).sent + count
                    case "SUBSCRIBED" =>
                        log(topic).subscribers = log(topic).subscribers + count
                    case "RECEIVED" =>
                        log(topic).received = log(topic).received + count
                }
            } else {
                println("[TEST] Someone wanted me to log "+count+" "+logType+" event(s) on unknown topic \""+topic+"\"")
            }
        case _ =>
            println("[TEST] Default case. Unknown message was received.")
    }

    //Get Test actor's reference so we can send them a message.
    def getTestActorRefFromAddress(actorIp:String, actorPort:String) : ActorSelection = {
        return context.actorSelection("akka.tcp://ASDProject@"+actorIp+":"+actorPort+"/user/test")
    }

    //Get Scribe actor's reference so we can send them a message.
    def getScribeActorRefFromAddress(contactIp:String, contactPort:String) : ActorSelection = {
        return context.actorSelection("akka.tcp://ASDProject@"+contactIp+":"+contactPort+"/user/scribe") //akka.<protocol>://<actor system name>@<hostname>:<port>/<actor path>
    }
}