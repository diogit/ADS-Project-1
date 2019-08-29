import akka.actor.Actor
import akka.actor.ActorRef
import scala.util.Random
import akka.actor.ActorSelection


class Chord(myHostname:String, myPort:String) extends Actor {
  val m: Int = 8 // Size of id = 2^m

  var thisNode:Node = new Node(createID(), myHostname, myPort) // this very node
  var fingerTable: Array[Finger] = new Array[Finger](m + 1) //First entry is ignored
  // initialize fingerTable
  for(i <- 1 until m+1){
	val start:Int = calculateStart(thisNode.id, i)
	val end:Int = calculateStart(thisNode.id, i+1)
	var node:Node = thisNode
	fingerTable(i) = new Finger(start, end, node)
  }
  var next:Int = 0 // Next finger table index to fix
  var predecessor: Node = null
  var successorSuccessor: Node = fingerTable(1).node

  var heartBeatCounterPredecessor: Int = 0
  val heartBeatLimitPredecessor: Int = 3

  var heartBeatCounterSuccessor: Int = 0
  val heartBeatLimitSuccessor: Int = 3

  var joinCompleted: Boolean = false

  def receive = {
  	// create a new network
  	case Create() =>
	  	// First node of the network. No Join being executed.
	  	joinCompleted = true;
  		println("[CHORD] I'm "+thisNode.id+"@"+myHostname+":"+myPort+", the first node of the network!")
		predecessor = null
		fingerTable(1) = new Finger(calculateStart(thisNode.id, 1), calculateStart(thisNode.id, 2), thisNode)
	// node n (us) joins the network;
	// nodeID is (our contact) an arbitrary node in the network
	case Join(contact:Node) =>
		println("[CHORD] I'm "+thisNode.id+"@"+myHostname+":"+myPort+"!")
		println("[CHORD] Joining a Chord network by contacting node @ "+contact.ip+":"+contact.port)
		predecessor = null
		getChordActorRefFromAddress(contact.ip, contact.port) ! Find_Successor(thisNode)
	// JOIN: ask node n to find the successor of id
	case Find_Successor(n:Node) =>
		println("[CHORD] Received Find_Successor")
		if (belongsToInterval(n.id, thisNode.id, fingerTable(1).node.id)) {
			//println("[CHORD] IF")
			getChordActorRefFromAddress(n.ip, n.port) ! Find_Successor_Response(fingerTable(1).node)
		} else {
			//println("[CHORD] ELSE")
			var otherN = Closest_Preceding_Node(n)
			//println("[CHORD] Returned "+otherN.id+" as the Closest_Preceding_Node")
			// TODO triple check this if
			if (otherN.id == thisNode.id){
				getChordActorRefFromAddress(n.ip, n.port) ! Find_Successor_Response(thisNode) // TODO check if this shouldn't be Find_Successor_Response instead
			} else {
				getChordActorRefFromAddress(otherN.ip, otherN.port) ! Find_Successor(n)
			}
		}
	// JOIN: The return from the Find_Successor for the end of the Join (yes this is confusing me too)
	case Find_Successor_Response(successor:Node) =>
		if (successor.id != thisNode.id) {
			//Our ID is unique. No collision happened.
			joinCompleted = true;
			fingerTable(1).node = successor
			successorSuccessor = successor
			heartBeatCounterSuccessor = 0
			println("[CHORD] Joined the network")
			//node_info_dump()
		} else {
			//Our ID is different
			thisNode = new Node(createID(), myHostname, myPort) // this very node
			// Regenerate fingerTable
			for(i <- 1 until m+1){
				var start:Int = calculateStart(thisNode.id, i)
				var end:Int = calculateStart(thisNode.id, i+1)
				var node:Node = thisNode
				fingerTable(i) = new Finger(start, end, node)
			}

			next = 0 // Next finger table index to fix
			predecessor = null
			successorSuccessor = fingerTable(1).node

			heartBeatCounterPredecessor = 0
			heartBeatCounterSuccessor = 0
			
			//  We'll use successor as our new contact
			println("[CHORD] Rejoining a Chord network (because of collision, "+successor.id+" already exists). Contacting node: "+successor.id+" @ "+successor.ip+":"+successor.port)
			println("[CHORD] I'm "+thisNode.id+"@"+myHostname+":"+myPort+"!")
			getChordActorRefFromAddress(successor.ip,successor.port) ! Find_Successor(thisNode)
		}
	// FIX FINGERS
	// Periodic Call
	case Fix_Fingers() =>
		if(joinCompleted == true) {
			next = next + 1
			if (next > m){
				next = 1
			}
			getChordActorRefFromAddress(thisNode.ip, thisNode.port) ! FF_Find_Successor(thisNode, fingerTable(next).intervalStart)
		}
	// FIX FINGERS: ask node n to find the successor of id
	case FF_Find_Successor(n:Node, start:Int) =>
		//println("[CHORD] [FIX FINGERS] Received Find_Successor from "+sender)
		if (belongsToInterval(start, thisNode.id, fingerTable(1).node.id)) {
			// println(start+" e "+"]"+thisNode.id+","+fingerTable(1).node.id+"]")
			// println("[CHORD] Sending my successor: "+fingerTable(1).node.id+", to node: "+n.id)
			getChordActorRefFromAddress(n.ip, n.port) ! FF_Find_Successor_Response(fingerTable(1).node)
		} else {
			// println(start+" -e "+"]"+thisNode.id+","+fingerTable(1).node.id+"]")
			var otherN = FF_Closest_Preceding_Node(start)
			//println("[CHORD] Returned "+otherN.id+" as the Closest_Preceding_Node")
			// TODO triple check this if
			if (otherN.id == thisNode.id){
				getChordActorRefFromAddress(n.ip, n.port) ! FF_Find_Successor_Response(thisNode)
			} else {
				// println("[CHORD] Asking "+otherN.id+" for his successor with start = "+start)
				getChordActorRefFromAddress(otherN.ip, otherN.port) ! FF_Find_Successor(n, start)
			}
		}
	// FIX FINGERS: The return from the Find_Successor for the end of the Join (yes this is confusing me too)
	case FF_Find_Successor_Response(successor:Node) =>
		//println("[CHORD] Got answer. Updating finger("+next+") from "+fingerTable(next).node.id+" to "+successor.id)
		fingerTable(next).node = successor
		//print("Fixed finger: ")
		//node_info_dump()
	// STABILIZE: Start
	// Periodic Call
	case Stabilize() =>
		if(joinCompleted == true) {
			//println("[CHORD] Stabilize start, asking pred from "+fingerTable(1).node.id)
			getChordActorRefFromAddress(fingerTable(1).node.ip, fingerTable(1).node.port) ! Ask_For_Predecessor(thisNode)
		}
	// STABILIZE: Asked what was my predecessor
	case Ask_For_Predecessor(n:Node) =>
		if (predecessor == null){
			// I don't have a predecessor, so I'll assume this guy is it
			//println("[CHORD] Don't have a pred, so assumming this guy is it: "+n.id)
			predecessor = n
			heartBeatCounterPredecessor = 0
		} else {
			// I already have a predecessor, so I'll let the asker deal with it
			// println("[CHORD] Responding to "+n.id+" with pred"+predecessor)
			getChordActorRefFromAddress(n.ip, n.port) ! Predecessor_Response(predecessor)
		}
	// STABILIZE: I know the predecessor of my successor
	case Predecessor_Response(predecessor:Node) =>
		// println("[CHORD] stablize end got: "+predecessor)
		// predecessor should never be null, because if it was our successor should assume we are his predecessor already
		if (belongsToIntervalBothExclusive(predecessor.id, thisNode.id, fingerTable(1).node.id)){
			// Turns out my old successor's predecessor is my successor, change my successor
			// println("[CHORD] switched successor")
			fingerTable(1).node = predecessor
		}
		getChordActorRefFromAddress(fingerTable(1).node.ip, fingerTable(1).node.port) ! Notify(thisNode)
	// STABILIZE: pretender believes it is my predecessor
	case Notify(pretender:Node) =>
		// println("[CHORD] my predecessor was "+predecessor)
		if (predecessor == null || belongsToIntervalBothExclusive(pretender.id, predecessor.id, thisNode.id)){
			// println("[CHORD] its now "+pretender.id)
			predecessor = pretender
			heartBeatCounterPredecessor = 0
		}
	// Periodic Call 
	case Trigger_Heart_Beat_Predecessor() =>
		if(joinCompleted == true) {
			if ( heartBeatCounterPredecessor > heartBeatLimitPredecessor) {
				// Predecessor is dead
				// TODO print counters because node predecessor is jumping between null & correct predecessor.  quando counter e posto a 0 e quando mata o gajo
				predecessor = null
				// New predecessor so restart counter
				heartBeatCounterPredecessor = 0
			} else if(predecessor != null) {
				getChordActorRefFromAddress(predecessor.ip, predecessor.port) ! Heart_Beat_Predecessor(thisNode)
				heartBeatCounterPredecessor = heartBeatCounterPredecessor + 1
			}
		}
	case Heart_Beat_Predecessor(node:Node) =>
		getChordActorRefFromAddress(node.ip, node.port) ! Heart_Beat_ACK_Predecessor()
	case Heart_Beat_ACK_Predecessor() =>
		heartBeatCounterPredecessor = 0
	// Periodic Call
	case Trigger_Heart_Beat_Successor() =>
		if(joinCompleted == true) {
			if ( heartBeatCounterSuccessor > heartBeatLimitSuccessor) {
				// Successor is dead
				fingerTable(1).node = successorSuccessor
				// New successor so restart counter
				heartBeatCounterSuccessor = 0
			} else {
				getChordActorRefFromAddress(fingerTable(1).node.ip, fingerTable(1).node.port) ! Heart_Beat_Successor(thisNode)
				heartBeatCounterSuccessor = heartBeatCounterSuccessor + 1
			}
		}
	case Heart_Beat_Successor(node:Node) =>
		getChordActorRefFromAddress(node.ip, node.port) ! Heart_Beat_ACK_Successor(fingerTable(1).node)
	case Heart_Beat_ACK_Successor(successorOfSuccessor:Node) =>
		successorSuccessor = successorOfSuccessor
		heartBeatCounterSuccessor = 0
	// Called from the upper layer (Scribe)
	case Send(msg:Message, node:Node) =>
		//Ask the Chord node to deliver the message
		getChordActorRefFromAddress(node.ip, node.port) ! Ask_To_Deliver(msg)
	case Ask_To_Deliver(msg:Message) =>
		//Propagate message to the upper layer (Scribe)
		println("[CHORD] Was asked to deliver a "+msg.msgType+" message")
		getScribeActorRefFromAddress(thisNode.ip, thisNode.port) ! Deliver(msg)
	case Route(msg:Message, topicId: Int) =>
		println("[CHORD] Received Route")
		if (belongsToInterval(topicId, thisNode.id, fingerTable(1).node.id)) {
			// I'm my successor's child
			//println("[CHORD] Asking to deliver")
			getChordActorRefFromAddress(fingerTable(1).node.ip,fingerTable(1).node.port) ! Ask_To_Deliver(msg)
		} else {
			var otherN = FF_Closest_Preceding_Node(topicId)
			//println("[CHORD] Returned "+otherN.id+" as the Closest_Preceding_Node")
			// TODO triple check this if
			if (otherN.id == thisNode.id){
				//println("[CHORD] Asking to deliver 2")
				getChordActorRefFromAddress(thisNode.ip,thisNode.port) ! Ask_To_Deliver(msg)
			} else {
				//println("[CHORD] Asking to forward")
				getChordActorRefFromAddress(otherN.ip, otherN.port) ! Ask_To_Forward(msg)
			}
		}
	case Ask_To_Forward(msg:Message) =>
		//Propagate message to the upper layer (Scribe)
		//println("[CHORD] Was asked to forward a "+msg.msgType+" message")
		getScribeActorRefFromAddress(thisNode.ip, thisNode.port) ! Forward(msg)
	case Print_Fingers_Table() =>
		node_info_dump()
	case whatevs =>
		println(whatevs)
	/*case _ =>
		println("[CHORD] Default case. Unknown message was received.")*/
  }
  
  def Closest_Preceding_Node(node:Node) : Node = {
	for (i <- m to 1 by -1) {
		if (belongsToIntervalBothExclusive(fingerTable(i).node.id, thisNode.id, node.id)) {
			return fingerTable(i).node
		}
	}
	return thisNode
  }
  
  // FIX FINGERS;
  def FF_Closest_Preceding_Node(start:Int) : Node = {
	for (i <- m to 1 by -1) {
		// print(i)
		if (belongsToIntervalBothExclusive(fingerTable(i).node.id, thisNode.id, start)) {
			// println(fingerTable(i).node.id+" e "+"]"+thisNode.id+","+start+"]")
			return fingerTable(i).node
		}
		// println(fingerTable(i).node.id+" -e "+"]"+thisNode.id+","+start+"]")
	}
	return thisNode
  }
  
  /////////AUX functions/////////
  
  def calculateStart(n:Int, k:Int) : Int = {
  	return (n + Math.pow(2, k-1).toInt) % Math.pow(2, m).toInt
  }

  def createID() : Int = {
	return new Random().nextInt(Math.pow(2, m).toInt) //nextInt(0,exclusive)
  }
  
  //Get Chord actor's reference so we can send them a message.
  def getChordActorRefFromAddress(contactIp:String, contactPort:String) : ActorSelection = {
	return context.actorSelection("akka.tcp://ASDProject@"+contactIp+":"+contactPort+"/user/chord") //akka.<protocol>://<actor system name>@<hostname>:<port>/<actor path>
  }
  
  //Get Scribe actor's reference so we can send them a message.
  def getScribeActorRefFromAddress(contactIp:String, contactPort:String) : ActorSelection = {
	return context.actorSelection("akka.tcp://ASDProject@"+contactIp+":"+contactPort+"/user/scribe") //akka.<protocol>://<actor system name>@<hostname>:<port>/<actor path>
  }

  // Check if id is inside the circular interval [start, end[
  def belongsToInterval(id:Int, intervalStart:Int, intervalEnd:Int) : Boolean = {
  	if (intervalStart == intervalEnd) {
  		return true
  	} else if (intervalStart < intervalEnd){
  		//println("[CHORD] ("+id+" > "+intervalStart+") && ("+id+" <= "+intervalEnd+")")
  		return (id > intervalStart) && (id <= intervalEnd)
  	} else {
  		//println("[CHORD] ("+id+" > "+intervalStart+") || ("+id+" <= "+intervalEnd+")")
  		return (id > intervalStart) || (id <= intervalEnd)
  	}
  }

  // Check if id is inside the circular interval ]start, end[
  def belongsToIntervalBothExclusive(id:Int, intervalStart:Int, intervalEnd:Int) : Boolean = {
  	if (intervalStart == intervalEnd) {
  		return true
  	} else if (intervalStart <= intervalEnd){
  		//println("[CHORD] ("+intervalStart+" < "+id+" < "+intervalEnd+")")
  		return (id > intervalStart) && (id < intervalEnd)
  	} else {
  		//println("[CHORD] ("+intervalStart+" > "+id+" > "+intervalEnd+")")
  		return (id > intervalStart) || (id < intervalEnd)
  	}
  }

  def node_info_dump() = {
  		println("[CHORD] {this:"+thisNode.id+", pred:"+predecessor)
		for (i <- 1 until m+1) {
			println(", finger("+i+"): "+fingerTable(i).node.id+" & start: "+fingerTable(i).intervalStart)
		}
		println("}\n")
  }
}
