import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.Actor
import akka.actor.Props
import com.typesafe.config.ConfigFactory
import akka.remote._
import collection.JavaConversions._
import scala.concurrent.duration._


object Main {
	def main(args: Array[String]): Unit = {
		if (args.length != 4 && args.length != 6){
			println("[MAIN] Usage: Main <testerIp> <testerPort> <myIp> <myPort> [contactIp] [contactPort]")
		}

		val config = ConfigFactory.parseMap(Map("myIp" -> args(2), "myPort" -> args(3)))
					.withFallback(ConfigFactory.parseResources("aplication.conf"))
					.withFallback(ConfigFactory.load()).resolve()
		val actorSystem = ActorSystem("ASDProject", config)

		val myHostname 	= actorSystem.settings.config.getString("akka.remote.netty.tcp.hostname")
		val myPort 		= actorSystem.settings.config.getString("akka.remote.netty.tcp.port")
		val mainTester = (args.length == 4 && args(0) == args(2) && args(1) == args(3))
		if (mainTester == true){
			println("[MAIN] This is the main tester")
		}
		val testerIp:String = args(0)
		val testerPort:String = args(1)

		// Create the actors for the protocols
		val chord = actorSystem.actorOf(Props(new Chord(myHostname, myPort)), name="chord")
		val scribe = actorSystem.actorOf(Props(new Scribe(myHostname, myPort)), name="scribe")
		val test = actorSystem.actorOf(Props(new Test(myHostname, myPort, testerIp, testerPort)), name="test")

		// Start Chord
		if (args.length == 4){
			chord ! Create()
		} else if (args.length == 6){
			val contactIp = args(4)
			val contactPort = args(5)
			chord ! Join(new Node(-1, contactIp, contactPort))
		}

		// Chord Timers
		import actorSystem.dispatcher
		val fixFingersTimer =
			actorSystem.scheduler.schedule(
				2000 milliseconds, //after 2 seconds
				3000 milliseconds, //every 3 seconds
				chord,
				Fix_Fingers()
			)
		val stabilizeTimer =
			actorSystem.scheduler.schedule(
				1000 milliseconds, //after 1 second
				3000 milliseconds, //every 3 seconds
				chord,
				Stabilize()
			)
		val heartBeatTimerPredecessor =
			actorSystem.scheduler.schedule(
				3000 milliseconds, //after 3 seconds
				5000 milliseconds, //every 5 seconds
				chord,
				Trigger_Heart_Beat_Predecessor()
			)
		val heartBeatTimerSuccessor =
			actorSystem.scheduler.schedule(
				3000 milliseconds, //after 3 seconds
				5000 milliseconds, //every 5 seconds
				chord,
				Trigger_Heart_Beat_Successor()
			)

		// Scribe Timers
		val subscriptionsRenewal =
			actorSystem.scheduler.schedule(
				3000 milliseconds, //after 3 seconds
				5000 milliseconds, //every 5 seconds
				scribe,
				Trigger_Subscriptions_Renewal()
			)

		// Command interpreter
		var command:String = ""
		do {
			var cmdArgs:Array[String] = readLine().split(" ")
			command = cmdArgs(0)
			command match {
				case "create" =>
					if (cmdArgs.size < 2) {
						println("[CONSOLE] Usage: create <TOPIC>")
					} else {
						scribe ! CreateTopic(cmdArgs(1))
					}
				case "subscribe" =>
					if (cmdArgs.size < 2) {
						println("[CONSOLE] Usage: subscribe <TOPIC>")
					} else {
						scribe ! Subscribe(cmdArgs(1))
					}
				case "publish" =>
					if (cmdArgs.size < 3) {
						println("[CONSOLE] Usage: publish <TOPIC> <MESSAGE>")
					} else {
						scribe ! Publish(cmdArgs(1), cmdArgs(2))
					}
				case "unsubscribe" =>
					if (cmdArgs.size < 2) {
						println("[CONSOLE] Usage: unsubscribe <TOPIC>")
					} else {
						scribe ! Unsubscribe(cmdArgs(1))
					}
				case "printfinger" | "fingertable" | "finger" =>
					chord ! Print_Fingers_Table()
				case "printsubs" | "subs" =>
					scribe ! Print_Subscriptions()
				case "printtopics" | "topics" | "topic" =>
					scribe ! Print_Topics()
				case "test" =>
					if (cmdArgs.size < 2) {
						println("[CONSOLE] Usage: test <create/subscribe/publish/print>")
					} else {
						println("[CONSOLE] [WARNING] This command should only be called if you are on the main tester and if the network was created using test.sh. Any other case should not be expected to work")
						if (mainTester == false) {
							println("[CONSOLE] [WARNING] Command should only be executed from the main tester, which should be the first node that created the network")
						}
						cmdArgs(1) match {
							case "create" =>
								test ! Test_Create()
							case "subscribe" =>
								test ! Test_Subscribe()
							case "publish" =>
								test ! Test_Publish()
							case "print" =>
								test ! Test_Print()
							case _ =>
								println("[CONSOLE] Usage: test <create/subscribe/publish/print>")
						}
					}
				case "name" =>
					if (cmdArgs.size < 2) {
						println("[CONSOLE] Usage: name <name>")
					} else {
						println("[CONSOLE] Hey "+cmdArgs(1)+"!")
					}
				case "help" =>
					println("> create")
					println("> subscribe")
					println("> publish")
					println("> unsubscribe")
					println("> printfinger / fingertable / finger")
					println("> printsubs / subs")
					println("> printtopics / topics / topic")
					println("> test")
					println("> name")
					println("> help")
					println("> exit / e / quit / q")
				case "exit" | "e" =>
					println("[CONSOLE] Exiting...")
				case "quit" | "q" =>
					println("[CONSOLE] Quitting...")
				case _ =>
					println("[CONSOLE] Unknown command. Type 'help' for a list of commands")
			}
		} while (command != "exit" && command != "e" && command != "quit" && command != "q")

		try {
			actorSystem.terminate()
		} catch {
			case e:Throwable => println(e)
		}
	}
}
