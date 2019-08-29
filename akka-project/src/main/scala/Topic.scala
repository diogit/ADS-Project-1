class Topic (titleIn:String) {
	var title:String = titleIn
	var parent:Node = null
	var children:Set[Node] = Set()
	var subscribed:Boolean = false
	var renewalCounter:Int = 0
}