/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */

		return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	MessageHdr *msg = (MessageHdr*) data;

	if(msg->msgType == JOINREQ){
		handleJoinReq(msg);
	}else if(msg->msgType == JOINREP){
		handleJoinReply(msg);
	}else if(msg->msgType == GOSSIP){
		handleGossip((GossipMessage*) msg);
	}

	return true;
}

// Called by nodeLoopOps function.
// Radomly selected nodes will receive GOSSIP message and this function handles it
// This function detects if any node is failed and removes it from its membershiplist
// If any new entry is found, then it adds to its membershiplist
void MP1Node::handleGossip(GossipMessage *gossipMsg){
	if(gossipMsg->numEntries == 0)
			return;

  MemberListEntry *gossipMsgEntries = (gossipMsg->entries);

	for(int i=0; i< gossipMsg->numEntries ; i++, gossipMsgEntries++){
		bool found = false;
    for(int j=0 ; j < memberNode->memberList.size(); j++){
			if( memberNode->memberList[j].id ==  gossipMsgEntries->id &&
								memberNode->memberList[j].port == gossipMsgEntries->port){
									  found = true;
										if( (*gossipMsgEntries).getheartbeat() > memberNode->memberList[j].getheartbeat()){
											memberNode->memberList[j].setheartbeat((*gossipMsgEntries).getheartbeat());
											memberNode->memberList[j].settimestamp(par->getcurrtime());
										}
										break;
								}
			}
			if(!found){
				#ifdef DEBUGLOG
						MemberListEntry mNode = (*gossipMsgEntries);
						if(!isMemberActive(mNode))
							continue;
						string address = to_string(mNode.getid())+":"+to_string(mNode.getport());
						Address toAddr = Address(address);
            log->logNodeAdd(&memberNode->addr, &toAddr);
    		#endif

				memberNode->memberList.push_back((*gossipMsgEntries));
				memberNode->memberList.back().settimestamp(par->getcurrtime());
				memberNode->nnb++;
			}
	}
}

// When introducer node sends the JOINREP message,
// the new node will receive the JOINREP message and calls this function.
// This function updates its membership table entries
void MP1Node::handleJoinReply(MessageHdr *msg){

	// new node will add its own entry to its membershiplist table
  memberNode->inGroup = true;
	MemberListEntry entry = MemberListEntry(memberNode->addr.addr[0],
															memberNode->addr.addr[4],
															memberNode->heartbeat,
															par->getcurrtime());

	memberNode->memberList.push_back(entry);
	memberNode->nnb++;
	memberNode->myPos = memberNode->memberList.begin();
	#ifdef DEBUGLOG
			log->logNodeAdd(&memberNode->addr, &memberNode->addr);
	#endif

	// joinMsgEntries the JOINREP message and unpacks it
  GossipMessage *joinMsg = (GossipMessage *)msg;
  int totalNewEntries = joinMsg->numEntries;
  MemberListEntry *joinMsgEntries = (joinMsg->entries);

	// Iterates to each entries in the gossip message.
	// If its already existing, then checks the heartbeat and updates it, if required
	// if the entry is new, then adds to its membership table
	for(int i=0; i< totalNewEntries ; i++, joinMsgEntries++){
		bool found = false;
    for(int j=0 ; j < memberNode->memberList.size(); j++){
			if( memberNode->memberList[j].id ==  (joinMsgEntries)->id &&
								memberNode->memberList[j].port == (joinMsgEntries)->port){
									  found = true;
										// updates the heartbeat
										if( (joinMsgEntries)->getheartbeat() > memberNode->memberList[j].getheartbeat()){
											memberNode->memberList[j].setheartbeat((*joinMsgEntries).getheartbeat());
											memberNode->memberList[j].settimestamp(par->getcurrtime());
										}
										break;
								}
		}

		// Adds the entry to its membership table and updates its timestamp
		if(!found){
				#ifdef DEBUGLOG
							MemberListEntry mNode = (*joinMsgEntries);
							if(!isMemberActive(mNode))
								continue;
							string address = to_string(mNode.getid())+":"+to_string(mNode.getport());
							Address toAddr = Address(address);
	            log->logNodeAdd(&memberNode->addr, &toAddr);
	    	#endif
			memberNode->memberList.push_back((*joinMsgEntries));
			memberNode->memberList.back().settimestamp(par->getcurrtime());
			memberNode->nnb++;
		}
	}
}

//1.0.0.0 node introduces other nodes.
void MP1Node::handleJoinReq(MessageHdr *msg){

	Address *addr = (Address *) malloc(sizeof(Address));

	// gets the address assigned in introduceSelfToGroup function while creating JOINREQ message.
	// The address will be the node introduced by 1.0.0.0 node
	memcpy(&addr->addr, ((char *)msg) + sizeof(MessageHdr) , sizeof(addr->addr));

	// just for testing
	if(addr->addr[0] == 0)
	   return;

	// if introducer(1.0.0.0) node does not have any member list. Then add the same node to the list.
	if(memberNode->memberList.size()==0){
		MemberListEntry entry = MemberListEntry(	memberNode->addr.addr[0],
																	memberNode->addr.addr[4],
																memberNode->heartbeat,
																par->getcurrtime());
		memberNode->memberList.push_back(entry);
		memberNode->myPos = memberNode->memberList.begin();
		memberNode->nnb++;
		#ifdef DEBUGLOG
				log->logNodeAdd(&memberNode->addr, &memberNode->addr);
		#endif
	}

  // Create a membership list entry to add the new node
	MemberListEntry entry = MemberListEntry(addr->addr[0],
															addr->addr[4],
															0,
															par->getcurrtime());
	memberNode->memberList.push_back(entry);
	memberNode->myPos = memberNode->memberList.begin();
	memberNode->nnb++;
	#ifdef DEBUGLOG
			log->logNodeAdd(&memberNode->addr, addr);
	#endif


  int totalMemberNode = memberNode->memberList.size();

	// Create gossip message and send the member ship list as JOINREP message
	GossipMessage *replyMessage;
	replyMessage = (GossipMessage *)malloc(sizeof(GossipMessage));
	replyMessage->msgType = JOINREP;
	replyMessage->entries = (MemberListEntry *) malloc(sizeof(MemberListEntry) * totalMemberNode);
  replyMessage->numEntries = 0;

  //Copy current membershiplist entries to reply message
  for(int i=0 ; i< totalMemberNode; i++){
		if(  isMemberActive(memberNode->memberList[i])){
			 memcpy( &(replyMessage->entries[i]) , &(memberNode->memberList[i]) , sizeof(MemberListEntry) );
			 replyMessage->entries[i].timestamp = par->getcurrtime();
			 replyMessage->numEntries++;
		}
	}

	//send as JOINREP message to new node
	emulNet->ENsend(&memberNode->addr, addr, (char *)replyMessage, sizeof(*replyMessage));
  free(replyMessage);
}


// checks if the membership list is active
bool MP1Node::isMemberActive(MemberListEntry &m){
	if( par->globaltime - m.timestamp <= TFAIL)
	   return true;

	return false;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

		// No neighbors
		if(memberNode->nnb == 0) return;

		//
		for(int i= memberNode->nnb-1; i>= 0 ; i--){
			// if a node is failed, remove it from membership list
			if( par->globaltime - memberNode->memberList[i].timestamp > (TFAIL + TREMOVE)){
				  MemberListEntry m = memberNode->memberList[i];
					string address = to_string(m.getid())+":"+to_string(m.getport());
					Address addr = Address(address);
					memberNode->memberList.erase(memberNode->memberList.begin() + i);
					memberNode->nnb--;
					#ifdef DEBUGLOG
							log->logNodeRemove(&memberNode->addr, &addr);
					#endif

			}
		}


		// update current node heartbeat
		memberNode->heartbeat++;

		// Update current node heartbeat and timestamp in its membershiplist
		for(int i=0;i<memberNode->nnb;i++){
			if(memberNode->memberList[i].id == memberNode->addr.addr[0]){
				memberNode->memberList[i].setheartbeat(memberNode->heartbeat);
				memberNode->memberList[i].settimestamp(par->getcurrtime());
			}
		}


		// create a gossip message
		GossipMessage *gossipMsg;
		int totalMemberNode = memberNode->memberList.size();
		gossipMsg = (GossipMessage *)malloc(sizeof(GossipMessage));
		gossipMsg->msgType = GOSSIP;
		gossipMsg->entries = (MemberListEntry *) malloc(sizeof(MemberListEntry) * totalMemberNode);
	  gossipMsg->numEntries = 0;

		// Copy current node membershiplist entries to GOSSIP message
	  for(int i=0 ; i< totalMemberNode; i++){
			if(isMemberActive(memberNode->memberList[i])){
				 memcpy( &(gossipMsg->entries[i]) , &(memberNode->memberList[i]) , sizeof(MemberListEntry) );
				 gossipMsg->entries[i].timestamp = par->getcurrtime();
				 gossipMsg->numEntries++;
			}
		}

		// Send the GOSSIP message to 3 random nodes
		for (int i = 0; i < 3 && memberNode->nnb>0; i++) {
        int n = (rand() % memberNode->nnb) + 1;
        MemberListEntry randomNode = memberNode->memberList[n];

				string address = to_string(randomNode.getid())+":"+to_string(randomNode.getport());
				Address toAddr = Address(address);
        emulNet->ENsend(&memberNode->addr, &toAddr, (char *)gossipMsg, sizeof(*gossipMsg));
    }

		//free(gossipMsg->entries);
    free(gossipMsg);

		return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;
}
