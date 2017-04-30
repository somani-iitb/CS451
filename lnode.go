package main

import (
	"fmt"
	"time"
	"math/rand"
	"math/big"
	"bytes"
	"log"
	"net/rpc"
	"strconv"
	"strings"
	"errors"
	//"os"
)

//------------------------------------LocalNode_Initialization_Functions---------------------------------------

func convert( b []byte ) string {
	s := make([]string,len(b))
	for i := range b {
		 s[i] = strconv.Itoa(int(b[i])) 
	} 
	return strings.Join(s,",") 
}


func (vn *LocalNode) genId() {
	// Use the hash funciton
	conf := vn.Ring.config
	hash := conf.HashFunc()
	hash.Write([]byte(conf.Hostname))
	vn.Id = hash.Sum(nil)
}

// Initializes a local vnode
func (vn *LocalNode) init() {
	vn.genId()
	vn.Host = vn.Ring.config.Hostname
	fmt.Println("HostName : ", vn.Host)
	fmt.Println("Hash ID : ", vn.Id)
	
	// Initialize all state
	
	vn.Successors = make([]*SNode, vn.Ring.config.NumSuccessors)
	//make yourself as the successor
	var sc SNode
	sc.Id=vn.Id
	sc.Host=vn.Host
	vn.Successors[0]=&sc
	vn.Predecessor=new(SNode)
	vn.Finger = make([]*SNode, vn.Ring.config.hashBits)	
	vn.Store = make(map[string]int)
	rpc.Register(vn)
	vn.schedule()
	
	//Start RPC Server for communicating with the other nodes in the ring
	go startRPCServer(vn.Host)

}

// Schedules the Vnode to do regular maintenence
func (vn *LocalNode) schedule() {
	// Setup our stabilize timer
	vn.timer = time.AfterFunc(randStabilize(vn.Ring.config), vn.stabilize)
}



//------------------------------------------RPC Calls for LocalNode-------------------------------------------


// RPC : Returns the nearest local vnode to the key
func (vn *LocalNode) FindCoordinator(key []byte, reply *SNode) error {

	if betweenRightIncl(vn.Predecessor.Id, vn.Id, key){	
		reply.Id=vn.Id
		reply.Host=vn.Host	

	}else{
		if vn.Successors[0].Host==vn.Host{
			reply.Id=vn.Id
			reply.Host=vn.Host
			return nil
		}else if bytes.Compare(vn.Successors[0].Id,vn.Id)==-1 && bytes.Compare(vn.Id,key)==-1{
			reply.Id=vn.Successors[0].Id
			reply.Host=vn.Successors[0].Host
			return nil
		}
		args:=key
		err:=RPC_caller1(vn.Successors[0].Host,"FindCoordinator",args,reply)
		checkError(err)
	}

	return nil
}

// RPC : Returns the nearest local vnode to the key
func (vn *LocalNode) FindCoordinatorWithFinger(key []byte, reply *([]*SNode)) error {
	
	*reply=make([]*SNode,vn.Ring.config.NumSuccessors+1)
	
	
	if betweenRightIncl(vn.Predecessor.Id, vn.Id, key){	
		
		var res []*SNode
		err:=RPC_caller(vn.Host,"FindSuccessors","",&(res))
		checkError(err)
		
		(*reply)[0]=new(SNode)
		(*reply)[0].Host=vn.Host
		(*reply)[0].Id=vn.Id
		for i,sc:=range res{
			if sc!=nil{
				(*reply)[i+1]=new(SNode)
				(*reply)[i+1].Id=sc.Id
				(*reply)[i+1].Host=sc.Host
			}else{
				(*reply)[i+1]=sc
			}
		}
		
		return nil

	}else if vn.Successors[0].Host==vn.Host{
			
			var res []*SNode
			err:=RPC_caller(vn.Host,"FindSuccessors","",&(res))
			checkError(err)

			(*reply)[0]=new(SNode)
			(*reply)[0].Host=vn.Host
			(*reply)[0].Id=vn.Id
			for i,sc:=range res{
				if sc!=nil{
					(*reply)[i+1]=new(SNode)
					(*reply)[i+1].Id=sc.Id
					(*reply)[i+1].Host=sc.Host
				}else{
					(*reply)[i+1]=sc
				}
			}
			
			return nil

	}else if bytes.Compare(vn.Successors[0].Id,vn.Id)==-1 && bytes.Compare(vn.Id,key)==-1{
			
			var res []*SNode
			err:=RPC_caller(vn.Successors[0].Host,"FindSuccessors","",&(res))
			checkError(err)

			(*reply)[0]=new(SNode)
			(*reply)[0].Host=vn.Host
			(*reply)[0].Id=vn.Id
			for i,sc:=range res{
				if sc!=nil{
					(*reply)[i+1]=new(SNode)
					(*reply)[i+1].Id=sc.Id
					(*reply)[i+1].Host=sc.Host
				}else{
					(*reply)[i+1]=sc
				}
			}
			
			return nil
	}

	// Try the closest preceeding nodes
	cp := closestPreceedingVnodeIterator{}
	cp.init(vn, key)
	for {
		// Get the next closest node
		closest := cp.Next()
		if closest == nil {
			break
		}

		var res []*SNode
		err:=RPC_caller(closest.Host,"FindCoordinatorWithFinger","",&(res))
		checkError(err)

		if err == nil {
			for i,sc:=range res{
				if(sc!=nil){
					(*reply)[i]=new(SNode)
					(*reply)[i].Id=sc.Id
					(*reply)[i].Host=sc.Host
				}else{
					(*reply)[i]=sc	
				}
			}
			return nil
		} else {
			log.Printf("[ERR] Failed to contact %s. Got %s", convert(closest.Id), err)
		}
	}

	// Determine how many successors we know of
	successors := vn.knownSuccessors()
	n:=vn.Ring.config.NumSuccessors

	// Check if the ID is between us and any non-immediate successors
	for i := 1; i <= successors-n; i++ {
		if betweenRightIncl(vn.Id, vn.Successors[i].Id, key) {
			remain := vn.Successors[i:]
			if len(remain) > n {
				remain = remain[:n]
			}
			reply=&remain
			return nil
		}
	}
	
	return nil
}

// RPC function : to return our successors list
func (vn *LocalNode) FindSuccessors(key string, succ_List *([]*SNode)) error{

	for _,sc := range vn.Successors{
		*succ_List = append(*succ_List,sc)
	}
	return nil
} 

// *RPC: Invoked to return out predecessor
func (vn *LocalNode) GetPredecessor(args interface{},reply *SNode) error{
	if vn.Predecessor!=nil {
		reply.Id = vn.Predecessor.Id
		reply.Host = vn.Predecessor.Host
	}	
	return nil
}


// RPC: Invoked to check if a node is live
func (vn *LocalNode) Ping(args interface{},reply *string) error{
	*reply= "active"
	return nil
}


// RPC: Invoked to check if a node is live
func (vn *LocalNode) Ping1(args interface{},reply *LocalNode) error{
	fmt.Println("----------------------Inside PING 1  ---------------------------------------")
	reply.Id = vn.Id
	reply.Host = vn.Host
	// Return our successors list
	for _,sc := range vn.Successors{
		reply.Successors = append(reply.Successors,sc)
	}

	return nil
}


// RPC: Notify is invoked when a Vnode gets notified
func (vn *LocalNode) Notify(maybe_pred *SNode, succ_List *([]*SNode)) error {

	fmt.Println("INSIDE NOTIFY...........")

	// Check if we should update our predecessor
	if vn.Predecessor!=nil{
		
		if vn.Predecessor.Host == "" || between(vn.Predecessor.Id, vn.Id, maybe_pred.Id) {
			vn.Predecessor.Id = maybe_pred.Id
			vn.Predecessor.Host = maybe_pred.Host
		}

		for _,sc := range vn.Successors{
			*succ_List = append(*succ_List,sc)
		}
	}else{
		vn.Predecessor=new(SNode)
		vn.Predecessor.Id = maybe_pred.Id
		vn.Predecessor.Host = maybe_pred.Host
		
		for _,sc := range vn.Successors{
			*succ_List = append(*succ_List,sc)
		}
	}
	fmt.Println("Exiting NOTIFY function")
	return nil
}



// RPC : Used to clear our predecessor when a node is leaving
func (vn *LocalNode) ClearPredecessor(p *SNode,reply *string) error {
	
	if vn.Predecessor != nil && convert((*(vn.Predecessor)).Id) == convert((*p).Id) {
		fmt.Println("Inside Clear Predecessor  at ", vn.Host)
		fmt.Println("-------------------------------------------------------------------------")
		vn.Predecessor = new(SNode)
		fmt.Println("Value of My predecessor before stabilize : ",vn.Predecessor)
		vn.stabilize()
		fmt.Println("Value of My predecessor after stabilize : ",vn.Predecessor)
		fmt.Println("--------------------------------------------------------------------------")
	}
	return nil
}

// RPC : Used to skip a successor when a node is leaving
func (vn *LocalNode) SkipSuccessor(s *SNode,reply *string) error {
	// Skip if we have a match
	if convert(vn.Successors[0].Id) == convert((*s).Id) {
		fmt.Println("Inside Skip Successor at ", vn.Host)
		fmt.Println("-------------------------------------------------------------------------")
		var reply []*SNode
		err:=RPC_caller(s.Host,"FindSuccessors","",&(reply))
		checkError(err)

		vn.Successors[0].Id=(*(reply[0])).Id
		vn.Successors[0].Host=(*(reply[0])).Host
		fmt.Println("Value of My Successor before stabilize : ",vn.Successors[0].Host)
	}

	return nil
}


// RPC : to get the value of a key from the Store
func (vn *LocalNode) Get(arg string, reply *int) error{
	val, ok := map[string]int(vn.Store)[arg] 
	if ok{
		*reply = val
		fmt.Println("Value here in get : ",*reply)
	}else{
		*reply = -1
		return errors.New("Key Not Present")
	}
	return nil
}


// RPC : to delete a key value pair from the Store
func (vn *LocalNode) Delete(arg string, reply *string) error{

	if _, ok := map[string]int(vn.Store)[arg]; ok {
    	delete(map[string]int(vn.Store),arg)
	}
	return nil
}

// RPC : to put a key value pair in the Store
func (vn *LocalNode) Put(arg Data, reply *string) error{

	if val, ok := map[string]int(vn.Store)[arg.Key]; ok {
    	fmt.Println("Case : Already Exists")
    	map[string]int(vn.Store)[arg.Key] = arg.Value
    	val = val 

	}else{
		vn.Store[arg.Key] = arg.Value
	}
	return nil
}

// RPC : to get the value of a key from the Store
func (vn *LocalNode) GetAll(arg string, reply *map[string]int) error{
	
	*reply=make(map[string]int)

	if len(vn.Store) != 0 {
		h := vn.Ring.config.HashFunc()
		h.Write([]byte(arg))
		new_node := h.Sum(nil)
		
		for key, value := range vn.Store {
			// Hash the key
			h := vn.Ring.config.HashFunc()
			h.Write([]byte(key))
			key_hash := h.Sum(nil)
			if betweenRightIncl(vn.Predecessor.Id,new_node,key_hash){
				//fmt.Println("eligible keys : ",key)
				(*reply)[key]=value
				//delete(vn.Store,key)
			}
		}
	}

	return nil
}

// RPC : to put a key value pair in the Store
func (vn *LocalNode) PutAll(arg *map[string]int, reply *string) error{
	for key, value := range *arg {
		vn.Store[key]=value
		*reply="successful"
	}
	return nil
}


//------------------------------Local_Procedure_Calls_For_Stabilization_Protocols-----------------------------

// Called to periodically stabilize the vnode
func (vn *LocalNode) stabilize() {
	
	// Clear the timer
	vn.timer = nil
	
	// Setup the next stabilize timer
	defer vn.schedule()
	// Check for new successor
	if err := vn.checkNewSuccessor(); err != nil {
		log.Printf("[ERR] Error checking for new successor: %s", err)
	}

	// Notify the successor
	if err := vn.notifySuccessor(); err != nil {
		log.Printf("[ERR] Error notifying successor: %s", err)
	}

	// Check the predecessor
	if err := vn.checkPredecessor(); err != nil {
		log.Printf("[ERR] Error checking predecessor: %s", err)
	}
	
	// Finger table fix up
	/*if err := vn.fixFingerTable(); err != nil {
		log.Printf("[ERR] Error fixing finger table: %s", err)
	}*/
	

	// Set the last stabilized time
	vn.stabilized = time.Now()
	
	fmt.Println("My Current LocalNode Value : ")
	fmt.Println("successor : ",vn.Successors[0].Host)
	if vn.Predecessor != nil{
		fmt.Println("predecessor : ",vn.Predecessor.Host)
	}else{
		fmt.Println("predecessor : ",vn.Predecessor)
	}
	fmt.Println("My Store : ",vn.Store)
	
	/*
	//fmt.Println("My last finger ",vn.last_finger," and it's value : ",vn.Finger[vn.last_finger])
	fmt.Println("My Current Finger Table : ")
	for i:=0;i<len(vn.Finger);i++{
		if vn.Finger[i]!=nil{
			fmt.Println(i," : ",vn.Finger[i].Host)
			}else{
				fmt.Println(i," : ",vn.Finger[i])
			}
		
	}
	*/
	
}


// Checks for a new successor
func (vn *LocalNode) checkNewSuccessor() error {

CHECK_NEW_SUC:
	
	succ := vn.Successors[0]
	if succ == nil {
		panic("Node has no successor!")
	}
	maybe_suc:= SNode{}
	err:=RPC_caller(succ.Host,"GetPredecessor",nil,&maybe_suc)
	
	if err != nil {
		// Check if we have succ list, try to contact next live succ
		
		known := vn.knownSuccessors()
	
		if known > 1 {
			for i := 0; i < known; i++ {
				reply:=""
				err:=RPC_caller(vn.Successors[0].Host,"Ping",nil,&reply)
				checkError(err)

				//"Reply for ping call"
				if reply!="active"{
					
				// Don't eliminate the last successor we know of
					if i+1 == known {
						return fmt.Errorf("All known successors dead!")
					}
					// Advance the successors list past the dead one
					copy(vn.Successors[0:], vn.Successors[1:])
					vn.Successors[known-1-i] = nil
					
				} else {
					// Found live successor, check for new one
					goto CHECK_NEW_SUC
				}
			}
		}
		return err
	}

	// Check if we should replace our successor
	if maybe_suc.Host!="" && between(vn.Id, succ.Id, maybe_suc.Id) {
		// Check if new successor is alive before switching
		rep:=""
		err:=RPC_caller(maybe_suc.Host,"Ping",nil,&rep)
		
		if rep=="active" && err == nil {
			if vn.Successors[0].Host!=vn.Host{
				copy(vn.Successors[1:], vn.Successors[0:len(vn.Successors)-1])
			}
			vn.Successors[0].Id = maybe_suc.Id
			vn.Successors[0].Host = maybe_suc.Host
		}else{
			return err
		} 

	}
	
	return nil
}

// Notifies our successor of us, updates successor list
func (vn *LocalNode) notifySuccessor() error {
	// Notify successor
	succ := vn.Successors[0]
	max_succ := vn.Ring.config.NumSuccessors
	succ_list := make([]*SNode, max_succ)

	
	err:=RPC_caller(succ.Host,"Notify",&vn,&succ_list) 
	checkError(err)
	

	if len(succ_list) > max_succ-1 {
		succ_list = succ_list[:max_succ-1]
	}
	
	
	if succ_list[0]!=nil && succ_list[0].Host!=vn.Successors[0].Host{
		
		// Update local successors list
		for idx, s := range succ_list {
			if s == nil{
				break
			}
			
			// Ensure we don't set ourselves as a successor!
			if s == nil || convert(s.Id) == convert(vn.Id) {
				break
			}
			
			vn.Successors[idx+1] = s
		}
	}
	
	fmt.Println("")
	fmt.Println("My Updated Successors list : ")
	for i, sc := range vn.Successors {
		if sc != nil{
			fmt.Println("Successor [",i,"] : ",sc.Host)
		}else{
			fmt.Println("Successor [",i,"] : ",sc)
		}
		
	}
	fmt.Println("")

	return nil
}



// Fixes up the finger table
func (vn *LocalNode) fixFingerTable() error {

	// Determine the offset
	hb := vn.Ring.config.hashBits
	offset := powerOffset(vn.Id, vn.last_finger, hb)

	// Find the successor
	var succ_node SNode
	err:=RPC_caller1(vn.Host,"FindCoordinator",offset,&succ_node)
	checkError(err)

	if &succ_node == nil || err != nil {
		return err
	}
	
	// Update the finger table
	vn.Finger[vn.last_finger] = &succ_node
	// Try to skip as many finger entries as possible
	for {
		next := vn.last_finger + 1
		if next >= hb {
			break
		}
		offset := powerOffset(vn.Id, next, hb)
		// While the node is the successor, update the finger entries
		if betweenRightIncl(vn.Id, succ_node.Id, offset) {
			vn.Finger[next] = &succ_node
			vn.last_finger = next
		} else {
			break
		}
	}
	// Increment to the index to repair
	if vn.last_finger+1 == hb {
		vn.last_finger = 0
	} else {
		vn.last_finger++
	}
	
	return nil
}



// Checks the health of our predecessor
func (vn *LocalNode) checkPredecessor() error {

	if vn.Predecessor!=nil && vn.Predecessor.Host!=""{
		res:=""
		err:=RPC_caller(vn.Predecessor.Host,"Ping",nil,&res)
		checkError(err)
		if res!="active" {
			vn.Predecessor = nil
		}
	}
	return nil
}




// Determine how many successors we know of
func (vn *LocalNode) knownSuccessors() (successors int) {
	for i := 0; i < len(vn.Successors); i++ {
		if vn.Successors[i] != nil {
			successors = i + 1
		}
	}
	return
}


// Instructs the vnode to leave
func (vn *LocalNode) leave() error {
	var err error
	var reply string

	// Notify predecessor to advance to their next successor	
	err=RPC_caller(vn.Successors[0].Host,"PutAll",&(vn.Store), &reply)
	checkError(err)

	var succ SNode
	succ.Id=vn.Id
	succ.Host=vn.Host
	fmt.Println("Reply for put alll ", reply)
	reply="successful"
	if(reply=="successful"){
		var pred LocalNode

		err:=RPC_caller(vn.Predecessor.Host,"Ping1",nil,&pred)
		checkError(err)
		fmt.Println("Predecessor's successor ",pred.Successors[0].Host)

		if vn.Predecessor != nil {
			err1:=RPC_caller(vn.Predecessor.Host,"SkipSuccessor", &succ, nil)
			checkError(err1)
		}
		fmt.Println("")
		
		
		err=RPC_caller(vn.Predecessor.Host,"Ping1",nil,&pred)
		checkError(err)
		fmt.Println("Now Predecessor's successor changed to ",pred.Successors[0].Host)


		// Notify successor to clear old predecessor
		err2:=RPC_caller(vn.Successors[0].Host,"ClearPredecessor", &succ, nil)
		checkError(err2)
		fmt.Println("")

		if vn.timer.Stop(){

		}
		time.Sleep(7 * time.Second)
	}

	return err
}





//---------------------------Utility__Functions--------------------------------------


// Generates a random stabilization time
func randStabilize(conf *Config) time.Duration {
	min := conf.StabilizeMin
	max := conf.StabilizeMax
	r := rand.Float64()
	return time.Duration((r * float64(max-min)) + float64(min))
}

// Checks if a key is STRICTLY between two ID's exclusively
func between(id1, id2, key []byte) bool {

	// Check for ring wrap around
	if bytes.Compare(id1, id2) >= 0 {
		return bytes.Compare(id1, key) == -1 || bytes.Compare(id2, key) == 1
	}
	// Handle the normal case

	return bytes.Compare(id1, key) == -1 && bytes.Compare(id2, key) == 1
}

// Merges errors together
func mergeErrors(err1, err2 error) error {
	if err1 == nil {
		return err2
	} else if err2 == nil {
		return err1
	} else {
		return fmt.Errorf("%s\n%s", err1, err2)
	}
}


// Checks if a key is between two ID's, right inclusive
func betweenRightIncl(id1, id2, key []byte) bool {
	// Check for ring wrap around
	if bytes.Compare(id1, id2) == 1 {
		return bytes.Compare(id1, key) == -1 ||
			bytes.Compare(id2, key) >= 0
	}
	return bytes.Compare(id1, key) == -1 &&
		   bytes.Compare(id2, key) >= 0
}


// Computes the offset by (n + 2^exp) % (2^mod)
func powerOffset(id []byte, exp int, mod int) []byte {
	// Copy the existing slice
	off := make([]byte, len(id))
	copy(off, id)
	// Convert the ID to a bigint
	idInt := big.Int{}
	idInt.SetBytes(id)
	// Get the offset
	two := big.NewInt(2)
	offset := big.Int{}
	offset.Exp(two, big.NewInt(int64(exp)), nil)
	// Sum
	sum := big.Int{}
	sum.Add(&idInt, &offset)
	// Get the ceiling
	ceil := big.Int{}
	ceil.Exp(two, big.NewInt(int64(mod)), nil)
	// Apply the mod
	idInt.Mod(&sum, &ceil)
	// Add together
	return idInt.Bytes()
}

