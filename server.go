package main

import (
	"log"
	"fmt"
	"net"
	"os"
	"bufio"
	"strconv"
	"strings"
	"math/rand"
	"io/ioutil"
	"time"
	"net/rpc/jsonrpc"
)

var signals chan bool

func startRPCServer(tcpadr string){
	log.Print("Starting Server...")
	tcpAddr, err := net.ResolveTCPAddr("tcp", tcpadr)
	checkError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	log.Print("listening for synchronous connection on: ", listener.Addr())
	
	for {
			//log.Print("waiting for connections ...")
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("accept error: %s", conn)
				os.Exit(0)
				continue
			}
			conn.Body.Close()
			//log.Printf("connection started: %v", conn.RemoteAddr())
			go jsonrpc.ServeConn(conn)
		}
}


func RPC_caller(nodeAddr string, funcName string, args interface{}, reply interface{}) error{

	client, err := jsonrpc.Dial("tcp", nodeAddr)
	if err != nil {
		fmt.Println("dialing:", err)
		return err
	}
	
	err = client.Call("LocalNode."+funcName, args, &reply)

	if err != nil {
			//log.Fatal("communication error:", err)
		fmt.Println("Communication error : ",err)
	}
	
	return err
}

func RPC_caller1(nodeAddr string, funcName string, args []byte, reply *SNode) error{

	client, err := jsonrpc.Dial("tcp", nodeAddr)
	if err != nil {
		fmt.Println("dialing:", err)
		return err
	}
	
	err = client.Call("LocalNode."+funcName, args, &reply)
	
	if err != nil {
			fmt.Println("communication error:", err)
	}
	
	return err
}


// Function to get the local address of my machine
func getLocalAddress() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		os.Stderr.WriteString("Oops: " + err.Error() + "\n")
		os.Exit(1)
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				//os.Stdout.WriteString(ipnet.IP.String() + "\n")
				return ipnet.IP.String()
			}
		}
	}
	return ""
}


//------------------------------------Main_Function_Starts_Here----------------------------------------------

func main() {

	filename := "testoutput.txt"
	fmt.Println("Launching server...")

	port:="8081"
	adr:=getLocalAddress()
	addr:=adr+":"+port
	conf := DefaultConfig(addr)
	
	/*
	ring,err := Join(conf,"192.168.1.107:8081")
	checkError(err)
	fmt.Println("Ring joined successfully : ", ring)
	*/
	
	
	ring,err := Create(conf)
	fmt.Println("Ring created successfully : ", ring)
	checkError(err)
	

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("\n\n")
	fmt.Println("Instructions for execution : \n")
	fmt.Println("Press 'G' for looking up the value for a key and Enter")
	fmt.Println("Press 'P' for putting a key-value pair and Enter")
	fmt.Println("Press 'D' for deleting a key from the ring and Enter")
	fmt.Println("Press 'L' if you want to leave the ring")
	fmt.Println("Press 'J' to join the ring once you have already left it")
	fmt.Println("For any transaction, use the following syntax : ")
	fmt.Println("	BEGIN")
	fmt.Println("	G <key>")
	fmt.Println("	P <key> <value>")
	fmt.Println("	D <key>")
	fmt.Println("	END")
	fmt.Println("If you want to simulate failure of this node press Ctrl+C")

for{
	
		command, _ := reader.ReadString('\n')
		
		switch command {

			case "G\n":
				fmt.Println("You chose to get the value for a key")
				var key string
				fmt.Printf("Enter Key :  ")
				w, _ := reader.ReadString('\n')
				key = strings.Trim(w,"\n")
				fmt.Println("key Entered : ",key)
				
				var ln *LocalNode
				ln = ring.lnodes[0]

				val, ok := map[string]int(ln.Store)[key] 
				if ok{
					fmt.Println("Obtained Result from local Node : ",val)
					fmt.Println("")	
				}else{

					// Hash the key
					h := ring.config.HashFunc()
					h.Write([]byte(key))
					key_hash := h.Sum(nil)
									
					var succ SNode
					var err error
					err=RPC_caller1(adr+":"+port,"FindCoordinator",key_hash,&succ)
					checkError(err)
					fmt.Println("Co-ordinator for the key ",key,)
					fmt.Println(succ.Host)

					var reply []*SNode
					err=RPC_caller(succ.Host,"FindSuccessors","",&(reply))
					checkError(err)


					possibleNodes:=make([]string,ring.config.NumSuccessors+1)
					possibleNodes[0]=succ.Host
					for i,sc := range reply{
						if sc!=nil{
							possibleNodes[i+1]=(*sc).Host
						}else{
							possibleNodes[i+1]=""	
						}
					}

					var value int

					var succ_host string
					i:=rand.Intn(len(possibleNodes))
					succ_host = possibleNodes[i]
					err = RPC_caller(succ_host,"Get",key,&value)
					if(err!=nil){
						err = RPC_caller(possibleNodes[0],"Get",key,&value)
						checkError(err)
					}

					fmt.Println("Obtained Result from host : ",succ_host," and value is ",value)
					fmt.Println("")	
				}

			case "P\n":
				fmt.Println("You chose to put a new key-value pair")
				var kv Data
				fmt.Printf("Enter the Key :  ")
				w, _ := reader.ReadString('\n')
				kv.Key = strings.Trim(w,"\n")

				fmt.Printf("Enter the Value :  ")
				t, _ := reader.ReadString('\n')
				v := strings.Trim(t,"\n")
				var err error
				kv.Value, err = strconv.Atoi(v)
				checkError(err)
				

				// Hash the key
				h := ring.config.HashFunc()
				h.Write([]byte(kv.Key))
				key_hash := h.Sum(nil)

				var succ SNode
				err=RPC_caller1(adr+":"+port,"FindCoordinator",key_hash,&succ)
				checkError(err)

				err = RPC_caller(succ.Host,"Put",kv,nil)
				if(err!=nil){
					fmt.Println("Put Unsuccessful!")
					fmt.Println("")
				}else{
					fmt.Print("Key value pair successfully Entered : ",kv.Key )
					fmt.Println(" - ",kv.Value)
					fmt.Println("")
				}

				fmt.Println("Now entering this key-value pair into this co-ordinator's successors ")
				
				var reply []*SNode
				err=RPC_caller(succ.Host,"FindSuccessors","",&(reply))
				checkError(err)
				
				for i,sc := range reply{
					if sc!=nil{
						fmt.Println("succ_list [",i,"] : ",(*sc).Host)
						go asyncPut(*sc,kv)
					}else{
						break
					}
				}
				


			case "D\n":
				fmt.Println("You chose to delete an existing key-value pair")
			
				var kv string
				fmt.Printf("Enter the Key :  ")
				w, _ := reader.ReadString('\n')
				kv = strings.Trim(w,"\n")

				// Hash the key
				h := ring.config.HashFunc()
				h.Write([]byte(kv))
				key_hash := h.Sum(nil)

				var succ SNode
				err:=RPC_caller1(adr+":"+port,"FindCoordinator",key_hash,&succ)
				checkError(err)
				

				err=RPC_caller(succ.Host,"Delete",kv, "")
				checkError(err)
				if err==nil{
					fmt.Println("Key value pair Deleted Successfully")
				}



			case "L\n":

				var err error
				err=ring.lnodes[0].leave()
				checkError(err)
				fmt.Println("Node successfully left the ring")
				//os.Exit(0)



			case "J\n":
				var existing string
				fmt.Println("Enter the address of existing node in the form of address:port ")
				w, _ := reader.ReadString('\n')
				existing = strings.Trim(w,"\n")

				fmt.Println("Enter the port on which you want to listen for your new entry in the ring ")
				v, _ := reader.ReadString('\n')
				port = v
				conf = DefaultConfig(adr+":"+port)
				ring,err=Join(conf, existing)	



			case "F\n":
				
				var key string
				fmt.Printf("Enter Key :  ")
				w, _ := reader.ReadString('\n')
				key = strings.Trim(w,"\n")
				fmt.Println("key Entered : ",key)


				// Hash the key
				h := ring.config.HashFunc()
				h.Write([]byte(key))
				key_hash := h.Sum(nil)


				var reply []*SNode
				err:=RPC_caller(adr+":"+port,"FindCoordinatorWithFinger",key_hash,&(reply))
				checkError(err)

				fmt.Println("Result Obtained for FindCoordinatorWithFinger : ")
				for i, value := range reply {
					if value!=nil{
						fmt.Println("Succceesssor ",i,"th : ",*(reply[i]))
					}

				}

			case "BEGIN\n":
				command, _ = reader.ReadString('\n')
				var cmds []string

				cnt:=0
				for command!="END\n"{
					cnt=cnt+1
					cmds=append(cmds,command)
					command, _ = reader.ReadString('\n')
				}
				fmt.Println("size of cnt : ",cnt)
				signals=make(chan bool, cnt)
				start := time.Now()
				for _,cmd:=range cmds{
					
					tok:=strings.Split(cmd," ")
					switch tok[0] {

						case "G":
							fmt.Println("You chose to get the value for a key")
							var key string
							key = strings.Trim(tok[1],"\n")
							
							var ln *LocalNode
							ln = ring.lnodes[0]

							val, ok := map[string]int(ln.Store)[key] 
							if ok{
								fmt.Println("Obtained Result from local Node : ",val)
								fmt.Println("")
								signals <- true	
							}else{

								// Hash the key
								h := ring.config.HashFunc()
								h.Write([]byte(key))
								key_hash := h.Sum(nil)
								
								go asyncGet(adr+":"+port,key_hash,key,ring)
								
							}

						case "P":
							fmt.Println("You chose to put a new key-value pair")
							var kv Data
							kv.Key = strings.Trim(tok[1],"\n")

							v := strings.Trim(tok[2],"\n")
							var err error
							kv.Value, err = strconv.Atoi(v)
							checkError(err)
							

							// Hash the key
							h := ring.config.HashFunc()
							h.Write([]byte(kv.Key))
							key_hash := h.Sum(nil)

							var succ SNode
							err=RPC_caller1(adr+":"+port,"FindCoordinator",key_hash,&succ)
							checkError(err)

							err = RPC_caller(succ.Host,"Put",kv,nil)
							if(err!=nil){
								fmt.Println("Put Unsuccessful!")
								fmt.Println("")
							}else{
								fmt.Print("Key value pair successfully Entered : ",kv.Key )
								fmt.Println(" - ",kv.Value)
								fmt.Println("")
							}

							fmt.Println("Now entering this key-value pair into this co-ordinator's successors ")
							
							var reply []*SNode
							err=RPC_caller(succ.Host,"FindSuccessors","",&(reply))
							checkError(err)
							
							for i,sc := range reply{
								if sc!=nil{
									fmt.Println("succ_list [",i,"] : ",(*sc).Host)
									go asyncPut(*sc,kv)
								}else{
									break
								}
							}
							

						case "D":
							fmt.Println("You chose to delete an existing key-value pair")
						
							var kv string
							kv = strings.Trim(tok[1],"\n")

							// Hash the key
							h := ring.config.HashFunc()
							h.Write([]byte(kv))
							key_hash := h.Sum(nil)

							var succ SNode
							err:=RPC_caller1(adr+":"+port,"FindCoordinator",key_hash,&succ)
							checkError(err)
							

							err=RPC_caller(succ.Host,"Delete",kv, "")
							checkError(err)
							if err==nil{
								fmt.Println("Key value pair Deleted Successfully")
							}

					}	
					
			}
			
			for i:=0; i < cnt; i++{
				<- signals
			}
			elapsed := time.Since(start)
			fmt.Println("Time elapsed for transaction : ",elapsed)
			if _, err = os.Stat(filename); err == nil {
				f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
				    panic(err)
				}

				defer f.Close()

				if _, err = f.WriteString(elapsed.String()+"\n"); err != nil {
				    panic(err)
				}
			}else{
				err = ioutil.WriteFile(filename, []byte(elapsed.String()+"\n"), 0644)
				checkError(err)
			}
		}
	}
}




func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
	}
}

func asyncPut(sc SNode,kv Data){
	err:=RPC_caller(sc.Host,"Put",kv,nil)
	if err!=nil{
		fmt.Println("Put Unsuccessful!")
		fmt.Println("")
	}else{
		fmt.Print("Key value pair successfully Entered : ",kv.Key )
		fmt.Println(" - ",kv.Value)
		fmt.Println("")
	}
	signals <- true
}

func asyncGet(address string,key_hash []byte,key string,ring*Ring){
	var succ SNode
	var err error
	err=RPC_caller1(address,"FindCoordinator",key_hash,&succ)
	checkError(err)
	fmt.Println("Co-ordinator for the key ",key,)
	fmt.Println(succ.Host)

	var reply []*SNode
	err=RPC_caller(succ.Host,"FindSuccessors","",&(reply))
	checkError(err)
	
	
	possibleNodes:=make([]string,ring.config.NumSuccessors+1)
	possibleNodes[0]=succ.Host
	for i,sc := range reply{
		if sc!=nil{
			possibleNodes[i+1]=(*sc).Host
		}else{
			possibleNodes[i+1]=""	
		}
	}

	var value int

	var succ_host string
	i:=rand.Intn(len(possibleNodes))
	succ_host = possibleNodes[i]
	err = RPC_caller(succ_host,"Get",key,&value)
	if(err!=nil){
		err = RPC_caller(possibleNodes[0],"Get",key,&value)
		checkError(err)
	}

	fmt.Println("Obtained Result from host : ",succ_host," and value is ",value)
	fmt.Println("")
	signals <- true
}

