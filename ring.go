package main

func (r *Ring) init(conf *Config) {
	// Set our variables
	r.config = conf
	r.lnodes = make([]*LocalNode,1)
	// Initializes the first lnode	
	vn := &LocalNode{}
	r.lnodes[0] = vn
	vn.Ring = r
	vn.init()
}

// Wait for all the vnodes to shutdown
func (r *Ring) stopVnodes() {
	r.shutdown = make(chan bool)
	<-r.shutdown
}

func (r *Ring) Shutdown() error {
	r.stopVnodes()
	var err error
	for _, vn := range r.lnodes {
		err = mergeErrors(err, vn.leave())
	}
	
	return err
}


