// Simple service that manages SimStores
package main

import (
	"fmt"
	"github.com/niven/simhashing"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// figure out what method to call, check some params
func (sim *SimService) handle_all_the_things(w http.ResponseWriter, r *http.Request) {
	wat := r.URL.Path[1:]
	fmt.Println("Handling", wat)

	method, exists := sim.methods[wat]

	// check some stuff
	if !exists {
		http.Error(w, "Method does not exist: "+wat, http.StatusNotFound)
		return
	}

	if method.method_type != r.Method {
		http.Error(w, "Method type should be "+method.method_type, http.StatusMethodNotAllowed)
		return
	}

	// check params
	r.ParseMultipartForm(10 * 1024) // implicitly does ParseForm() in case there are some url params

	for _, param := range method.required_parameters {
		if _, present := r.Form[param]; !present {
			http.Error(w, "Missing required parameter: "+param, http.StatusBadRequest)
			return
		}
	}

	// we're ok to handle it
	method.function(sim, w, r.Form)
}

func create_store(sim *SimService, w http.ResponseWriter, values url.Values) {
	for _, name := range values["name"] {
		fmt.Printf("Create store named %v", name)
		if _, exists := sim.stores[name]; exists {
			// creating a store that already exists is a nop
			fmt.Fprintf(w, "Store %v already exists\n", name)
			fmt.Println(" - Already exists")
		} else {
			sim.stores[name] = simhashing.NewSimStore()
			fmt.Fprintf(w, "Created store named %v\n", name)
			fmt.Println(" - Success")
		}
	}

}
func delete_store(sim *SimService, w http.ResponseWriter, values url.Values) {
	for _, name := range values["name"] {
		delete(sim.stores, name)
		fmt.Printf("Delete store named %v\n", name)
		fmt.Fprintf(w, "Store %v deleted\n", name)
	}
}
func insert(sim *SimService, w http.ResponseWriter, values url.Values) {

	if len(values["name"]) != 1 {
		fmt.Printf("Incorrect store name %v\n", values["name"])
		http.Error(w, "Don't know how to insert into multiple stores at a time: "+strings.Join(values["name"], ", "), http.StatusBadRequest)
		return

	}

	if len(values["id"]) != len(values["content"]) {
		err := fmt.Sprintf("Number of ids (%v) not equal to the number of contents (%v)\n", len(values["id"]), len(values["content"]))
		fmt.Printf(err)
		http.Error(w, err, http.StatusBadRequest)
		return

	}

	fmt.Println("Insert into store", values["name"][0])

	store, exists := sim.stores[values["name"][0]]
	if !exists {
		fmt.Printf("Incorrect store name %v\n", values["name"])
		http.Error(w, fmt.Sprintf("Store does not exist: %v\n", values["name"]), http.StatusBadRequest)
		return

	}

	for i := 0; i < len(values["id"]); i++ {
		id, err := strconv.ParseInt(values["id"][i], 10, 64)
		if err != nil {
			fmt.Printf("ID %v not an int: %v\n", values["id"][i], err)
			http.Error(w, fmt.Sprintf("ID %v not an int: %v\n", values["id"][i], err), http.StatusBadRequest)

		}
		store.Insert(values["content"][i], id)
	}

}

// for the test app: return the ID which the other ones are closest to (the most "generic" content)
// maybe also return the most outlier content (the one that the least of others are close to)
func consensus(sim *SimService, w http.ResponseWriter, values url.Values) {
	fmt.Println("Consensus of store", values)
}

type Method struct {
	name                string
	method_type         string
	required_parameters []string
	function            func(*SimService, http.ResponseWriter, url.Values)
}

type SimService struct {
	methods map[string]Method
	port    int
	stores  map[string]*simhashing.SimStore
}

func NewSimService(port int) *SimService {
	out := &SimService{port: port}
	out.methods = make(map[string]Method, 0)
	out.stores = make(map[string]*simhashing.SimStore, 0)
	return out
}

func (sim *SimService) Register(m Method) {
	fmt.Println("Registering", m)
	sim.methods[m.name] = m
}

func (sim *SimService) Start() {
	fmt.Println("Listening on port", sim.port)
	http.HandleFunc("/", sim.handle_all_the_things)
	http.ListenAndServe(fmt.Sprintf(":%d", sim.port), nil)
}


func main() {
	fmt.Println("Starting Sim Service")

	// handle things
	// 0) POST create named simstore (options like persist)
	// a) POST content+id to a named simstore
	// b) GET consus scores for a named simtore
	// c) DELETE named store

	sim := NewSimService(8080)
	sim.Register(Method{"create", "POST", []string{"name"}, create_store})
	sim.Register(Method{"insert", "POST", []string{"name", "id", "content"}, insert})
	sim.Register(Method{"consensus", "GET", []string{"name"}, consensus})
	sim.Register(Method{"delete", "DELETE", []string{"name"}, delete_store})

	sim.Start()

}
